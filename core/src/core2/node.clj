(ns core2.node
  (:require [clojure.pprint :as pp]
            [core2.api :as api]
            [core2.datalog :as d]
            [core2.expression :as expr]
            [core2.ingester :as ingest]
            [core2.operator :as op]
            [core2.sql :as sql]
            [core2.tx-producer :as txp]
            [core2.util :as util]
            [core2.vector.indirect :as iv]
            [juxt.clojars-mirrors.integrant.core :as ig])
  (:import (core2 IResultCursor IResultSet)
           core2.ingester.Ingester
           (core2.tx_producer ITxProducer)
           (java.io Closeable Writer)
           (java.lang AutoCloseable)
           (java.time Duration ZoneId)
           (java.util.concurrent CompletableFuture TimeUnit)
           (java.util.function Consumer)
           java.util.Iterator
           (org.apache.arrow.memory BufferAllocator RootAllocator)))

(set! *unchecked-math* :warn-on-boxed)

(defmethod ig/init-key :core2/allocator [_ _] (RootAllocator.))
(defmethod ig/halt-key! :core2/allocator [_ ^BufferAllocator a] (.close a))

(defmethod ig/prep-key :core2/default-tz [_ default-tz]
  (cond
    (instance? ZoneId default-tz) default-tz
    (string? default-tz) (ZoneId/of default-tz)
    :else util/utc))

(defmethod ig/init-key :core2/default-tz [_ default-tz] default-tz)

(defprotocol PNode
  (snapshot-async
    ^java.util.concurrent.CompletableFuture #_<ScanSource> [_]
    ^java.util.concurrent.CompletableFuture #_<ScanSource> [_ tx]
    ^java.util.concurrent.CompletableFuture #_<ScanSource> [_ tx ^Duration timeout]))

(deftype CursorResultSet [^IResultCursor cursor
                          ^:unsynchronized-mutable ^Iterator next-values]
  IResultSet
  (columnTypes [_] (.columnTypes cursor))

  (hasNext [res]
    (boolean
     (or (and next-values (.hasNext next-values))
         ;; need to call rel->rows eagerly - the rel may have been reused/closed after
         ;; the tryAdvance returns.
         (do
           (while (and (.tryAdvance cursor
                                    (reify Consumer
                                      (accept [_ rel]
                                        (set! (.-next-values res)
                                              (.iterator (iv/rel->rows rel))))))
                       (not (and next-values (.hasNext next-values)))))
           (and next-values (.hasNext next-values))))))

  (next [_] (.next next-values))
  (close [_]
    (.close cursor)))

(defn- cursor->result-set ^core2.IResultSet [^IResultCursor cursor]
   (CursorResultSet. cursor nil))

(defn- validate-tx-ops [tx-ops]
  (try
    (doseq [{:keys [op] :as tx-op} (txp/conform-tx-ops tx-ops)
            :when (= :sql op)
            :let [{:keys [query]} tx-op]]
      (sql/compile-query query))
    (catch Throwable e
      (CompletableFuture/failedFuture e))))

(defrecord Node [^Ingester ingester
                 ^ITxProducer tx-producer
                 default-tz
                 !system
                 close-fn]
  api/PClient
  (-open-datalog-async [this query args]
    (let [!db (snapshot-async this (get-in query [:basis :tx]) (:basis-timeout query))
          query (into {:default-tz default-tz} query)
          {ra-query :query, :keys [params table-args]} (-> (dissoc query :basis :basis-timeout :default-tz)
                                                           (d/compile-query args))
          pq (op/prepare-ra ra-query)]

      (-> !db
          (util/then-apply
            (fn [db]
              (-> (.bind pq {:srcs {'$ db}, :params params, :table-args table-args
                             :current-time (get-in query [:basis :current-time])
                             :default-tz (:default-tz query)})
                  (.openCursor)
                  (cursor->result-set)))))))

  (-open-sql-async [this query query-opts]
    (let [!db (snapshot-async this (get-in query-opts [:basis :tx]) (:basis-timeout query-opts))
          query-opts (into {:default-tz default-tz} query-opts)
          plan (sql/compile-query query (select-keys query-opts [:app-time-as-of-now? :default-tz :decorrelate?]))
          params (zipmap (map #(symbol (str "?_" %)) (range))
                         (:? query-opts))
          pq (op/prepare-ra plan)]
      (-> !db
          (util/then-apply
            (fn [db]
              (-> (.bind pq {:srcs {'$ db}, :params params
                             :current-time (get-in query-opts [:basis :current-time])
                             :default-tz (:default-tz query-opts)})
                  (.openCursor)
                  (cursor->result-set)))))))

  PNode
  (snapshot-async [this] (snapshot-async this nil))
  (snapshot-async [this tx] (snapshot-async this tx nil))
  (snapshot-async [_ tx timeout]
    (-> (if-not (instance? CompletableFuture tx)
          (CompletableFuture/completedFuture (or tx (.latestCompletedTx ingester)))
          tx)
        (util/then-compose (fn [tx]
                             (.snapshot ingester tx)))
        (cond-> timeout (.orTimeout (.toMillis ^Duration timeout) TimeUnit/MILLISECONDS))))

  api/PStatus
  (status [_] {:latest-completed-tx (.latestCompletedTx ingester)})

  api/PSubmitNode
  (submit-tx [_ tx-ops]
    (or (validate-tx-ops tx-ops)
        (.submitTx tx-producer tx-ops)))

  (submit-tx [_ tx-ops opts]
    (or (validate-tx-ops tx-ops)
        (.submitTx tx-producer tx-ops opts)))

  Closeable
  (close [_]
    (when close-fn
      (close-fn))))

(defmethod print-method Node [_node ^Writer w] (.write w "#<Core2Node>"))
(defmethod pp/simple-dispatch Node [it] (print-method it *out*))

(defmethod ig/prep-key ::node [_ opts]
  (merge {:ingester (ig/ref :core2/ingester)
          :tx-producer (ig/ref ::txp/tx-producer)
          :default-tz (ig/ref :core2/default-tz)}
         opts))

(defmethod ig/init-key ::node [_ deps]
  (map->Node (assoc deps :!system (atom nil))))

(defmethod ig/halt-key! ::node [_ ^Node node]
  (.close node))

(defn- with-default-impl [opts parent-k impl-k]
  (cond-> opts
    (not (ig/find-derived opts parent-k)) (assoc impl-k {})))

(defn start-node ^core2.node.Node [opts]
  (let [system (-> (into {::node {}
                          :core2/allocator {}
                          :core2/default-tz nil
                          :core2/row-counts {}
                          :core2.indexer/indexer {}
                          :core2.indexer/internal-id-manager {}
                          :core2/ingester {}
                          :core2.metadata/metadata-manager {}
                          :core2.temporal/temporal-manager {}
                          :core2.buffer-pool/buffer-pool {}
                          :core2.watermark/watermark-manager {}
                          ::txp/tx-producer {}}
                         opts)
                   (doto ig/load-namespaces)
                   (with-default-impl :core2/log :core2.log/memory-log)
                   (with-default-impl :core2/object-store :core2.object-store/memory-object-store)
                   (doto ig/load-namespaces)
                   ig/prep
                   ig/init)]

    (-> (::node system)
        (doto (-> :!system (reset! system)))
        (assoc :close-fn #(do (ig/halt! system)
                              #_(println (.toVerboseString ^RootAllocator (:core2/allocator system))))))))

(defrecord SubmitNode [^ITxProducer tx-producer, !system, close-fn]
  api/PSubmitNode
  (submit-tx [_ tx-ops]
    (or (validate-tx-ops tx-ops)
        (.submitTx tx-producer tx-ops)))

  (submit-tx [_ tx-ops opts]
    (or (validate-tx-ops tx-ops)
        (.submitTx tx-producer tx-ops opts)))

  AutoCloseable
  (close [_]
    (when close-fn
      (close-fn))))

(defmethod print-method SubmitNode [_node ^Writer w] (.write w "#<Core2SubmitNode>"))
(defmethod pp/simple-dispatch SubmitNode [it] (print-method it *out*))

(defmethod ig/prep-key ::submit-node [_ opts]
  (merge {:tx-producer (ig/ref :core2.tx-producer/tx-producer)}
         opts))

(defmethod ig/init-key ::submit-node [_ {:keys [tx-producer]}]
  (map->SubmitNode {:tx-producer tx-producer, :!system (atom nil)}))

(defmethod ig/halt-key! ::submit-node [_ ^SubmitNode node]
  (.close node))

(defn start-submit-node ^core2.node.SubmitNode [opts]
  (let [system (-> (into {::submit-node {}
                          :core2.tx-producer/tx-producer {}
                          :core2/allocator {}
                          :core2/default-tz nil}
                         opts)
                   ig/prep
                   (doto ig/load-namespaces)
                   ig/init)]

    (-> (::submit-node system)
        (doto (-> :!system (reset! system)))
        (assoc :close-fn #(ig/halt! system)))))
