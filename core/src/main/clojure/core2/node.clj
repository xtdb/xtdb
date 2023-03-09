(ns core2.node
  (:require [clojure.pprint :as pp]
            [core2.api.impl :as api]
            [core2.core.datalog :as d]
            [core2.core.sql :as sql]
            core2.indexer
            [core2.ingester :as ingest]
            [core2.operator :as op]
            [core2.tx-producer :as txp]
            [core2.util :as util]
            [core2.vector.writer :as vw]
            [juxt.clojars-mirrors.integrant.core :as ig])
  (:import core2.indexer.IIndexer
           core2.ingester.Ingester
           core2.operator.IRaQuerySource
           (core2.tx_producer ITxProducer)
           core2.vector.IIndirectRelation
           (java.io Closeable Writer)
           (java.lang AutoCloseable)
           (java.time ZoneId)
           (java.util.concurrent CompletableFuture)
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

(defn- validate-tx-ops [tx-ops]
  (try
    (doseq [{:keys [op] :as tx-op} (txp/conform-tx-ops tx-ops)
            :when (= :sql op)
            :let [{:keys [query]} tx-op]]
      (sql/compile-query query))
    (catch Throwable e
      (CompletableFuture/failedFuture e))))

(defprotocol PNode
  (await-tx&
    ^java.util.concurrent.CompletableFuture [node tx]
    ^java.util.concurrent.CompletableFuture [node tx timeout]))

(defrecord Node [^BufferAllocator allocator
                 ^Ingester ingester, ^IIndexer indexer
                 ^ITxProducer tx-producer
                 ^IRaQuerySource ra-src, wm-src
                 default-tz
                 system, close-fn]
  PNode
  (await-tx& [this tx] (await-tx& this tx nil))
  (await-tx& [_ tx timeout] (.awaitTxAsync ingester tx timeout))

  api/PNode
  (open-datalog& [this query args]
    (let [query (into {:default-tz default-tz} query)]
      (-> (await-tx& this (get-in query [:basis :tx]) (:basis-timeout query))
          (util/then-apply
            (fn [tx]
              (d/open-datalog-query allocator ra-src wm-src
                                    (-> query
                                        (update-in [:basis :tx] (fnil identity tx)))
                                    args))))))

  (open-sql& [this query query-opts]
    (let [query-opts (into {:default-tz default-tz} query-opts)
          !await-tx (await-tx& this (get-in query-opts [:basis :tx]) (:basis-timeout query))
          pq (.prepareRaQuery ra-src (sql/compile-query query query-opts))]
      (-> !await-tx
          (util/then-apply
            (fn [tx]
              (sql/open-sql-query allocator wm-src pq
                                  (-> query-opts
                                      (update-in [:basis :tx] (fnil identity tx)))))))))

  api/PStatus
  (status [_] {:latest-completed-tx (.latestCompletedTx indexer)})

  api/PSubmitNode
  (submit-tx& [_ tx-ops]
    (or (validate-tx-ops tx-ops)
        (.submitTx tx-producer tx-ops)))

  (submit-tx& [_ tx-ops opts]
    (or (validate-tx-ops tx-ops)
        (.submitTx tx-producer tx-ops opts)))

  Closeable
  (close [_]
    (when close-fn
      (close-fn))))

(defmethod print-method Node [_node ^Writer w] (.write w "#<Core2Node>"))
(defmethod pp/simple-dispatch Node [it] (print-method it *out*))

(defmethod ig/prep-key ::node [_ opts]
  (merge {:allocator (ig/ref :core2/allocator)
          :indexer (ig/ref :core2/indexer)
          :wm-src (ig/ref :core2/indexer)
          :ingester (ig/ref :core2/ingester)
          :tx-producer (ig/ref ::txp/tx-producer)
          :default-tz (ig/ref :core2/default-tz)
          :ra-src (ig/ref :core2.operator/ra-query-source)}
         opts))

(defmethod ig/init-key ::node [_ deps]
  (map->Node deps))

(defmethod ig/halt-key! ::node [_ node]
  (util/try-close node))

(defn- with-default-impl [opts parent-k impl-k]
  (cond-> opts
    (not (ig/find-derived opts parent-k)) (assoc impl-k {})))

(defn start-node ^core2.node.Node [opts]
  (let [system (-> (into {::node {}
                          :core2/allocator {}
                          :core2/default-tz nil
                          :core2/indexer {}
                          :core2.indexer/internal-id-manager {}
                          :core2/live-chunk {}
                          :core2.indexer/log-indexer {}
                          :core2/ingester {}
                          :core2.metadata/metadata-manager {}
                          :core2.temporal/temporal-manager {}
                          :core2.buffer-pool/buffer-pool {}
                          :core2.operator.scan/scan-emitter {}
                          :core2.operator/ra-query-source {}
                          ::txp/tx-producer {}}
                         opts)
                   (doto ig/load-namespaces)
                   (with-default-impl :core2/log :core2.log/memory-log)
                   (with-default-impl :core2/object-store :core2.object-store/memory-object-store)
                   (doto ig/load-namespaces)
                   ig/prep
                   ig/init)]

    (-> (::node system)
        (assoc :system system
               :close-fn #(do (ig/halt! system)
                              #_(println (.toVerboseString ^RootAllocator (:core2/allocator system))))))))

(defrecord SubmitNode [^ITxProducer tx-producer, !system, close-fn]
  api/PSubmitNode
  (submit-tx& [_ tx-ops]
    (or (validate-tx-ops tx-ops)
        (.submitTx tx-producer tx-ops)))

  (submit-tx& [_ tx-ops opts]
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
                   (doto ig/load-namespaces)
                   (with-default-impl :core2/log :core2.log/memory-log)
                   (doto ig/load-namespaces)
                   ig/prep
                   ig/init)]

    (-> (::submit-node system)
        (doto (-> :!system (reset! system)))
        (assoc :close-fn #(ig/halt! system)))))
