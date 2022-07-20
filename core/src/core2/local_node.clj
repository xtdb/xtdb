(ns core2.local-node
  (:require [clojure.pprint :as pp]
            [core2.api :as api]
            [core2.datalog :as d]
            [core2.error :as err]
            [core2.indexer :as idx]
            core2.ingester
            [core2.operator :as op]
            [core2.snapshot :as snap]
            [core2.sql.parser :as p]
            [core2.sql.plan :as sql.plan]
            [core2.tx-producer :as txp]
            [core2.util :as util]
            [juxt.clojars-mirrors.integrant.core :as ig])
  (:import (core2.indexer TransactionIndexer)
           core2.ingester.Ingester
           (core2.snapshot ISnapshotFactory)
           (core2.tx_producer ITxProducer)
           (java.io Closeable Writer)
           (java.lang AutoCloseable)
           (java.time Duration Instant)
           (java.util.concurrent CompletableFuture TimeUnit)
           (org.apache.arrow.memory BufferAllocator RootAllocator)))

(set! *unchecked-math* :warn-on-boxed)

(defprotocol PNode
  ;; TODO in theory we shouldn't need this, but it's still used in tests
  (await-tx-async
    ^java.util.concurrent.CompletableFuture #_<TransactionInstant> [node tx]))

(defrecord Node [^TransactionIndexer indexer
                 ^Ingester ingester
                 ^ISnapshotFactory snapshot-factory
                 ^ITxProducer tx-producer
                 !system
                 close-fn]
  api/PClient
  (-open-datalog-async [_ query args]
    (let [{:keys [basis ^Duration basis-timeout]} query
          {:keys [default-valid-time tx], :or {default-valid-time (Instant/now)}} basis]
      (-> (snap/snapshot-async snapshot-factory tx)
          (cond-> basis-timeout (.orTimeout (.toMillis basis-timeout) TimeUnit/MILLISECONDS))
          (util/then-apply
            (fn [db]
              (let [{:keys [query args]} (-> (dissoc query :basis :basis-timeout)
                                             (d/compile-query args))]
                (-> (op/open-ra query (merge args {'$ db}) {:default-valid-time default-valid-time})
                    (op/cursor->result-set))))))))

  (-open-sql-async [_ query {:keys [basis ^Duration basis-timeout] :as query-opts}]
    (let [{:keys [default-valid-time tx], :or {default-valid-time (Instant/now)}} basis]
      (-> (snap/snapshot-async snapshot-factory tx)
          (cond-> basis-timeout (.orTimeout (.toMillis basis-timeout) TimeUnit/MILLISECONDS))
          (util/then-apply
            (fn [db]
              (let [{:keys [errs plan]} (-> (p/parse query)
                                            (sql.plan/plan-query))]
                (if errs
                  (throw (err/illegal-arg :invalid-sql-query
                                          {::err/message "Invalid SQL query:"
                                           :errs errs}))
                  (-> (op/open-ra plan
                                  (into {'$ db}
                                        (zipmap (map #(symbol (str "?_" %)) (range))
                                                (:? query-opts)))
                                  {:default-valid-time default-valid-time})
                      (op/cursor->result-set)))))))))

  PNode
  (await-tx-async [_ tx]
    (-> (if-not (instance? CompletableFuture tx)
          (CompletableFuture/completedFuture tx)
          tx)
        (util/then-compose (fn [tx]
                             (.awaitTxAsync ingester tx)))))

  api/PStatus
  (status [_] {:latest-completed-tx (.latestCompletedTx indexer)})

  api/PSubmitNode
  (submit-tx [_ tx-ops]
    (.submitTx tx-producer tx-ops))

  (submit-tx [_ tx-ops opts]
    (.submitTx tx-producer tx-ops opts))

  Closeable
  (close [_]
    (when close-fn
      (close-fn))))

(defmethod print-method Node [_node ^Writer w] (.write w "#<Core2Node>"))
(defmethod pp/simple-dispatch Node [it] (print-method it *out*))

(defmethod ig/prep-key ::node [_ opts]
  (merge {:indexer (ig/ref ::idx/indexer)
          :ingester (ig/ref :core2/ingester)
          :snapshot-factory (ig/ref ::snap/snapshot-factory)
          :tx-producer (ig/ref ::txp/tx-producer)}
         opts))

(defmethod ig/init-key ::node [_ deps]
  (map->Node (assoc deps :!system (atom nil))))

(defmethod ig/halt-key! ::node [_ ^Node node]
  (.close node))

(defmethod ig/init-key :core2/allocator [_ _] (RootAllocator.))
(defmethod ig/halt-key! :core2/allocator [_ ^BufferAllocator a] (.close a))

(defn- with-default-impl [opts parent-k impl-k]
  (cond-> opts
    (not (ig/find-derived opts parent-k)) (assoc impl-k {})))

(defn start-node ^core2.local_node.Node [opts]
  (let [system (-> (into {::node {}
                          :core2/allocator {}
                          :core2/row-counts {}
                          ::idx/indexer {}
                          :core2/ingester {}
                          :core2.metadata/metadata-manager {}
                          :core2.temporal/temporal-manager {}
                          :core2.buffer-pool/buffer-pool {}
                          :core2.watermark/watermark-manager {}
                          ::snap/snapshot-factory {}
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
    (.submitTx tx-producer tx-ops))

  (submit-tx [_ tx-ops opts]
    (.submitTx tx-producer tx-ops opts))

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

(defn start-submit-node ^core2.local_node.SubmitNode [opts]
  (let [system (-> (into {::submit-node {}
                          :core2.tx-producer/tx-producer {}
                          :core2/allocator {}}
                         opts)
                   ig/prep
                   (doto ig/load-namespaces)
                   ig/init)]

    (-> (::submit-node system)
        (doto (-> :!system (reset! system)))
        (assoc :close-fn #(ig/halt! system)))))
