(ns core2.core
  (:require [clojure.pprint :as pp]
            [core2.indexer :as indexer]
            core2.ingest-loop
            [core2.operator :as op]
            [core2.relation :as rel]
            [core2.system :as sys]
            core2.tx-producer
            [core2.util :as util])
  (:import clojure.lang.IReduceInit
           core2.data_source.IDataSourceFactory
           [core2.indexer IChunkManager TransactionIndexer]
           core2.tx_producer.ITxProducer
           [java.io Closeable Writer]
           java.lang.AutoCloseable
           java.time.Duration
           java.util.concurrent.TimeoutException
           org.apache.arrow.memory.RootAllocator))

(set! *unchecked-math* :warn-on-boxed)

(defprotocol PNode
  (await-tx
    ^core2.tx.TransactionInstant [node tx]
    ^core2.tx.TransactionInstant [node tx timeout])

  (latest-completed-tx ^core2.tx.TransactionInstant [node])

  (open-db ^core2.data_source.QueryDataSource [node]))

(defprotocol PSubmitNode
  (submit-tx
    ^java.util.concurrent.CompletableFuture [tx-producer tx-ops]))

(defrecord Node [^TransactionIndexer indexer
                 ^IDataSourceFactory data-source-factory
                 ^ITxProducer tx-producer
                 !system
                 close-fn]
  PNode
  (await-tx [this tx]
    @(.awaitTxAsync indexer tx))

  (await-tx [_ tx timeout]
    (let [res (deref (.awaitTxAsync indexer tx) (.toMillis ^Duration timeout) ::timeout)]
      (if (= res ::timeout)
        (throw (TimeoutException. "await-tx timed out"))
        res)))

  (latest-completed-tx [_] (.latestCompletedTx indexer))

  (open-db [_]
    (.openDataSource data-source-factory (.getWatermark ^IChunkManager indexer)))

  PSubmitNode
  (submit-tx [_ tx-ops]
    (.submitTx tx-producer tx-ops))

  Closeable
  (close [_]
    (when close-fn
      (close-fn))))

(defmethod print-method Node [_node ^Writer w] (.write w "#<Core2Node>"))
(defmethod pp/simple-dispatch Node [it] (print-method it *out*))

(defn ->node {::sys/deps {:indexer :core2/indexer
                          :data-source-factory :core2/data-source-factory
                          :tx-producer :core2/tx-producer}}
  [deps]
  (map->Node (assoc deps :!system (atom nil))))

(defn open-q ^core2.ICursor [db-or-dbs query]
  (let [allocator (RootAllocator.)]
    (try
      (-> (op/open-q allocator (if (map? db-or-dbs) db-or-dbs {'$ db-or-dbs}) query)
          (util/and-also-close allocator))
      (catch Throwable t
        (util/try-close allocator)
        (throw t)))))

(defn plan-q [db-or-dbs query]
  (reify IReduceInit
    (reduce [_ f init]
      (with-open [res (open-q db-or-dbs query)]
        (util/reduce-cursor (fn [acc rel]
                              (reduce f acc (rel/rel->rows rel)))
                            init
                            res)))))

(defn ->allocator [_]
  (RootAllocator.))

(defn start-node ^core2.core.Node [opts]
  (let [system (-> (sys/prep-system (into [{:core2/node `->node
                                            :core2/allocator `->allocator
                                            :core2/indexer 'core2.indexer/->indexer
                                            :core2/ingest-loop 'core2.ingest-loop/->ingest-loop
                                            :core2/log 'core2.log/->log
                                            :core2/tx-producer 'core2.tx-producer/->tx-producer
                                            :core2/metadata-manager 'core2.metadata/->metadata-manager
                                            :core2/temporal-manager 'core2.temporal/->temporal-manager
                                            :core2/object-store 'core2.object-store/->object-store
                                            :core2/buffer-pool 'core2.buffer-pool/->buffer-pool
                                            :core2/data-source-factory 'core2.data-source/->data-source-factory}]
                                          (cond-> opts (not (vector? opts)) vector)))
                   (sys/start-system))]

    (-> (:core2/node system)
        (doto (-> :!system (reset! system)))
        (assoc :close-fn #(do (.close ^AutoCloseable system)
                              #_(println (.toVerboseString ^RootAllocator (:core2/allocator system))))))))

(defrecord SubmitNode [^ITxProducer tx-producer, !system, close-fn]
  PSubmitNode
  (submit-tx [_ tx-ops]
    (.submitTx tx-producer tx-ops))

  AutoCloseable
  (close [_]
    (when close-fn
      (close-fn))))

(defmethod print-method SubmitNode [_node ^Writer w] (.write w "#<Core2SubmitNode>"))
(defmethod pp/simple-dispatch SubmitNode [it] (print-method it *out*))

(defn ->submit-node {::sys/deps {:tx-producer :core2/tx-producer}}
  [{:keys [tx-producer]}]
  (map->SubmitNode {:tx-producer tx-producer, :!system (atom nil)}))

(defn start-submit-node ^core2.core.SubmitNode [opts]
  (let [system (-> (sys/prep-system (into [{:core2/submit-node `->submit-node
                                            :core2/tx-producer 'core2.tx-producer/->tx-producer
                                            :core2/allocator `->allocator
                                            :core2/log 'core2.log/->local-directory-log-writer}]
                                          (cond-> opts (not (vector? opts)) vector)))
                   (sys/start-system))]

    (-> (:core2/submit-node system)
        (doto (-> :!system (reset! system)))
        (assoc :close-fn #(.close ^AutoCloseable system)))))
