(ns core2.core
  (:require [clojure.pprint :as pp]
            core2.data-source
            [core2.indexer :as indexer]
            core2.ingest-loop
            [core2.operator :as op]
            [core2.relation :as rel]
            [core2.system :as sys]
            core2.tx-producer
            [core2.util :as util]
            [core2.datalog :as d])
  (:import clojure.lang.IReduceInit
           [core2.data_source IDataSourceFactory IQueryDataSource]
           [core2.indexer IChunkManager TransactionIndexer]
           core2.tx_producer.ITxProducer
           [java.io Closeable Writer]
           java.lang.AutoCloseable
           java.time.Duration
           java.util.Date
           [java.util.concurrent CompletableFuture TimeUnit]
           org.apache.arrow.memory.RootAllocator))

(set! *unchecked-math* :warn-on-boxed)

(defprotocol PNode
  (latest-completed-tx ^core2.tx.TransactionInstant [node])

  (await-tx-async
    ^java.util.concurrent.CompletableFuture #_<core2.tx.TransactionInstant> [node tx])

  (open-db-async
    ^java.util.concurrent.CompletableFuture #_<core2.data_source.IQueryDataSource> [node]
    ^java.util.concurrent.CompletableFuture #_<core2.data_source.IQueryDataSource> [node db-opts]))

(defn with-db-async* [node db-opts f]
  (-> (open-db-async node db-opts)
      (util/then-apply (bound-fn [^IQueryDataSource db]
                         (try
                           (f db)
                           (finally
                             (util/try-close db)))))))

(defmacro with-db-async [[db-binding node db-opts] & body]
  `(with-db-async* ~node ~db-opts (bound-fn [~db-binding] ~@body)))

(defmacro with-db [& args]
  `@(with-db-async ~@args))

(def ^:private with-db-arglists
  '([[db-binding node] & body]
    [[db-binding node {:keys [tx valid-time timeout]}] & body]))

(alter-meta! #'with-db assoc :arglists with-db-arglists)
(alter-meta! #'with-db-async assoc :arglists with-db-arglists)

(defprotocol PSubmitNode
  (submit-tx
    ^java.util.concurrent.CompletableFuture [tx-producer tx-ops]))

(defrecord Node [^TransactionIndexer indexer
                 ^IDataSourceFactory data-source-factory
                 ^ITxProducer tx-producer
                 !system
                 close-fn]
  PNode
  (latest-completed-tx [_] (.latestCompletedTx indexer))

  (await-tx-async [_ tx]
    (-> (if-not (instance? CompletableFuture tx)
          (CompletableFuture/completedFuture tx)
          tx)
        (util/then-compose (fn [tx]
                             (.awaitTxAsync indexer tx)))))

  (open-db-async [this]
    (open-db-async this {}))

  (open-db-async [this db-opts]
    (let [{:keys [valid-time tx ^Duration timeout]} db-opts]
      (-> (if tx
            (-> (await-tx-async this tx)
                (cond-> timeout (.orTimeout (.toMillis timeout) TimeUnit/MILLISECONDS)))
            (CompletableFuture/completedFuture (latest-completed-tx this)))
          (util/then-apply (fn [tx]
                             (.openDataSource data-source-factory
                                              (.getWatermark ^IChunkManager indexer)
                                              tx
                                              (or valid-time (Date.))))))))

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

(defn open-ra ^core2.ICursor [query db-or-dbs]
  (let [allocator (RootAllocator.)]
    (try
      (-> (op/open-ra allocator query (if (map? db-or-dbs) db-or-dbs {'$ db-or-dbs}))
          (util/and-also-close allocator))
      (catch Throwable t
        (util/try-close allocator)
        (throw t)))))

(defn plan-ra [query db-or-dbs]
  (reify IReduceInit
    (reduce [_ f init]
      (with-open [res (open-ra query db-or-dbs)]
        (util/reduce-cursor (fn [acc rel]
                              (reduce f acc (rel/rel->rows rel)))
                            init
                            res)))))

(defn plan-q [query & params]
  (let [[query srcs] (apply d/compile-query query params)]
    (plan-ra query srcs)))

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
