(ns core2.core
  (:require [clojure.pprint :as pp]
            [core2.data-source :as ds]
            [core2.datalog :as d]
            [core2.indexer :as idx]
            core2.ingest-loop
            [core2.operator :as op]
            [core2.relation :as rel]
            [core2.tx-producer :as txp]
            [core2.util :as util]
            [juxt.clojars-mirrors.integrant.core :as ig])
  (:import clojure.lang.IReduceInit
           core2.data_source.IDataSourceFactory
           core2.indexer.TransactionIndexer
           core2.tx_producer.ITxProducer
           [java.io Closeable Writer]
           java.lang.AutoCloseable
           java.time.Duration
           [java.util.concurrent CompletableFuture TimeUnit]
           java.util.Date
           [org.apache.arrow.memory BufferAllocator RootAllocator]))

(set! *unchecked-math* :warn-on-boxed)

(defprotocol PNode
  (latest-completed-tx ^core2.tx.TransactionInstant [node])

  (await-tx-async
    ^java.util.concurrent.CompletableFuture #_<core2.tx.TransactionInstant> [node tx])

  (db-async
    ^java.util.concurrent.CompletableFuture #_<core2.data_source.IDataSource> [node]
    ^java.util.concurrent.CompletableFuture #_<core2.data_source.IDataSource> [node db-opts]))

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

  (db-async [this]
    (db-async this {}))

  (db-async [this db-opts]
    (let [{:keys [tx ^Duration timeout]} db-opts]
      (-> (if tx
            (-> (await-tx-async this tx)
                (cond-> timeout (.orTimeout (.toMillis timeout) TimeUnit/MILLISECONDS)))
            (CompletableFuture/completedFuture (latest-completed-tx this)))
          (util/then-apply (fn [tx]
                             (.getDataSource data-source-factory tx))))))

  PSubmitNode
  (submit-tx [_ tx-ops]
    (.submitTx tx-producer tx-ops))

  Closeable
  (close [_]
    (when close-fn
      (close-fn))))

(defn db
  ([node] @(db-async node))
  ([node db-opts] @(db-async node db-opts)))

(defmethod print-method Node [_node ^Writer w] (.write w "#<Core2Node>"))
(defmethod pp/simple-dispatch Node [it] (print-method it *out*))

(defmethod ig/prep-key :core2/node [_ opts]
  (merge {:indexer (ig/ref ::idx/indexer)
          :data-source-factory (ig/ref ::ds/data-source-factory)
          :tx-producer (ig/ref ::txp/tx-producer)}
         opts))

(defmethod ig/init-key :core2/node [_ deps]
  (map->Node (assoc deps :!system (atom nil))))

(defmethod ig/halt-key! :core2/node [_ ^Node node]
  (.close node))

(defn ^core2.ICursor open-ra
  ([query db-or-dbs] (open-ra query db-or-dbs {}))

  ([query db-or-dbs opts]
   (let [allocator (RootAllocator.)]
     (try
       (-> (op/open-ra query
                       (if (map? db-or-dbs) db-or-dbs {'$ db-or-dbs})
                       (-> (merge {:default-valid-time (Date.)} opts)
                           (assoc :allocator allocator)))
           (util/and-also-close allocator))
       (catch Throwable t
         (util/try-close allocator)
         (throw t))))))

(defn plan-ra
  ([query db-or-dbs] (plan-ra query db-or-dbs {}))
  ([query db-or-dbs query-opts]
   (reify IReduceInit
     (reduce [_ f init]
       (with-open [res (open-ra query db-or-dbs query-opts)]
         (util/reduce-cursor (fn [acc rel]
                               (reduce f acc (rel/rel->rows rel)))
                             init
                             res))))))

(defn plan-q [query & params]
  (let [{:keys [query srcs query-opts]} (apply d/compile-query query params)]
    (plan-ra query srcs query-opts)))

(alter-meta! #'plan-q assoc :arglists '([query & params]
                                        [query opts & params]))

(defmethod ig/init-key :core2/allocator [_ _] (RootAllocator.))
(defmethod ig/halt-key! :core2/allocator [_ ^BufferAllocator a] (.close a))

(defn- with-default-impl [opts parent-k impl-k]
  (cond-> opts
    (not (ig/find-derived opts parent-k)) (assoc impl-k {})))

(defn start-node ^core2.core.Node [opts]
  (let [system (-> (into {:core2/node {}
                          :core2/allocator {}
                          ::idx/indexer {}
                          :core2.ingest-loop/ingest-loop {}
                          :core2.metadata/metadata-manager {}
                          :core2.temporal/temporal-manager {}
                          :core2.buffer-pool/buffer-pool {}
                          ::ds/data-source-factory {}
                          ::txp/tx-producer {}}
                         opts)
                   (with-default-impl :core2/log :core2.log/memory-log)
                   (with-default-impl :core2/object-store :core2.object-store/memory-object-store)
                   (doto ig/load-namespaces)
                   ig/prep
                   ig/init)]

    (-> (:core2/node system)
        (doto (-> :!system (reset! system)))
        (assoc :close-fn #(do (ig/halt! system)
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

(defmethod ig/prep-key :core2/submit-node [_ opts]
  (merge {:tx-producer (ig/ref :core2.tx-producer/tx-producer)}
         opts))

(defmethod ig/init-key :core2/submit-node [_ {:keys [tx-producer]}]
  (map->SubmitNode {:tx-producer tx-producer, :!system (atom nil)}))

(defmethod ig/halt-key! :core2/submit-node [_ ^SubmitNode node]
  (.close node))

(defn start-submit-node ^core2.core.SubmitNode [opts]
  (let [system (-> (into {:core2/submit-node {}
                          :core2.tx-producer/tx-producer {}
                          :core2/allocator {}}
                         opts)
                   ig/prep
                   (doto ig/load-namespaces)
                   ig/init)]

    (-> (:core2/submit-node system)
        (doto (-> :!system (reset! system)))
        (assoc :close-fn #(ig/halt! system)))))
