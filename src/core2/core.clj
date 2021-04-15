(ns core2.core
  (:require [core2.indexer :as indexer]
            core2.ingest-loop
            [core2.log :as log]
            [core2.logical-plan :as lp]
            [core2.system :as sys]
            [core2.tx-producer :as txp]
            [core2.util :as util])
  (:import [core2.indexer IChunkManager TransactionIndexer]
           core2.ingest_loop.IIngestLoop
           core2.log.LogWriter
           core2.operator.IOperatorFactory
           java.io.Closeable
           java.lang.AutoCloseable
           [org.apache.arrow.memory BufferAllocator RootAllocator]))

(set! *unchecked-math* :warn-on-boxed)

(defprotocol PNode
  (await-tx
    ^core2.tx.TransactionInstant [node tx]
    ^core2.tx.TransactionInstant [node tx timeout])

  (latest-completed-tx ^core2.tx.TransactionInstant [node])
  (open-watermark ^core2.tx.Watermark [node])

  (open-q
    ^java.lang.AutoCloseable [node watermark query]))

(defprotocol PTxProducer
  (submit-tx
    ^java.util.concurrent.CompletableFuture [tx-producer tx-ops]))

(defrecord Node [^TransactionIndexer indexer
                 ^IIngestLoop ingest-loop
                 ^IOperatorFactory op-factory
                 !system
                 close-fn]
  PNode
  (await-tx [this tx] (await-tx this tx nil))
  (await-tx [_ tx timeout] (.awaitTx ingest-loop tx timeout))

  (latest-completed-tx [_] (.latestCompletedTx indexer))

  (open-watermark [_] (.getWatermark ^IChunkManager indexer))

  (open-q [_ watermark query] (lp/open-q op-factory watermark query))

  Closeable
  (close [_]
    (when close-fn
      (close-fn))))

(defn ->node {::sys/deps {:indexer :core2/indexer
                          :ingest-loop :core2/ingest-loop
                          :op-factory :core2/op-factory}}
  [deps]
  (map->Node (assoc deps :!system (atom nil))))

(defn ->allocator [_]
  (RootAllocator.))

(defn start-node [opts]
  (let [system (-> (sys/prep-system (into [{:core2/node `->node
                                            :core2/allocator `->allocator
                                            :core2/indexer 'core2.indexer/->indexer
                                            :core2/ingest-loop 'core2.ingest-loop/->ingest-loop
                                            :core2/log-reader 'core2.log/->local-directory-log-reader
                                            :core2/metadata-manager 'core2.metadata/->metadata-manager
                                            :core2/temporal-manager 'core2.temporal/->temporal-manager
                                            :core2/object-store 'core2.object-store/->file-system-object-store
                                            :core2/buffer-pool 'core2.buffer-pool/->buffer-pool
                                            :core2/op-factory 'core2.operator/->operator-factory}]
                                          (cond-> opts (not (vector? opts)) vector)))
                   (sys/start-system))]

    (-> (:core2/node system)
        (doto (-> :!system (reset! system)))
        (assoc :close-fn #(.close ^AutoCloseable system)))))

(defrecord TxProducer [^LogWriter log-writer
                       ^BufferAllocator allocator
                       !system
                       close-fn]
  PTxProducer
  (submit-tx [_ tx-ops]
    (txp/submit-tx log-writer allocator tx-ops))

  AutoCloseable
  (close [_]
    (when close-fn
      (close-fn))))

(defn ->tx-producer {::sys/deps {:log-writer :core2/log-writer
                                 :allocator :core2/allocator}}
  [deps]
  (map->TxProducer (assoc deps :!system (atom nil))))

(defn start-tx-producer [opts]
  (let [system (-> (sys/prep-system (into [{:core2/tx-producer `->tx-producer
                                            :core2/allocator `->allocator
                                            :core2/log-writer 'core2.log/->local-directory-log-writer}]
                                          (cond-> opts (not (vector? opts)) vector)))
                   (sys/start-system))]

    (-> (:core2/tx-producer system)
        (doto (-> :!system (reset! system)))
        (assoc :close-fn #(.close ^AutoCloseable system)))))

(defn -main [& [node-dir :as args]]
  (if node-dir
    (let [node-dir (util/->path node-dir)]
      (start-node {:core2/log-reader {:root-path (.resolve node-dir "log")}
                   :core2/buffer-pool {:root-path (.resolve node-dir "buffers")}
                   :core2/object-store {:root-path (.resolve node-dir "objects")}})
      (println "core2 started in" (str node-dir)))
    (binding [*out* *err*]
      (println "node directory argument required")
      (System/exit 1))))
