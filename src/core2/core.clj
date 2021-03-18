(ns core2.core
  (:require [core2.buffer-pool :as bp]
            [core2.indexer :as indexer]
            [core2.ingest-loop :as ingest-loop]
            [core2.log :as log]
            [core2.metadata :as meta]
            [core2.object-store :as os]
            [core2.temporal :as temporal]
            [core2.tx-producer :as txp]
            [core2.util :as util])
  (:import core2.buffer_pool.BufferPool
           [core2.indexer IChunkManager TransactionIndexer]
           core2.ingest_loop.IIngestLoop
           core2.log.LogReader
           core2.metadata.IMetadataManager
           core2.object_store.ObjectStore
           core2.temporal.ITemporalManager
           core2.tx_producer.TxProducer
           java.io.Closeable
           java.nio.file.Path
           java.time.Duration
           [org.apache.arrow.memory BufferAllocator RootAllocator]))

(set! *unchecked-math* :warn-on-boxed)

(deftype Node [^BufferAllocator allocator
               ^LogReader log-reader
               ^ObjectStore object-store
               ^IMetadataManager metadata-manager
               ^ITemporalManager temporal-manager
               ^TransactionIndexer indexer
               ^Closeable ingest-loop
               ^BufferPool buffer-pool]
  Closeable
  (close [_]
    (util/try-close ingest-loop)
    (util/try-close indexer)
    (util/try-close metadata-manager)
    (util/try-close buffer-pool)
    (util/try-close object-store)
    (util/try-close log-reader)
    (util/try-close allocator)))

(defn ->local-node
  (^core2.core.Node [^Path node-dir]
   (->local-node node-dir {}))
  (^core2.core.Node [^Path node-dir opts]
   (let [object-dir (.resolve node-dir "objects")
         log-dir (.resolve node-dir "log")
         allocator (RootAllocator.)
         log-reader (log/->local-directory-log-reader log-dir)
         object-store (os/->file-system-object-store object-dir opts)
         buffer-pool (bp/->memory-mapped-buffer-pool (.resolve node-dir "buffers") allocator object-store)
         metadata-manager (meta/->metadata-manager allocator object-store buffer-pool)
         temporal-manager (temporal/->temporal-manager allocator object-store buffer-pool metadata-manager)
         indexer (indexer/->indexer allocator object-store metadata-manager temporal-manager opts)
         ingest-loop (ingest-loop/->ingest-loop log-reader indexer opts)]
     (Node. allocator log-reader object-store metadata-manager temporal-manager indexer ingest-loop buffer-pool))))

(defn ->local-tx-producer
  (^core2.tx_producer.LogTxProducer [^Path node-dir] (txp/->local-tx-producer node-dir))
  (^core2.tx_producer.LogTxProducer [^Path node-dir log-writer-opts] (txp/->local-tx-producer node-dir log-writer-opts)))

(defn submit-tx ^java.util.concurrent.CompletableFuture [^TxProducer tx-producer, tx-ops]
  (.submitTx tx-producer tx-ops))

(defn await-tx
  (^java.util.concurrent.CompletableFuture [^Node node, tx]
   (.awaitTx ^IIngestLoop (.ingest-loop node) tx))
  (^java.util.concurrent.CompletableFuture [^Node node, tx, ^Duration timeout]
   (.awaitTx ^IIngestLoop (.ingest-loop node) tx timeout)))

(defn latest-completed-tx ^core2.tx.TransactionInstant [^Node node]
  (.latestCompletedTx ^TransactionIndexer (.indexer node)))

(defn open-watermark ^core2.tx.Watermark [^Node node]
  (.getWatermark ^IChunkManager (.indexer node)))

(defn -main [& [node-dir :as args]]
  (if node-dir
    (let [node-dir (util/->path node-dir)]
      (->local-node node-dir)
      (println "core2 started in" (str node-dir)))
    (binding [*out* *err*]
      (println "node directory argument required")
      (System/exit 1))))
