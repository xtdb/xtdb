(ns xtdb.log.watcher
  (:require [clojure.tools.logging :as log]
            [integrant.core :as ig]
            xtdb.api
            xtdb.indexer
            [xtdb.log :as xt-log]
            [xtdb.metrics :as metrics]
            xtdb.operator.scan
            [xtdb.serde :as serde]
            [xtdb.util :as util])
  (:import java.lang.AutoCloseable
           (java.nio ByteBuffer)
           org.apache.arrow.memory.BufferAllocator
           org.apache.arrow.vector.ipc.ArrowStreamReader
           [xtdb.api.log Log TxLog$Subscriber]
           xtdb.indexer.IIndexer))

(defmethod ig/prep-key :xtdb.log/watcher [_ opts]
  (into {:allocator (ig/ref :xtdb/allocator)
         :log (ig/ref :xtdb/log)
         :indexer (ig/ref :xtdb/indexer)
         :metrics-registry (ig/ref :xtdb.metrics/registry)}
        opts))

(defn- get-bb-long [^ByteBuffer buf ^long pos default]
  (if (< (+ pos 8) (.limit buf))
    (.getLong buf pos)
    default))

(defn- watch-log! [{:keys [^BufferAllocator allocator, ^Log log, ^IIndexer indexer]}]
  (let [!cancel-hook (promise)]
    (.subscribeTxs log
                   (some-> (.latestCompletedTx indexer) (.getTxId))
                   (reify TxLog$Subscriber
                     (onSubscribe [_ cancel-hook]
                       (deliver !cancel-hook cancel-hook))

                     (accept [_ record]
                       (if (Thread/interrupted)
                         (throw (InterruptedException.))

                         (condp = (Byte/toUnsignedInt (.get (.getRecord record) 0))
                           xt-log/hb-user-arrow-transaction
                           (with-open [tx-ops-ch (util/->seekable-byte-channel (.getRecord record))
                                       sr (ArrowStreamReader. tx-ops-ch allocator)
                                       tx-root (.getVectorSchemaRoot sr)]
                             (.loadNextBatch sr)

                             (.indexTx indexer (.getTxId record) (.getTimestamp record) tx-root))

                           xt-log/hb-flush-chunk
                           (let [expected-chunk-tx-id (get-bb-long (.getRecord record) 1 -1)]
                             (log/debugf "received flush-chunk signal: %d" expected-chunk-tx-id)
                             (.forceFlush indexer (serde/->TxKey (.getTxId record) (.getTimestamp record)) expected-chunk-tx-id))

                           (throw (IllegalStateException. (format "Unrecognized log record type %d" (Byte/toUnsignedInt (.get (.getRecord record) 0))))))))))
    !cancel-hook))

(defmethod ig/init-key :xtdb.log/watcher [_ {:keys [allocator metrics-registry] :as deps}]
  (util/with-close-on-catch [allocator (util/->child-allocator allocator "watcher")]
    (metrics/add-allocator-gauge metrics-registry "watcher.allocator.allocated_memory" allocator)
    (let [!watcher-cancel-hook (watch-log! (-> deps
                                               (assoc :allocator allocator)))]


      (reify
        AutoCloseable
        (close [_]
          (util/try-close @!watcher-cancel-hook)
          (util/close allocator))))))

(defmethod ig/halt-key! :xtdb.log/watcher [_ log-watcher]
  (util/close log-watcher))
