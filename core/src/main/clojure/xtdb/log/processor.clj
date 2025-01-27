(ns xtdb.log.processor
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
           [java.time Duration Instant]
           org.apache.arrow.memory.BufferAllocator
           org.apache.arrow.vector.ipc.ArrowStreamReader
           [xtdb.api.log Log$Processor Log$Record]
           xtdb.indexer.IIndexer))

(defmethod ig/prep-key :xtdb.log/processor [_ opts]
  (when opts
    (into {:allocator (ig/ref :xtdb/allocator)
           :indexer (ig/ref :xtdb/indexer)
           :metrics-registry (ig/ref :xtdb.metrics/registry)
           :chunk-flush-duration #xt/duration "PT4H"}
          opts)))

(defn check-chunk-timeout [^Instant now, ^Duration flush-timeout
                           {:keys [^long current-chunk-tx-id latest-completed-tx-id]}
                           {:keys [^Instant last-flush-check, ^long prev-chunk-tx-id, ^long flushed-tx-id]}]
  (when (and (neg? (compare (.plus last-flush-check flush-timeout) now))
             (= prev-chunk-tx-id current-chunk-tx-id)
             (> latest-completed-tx-id flushed-tx-id))

    (log/debugf "last chunk tx-id %s, flushing any pending writes" prev-chunk-tx-id)

    [(doto (ByteBuffer/allocate 9)
       (.put (byte xt-log/hb-flush-chunk))
       (.putLong current-chunk-tx-id)
       (.flip))
     {:last-flush-check now, :chunk-tx-id current-chunk-tx-id}]))

(defrecord Processor [^BufferAllocator allocator
                      ^IIndexer indexer
                      !flush-state
                      ^Duration flush-timeout]
  Log$Processor
  (getLatestCompletedOffset [_]
    (:tx-id (.latestCompletedTx indexer) -1))

  (processRecords [_ log records]
    (let [flush-state @!flush-state]
      (when-let [[msg-buf new-flush-state]
                 (check-chunk-timeout (Instant/now) flush-timeout
                                      {:current-chunk-tx-id (long (:tx-id (.latestCompletedChunkTx indexer) -1))
                                       :latest-completed-tx-id (long (:tx-id (.latestCompletedTx indexer) -1))}
                                      flush-state)]
        (let [latest-flushed-tx-id @(.appendMessage log msg-buf)]
          (doto (compare-and-set! !flush-state flush-state (-> new-flush-state (assoc :flushed-tx-id latest-flushed-tx-id)))
            (assert "multi-threaded?!!")))))

    (doseq [^Log$Record record records]
      (let [payload (.getPayload record)
            log-offset (.getLogOffset record)
            log-ts (.getLogTimestamp record)]
        (condp = (Byte/toUnsignedInt (.get payload 0))
          xt-log/hb-user-arrow-transaction
          (with-open [tx-ops-ch (util/->seekable-byte-channel payload)
                      sr (ArrowStreamReader. tx-ops-ch allocator)
                      tx-root (.getVectorSchemaRoot sr)]
            (.loadNextBatch sr)

            (.indexTx indexer log-offset log-ts tx-root))

          xt-log/hb-flush-chunk
          (let [expected-chunk-idx (.getLong payload 1)]
            (log/debugf "received flush-chunk signal: %d" expected-chunk-idx)
            (.forceFlush indexer (serde/->TxKey log-offset log-ts) expected-chunk-idx))

          (throw (IllegalStateException. (format "Unrecognized log record type %d" (Byte/toUnsignedInt (.get payload 0)))))))))

  AutoCloseable
  (close [_]
    (util/close allocator)))

(defmethod ig/init-key :xtdb.log/processor [_ {:keys [allocator ^IIndexer indexer metrics-registry chunk-flush-duration] :as deps}]
  (when deps
    (util/with-close-on-catch [allocator (util/->child-allocator allocator "watcher")]
      (metrics/add-allocator-gauge metrics-registry "watcher.allocator.allocated_memory" allocator)
      (->Processor allocator indexer
                   (atom {:last-flush-check (Instant/now)
                          :prev-chunk-tx-id (:tx-id (.latestCompletedChunkTx indexer) -1)
                          :flushed-tx-id -1})
                   chunk-flush-duration))))

(defmethod ig/halt-key! :xtdb.log/processor [_ log-processor]
  (util/close log-processor))
