(ns xtdb.ingester
  (:require [clojure.tools.logging :as log]
            xtdb.api.protocols
            [xtdb.await :as await]
            xtdb.indexer
            [xtdb.log :as xt-log]
            xtdb.operator.scan
            [xtdb.util :as util]
            xtdb.watermark
            [juxt.clojars-mirrors.integrant.core :as ig])
  (:import xtdb.api.protocols.TransactionInstant
           xtdb.indexer.IIndexer
           (java.nio ByteBuffer)
           [xtdb.log Log LogSubscriber]
           java.lang.AutoCloseable
           (java.util.concurrent CompletableFuture PriorityBlockingQueue TimeUnit)
           org.apache.arrow.memory.BufferAllocator
           org.apache.arrow.vector.ipc.ArrowStreamReader
           org.apache.arrow.vector.TimeStampMicroTZVector))

(defmethod ig/prep-key :xtdb/ingester [_ opts]
  (-> (merge {:allocator (ig/ref :xtdb/allocator)
              :log (ig/ref :xtdb/log)
              :indexer (ig/ref :xtdb/indexer)}
             opts)
      (util/maybe-update :poll-sleep-duration util/->duration)))

(defn- get-bb-long [^ByteBuffer buf ^long pos default]
  (if (< (+ pos 8) (.limit buf))
    (.getLong buf pos)
    default))

(defmethod ig/init-key :xtdb/ingester [_ {:keys [^BufferAllocator allocator, ^Log log, ^IIndexer indexer]}]
  (let [!cancel-hook (promise)
        !ingester-error (atom nil)]
    (.subscribe log
                (:tx-id (.latestCompletedTx indexer))
                (reify LogSubscriber
                  (onSubscribe [_ cancel-hook]
                    (deliver !cancel-hook cancel-hook))

                  (acceptRecord [_ record]
                    (if (Thread/interrupted)
                      (throw (InterruptedException.))

                      (condp = (Byte/toUnsignedInt (.get ^ByteBuffer (.-record record) 0))
                        xt-log/hb-user-arrow-transaction
                        (with-open [tx-ops-ch (util/->seekable-byte-channel (.record record))
                                    sr (ArrowStreamReader. tx-ops-ch allocator)
                                    tx-root (.getVectorSchemaRoot sr)]
                          (.loadNextBatch sr)

                          (let [^TimeStampMicroTZVector system-time-vec (.getVector tx-root "system-time")
                                ^TransactionInstant tx-key (cond-> (.tx record)
                                                             (not (.isNull system-time-vec 0))
                                                             (assoc :system-time (-> (.get system-time-vec 0) (util/micros->instant))))]

                            (.indexTx indexer tx-key tx-root)))

                        xt-log/hb-flush-chunk
                        (let [expected-chunk-tx-id (get-bb-long (:record record) 1 -1)]
                          (log/debugf "received flush-chunk signal: %d" expected-chunk-tx-id)
                          (.forceFlush indexer (:tx record) expected-chunk-tx-id))

                        (throw (IllegalStateException. (format "Unrecognized log record type %d" (Byte/toUnsignedInt (.get ^ByteBuffer (.-record record) 0))))))))))

    (reify
      AutoCloseable
      (close [_]
        (util/try-close @!cancel-hook)))))

(defmethod ig/halt-key! :xtdb/ingester [_ ingester]
  (util/try-close ingester))
