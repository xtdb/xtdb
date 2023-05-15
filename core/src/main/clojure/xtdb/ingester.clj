(ns xtdb.ingester
  (:require [clojure.tools.logging :as log]
            xtdb.api.protocols
            [xtdb.await :as await]
            xtdb.indexer
            xtdb.log
            xtdb.operator.scan
            [xtdb.util :as util]
            xtdb.watermark
            [juxt.clojars-mirrors.integrant.core :as ig])
  (:import xtdb.api.TransactionInstant
           xtdb.indexer.IIndexer
           [xtdb.log Log LogSubscriber]
           java.lang.AutoCloseable
           (java.util.concurrent CompletableFuture PriorityBlockingQueue TimeUnit)
           org.apache.arrow.memory.BufferAllocator
           org.apache.arrow.vector.ipc.ArrowStreamReader
           org.apache.arrow.vector.TimeStampMicroTZVector))

#_{:clj-kondo/ignore [:unused-binding :clojure-lsp/unused-public-var]}
(definterface Ingester
  (^java.util.concurrent.CompletableFuture #_<TransactionInstant> awaitTxAsync [^xtdb.api.TransactionInstant tx, ^java.time.Duration timeout]))

(defmethod ig/prep-key :xtdb/ingester [_ opts]
  (-> (merge {:allocator (ig/ref :xtdb/allocator)
              :log (ig/ref :xtdb/log)
              :indexer (ig/ref :xtdb/indexer)}
             opts)
      (util/maybe-update :poll-sleep-duration util/->duration)))

(defmethod ig/init-key :xtdb/ingester [_ {:keys [^BufferAllocator allocator, ^Log log, ^IIndexer indexer]}]
  (let [!cancel-hook (promise)
        awaiters (PriorityBlockingQueue.)
        !ingester-error (atom nil)]
    (.subscribe log
                (:tx-id (.latestCompletedTx indexer))
                (reify LogSubscriber
                  (onSubscribe [_ cancel-hook]
                    (deliver !cancel-hook cancel-hook))

                  (acceptRecord [_ record]
                    (if (Thread/interrupted)
                      (throw (InterruptedException.))

                      (try
                        (with-open [tx-ops-ch (util/->seekable-byte-channel (.record record))
                                    sr (ArrowStreamReader. tx-ops-ch allocator)
                                    tx-root (.getVectorSchemaRoot sr)]
                          (.loadNextBatch sr)

                          (let [^TimeStampMicroTZVector system-time-vec (.getVector tx-root "system-time")
                                ^TransactionInstant tx-key (cond-> (.tx record)
                                                             (not (.isNull system-time-vec 0))
                                                             (assoc :system-time (-> (.get system-time-vec 0) (util/micros->instant))))
                                latest-completed-tx (.latestCompletedTx indexer)]

                            (if (and (not (nil? latest-completed-tx))
                                     (neg? (compare (.system-time tx-key)
                                                    (.system-time latest-completed-tx))))
                              ;; TODO: we don't yet have the concept of an aborted tx
                              ;; so anyone awaiting this tx will have a Bad Time™.
                              (log/warnf "specified system-time '%s' older than current tx '%s'"
                                         (pr-str tx-key)
                                         (pr-str latest-completed-tx))

                              (do
                                (.indexTx indexer tx-key tx-root)
                                (await/notify-tx tx-key awaiters)))))

                        (catch Throwable e
                          (reset! !ingester-error e)
                          (await/notify-ex e awaiters)
                          (throw e)))))))

    (reify
      Ingester
      (awaitTxAsync [_ tx timeout]
        (-> (if tx
              (await/await-tx-async tx
                                    #(or (some-> @!ingester-error throw)
                                         (.latestCompletedTx indexer))
                                    awaiters)
              (CompletableFuture/completedFuture (.latestCompletedTx indexer)))
            (cond-> timeout (.orTimeout (.toMillis timeout) TimeUnit/MILLISECONDS))))

      AutoCloseable
      (close [_]
        (util/try-close @!cancel-hook)))))

(defmethod ig/halt-key! :xtdb/ingester [_ ingester]
  (util/try-close ingester))

