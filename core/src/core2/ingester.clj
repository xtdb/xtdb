(ns core2.ingester
  (:require [clojure.tools.logging :as log]
            core2.api
            [core2.await :as await]
            core2.indexer
            core2.log
            core2.operator.scan
            [core2.util :as util]
            [juxt.clojars-mirrors.integrant.core :as ig])
  (:import core2.api.TransactionInstant
           core2.indexer.TransactionIndexer
           core2.operator.scan.Snapshot
           [core2.log Log LogSubscriber]
           java.lang.AutoCloseable
           java.time.Duration
           (java.util.concurrent CompletableFuture PriorityBlockingQueue TimeUnit)
           org.apache.arrow.memory.BufferAllocator
           org.apache.arrow.vector.TimeStampMicroTZVector
           org.apache.arrow.vector.ipc.ArrowStreamReader))

#_{:clj-kondo/ignore [:unused-binding]}
(definterface Ingester
  (^core2.api.TransactionInstant latestCompletedTx [])
  (^java.util.concurrent.CompletableFuture #_<TransactionInstant> awaitTxAsync [^core2.api.TransactionInstant tx])
  (^java.util.concurrent.CompletableFuture #_<Snapshot> snapshot [^core2.api.TransactionInstant tx]))

(defmethod ig/prep-key :core2/ingester [_ opts]
  (-> (merge {:allocator (ig/ref :core2/allocator)
              :log (ig/ref :core2/log)
              :indexer (ig/ref :core2.indexer/indexer)
              :metadata-mgr (ig/ref :core2.metadata/metadata-manager)
              :temporal-mgr (ig/ref :core2.temporal/temporal-manager)
              :watermark-mgr (ig/ref :core2.watermark/watermark-manager)
              :buffer-pool (ig/ref :core2.buffer-pool/buffer-pool)}
             opts)
      (util/maybe-update :poll-sleep-duration util/->duration)))

(defmethod ig/init-key :core2/ingester [_ {:keys [^BufferAllocator allocator, ^Log log, ^TransactionIndexer indexer
                                                  metadata-mgr temporal-mgr watermark-mgr buffer-pool]}]
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

                          (let [^TimeStampMicroTZVector tx-time-vec (.getVector tx-root "tx-time")
                                ^TransactionInstant tx-key (cond-> (.tx record)
                                                             (not (.isNull tx-time-vec 0))
                                                             (assoc :tx-time (-> (.get tx-time-vec 0) (util/micros->instant))))
                                latest-completed-tx (.latestCompletedTx indexer)]

                            (if (and (not (nil? latest-completed-tx))
                                     (neg? (compare (.tx-time tx-key)
                                                    (.tx-time latest-completed-tx))))
                              ;; TODO: we don't yet have the concept of an aborted tx
                              ;; so anyone awaiting this tx will have a Bad Time™.
                              (log/warnf "specified tx-time '%s' older than current tx '%s'"
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
      (latestCompletedTx [_] (.latestCompletedTx indexer))

      (awaitTxAsync [this tx]
        (await/await-tx-async tx
                              #(or (some-> @!ingester-error throw)
                                   (.latestCompletedTx this))
                              awaiters))

      (snapshot [this tx]
        (-> (if tx
              (.awaitTxAsync this tx)
              (CompletableFuture/completedFuture (.latestCompletedTx this)))
            (util/then-apply (fn [tx]
                               (assert watermark-mgr)
                               (Snapshot. metadata-mgr temporal-mgr watermark-mgr buffer-pool tx)))))

      AutoCloseable
      (close [_]
        (util/try-close @!cancel-hook)))))

(defmethod ig/halt-key! :core2/ingester [_ ingester]
  (util/try-close ingester))

(defn snapshot-async ^java.util.concurrent.CompletableFuture [^Ingester ingester, tx]
  (-> (if-not (instance? CompletableFuture tx)
        (CompletableFuture/completedFuture tx)
        tx)
      (util/then-compose (fn [tx]
                           (.snapshot ingester tx)))))

(defn snapshot
  ([^Ingester ingester]
   (snapshot ingester nil))

  ([^Ingester ingester, tx]
   (snapshot ingester tx nil))

  ([^Ingester ingester, tx, ^Duration timeout]
   @(-> (snapshot-async ingester tx)
        (cond-> timeout (.orTimeout (.toMillis timeout) TimeUnit/MILLISECONDS)))))
