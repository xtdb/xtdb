(ns xtdb.stagnant-log-flusher
  (:require [clojure.tools.logging :as log]
            [juxt.clojars-mirrors.integrant.core :as ig]
            xtdb.indexer
            [xtdb.log :as xt-log]
            [xtdb.util :as util])
  (:import (java.nio ByteBuffer)
           (java.nio.channels ClosedByInterruptException)
           (java.time Duration)
           (java.util.concurrent ExecutorService Executors TimeUnit)
           (xtdb.api IndexerConfig TransactionKey)
           (xtdb.api.log Log)
           (xtdb.indexer IIndexer)))

;; see https://github.com/xtdb/xtdb/issues/2548

;; callback hook used to control timing in tests
;; receives a map of the :last-flush, :last-seen tx-keys
(def ^:dynamic *on-heartbeat*
  (constantly nil))

(defmethod ig/prep-key ::flusher [_ indexer-config]
  {:indexer (ig/ref :xtdb/indexer)
   :log (ig/ref :xtdb/log)
   :indexer-config indexer-config})

(defmethod ig/init-key ::flusher [_ {:keys [^IIndexer indexer, ^Log log, ^IndexerConfig indexer-config]}]
  (let [duration (.getFlushDuration indexer-config)
        exr-tf (util/->prefix-thread-factory "xtdb.stagnant-log-flush")
        exr (Executors/newSingleThreadScheduledExecutor exr-tf)

        ;; the tx-key of the last seen chunk tx
        previously-seen-chunk-tx-id (atom nil)
        ;; the tx-key of the last flush msg sent by me
        !last-flush-tx-id (atom nil)

        f (bound-fn heartbeat []
            (*on-heartbeat* {:last-flush @!last-flush-tx-id, :last-seen @previously-seen-chunk-tx-id})
            (when-some [latest-tx-id (some-> (.latestCompletedTx indexer) (.getTxId))]
              (let [latest-chunk-tx-id (some-> (.latestCompletedChunkTx indexer) (.getTxId))]
                (try
                  (when (and (= @previously-seen-chunk-tx-id latest-chunk-tx-id)
                             (or (nil? @!last-flush-tx-id)
                                 (< (long @!last-flush-tx-id) (long latest-tx-id))))
                    (log/debugf "last chunk tx-id %s, flushing any pending writes" latest-chunk-tx-id)

                    (let [record-buf (-> (ByteBuffer/allocate 9)
                                         (.put (byte xt-log/hb-flush-chunk))
                                         (.putLong (or latest-chunk-tx-id -1))
                                         .flip)]
                      (reset! !last-flush-tx-id @(.appendTx log record-buf))))
                  (catch InterruptedException _)
                  (catch ClosedByInterruptException _)
                  (catch Throwable e
                    (log/error e "exception caught submitting flush record"))
                  (finally
                    (reset! previously-seen-chunk-tx-id latest-chunk-tx-id))))))]
    {:executor exr
     :task (.scheduleAtFixedRate exr f (.toMillis duration) (.toMillis duration) TimeUnit/MILLISECONDS)}))

(defmethod ig/halt-key! ::flusher [_ {:keys [^ExecutorService executor, task]}]
  (future-cancel task)
  (.shutdownNow executor)

  (when-not (.awaitTermination executor 10 TimeUnit/SECONDS)
    (log/warnf "flusher did not shutdown within %d seconds" 10)))
