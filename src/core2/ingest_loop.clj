(ns core2.ingest-loop
  (:require [clojure.tools.logging :as log]
            core2.indexer
            core2.log
            [core2.util :as util]
            [integrant.core :as ig])
  (:import core2.indexer.TransactionIndexer
           [core2.log LogReader LogRecord]
           java.io.Closeable
           java.time.Duration
           [java.util.concurrent Executors ExecutorService]))

(defn- ingest-loop [^LogReader log, ^TransactionIndexer indexer
                    {:keys [^Duration poll-sleep-duration ^long batch-size],
                     :or {batch-size 100}}]
  (let [poll-sleep-ms (.toMillis poll-sleep-duration)]
    (try
      (while true
        (if-let [log-records (not-empty (.readRecords log (some-> (.latestCompletedTx indexer) .tx-id) batch-size))]
          (doseq [^LogRecord record log-records]
            (if (Thread/interrupted)
              (throw (InterruptedException.))
              (.indexTx indexer (.tx record) (.record record))))
          (Thread/sleep poll-sleep-ms)))
      (catch InterruptedException _)
      (catch Throwable t
        (if (Thread/interrupted)
          (log/warn t "exception while closing")
          (do (log/fatal t "ingest loop stopped")
              (throw t)))))))

(deftype IngestLoop [^ExecutorService pool]
  Closeable
  (close [_]
    (util/shutdown-pool pool)))

(defmethod ig/prep-key ::ingest-loop [_ opts]
  (-> (merge {:log (ig/ref :core2/log)
              :indexer (ig/ref :core2.indexer/indexer)
              :poll-sleep-duration "PT0.1S"}
             opts)
      (util/maybe-update :poll-sleep-duration util/->duration)))

(defmethod ig/init-key ::ingest-loop [_ {:keys [log indexer poll-sleep-duration]}]
  (let [pool (doto (Executors/newSingleThreadExecutor (util/->prefix-thread-factory "ingest-loop-"))
               (.submit ^Runnable #(ingest-loop log indexer {:poll-sleep-duration poll-sleep-duration})))]
    (IngestLoop. pool)))

(defmethod ig/halt-key! ::ingest-loop [_ ^IngestLoop il]
  (.close il))
