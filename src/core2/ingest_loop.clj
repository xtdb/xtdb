(ns core2.ingest-loop
  (:require [clojure.tools.logging :as log]
            core2.indexer
            [core2.log :as c2-log]
            [core2.util :as util])
  (:import [core2.log LogReader LogRecord]
           [core2.indexer TransactionIndexer]
           java.io.Closeable
           java.time.Duration
           [java.util.concurrent Executors ExecutorService TimeoutException]))

(definterface IIngestLoop
  (^core2.tx.TransactionInstant awaitTx [^core2.tx.TransactionInstant tx])
  (^core2.tx.TransactionInstant awaitTx [^core2.tx.TransactionInstant tx, ^java.time.Duration timeout]))

(defn- ingest-loop [^LogReader log-reader, ^TransactionIndexer indexer
                    {:keys [^Duration poll-sleep-duration ^long batch-size],
                     :or {poll-sleep-duration (Duration/ofMillis 100)
                          batch-size 100}}]
  (let [poll-sleep-ms (.toMillis poll-sleep-duration)]
    (try
      (while true
        (if-let [log-records (not-empty (.readRecords log-reader (some-> (.latestCompletedTx indexer) .tx-id) batch-size))]
          (doseq [^LogRecord record log-records]
            (if (Thread/interrupted)
              (throw (InterruptedException.))
              (.indexTx indexer (c2-log/log-record->tx-instant record) (.record record))))
          (Thread/sleep poll-sleep-ms)))
      (catch InterruptedException _)
      (catch Throwable t
        (if (Thread/interrupted)
          (log/warn t "exception while closing")
          (do (log/fatal t "ingest loop stopped")
              (throw t)))))))

(deftype IngestLoop [^TransactionIndexer indexer
                     ^ExecutorService pool
                     ingest-opts]
  IIngestLoop
  (awaitTx [this tx] (.awaitTx this tx nil))

  (awaitTx [_this tx timeout]
    (if tx
      (let [{:keys [^Duration poll-sleep-duration],
             :or {poll-sleep-duration (Duration/ofMillis 100)}} ingest-opts
            poll-sleep-ms (.toMillis poll-sleep-duration)
            end-ns (+ (System/nanoTime) (.toNanos timeout))
            tx-id (.tx-id tx)]
        (loop []
          (let [latest-completed-tx (.latestCompletedTx indexer)]
            (cond
              (and latest-completed-tx
                   (>= (.tx-id latest-completed-tx) tx-id))
              latest-completed-tx

              (.isShutdown pool) (throw (IllegalStateException. "node closed"))

              (or (nil? timeout)
                  (neg? (- (System/nanoTime) end-ns)))
              (do
                (Thread/sleep poll-sleep-ms)
                (recur))

              :else (throw (TimeoutException.))))))
      (.latestCompletedTx indexer)))

  Closeable
  (close [_]
    (util/shutdown-pool pool)))

(defn ->ingest-loop
  (^java.io.Closeable [^LogReader log-reader, ^TransactionIndexer indexer]
   (->ingest-loop log-reader indexer {}))

  (^java.io.Closeable [^LogReader log-reader, ^TransactionIndexer indexer, ingest-opts]
   (let [pool (doto (Executors/newSingleThreadExecutor (util/->prefix-thread-factory "ingest-loop-"))
                (.submit ^Runnable #(ingest-loop log-reader indexer ingest-opts)))]
     (IngestLoop. indexer pool ingest-opts))))
