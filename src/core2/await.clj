(ns core2.await
  (:require core2.indexer
            [core2.system :as sys])
  (:import core2.indexer.TransactionIndexer
           java.io.Closeable
           java.time.Duration
           java.util.concurrent.TimeoutException))

(definterface ITxAwaiter
  (^core2.tx.TransactionInstant awaitTx [^core2.tx.TransactionInstant tx])
  (^core2.tx.TransactionInstant awaitTx [^core2.tx.TransactionInstant tx, ^java.time.Duration timeout]))

(deftype TxAwaiter [^TransactionIndexer indexer
                    ^Duration poll-sleep-duration
                    ^:unsynchronized-mutable ^boolean closed?]
  ITxAwaiter
  (awaitTx [this tx] (.awaitTx this tx nil))

  (awaitTx [_this tx timeout]
    (if tx
      (let [poll-sleep-ms (.toMillis poll-sleep-duration)
            end-ns (when timeout
                     (+ (System/nanoTime) (.toNanos timeout)))
            tx-id (.tx-id tx)]
        (loop []
          (let [latest-completed-tx (.latestCompletedTx indexer)]
            (cond
              (and latest-completed-tx
                   (>= (.tx-id latest-completed-tx) tx-id))
              latest-completed-tx

              closed? (throw (IllegalStateException. "node closed"))

              (or (nil? timeout)
                  (neg? (- (System/nanoTime) (long end-ns))))
              (do
                (Thread/sleep poll-sleep-ms)
                (recur))

              :else (throw (TimeoutException.))))))
      (.latestCompletedTx indexer)))

  Closeable
  (close [this]
    (set! (.closed? this) true)))

(defn ->tx-awaiter {::sys/deps {:indexer :core2/indexer}
                    ::sys/args {:poll-sleep-duration {:spec ::sys/duration, :default "PT0.1S"}}}
  [{:keys [indexer poll-sleep-duration]}]
  (TxAwaiter. indexer poll-sleep-duration false))
