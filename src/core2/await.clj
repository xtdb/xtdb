(ns core2.await
  (:require [core2.tx :as tx])
  (:import core2.tx.TransactionInstant
           [java.util.concurrent CompletableFuture PriorityBlockingQueue]))

(deftype AwaitingTx [^TransactionInstant tx, ^CompletableFuture fut]
  Comparable
  (compareTo [_ other]
    (.compareTo tx (.tx ^AwaitingTx other))))

(defn- await-done? [^TransactionInstant awaited-tx, ^TransactionInstant completed-tx]
  (or (nil? awaited-tx)
      (when completed-tx
        (not (pos? (.compareTo awaited-tx completed-tx))))))

(defn- ->ingester-ex [^Throwable cause]
  (ex-info (str "Ingestion stopped: " (.getMessage cause))
           {}
           cause))

(defn await-tx-async
  ^java.util.concurrent.CompletableFuture
  [^TransactionInstant awaited-tx, ->latest-completed-tx, ^PriorityBlockingQueue awaiters]

  (let [fut (CompletableFuture.)]
    (or (try
          (let [completed-tx (->latest-completed-tx)]
            (when (await-done? awaited-tx completed-tx)
              ;; fast path - don't bother with the PBQ unless we need to
              (.complete fut completed-tx)
              true))
          (catch Exception e
            (.completeExceptionally fut (->ingester-ex e))
            true))

        (let [awaiting-tx (AwaitingTx. awaited-tx fut)]
          (.offer awaiters awaiting-tx)

          (try
            (let [completed-tx (->latest-completed-tx)]
              (when (await-done? awaited-tx completed-tx)
                (.remove awaiters awaiting-tx)
                (.complete fut completed-tx)))
            (catch Exception e
              (.completeExceptionally fut (->ingester-ex e))
              true))))
    fut))

(defn notify-tx [completed-tx ^PriorityBlockingQueue awaiters]
  (while (when-let [^AwaitingTx awaiting-tx (.peek awaiters)]
           (when (await-done? (.tx awaiting-tx) completed-tx)
             (.remove awaiters awaiting-tx)
             (.complete ^CompletableFuture (.fut awaiting-tx) completed-tx)
             true))))

(defn notify-ex [^Exception ex ^PriorityBlockingQueue awaiters]
  ;; NOTE: by this point, any calls to `->latest-completed-tx` (above) must also throw this exception.
  (doseq [^AwaitingTx awaiting-tx awaiters]
    (.completeExceptionally ^CompletableFuture (.fut awaiting-tx)
                            (->ingester-ex ex)))

  (.clear awaiters))
