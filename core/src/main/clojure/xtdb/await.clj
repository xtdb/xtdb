(ns xtdb.await
  (:require xtdb.api.protocols)
  (:import xtdb.api.TransactionInstant
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

  (or (try
        ;; fast path - don't bother with the PBQ unless we need to
        (when (await-done? awaited-tx (->latest-completed-tx))
          (CompletableFuture/completedFuture awaited-tx))
        (catch Exception e
          (CompletableFuture/failedFuture (->ingester-ex e))))

      (let [fut (CompletableFuture.)
            awaiting-tx (AwaitingTx. awaited-tx fut)]
        (.offer awaiters awaiting-tx)

        (try
          (when (await-done? awaited-tx (->latest-completed-tx))
            (.remove awaiters awaiting-tx)
            (.complete fut awaited-tx))
          (catch Exception e
            (.completeExceptionally fut (->ingester-ex e))
            true))

        fut)))

(defn notify-tx [completed-tx ^PriorityBlockingQueue awaiters]
  (while (when-let [^AwaitingTx awaiting-tx (.peek awaiters)]
           (let [awaited-tx (.tx awaiting-tx)]
             (when (await-done? awaited-tx completed-tx)
               (.remove awaiters awaiting-tx)
               (.complete ^CompletableFuture (.fut awaiting-tx) awaited-tx)
               true)))))

(defn notify-ex [^Exception ex ^PriorityBlockingQueue awaiters]
  ;; NOTE: by this point, any calls to `->latest-completed-tx` (above) must also throw this exception.
  (doseq [^AwaitingTx awaiting-tx awaiters]
    (.completeExceptionally ^CompletableFuture (.fut awaiting-tx)
                            (->ingester-ex ex)))

  (.clear awaiters))
