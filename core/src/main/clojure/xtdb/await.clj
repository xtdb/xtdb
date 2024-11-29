(ns xtdb.await
  (:require xtdb.api)
  (:import [java.util.concurrent CompletableFuture PriorityBlockingQueue]
           xtdb.api.TransactionKey))

(deftype AwaitingTx [^long tx-id, ^CompletableFuture fut]
  Comparable
  (compareTo [_ other]
    (Long/compare tx-id (.tx_id ^AwaitingTx other))))

(defn- await-done? [^long awaited-tx-id, ^TransactionKey completed-tx]
  (or (neg? awaited-tx-id)
      (and completed-tx
           (not (pos? (Long/compare awaited-tx-id (.getTxId completed-tx)))))))

(defn- ->ingester-ex [^Throwable cause]
  (ex-info (str "Ingestion stopped: " (.getMessage cause)) {} cause))

(defn await-tx-async ^java.util.concurrent.CompletableFuture [awaited-tx, ->latest-completed-tx, ^PriorityBlockingQueue awaiters]
  (or (try
        ;; fast path - don't bother with the PBQ unless we need to
        (let [latest-completed-tx (->latest-completed-tx)]
          (when (await-done? awaited-tx latest-completed-tx)
            (CompletableFuture/completedFuture latest-completed-tx)))

        (catch Exception e
          (CompletableFuture/failedFuture (->ingester-ex e))))

      (let [fut (CompletableFuture.)
            awaiting-tx (AwaitingTx. awaited-tx fut)]
        (.offer awaiters awaiting-tx)

        (try
          (let [latest-completed-tx (->latest-completed-tx)]
            (when (await-done? awaited-tx latest-completed-tx)
              (.remove awaiters awaiting-tx)
              (.complete fut latest-completed-tx)))
          (catch Exception e
            (.completeExceptionally fut (->ingester-ex e))
            true))

        fut)))

(defn notify-tx [^TransactionKey completed-tx, ^PriorityBlockingQueue awaiters]
  (while (when-let [^AwaitingTx awaiting-tx (.peek awaiters)]
           (let [awaited-tx-id (.tx_id awaiting-tx)]
             (when (await-done? awaited-tx-id completed-tx)
               (.remove awaiters awaiting-tx)
               (.completeAsync ^CompletableFuture (.fut awaiting-tx)
                               (fn [] completed-tx))
               true)))))

(defn notify-ex [^Exception ex ^PriorityBlockingQueue awaiters]
  ;; NOTE: by this point, any calls to `->latest-completed-tx` (above) must also throw this exception.
  (doseq [^AwaitingTx awaiting-tx awaiters]
    (.completeExceptionally ^CompletableFuture (.fut awaiting-tx)
                            (->ingester-ex ex)))

  (.clear awaiters))
