(ns core2.api
  (:import java.io.Writer
           java.util.concurrent.ExecutionException
           java.util.Date))

(defrecord TransactionInstant [^long tx-id, ^Date tx-time]
  Comparable
  (compareTo [_ other]
    (- tx-id (.tx-id ^TransactionInstant other))))

(defmethod print-method TransactionInstant [tx-instant ^Writer w]
  (.write w (str "#core2/tx-instant " (select-keys tx-instant [:tx-id :tx-time]))))

(defprotocol PClient
  (latest-completed-tx ^core2.api.TransactionInstant [node])

  ;; we may want to go to `open-query-async`/`Stream` instead, when we have a Java API
  (plan-query-async ^java.util.concurrent.CompletableFuture [node query params]))

(defprotocol PSubmitNode
  (submit-tx
    ^java.util.concurrent.CompletableFuture #_<TransactionInstant> [tx-producer tx-ops]))

(defn plan-query [node query & params]
  (try
    @(plan-query-async node query params)
    (catch ExecutionException e
      (throw (.getCause e)))))
