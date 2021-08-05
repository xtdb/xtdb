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
    ^java.util.concurrent.CompletableFuture #_<TransactionInstant> [node tx-ops]))

(defprotocol PStatus
  (status [node]))

(defn plan-query [node query & params]
  (try
    @(plan-query-async node query params)
    (catch ExecutionException e
      (throw (.getCause e)))))

(def http-routes
  [["/status" {:name :status
               :summary "Status"
               :description "Get status information from the node"}]

   ["/tx" {:name :tx
           :summary "Transaction"
           :description "Submits a transaction to the cluster"}]

   ["/query" {:name :query
              :summary "Query"}]

   ["/latest-completed-tx" {:name :latest-completed-tx
                            :summary "Latest completed transaction"
                            :description "Get the latest transaction to have been indexed by this node"}]])
