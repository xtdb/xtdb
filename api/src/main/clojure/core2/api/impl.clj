(ns core2.api.impl
  (:import java.util.concurrent.ExecutionException))

(defprotocol PNode
  (^java.util.concurrent.CompletableFuture open-datalog& [node q args])
  (^java.util.concurrent.CompletableFuture open-sql& [node q opts]))

(defprotocol PSubmitNode
  (^java.util.concurrent.CompletableFuture #_<TransactionInstant>
   submit-tx&
   [node tx-ops]
   [node tx-ops opts]))

(defprotocol PStatus
  (status [node]))

(defmacro rethrowing-cause [form]
  `(try
     ~form
     (catch ExecutionException e#
       (throw (.getCause e#)))))

(def http-routes
  [["/status" {:name :status
               :summary "Status"
               :description "Get status information from the node"}]

   ["/tx" {:name :tx
           :summary "Transaction"
           :description "Submits a transaction to the cluster"}]

   ["/datalog" {:name :datalog-query
                :summary "Datalog Query"}]

   ["/sql" {:name :sql-query
            :summary "SQL"}]])
