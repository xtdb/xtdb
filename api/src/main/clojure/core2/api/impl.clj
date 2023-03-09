(ns core2.api.impl
  (:import java.util.concurrent.ExecutionException))

(defprotocol PNode
  (^java.util.concurrent.CompletableFuture open-datalog& [node q args])
  (^java.util.concurrent.CompletableFuture open-sql& [node q opts])
  (^core2.api.TransactionInstant latest-submitted-tx [node]))

(defprotocol PSubmitNode
  (^java.util.concurrent.CompletableFuture #_<TransactionInstant> submit-tx&
   [node tx-ops]
   [node tx-ops opts]))

(defprotocol PStatus
  (status [node]))

(defmacro rethrowing-cause [form]
  `(try
     ~form
     (catch ExecutionException e#
       (throw (.getCause e#)))))

(defn max-tx [l r]
  (if (or (nil? l)
          (and r (neg? (compare l r))))
    r
    l))

(defn after-latest-submitted-tx [basis node]
  (cond-> basis
    (not (or (contains? basis :tx)
             (contains? basis :after-tx)))
    (assoc :after-tx (latest-submitted-tx node))))

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

