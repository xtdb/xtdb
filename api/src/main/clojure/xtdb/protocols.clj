(ns ^{:clojure.tools.namespace.repl/load false, :clojure.tools.namespace.repl/unload false}
    xtdb.protocols)

(defprotocol PNode
  (^java.util.stream.Stream open-sql-query [node ^String query opts])
  (^java.util.stream.Stream open-xtql-query [node query opts])
  (^long submit-tx [node tx-ops tx-opts])
  (^xtdb.api.TransactionResult execute-tx [node tx-ops tx-opts]))

(defprotocol PStatus
  (latest-submitted-tx-id [node])
  (status [node]))

(def http-routes
  [["/status" {:name :status
               :summary "Status"
               :description "Get status information from the node"}]

   ["/tx" {:name :tx
           :summary "Transaction"
           :description "Submits a transaction to the cluster"}]

   ["/query" {:name :query
              :summary "Query"}]

   ["/openapi.yaml" {:name :openapi
                     :no-doc true}]])
