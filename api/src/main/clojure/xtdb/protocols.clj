(ns ^{:clojure.tools.namespace.repl/load false, :clojure.tools.namespace.repl/unload false}
    xtdb.protocols)

(defprotocol PNode
  (^java.util.stream.Stream open-sql-query [node ^String query opts])
  (^java.util.stream.Stream open-xtql-query [node query opts])
  (^long submit-tx [node tx-ops tx-opts])
  (^xtdb.api.TransactionResult execute-tx [node tx-ops tx-opts]))

(defprotocol PLocalNode
  (^xtdb.query.PreparedQuery prepare-sql [node query query-opts])
  (^xtdb.query.PreparedQuery prepare-xtql [node query query-opts])
  (^xtdb.query.PreparedQuery prepare-ra [node ra-plan query-opts])

  (^void create-db! [node db-name]))

(defprotocol PStatus
  (latest-submitted-tx-ids [node])
  (latest-completed-txs [node])

  (await-token [node])
  (snapshot-token [node])

  (status [node] [node opts]))

(def http-routes
  [["/tx" {:name :tx
           :summary "Transaction"
           :description "Submits a transaction to the cluster"}]

   ["/query" {:name :query
              :summary "Query"}]

   ["/openapi.yaml" {:name :openapi
                     :no-doc true}]])

(defprotocol ExecuteOp
  (execute-op! [tx-op conn]))
