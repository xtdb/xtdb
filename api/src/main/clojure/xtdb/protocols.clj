(ns ^{:clojure.tools.namespace.repl/load false, :clojure.tools.namespace.repl/unload false}
    xtdb.protocols)

(defprotocol PNode
  (^java.util.stream.Stream open-sql-query [node ^String query opts])
  (^long submit-tx [node tx-ops tx-opts])
  (^xtdb.api.TransactionResult execute-tx [node tx-ops tx-opts])
  (^xtdb.api.TransactionResult attach-db [node db-name db-config])
  (^xtdb.api.TransactionResult detach-db [node db-name]))

(defprotocol PLocalNode
  (^xtdb.query.PreparedQuery prepare-sql [node query query-opts])
  (^xtdb.query.PreparedQuery prepare-ra [node ra-plan query-opts]))

(defprotocol PStatus
  (latest-submitted-msg-ids [node])
  (latest-processed-msg-ids [node])
  (latest-completed-txs [node])

  (await-token [node])
  (snapshot-token [node])

  (status [node] [node opts]))

(defprotocol ExecuteOp
  (execute-op! [tx-op conn]))
