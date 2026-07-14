(ns ^{:clojure.tools.namespace.repl/load false, :clojure.tools.namespace.repl/unload false}
    xtdb.protocols)

(defprotocol PNode
  (^long submit-tx [node tx-ops tx-opts])
  (^xtdb.api.Xtdb$ExecutedTx execute-tx [node tx-ops tx-opts])
  (^xtdb.api.Xtdb$Connection open-connection [node db-name]))

(defprotocol PStatus
  (latest-submitted-msg-ids [node])
  (latest-completed-txs [node])

  (await-token [node]))

(defprotocol ExecuteOp
  (execute-op! [tx-op conn]))
