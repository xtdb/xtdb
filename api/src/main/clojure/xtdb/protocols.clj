(ns ^{:clojure.tools.namespace.repl/load false, :clojure.tools.namespace.repl/unload false}
    xtdb.protocols)

(defprotocol PNode
  (^xtdb.api.Xtdb$Connection open-connection [node db-name]))

(defprotocol ExecuteOp
  (execute-op! [tx-op conn]))
