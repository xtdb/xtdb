(ns hooks
  (:require [clj-kondo.hooks-api :as kondo]))

(defn pgwire-def-msg [{{[_def-msg nm & opts] :children} :node}]
  {:node (kondo/list-node
          (list (kondo/token-node 'def)
                nm
                (kondo/map-node opts)))})

(defn sql-def-sql-fns [{{[_def-sql-fn nms min-arity max-arity] :children} :node}]
  {:node (kondo/list-node
          (list (kondo/token-node 'do)
                (kondo/list-node
                 (list (kondo/token-node 'quote) nms))

                (kondo/vector-node (list min-arity max-arity))))})
