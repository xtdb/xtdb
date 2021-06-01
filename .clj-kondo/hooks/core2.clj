(ns hooks.core2
  (:require [clj-kondo.hooks-api :as api]))

(defn with-db [{{[_ binding-vec & body] :children} :node}]
  (let [{[db-binding node db-opts] :children} binding-vec]
    ;; not strictly the same as the macro, but enough for Kondo
    {:node (api/list-node
            (list* (api/token-node 'let)
                   (api/vector-node [db-binding (api/keyword-node :db)])
                   node
                   db-opts
                   body))}))
