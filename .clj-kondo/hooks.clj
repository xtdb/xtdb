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

(defn with-tmp-dirs [{{[_with-tmp-dirs binding-set & body] :children} :node}]
  (let [syms (some-> binding-set kondo/sexpr seq)
        body (or (seq body) [(kondo/token-node nil)])]
    (if (seq syms)
      (let [binding-nodes (mapcat (fn [sym]
                                    [(kondo/token-node sym)
                                     (kondo/token-node nil)])
                                  syms)]
        {:node (kondo/list-node
                (concat [(kondo/token-node 'let)
                         (kondo/vector-node binding-nodes)]
                        body))})
      {:node (kondo/list-node
              (cons (kondo/token-node 'do) body))})))
