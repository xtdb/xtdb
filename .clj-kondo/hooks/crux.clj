(ns hooks.crux
  (:require [clj-kondo.hooks-api :as api]))

(defn with-tmp-dirs [{{[_ dir-set & body] :children} :node}]
  ;; not strictly the same as the macro, but enough for Kondo
  {:node (api/list-node
          (list* (api/token-node 'let)
                 (api/vector-node (->> (mapcat (fn [dir]
                                                 [dir (api/keyword-node :dir)])
                                               (:children dir-set))))
                 body))})
