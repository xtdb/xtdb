(ns hooks
  (:require [clj-kondo.hooks-api :as kondo]))

(defn pgwire-def-msg [{{[_def-msg nm & opts] :children} :node}]
  {:node (kondo/list-node
          (list (kondo/token-node 'def)
                nm
                (kondo/map-node opts)))})
