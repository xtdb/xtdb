(ns tmt.graph
  (:require [crux.api :as api]))

(def opts
  {:crux.node/topology :crux.standalone/topology
   :crux.node/kv-store "crux.kv.memdb/kv"
   :crux.kv/db-dir "data/db-dir-1"
   :crux.standalone/event-log-dir "data/eventlog-1"
   :crux.standalone/event-log-kv-store "crux.kv.memdb/kv"})

(def node
  (api/start-node opts))

;; A -> C true
;; C -> A false
(api/submit-tx
  node
  [[:crux.tx/put
    {:crux.db/id :A
     :edges [:B]}]
   [:crux.tx/put
    {:crux.db/id :B
     :edges [:C]}]
   [:crux.tx/put
    {:crux.db/id :C
     :edges [:A]}]])

(api/q (api/db node) {:find ['start 'end]
                      :where [['start :edges 'mid]
                              ['mid :edges 'end]]
                      :args [{'start :A}{'start :B}{'start :C}]})
