(ns tmt.tx-log
  (:require [crux.api :as api]))

(def node (api/start-node {:crux.node/topology :crux.standalone/topology
                           :crux.node/kv-store "crux.kv.memdb/kv"
                           :crux.kv/db-dir "data/db-dir-1"
                           :crux.standalone/event-log-dir "data/eventlog-1"
                           :crux.standalone/event-log-kv-store "crux.kv.memdb/kv"}))

(api/submit-tx node [[:crux.tx/put {:crux.db/id :test :val :doc}]])
(api/submit-tx node [[:crux.tx/evict :test]])
(api/q (api/db node) {:find ['e] :where [['e nil 'e]] :full-results? true})
(api/tx-log node (api/new-tx-log-context node) nil true)

(.close node)
