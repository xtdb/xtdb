(ns tmt.nil-query
  (:require [crux.api :as api]))

(def node (api/start-node {:crux.node/topology :crux.standalone/topology
                           :crux.node/kv-store "crux.kv.memdb/kv"
                           :crux.kv/db-dir "data/db-dir-1"
                           :crux.standalone/event-log-dir "data/eventlog-1"
                           :crux.standalone/event-log-kv-store "crux.kv.memdb/kv"}))

(api/submit-tx node [[:crux.tx/put {:crux.db/id :this}]])

(api/q (api/db node) {:find ['e] :where [['_ nil 'e]]})

(.close node)
