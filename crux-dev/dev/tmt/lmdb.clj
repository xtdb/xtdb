(ns tmt.lmdb
  (:require [crux.api :as api]))

(def node (api/start-node {:crux.node/topology 'crux.standalone/topology
                           :crux.node/kv-store 'crux.kv.lmdb/kv
                           :crux.kv/db-dir "data/db-dir-1"
                           :crux.standalone/event-log-dir "data/eventlog-1"
                           :crux.standalone/event-log-kv-store 'crux.kv.lmdb/kv}))

(api/submit-tx node [[:crux.tx/put {:crux.db/id :this
                                    :val :that}]])

(api/q (api/db node) {:find ['e] :where [['e nil 'e]] :full-results? true})
