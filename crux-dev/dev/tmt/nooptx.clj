(ns tmt.nooptx
  (:require [crux.api :as api]))

(def node (api/start-node {:crux.node/topology :crux.standalone/topology
                           :crux.node/kv-store "crux.kv.memdb/kv"
                           :crux.kv/db-dir "data/db-dir-1"
                           :crux.standalone/event-log-dir "data/eventlog-1"
                           :crux.standalone/event-log-kv-store "crux.kv.memdb/kv"}))

(def old-doc {:crux.db/id :old-doc
              :val "old"})
(def new-doc {:crux.db/id :old-doc
              :val "new"})

(api/submit-tx node [[:crux.tx/put old-doc]])

(api/submit-tx node [[:crux.tx/cas new-doc new-doc]])

(api/q (api/db node) {:find ['e] :where [['e :crux.db/id 'e]] :full-results? true})
