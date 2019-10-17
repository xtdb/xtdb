(require '[crux.api :as crux])

(def n
  (crux/start-node
   {:crux.node/topology :crux.standalone/topology
    :crux.node/kv-store "crux.kv.rocksdb.RocksKv"
    :crux.standalone/event-log-dir "data/eventlog-2"
    :crux.kv/db-dir "data/db-dir-2"}))

(crux/submit-tx n
  [[:crux.tx/put
    {:crux.db/id :ids/leno}]
   [:crux.tx/put
    {:crux.db/id :ids/deni}]
   [:crux.tx/put
    {:crux.db/id :ids/reni}]])

(crux/q (crux/db n)
       '[:find e
         :where
         [e :crux.db/id]
         [(contains? ids e)]
         :args {ids #{:ids/leno :ids/reni}}
         :limit 10])

(crux/q (crux/db n)
       '{:find [e]
         :args [{id [in #{:ids/leno}]}]
         :in [id]
         :where [[e :crux.db/id id]]
         :limit 1})
