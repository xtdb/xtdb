(ns tmt.server
  (:require [crux.api :as api]
            [crux.http-server :as srv]))

(def opts
  {:crux.node/topology :crux.standalone/topology
   :crux.node/kv-store "crux.kv.memdb/kv"
   :crux.kv/db-dir "data/db-dir-1"
   :crux.standalone/event-log-dir "data/eventlog-1"
   :crux.standalone/event-log-kv-store "crux.kv.memdb/kv"})

(def simple-node
  (api/start-node opts))

#_(def srv
  (srv/start-http-server simple-node {:server-port 80008}))

(api/submit-tx
    node
    [[:crux.tx/put
      {:crux.db/id :me
       :list ["carrots" "peas" "shampoo"]
       :pockets/left ["lint" "change"]
       :pockets/right ["phone"]}]
     [:crux.tx/put
      {:crux.db/id :you
       :list ["carrots" "tomatoes" "wig"]
       :pockets/left ["wallet" "watch"]
       :pockets/right ["spectacles"]}]])
