(ns tmt.server
  (:require [crux.api :as api]
            [crux.http-server :as srv]
            [crux.remote-api-client :as cli]))

(def node-rocks
  (api/start-node {:crux.node/topology :crux.standalone/topology
                   :crux.kv/db-dir "data/db-dir-rocks"
                   :crux.standalone/event-log-dir "data/eventlog-rocks"}))

(def node-mem
  (api/start-node {:crux.node/topology :crux.standalone/topology
                   :crux.node/kv-store "crux.kv.memdb/kv"
                   :crux.kv/db-dir "data/db-dir-mem"
                   :crux.standalone/event-log-kv-store "crux.kv.memdb/kv"
                   :crux.standalone/event-log-dir "data/eventlog-mem"}))

(def srv-rocks
  (srv/start-http-server node-rocks {:server-port 17777}))
(def srv-mem
  (srv/start-http-server node-mem {:server-port 17778}))

(def data [[:crux.tx/put
            {:crux.db/id :me
             :list ["carrots" "peas" "shampoo"]
             :pockets/left ["lint" "change"]
             :pockets/right ["phone"]}]
           [:crux.tx/put
            {:crux.db/id :you
             :list ["carrots" "tomatoes" "wig"]
             :pockets/left ["wallet" "watch"]
             :pockets/right ["spectacles"]}]])

(api/submit-tx
  node-mem data)
(api/submit-tx
  node-rocks data)

(def http-rocks
  (cli/new-api-client "http://localhost:17777"))
(def http-mem
  (cli/new-api-client "http://localhost:17778"))

(defn testq [nd] (api/q (api/db nd) '{:find [e]
                                      :where [[_ :crux.db/id e]]}))

(= (testq http-mem) (testq node-mem)) ; => false sometimes??
(= (testq http-rocks) (testq node-rocks)) ; => true

(.close srv-rocks)
(.close http-rocks)
(.close node-rocks)
(.close srv-mem)
(.close http-mem)
(.close node-mem)
