(ns tmt.maplistq
  (:require [crux.api :as api]))

(def opts
  {:crux.node/topology 'crux.standalone/topology
   :crux.node/kv-store 'crux.kv.memdb/kv
   :crux.kv/db-dir "data/db-dir-1"
   :crux.standalone/event-log-dir "data/eventlog-1"
   :crux.standalone/event-log-kv-store 'crux.kv.memdb/kv})

(def node
  (api/start-node opts))

(api/submit-tx
  node
  [[:crux.tx/put
    {:crux.db/id :me
     :change 20
     :list ["carrots" "peas" "shampoo"]
     :pockets/left ["lint" "change"]
     :pockets/right ["phone"]}]
   [:crux.tx/put
    {:crux.db/id :you
     :change 30
     :list ["carrots" "tomatoes" "wig"]
     :pockets/left ["wallet" "watch"]
     :pockets/right ["spectacles"]}]])

(api/q (api/db node) '{:find [e l]
                       :where [[e :list l]]
                       :args [{l "carrots"}]})
;; => #{[:you "carrots"] [:me "carrots"]}

(try (mapv first (api/q (api/db node) '{:find [e p]
                                        :where [[e :pockets/left p]
                                                [e :change c]]
                                        :args [{p "watch"}]
                                        :order-by [[c :asc]]}))
     (catch IllegalArgumentException e
       (re-find #"Order by requires a var from :find\. unreturned var:"
                (.getMessage e))))


;; => #{[:you "watch"]}
