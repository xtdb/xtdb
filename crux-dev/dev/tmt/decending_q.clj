(ns tmt.decending-q
  (:require [crux.api :as api]
            [clojure.tools.logging :as log]))

(log/debug "this")

(def node (api/start-node {:crux.node/topology :crux.standalone/topology
                           :crux.node/kv-store "crux.kv.memdb/kv"
                           :crux.kv/db-dir "data/db-dir-1"
                           :crux.standalone/event-log-dir "data/eventlog-1"
                           :crux.standalone/event-log-kv-store "crux.kv.memdb/kv"}))

(api/submit-tx node [[:crux.tx/put
                      {:crux.db/id :paul
                       :weight 70}]
                     [:crux.tx/put
                      {:crux.db/id :tom
                       :age 0
                       :weight 2.5}
                      #inst "1996-05-21T00:00:00.000-00:00"]
                     [:crux.tx/put
                      {:crux.db/id :tom
                       :age 10
                       :weight 38}
                      #inst "2006-05-21T00:00:00.000-00:00"]
                     [:crux.tx/put
                      {:crux.db/id :tom
                       :age 20
                       :weight 65}
                      #inst "2016-05-21T00:00:00.000-00:00"]])

(api/submit-tx node [[:crux.tx/put {:crux.db/id {:this :that :a :b}}]])

(api/q (api/db node) {:find ['e] :where [['e :crux.db/id {:a :b :this :that}]]})

(let [db (api/db node)]
  (api/history-descending db (api/new-snapshot db) :tom))

(defn query-descending
  [node query]
  (let [db (api/db node)
        snapshot (api/new-snapshot db)
        doc-es (into #{} (map first (:where query)))
        doc-ids (map first (api/q db {:find (vec doc-es) :where (:where query)}))
        doc-times (flatten (map #(map :crux.db/valid-time (api/history-descending db snapshot %)) doc-ids))
        query-hist (map (fn [vtime] [vtime (api/q (api/db node vtime) query)]) doc-times)]
    query-hist))

(first (into (sorted-map) {:c :d :a :b}))

(query-descending node
                  {:find ['e 'w] :where [['e :weight 'w]]})

(.close node)
