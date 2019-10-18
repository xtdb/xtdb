(ns ivan.http-server
  "Barbarous bootstrap for http server"
  (:require [crux.api :as api]
            [crux.http-server :as http-server]))


(def opts
  {:crux.node/topology :crux.standalone/topology
   :crux.node/kv-store "crux.kv.rocksdb/kv"
   :crux.standalone/event-log-dir "data/eventlog-1"
   :crux.kv/db-dir "data/db-dir-1"})

(def simple-node
  (api/start-node opts))

(def srv
  (http-server/start-http-server simple-node))

(api/submit-tx
  simple-node
  [[:crux.tx/put
    {:crux.db/id :ids/bird}
    #inst "2017-06-09T06:59:40.829-04:00"]
   [:crux.tx/put
    {:crux.db/id :ids/bird}
    #inst "2017-06-09T06:59:40.829-02:00"]])

(api/history-range
  simple-node
  :ids/bird
  nil nil nil nil)

(api/q (api/db simple-node #inst "2017-06-09T06:59:40.829-01:00")
       '{:find [e] :where [[e :crux.db/id :ids/bird]]})

(api/document simple-node "686c3e1f00fb8ccabd43e93f5cd2da546d50d80d")

(api/q (api/db simple-node)
       '{:find [e]
         :where
         [[e :crux.db/id _]]})

(api/documents
  simple-node
  #{"686c3e1f00fb8ccabd43e93f5cd2da546d50d80d"
    "773d1c878c512d5d50bb1e74e46d4e5e315046de"})
