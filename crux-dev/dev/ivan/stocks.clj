(ns ivan.stocks
  (:require [clojure.test :as t]
            [crux.fixtures :as f]
            [crux.fixtures.api :refer [*api*]]
            [crux.fixtures.kafka :as fk]
            [crux.api :as api]))

(comment

  ; see crux-microbench neighbor for tickers data

  (def node
    (api/start-node
     {:crux.node/topology :crux.standalone/topology
      :kv-backend "crux.kv.memdb.MemKv"
      :db-dir     "data/db-dir-1"
      :event-log-dir "data/eventlog-1"})))
