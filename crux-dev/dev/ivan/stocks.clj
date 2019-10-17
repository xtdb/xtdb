(ns ivan.stocks
  (:require [clojure.test :as t]
            [crux.fixtures :as f]
            [crux.fixtures.api :refer [*api*]]
            [crux.fixtures.kafka :as fk]
            [crux.api :as api]))

(comment
  ; see crux-microbench neighbor for tickers data
  (def node
    (crux.api/start-node
     {:crux.node/topology :crux.standalone/topology
      :crux.node/kv-store "crux.kv.memdb.MemKv"
      :crux.kv/db-dir "data/db-dir-1"
      :crux.standalone/event-log-dir "data/eventlog-1"})))
