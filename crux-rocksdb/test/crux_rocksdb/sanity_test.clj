(ns crux-rocksdb.sanity-test
  (:require [clojure.test :as t]
            [crux.api :as api]))

(t/deftest test-sanity-check-setup
  (t/testing "things are linked properly and RocksDB can be used as storage"
    (let [node
            (api/start-standalone-node
              {:db-dir        "console-data"
               :event-log-dir "console-data-log"
               :kv-backend    "crux.kv.rocksdb.RocksKv"})]
      (t/is (api/q node '{:find e :where [[e :crux.db/id]]})))))
