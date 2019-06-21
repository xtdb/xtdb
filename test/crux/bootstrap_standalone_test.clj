(ns crux.bootstrap-standalone-test
  (:require [clojure.test :as t]
            [crux.api :as api]))

(t/deftest test-api-standalone-system
  (t/testing "System starts with just :kv-backend, :event-log-dir and :db-dir"
    (t/is (api/start-standalone-system
              {:kv-backend "crux.kv.memdb.MemKv"
               :event-log-dir  "data/evt-dir-1"
               :db-dir "data/db-dir-1"})))
  (t/testing "System starts with just kv-backend and db-dir"
    (t/is (api/start-standalone-system
            {:kv-backend "crux.kv.memdb.MemKv"
             :db-dir "data/db-dir-1"}))))

; (t/run-tests 'crux.standalone-bootstrap-test)
