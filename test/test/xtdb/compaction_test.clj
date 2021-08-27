(ns xtdb.compaction-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.fixtures :as fix]
            [xtdb.fixtures.jdbc :as fj]))

(t/use-fixtures :each fj/with-h2-opts fj/with-jdbc-node)

(t/deftest test-compaction-leaves-replayable-log
  (let [tx (with-open [api (xt/start-node fix/*opts*)]
             (xt/submit-tx api [[:xt/put {:xt/id :foo}]])
             (Thread/sleep 10) ; to avoid two txs at the same ms
             (xt/submit-tx api [[:xt/put {:xt/id :foo}]]))]

    (with-open [api2 (xt/start-node fix/*opts*)]
      (xt/await-tx api2 tx nil)
      (t/is (= 2 (count (xt/entity-history (xt/db api2) :foo :asc)))))))
