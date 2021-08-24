(ns crux.compaction-test
  (:require [clojure.test :as t]
            [crux.api :as crux]
            [crux.fixtures :as fix]
            [crux.fixtures.jdbc :as fj]))

(t/use-fixtures :each fj/with-h2-opts fj/with-jdbc-node)

(t/deftest test-compaction-leaves-replayable-log
  (let [tx (with-open [api (crux/start-node fix/*opts*)]
             (crux/submit-tx api [[:crux.tx/put {:xt/id :foo}]])
             (Thread/sleep 10) ; to avoid two txs at the same ms
             (crux/submit-tx api [[:crux.tx/put {:xt/id :foo}]]))]

    (with-open [api2 (crux/start-node fix/*opts*)]
      (crux/await-tx api2 tx nil)
      (t/is (= 2 (count (crux/entity-history (crux/db api2) :foo :asc)))))))
