(ns xtdb.metrics-test
  (:require [clojure.test :as t]
            [next.jdbc :as jdbc]
            [xtdb.api :as xt]
            [xtdb.test-util :as tu]
            [xtdb.types])
  (:import io.micrometer.core.instrument.composite.CompositeMeterRegistry
           io.micrometer.core.instrument.Counter))

(t/use-fixtures :each tu/with-mock-clock tu/with-node tu/with-simple-registry)

(t/deftest test-query-error-and-warning-counter
  (let [registry ^CompositeMeterRegistry (tu/component tu/*node* :xtdb.metrics/registry)]
    (t/is (thrown? Exception (xt/q tu/*node* "SLECT 1"))
          "parsing error via the node")
    (t/is (thrown? Exception (jdbc/execute! tu/*conn* ["SLECT 1"]))
          "parsing error via pgwire")
    (t/is (thrown? Exception (xt/q tu/*node* "SELECT 1/0"))
          "runtime error via the node")
    (t/is (thrown? Exception (jdbc/execute! tu/*conn* ["SELECT 1/0"]))
          "runtime error via pgwire")

    ;; producing some unknown column/table warnings
    (xt/q tu/*node* "SELECT foo FROM bar")
    (jdbc/execute! tu/*conn* ["SELECT foo FROM bar"])

    (t/is (= 4.0 (.count ^Counter (.counter (.find registry "query.error")))))
    (t/is (= 2.0 (.count ^Counter (.counter (.find registry "query.warning")))))))

(t/deftest test-transaction-exception-counter
  (let [registry ^CompositeMeterRegistry (tu/component tu/*node* :xtdb.metrics/registry)]
    (t/is (thrown? Exception (jdbc/execute! tu/*conn* ["INSERT INTO foo (a) VALUES (42)"]))
          "presubmit error via pgwire")

    (t/testing "producing errors on transaction commit"
      (jdbc/execute! tu/*conn* ["START TRANSACTION"])
      (jdbc/execute! tu/*conn* ["INSERT INTO foo (a) VALUES (42)"])
      (t/is (thrown? Exception (jdbc/execute! tu/*conn* ["COMMIT"]))))

    (t/is (thrown? Exception (xt/execute-tx tu/*node* ["INSERT INTO foo (a) VALUES (42)"]))
          "presubmit error via the node")
    (t/is (thrown? Exception (jdbc/execute! tu/*conn* ["INSERT INTO docs SELECT 1/0 AS _id"]))
          "runtime error in the indexer")

    (t/is (= 0.0 (.count ^Counter (.counter (.find registry "query.error")))))
    (t/is (= 0.0 (.count ^Counter (.counter (.find registry "query.warning")))))
    (t/is (= 4.0 (.count ^Counter (.counter (.find registry "tx.error")))))))

(t/deftest test-transaction-exception-counter-on-submit-tx
  (let [registry ^CompositeMeterRegistry (tu/component tu/*node* :xtdb.metrics/registry)]
    (t/is (thrown? Exception (xt/submit-tx tu/*node* ["INSERT INTO foo (a) VALUES (42)"]))
          "presubmit error via the node (submit-tx)")

    (t/testing "async errors in the indexer"
      (xt/submit-tx tu/*node* ["INSERT foo"])
      (Thread/sleep 200))

    (t/is (= 2.0 (.count ^Counter (.counter (.find registry "tx.error")))))))
