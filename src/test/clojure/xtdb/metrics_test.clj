(ns xtdb.metrics-test
  (:require [clojure.test :as t]
            [next.jdbc :as jdbc]
            [xtdb.api :as xt]
            [xtdb.log :as xt-log]
            [xtdb.test-util :as tu]
            [xtdb.types])
  (:import io.micrometer.core.instrument.composite.CompositeMeterRegistry
           io.micrometer.core.instrument.Counter
           java.util.concurrent.ExecutionException))

(t/use-fixtures :each tu/with-mock-clock tu/with-node tu/with-simple-registry)

(t/deftest test-query-error-and-warning-counter
  (let [registry ^CompositeMeterRegistry (tu/component tu/*node* :xtdb.metrics/registry)]
    (t/is (thrown? Exception (xt/q tu/*node* "SLECT 1")))
    (t/is (thrown? Exception (jdbc/execute! tu/*conn* ["SLECT 1"])))
    (t/is (thrown? Exception (xt/q tu/*node* "SELECT 1/0")))
    (t/is (thrown? Exception (jdbc/execute! tu/*conn* ["SELECT 1/0"])))

    (xt/q tu/*node* "SELECT foo FROM bar")
    (jdbc/execute! tu/*conn* ["SELECT foo FROM bar"])

    (t/is (= 4.0 (.count ^Counter (.counter (.find registry "query.error")))))

    (t/is (= 2.0 (.count ^Counter (.counter (.find registry "query.warning")))))))

(t/deftest test-transaction-exception-counter
  (let [registry ^CompositeMeterRegistry (tu/component tu/*node* :xtdb.metrics/registry)]
    (t/is (thrown? Exception (jdbc/execute! tu/*conn* ["INSERT INTO foo (a) VALUES (42)"])))

    (t/testing "DML enabled with BEGIN READ WRITE"
      (jdbc/execute! tu/*conn* ["START TRANSACTION"])
      (jdbc/execute! tu/*conn* ["INSERT INTO foo (a) VALUES (42)"])
      (t/is (thrown? Exception (jdbc/execute! tu/*conn* ["COMMIT"]))))

    (t/is (thrown? Exception (xt/submit-tx tu/*node* ["INSERT INTO foo (a) VALUES (42)"])) "presubmit error")
    (t/is (thrown? Exception (xt/execute-tx tu/*node* ["INSERT INTO foo (a) VALUES (42)"])))
    (t/is (thrown? Exception (jdbc/execute! tu/*conn* ["INSERT INTO docs SELECT 1/0 AS _id"])))

    (t/is (= 0.0 (.count ^Counter (.counter (.find registry "query.error")))))
    (t/is (= 0.0 (.count ^Counter (.counter (.find registry "query.warning")))))
    (t/is (= 5.0 (.count ^Counter (.counter (.find registry "tx.error")))))))

(t/deftest test-transaction-exception-counter-on-submit-tx
  (let [registry ^CompositeMeterRegistry (tu/component tu/*node* :xtdb.metrics/registry)]
    (t/is (thrown? Exception (jdbc/execute! tu/*conn* ["INSERT INTO foo (a) VALUES (42)"])))
    (t/is (thrown? Exception (xt/submit-tx tu/*node* ["INSERT INTO foo (a) VALUES (42)"])))

    (t/testing "counts async errors in indexer"
      (xt/submit-tx tu/*node* ["INSERT foo"])
      (Thread/sleep 200))

    (t/is (= 3.0 (.count ^Counter (.counter (.find registry "tx.error")))))))
