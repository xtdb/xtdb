(ns xtdb.metrics-test
  (:require [clojure.test :as t]
            [next.jdbc :as jdbc]
            [xtdb.api :as xt]
            [xtdb.node :as xtn]
            [xtdb.test-util :as tu]
            [xtdb.types]
            [xtdb.util :as util])
  (:import (io.micrometer.core.instrument Counter Gauge)
           io.micrometer.core.instrument.MeterRegistry))

(t/use-fixtures :each tu/with-mock-clock)

(t/deftest test-error-and-warning-counter
  (let [node (xtn/start-node tu/*node-opts*)
        conn (jdbc/get-connection node)
        ^MeterRegistry registry (util/component node :xtdb.metrics/registry)]

    ;; it's only the runtime errors that increment the error counter
    (t/is (thrown? Exception (xt/q node "SLECT 1"))
          "parsing error via the node")

    (t/is (thrown? Exception (jdbc/execute! conn ["SLECT 1"]))
          "parsing error via pgwire")

    (t/is (thrown? Exception (xt/q node "SELECT 1/0"))
          "runtime error via the node")

    (t/is (thrown? Exception (jdbc/execute! conn ["SELECT 1/0"]))
          "runtime error via pgwire")

    ;; producing some unknown column/table warnings
    (xt/q node "SELECT foo FROM bar")
    (jdbc/execute! conn ["SELECT foo FROM bar"])

    (t/is (= 2.0 (.count ^Counter (.counter (.find registry "query.error")))))
    (t/is (= 2.0 (.count ^Counter (.counter (.find registry "query.warning")))))))

(t/deftest test-transaction-exception-counter
  (let [node (xtn/start-node tu/*node-opts*)
        conn (jdbc/get-connection node)
        ^MeterRegistry registry (util/component node :xtdb.metrics/registry)]
    (t/is (thrown? Exception (jdbc/execute! conn ["INSERT INTO foo (a) VALUES (42)"]))
          "presubmit error via pgwire")

    (t/testing "producing errors on transaction commit"
      (jdbc/execute! conn ["START TRANSACTION"])
      (jdbc/execute! conn ["INSERT INTO foo (a) VALUES (42)"])
      (t/is (thrown? Exception (jdbc/execute! conn ["COMMIT"]))))

    (t/is (thrown? Exception (xt/execute-tx node ["INSERT INTO foo (a) VALUES (42)"]))
          "presubmit error via the node")
    (t/is (thrown? Exception (jdbc/execute! conn ["INSERT INTO docs SELECT 1/0 AS _id"]))
          "runtime error in the indexer")

    (t/is (= 0.0 (.count ^Counter (.counter (.find registry "query.error")))))
    (t/is (= 0.0 (.count ^Counter (.counter (.find registry "query.warning")))))
    (t/is (= 4.0 (.count ^Counter (.counter (.find registry "tx.error")))))))

(t/deftest test-transaction-exception-counter-on-submit-tx
  (let [node (xtn/start-node tu/*node-opts*)
        ^MeterRegistry registry (util/component node :xtdb.metrics/registry)]
    (t/is (anomalous? [:incorrect :missing-id] (xt/submit-tx node ["INSERT INTO foo (a) VALUES (42)"]))
          "presubmit error via the node (submit-tx)")

    (t/is (= 1.0 (.count ^Counter (.counter (.find registry "tx.error")))))

    (t/testing "async errors in the indexer"
      (t/is (anomalous? [:incorrect :xtdb/sql-error
                         "missing 'INTO'"]
                        (xt/submit-tx node ["INSERT foo"])))

      (t/is (= 2.0 (.count ^Counter (.counter (.find registry "tx.error"))))))))

(t/deftest test-total-and-active-connections
  (let [node (xtn/start-node tu/*node-opts*)
        ^MeterRegistry registry (util/component node :xtdb.metrics/registry)]

    (with-open [conn1 (jdbc/get-connection node)]
      (jdbc/execute! conn1 ["SELECT 1"]))

    (with-open [conn2 (jdbc/get-connection node)]
      ;; We have a connection open so this should be equal to 1.0
      (t/is (= 1.0 (.value ^Gauge (.gauge (.find registry "pgwire.active_connections")))))
      (t/is (= 2.0 (.count ^Counter (.counter (.find registry "pgwire.total_connections"))))))))
