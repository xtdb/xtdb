(ns xtdb.metrics-test
  (:require [clojure.test :as t]
            [next.jdbc :as jdbc]
            [xtdb.api :as xt]
            [xtdb.compactor :as c]
            [xtdb.db-catalog :as db]
            [xtdb.node :as xtn]
            [xtdb.test-util :as tu]
            [xtdb.trie-catalog :as cat]
            [xtdb.types]
            [xtdb.util :as util])
  (:import (io.micrometer.core.instrument Counter Gauge Timer)
           (java.time Duration)))

(t/use-fixtures :each tu/with-mock-clock)

(t/deftest test-error-and-warning-counter
  (let [node (xtn/start-node tu/*node-opts*)
        conn (jdbc/get-connection node)
        registry (.getMeterRegistry (util/node-base node))]

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
        registry (.getMeterRegistry (util/node-base node))]
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
    (t/is (= 5.0 (.count ^Counter (.counter (.find registry "tx.error")))))))

(t/deftest test-transaction-exception-counter-on-submit-tx
  (let [node (xtn/start-node tu/*node-opts*)
        registry (.getMeterRegistry (util/node-base node))]
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
        registry (.getMeterRegistry (util/node-base node))]

    (with-open [conn1 (jdbc/get-connection node)]
      (jdbc/execute! conn1 ["SELECT 1"]))

    (with-open [conn2 (jdbc/get-connection node)]
      ;; We have a connection open so this should be equal to 1.0
      (t/is (= 1.0 (.value ^Gauge (.gauge (.find registry "pgwire.active_connections")))))
      (t/is (= 2.0 (.count ^Counter (.counter (.find registry "pgwire.total_connections"))))))))

(t/deftest test-per-database-metrics
  (let [node-dir (util/->path "target/metrics/multi-db")
        find-db-tagged-metric (fn [registry metric-name db-name]
                                (.gauge (.tag (.find registry metric-name) "db" db-name)))]
    (util/delete-dir node-dir)
    (util/delete-dir (util/->path "target/metrics/multi-db-new"))

    (with-open [node (tu/->local-node {:node-dir node-dir, :compactor-threads 0})]
      (let [registry (.getMeterRegistry (util/node-base node))]

        (prn (.find registry "node.tx.latestCompletedTxId"))
        (t/testing "xtdb database has tagged gauges"
          (t/is (some? (find-db-tagged-metric registry "node.tx.latestCompletedTxId" "xtdb")))
          (t/is (some? (find-db-tagged-metric registry "node.tx.latestSubmittedMsgId" "xtdb")))
          (t/is (some? (find-db-tagged-metric registry "node.tx.latestProcessedMsgId" "xtdb")))
          (t/is (some? (find-db-tagged-metric registry "node.tx.lag.MsgId" "xtdb"))))

        (t/testing "no gauges for non-existent db"
          (t/is (nil? (find-db-tagged-metric registry "node.tx.latestCompletedTxId" "new_db"))))

        (jdbc/execute! node ["
ATTACH DATABASE new_db WITH $$
  log: !Local
    path: 'target/metrics/multi-db-new/log'
  storage: !Local
    path: 'target/metrics/multi-db-new/storage'
$$"])

        (t/testing "attached database gets its own tagged gauges"
          (t/is (some? (find-db-tagged-metric registry "node.tx.latestCompletedTxId" "new_db")))
          (t/is (some? (find-db-tagged-metric registry "node.tx.latestSubmittedMsgId" "new_db")))
          (t/is (some? (find-db-tagged-metric registry "node.tx.latestProcessedMsgId" "new_db")))
          (t/is (some? (find-db-tagged-metric registry "node.tx.lag.MsgId" "new_db"))))

        (t/testing "xtdb gauges still present"
          (t/is (some? (find-db-tagged-metric registry "node.tx.lag.MsgId" "xtdb"))))))))

(t/deftest test-query-timer
  (let [node (xtn/start-node tu/*node-opts*)
        registry (.getMeterRegistry (util/node-base node))]

    (with-open [conn (jdbc/get-connection node)]
      (t/testing "normal pgwire queries should be added to timer"
        (jdbc/execute! conn ["SELECT 1"])
        (jdbc/execute! conn ["SELECT COUNT(*) FROM generate_series(1, 100) AS t(x)"])
        (let [^Timer timer (.timer (.find registry "query.timer"))]
          (t/is (= (.count timer) 2))
          (t/is (> (.totalTime timer java.util.concurrent.TimeUnit/NANOSECONDS) 0))))

      (t/testing "runtime pgwire error queries should be added to timer"
        (t/is (thrown? Exception (jdbc/execute! conn ["SELECT 1/0"])))
        (let [^Timer timer (.timer (.find registry "query.timer"))]
          (t/is (= (.count timer) 3)))))

    (t/testing "queries via the node should be added to timer"
      (xt/q node "SELECT 1")
      (let [^Timer timer (.timer (.find registry "query.timer"))]
        (t/is (= (.count timer) 4))))))

(t/deftest test-pgwire-tx-latency-timer
  (let [node (xtn/start-node tu/*node-opts*)
        registry (.getMeterRegistry (util/node-base node))]

    (with-open [conn (jdbc/get-connection node)]
      (t/testing "synchronous transactions via pgwire are recorded in pgwire.tx.latency timer"
        (jdbc/execute! conn ["INSERT INTO foo (_id, a) VALUES (1, 42)"])
        (let [^Timer timer (.timer (.find registry "pgwire.tx.latency"))]
          (t/is (= (.count timer) 1))
          (t/is (> (.totalTime timer java.util.concurrent.TimeUnit/NANOSECONDS) 0))))

      (t/testing "explicit transactions are recorded on commit"
        (jdbc/execute! conn ["START TRANSACTION"])
        (jdbc/execute! conn ["INSERT INTO foo (_id, a) VALUES (2, 43)"])
        (jdbc/execute! conn ["COMMIT"])
        (let [^Timer timer (.timer (.find registry "pgwire.tx.latency"))]
          (t/is (= (.count timer) 2))))

      (t/testing "failed transactions are also recorded"
        (t/is (thrown? Exception (jdbc/execute! conn ["INSERT INTO foo (a) VALUES (42)"])))
        (let [^Timer timer (.timer (.find registry "pgwire.tx.latency"))]
          (t/is (= (.count timer) 3)))))))

(t/deftest test-pgwire-tx-submit-and-execute-timers
  (let [node (xtn/start-node tu/*node-opts*)
        registry (.getMeterRegistry (util/node-base node))
        timer-count #(.count ^Timer (.timer (.find registry %)))]

    (with-open [conn (jdbc/get-connection node)]
      (jdbc/execute! conn ["INSERT INTO foo (_id, a) VALUES (1, 42)"])
      (t/is (= 1 (timer-count "pgwire.tx.execute")) "sync commit bumps execute timer")
      (t/is (= 0 (timer-count "pgwire.tx.submit")) "sync commit does not bump submit timer")

      (jdbc/execute! conn ["BEGIN READ WRITE WITH (ASYNC = TRUE)"])
      (jdbc/execute! conn ["INSERT INTO foo (_id, a) VALUES (2, 43)"])
      (jdbc/execute! conn ["COMMIT"])
      (t/is (= 1 (timer-count "pgwire.tx.submit")) "async commit bumps submit timer")
      (t/is (= 1 (timer-count "pgwire.tx.execute")) "async commit does not bump execute timer"))))

(t/deftest test-tx-await-timer
  (let [node (xtn/start-node tu/*node-opts*)
        registry (.getMeterRegistry (util/node-base node))
        timer-count #(.count ^Timer (.timer (.find registry "node.tx.await")))]

    (with-open [conn (jdbc/get-connection node)]
      (jdbc/execute! conn ["INSERT INTO foo (_id, a) VALUES (1, 42)"])
      (t/is (= 1 (timer-count)) "sync commit records the indexer-await time")

      (jdbc/execute! conn ["BEGIN READ WRITE WITH (ASYNC = TRUE)"])
      (jdbc/execute! conn ["INSERT INTO foo (_id, a) VALUES (2, 43)"])
      (jdbc/execute! conn ["COMMIT"])
      (t/is (= 1 (timer-count)) "async commit does not bump the await timer"))))

(t/deftest test-block-uploaded-timer
  (let [node (xtn/start-node tu/*node-opts*)
        registry (.getMeterRegistry (util/node-base node))]
    (t/testing "blocks uploaded to storage are recorded in block.upload.timer"
      (xt/submit-tx node [[:put-docs :foo {:xt/id 1}]])
      (tu/flush-block! node)
      (let [^Timer timer (.timer (.find registry "block.upload.timer"))]
        (t/is (= (.count timer) 1))
        (t/is (> (.totalTime timer java.util.concurrent.TimeUnit/NANOSECONDS) 0))))))

(t/deftest test-gc-metrics
  (binding [c/*ignore-signal-block?* true
            ;; Drive L1 supersession: each new L1 trie marks earlier same-recency L1 tries as
            ;; garbage when their data-file-size < `*file-size-target*`. A small target keeps
            ;; the test scoped — without this trie GC has no eligible tries to delete.
            cat/*file-size-target* 16]
    (let [node-dir (util/->path "target/metrics-test/test-gc-metrics")
          clock (tu/->mock-clock (tu/->instants :hour))
          opts {:node-dir node-dir, :compactor-threads 1, :instant-src clock
                :gc? false, :blocks-to-keep 2, :garbage-lifetime (Duration/ofHours 0)
                :instant-source-for-non-tx-msgs? true}]
      (util/delete-dir node-dir)

      (with-open [node (tu/->local-node opts)]
        (let [primary (db/primary-db node)
              registry (.getMeterRegistry (util/node-base node))]

          (doseq [i (range 6)]
            (xt/execute-tx node [[:put-docs :foo {:xt/id i}]])
            (tu/flush-block! node)
            (c/compact-all! node #xt/duration "PT1S"))

          (.gcAll primary)

          (t/testing "block GC meters"
            (t/is (= 4 (.count ^Timer (.timer (.tag (.find registry "xtdb.gc.block_files.delete.timer") "db" "xtdb")))))
            ;; Deletes table blocks for foo and xt$txs (4 each)
            (t/is (= 8 (.count ^Timer (.timer (.tag (.find registry "xtdb.gc.table_block_files.delete.timer") "db" "xtdb"))))))

          (t/testing "trie GC meters"
            (t/is (= 8 (.count ^Timer (.timer (.tag (.find registry "xtdb.gc.tries.delete.timer") "db" "xtdb")))))))))))