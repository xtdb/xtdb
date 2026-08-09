(ns xtdb.api-flight-sql-test
  "Exercises `xtdb.api` over a FlightSQL connection — the same cases as `xtdb.api-adbc-test`, but
  driven over the wire."
  (:require [clojure.test :as t :refer [deftest]]
            [xtdb.api :as xt]
            [xtdb.flight-sql :as fsql]
            [xtdb.serde :as serde]
            [xtdb.test-util :as tu :refer [*node*]]
            [xtdb.time :as time])
  (:import (java.time ZoneId)
           (xtdb.api Xtdb)))

(t/use-fixtures :each tu/with-mock-clock tu/with-node)

(defn- open-conn ^java.io.Closeable []
  (fsql/open-conn {:port (.getFlightSqlPort ^Xtdb *node*)}))

(deftest execute-tx-and-query
  (with-open [conn (open-conn)]
    (t/is (= (serde/->TxKey 0 (time/->instant #inst "2020-01-01"))
             (xt/execute-tx conn [[:put-docs :docs {:xt/id :foo, :n 1}]]))
          "execute-tx over a FlightSQL connection returns the tx-key")

    (t/is (= [{:xt/id :foo, :n 1}]
             (xt/q conn "SELECT _id, n FROM docs"))
          "the connection reads its own write (read-your-writes)")

    (t/is (= {:tx-id 1} (xt/submit-tx conn [[:put-docs :docs {:xt/id :bar, :n 2}]]))
          "submit-tx returns the tx-id")

    (t/is (= #{{:xt/id :foo, :n 1} {:xt/id :bar, :n 2}}
             (set (xt/q conn "SELECT _id, n FROM docs"))))))

(deftest client-op-conversions
  (with-open [conn (open-conn)]
    (xt/execute-tx conn [[:put-docs :docs {:xt/id "a", :n 1}]
                         [:put-docs :docs {:xt/id "b", :n 2}]
                         [:put-docs :docs {:xt/id "c", :n 3}]])

    (xt/execute-tx conn [[:patch-docs :docs {:xt/id "a", :n 10}]
                         [:delete-docs :docs "b"]
                         [:sql "INSERT INTO docs (_id, n) VALUES ('d', ?)" [4]]])

    (t/is (= #{{:xt/id "a", :n 10} {:xt/id "c", :n 3} {:xt/id "d", :n 4}}
             (set (xt/q conn "SELECT _id, n FROM docs")))
          "patch updated a, delete removed b, raw sql inserted d")

    (xt/execute-tx conn [[:erase-docs :docs "a"]])
    (t/is (= #{{:xt/id "c", :n 3} {:xt/id "d", :n 4}}
             (set (xt/q conn "SELECT _id, n FROM docs")))
          "erase removed a")))

(deftest multi-op-tx-is-atomic
  (with-open [conn (open-conn)]
    (xt/execute-tx conn [[:put-docs :docs {:xt/id :seed, :n 0}]])

    (t/is (anomalous? [:conflict nil #"Assert failed"]
                      (xt/execute-tx conn [[:put-docs :docs {:xt/id :a, :n 1}]
                                           [:sql "ASSERT (SELECT COUNT(*) FROM docs) = 0"]]))
          "the failed assert surfaces from execute-tx")

    (t/is (= [{:xt/id :seed}] (xt/q conn "SELECT _id FROM docs"))
          "and the put in the same tx didn't land - the ops are one transaction")))

(deftest multi-row-batches
  (with-open [conn (open-conn)]
    (xt/execute-tx conn [[:sql "INSERT INTO docs (_id, n) VALUES (?, ?)" [1 10] [2 20] [3 30]]])
    (t/is (= #{{:xt/id 1, :n 10} {:xt/id 2, :n 20} {:xt/id 3, :n 30}}
             (set (xt/q conn "SELECT _id, n FROM docs")))
          "a batched :sql insert applies every arg-row")

    (xt/execute-tx conn [[:delete-docs :docs 1 2]])
    (t/is (= #{{:xt/id 3, :n 30}}
             (set (xt/q conn "SELECT _id, n FROM docs")))
          "delete-docs with several ids removes them all")))

(deftest put-docs-valid-time
  (with-open [conn (open-conn)]
    (xt/execute-tx conn [[:put-docs {:into :docs, :valid-from #inst "2018-01-01", :valid-to #inst "2019-01-01"}
                          {:xt/id :foo, :n 1}]])

    (t/is (= [] (xt/q conn "SELECT _id FROM docs"))
          "the doc's valid-time window has closed by the current time")

    (t/is (= [{:xt/id :foo, :n 1}]
             (xt/q conn "SELECT _id, n FROM docs FOR VALID_TIME AS OF DATE '2018-06-01'"))
          "and it's there when we look inside the window")))

(deftest query-args-and-plan-q
  (with-open [conn (open-conn)]
    (xt/execute-tx conn [[:put-docs :docs {:xt/id :foo, :n 1}]
                         [:put-docs :docs {:xt/id :bar, :n 2}]])

    (t/is (= [{:xt/id :foo, :n 1}]
             (xt/q conn ["SELECT _id, n FROM docs WHERE _id = ?" :foo]))
          "positional args bind over the wire")

    (t/is (= [1 2]
             (sort (into [] (map :n) (xt/plan-q conn "SELECT n FROM docs"))))
          "plan-q streams rows over the wire")

    (t/is (= [{"_id" :foo, "n" 1}]
             (xt/q conn ["SELECT _id, n FROM docs WHERE _id = ?" :foo] {:key-fn :snake-case-string}))
          ":key-fn is honoured")))

(deftest query-basis-opts
  (with-open [conn (open-conn)]
    (xt/execute-tx conn [[:put-docs :docs {:xt/id :foo, :n 1}]])
    (let [snapshot-token (:tok (first (xt/q conn "SELECT SNAPSHOT_TOKEN tok")))]
      (xt/execute-tx conn [[:put-docs :docs {:xt/id :bar, :n 2}]])

      (t/is (= [{:xt/id :foo, :n 1}]
               (xt/q conn "SELECT _id, n FROM docs" {:snapshot-token snapshot-token}))
            "a snapshot-token pins the read basis to before the second tx"))

    (t/is (= [{:ts (.atZone (time/->instant #inst "2023-06-01") (ZoneId/of "Europe/London"))}]
             (xt/q conn "SELECT CURRENT_TIMESTAMP AS ts"
                   {:current-time #inst "2023-06-01", :default-tz (ZoneId/of "Europe/London")}))
          ":current-time and :default-tz both reach the query")))

(deftest tx-opts
  (with-open [conn (open-conn)]
    (t/is (= (serde/->TxKey 0 (time/->instant #inst "2021-06-01"))
             (xt/execute-tx conn [[:put-docs :docs {:xt/id :foo}]] {:system-time #inst "2021-06-01"}))
          ":system-time is honoured")

    (t/is (anomalous? [:unsupported :xtdb.flight-sql/metadata-unsupported]
                      (xt/execute-tx conn [[:put-docs :docs {:xt/id :bar}]] {:metadata {:foo "bar"}}))
          ":metadata has no FlightSQL rendering, so it's rejected rather than dropped")))

(deftest submit-tx-then-query-sees-write
  (with-open [conn (open-conn)]
    (xt/submit-tx conn [[:put-docs :docs {:xt/id :foo, :n 1}]])
    (t/is (= [{:xt/id :foo, :n 1}]
             (xt/q conn "SELECT _id, n FROM docs"))
          "an async submit-tx is awaited by a subsequent read on the same connection")))

(deftest connection-rejects-database-opt
  (with-open [conn (open-conn)]
    (t/is (anomalous? [:incorrect :cannot-set-db]
                      (xt/execute-tx conn [[:put-docs :docs {:xt/id 1}]] {:database :some-db})))

    (t/is (anomalous? [:incorrect :cannot-set-db]
                      (xt/submit-tx conn [[:put-docs :docs {:xt/id 1}]] {:database :some-db})))

    (t/is (anomalous? [:incorrect :cannot-set-db]
                      (into [] (xt/plan-q conn "SELECT 1" {:database :some-db}))))))

(deftest unknown-db-is-rejected-at-open
  (t/is (anomalous? [:incorrect :xtdb/unknown-db]
                    (fsql/open-conn {:port (.getFlightSqlPort ^Xtdb *node*), :dbname "no-such-db"}))))

(deftest sql-anomaly-propagates
  (with-open [conn (open-conn)]
    (t/is (anomalous? [:incorrect nil "Cannot parse date"]
                      (xt/q conn "SELECT DATE 'not-a-date'"))
          "a server-side anomaly comes back as the same category of anomaly")))

(deftest a-committed-write-is-visible-to-another-connection
  (with-open [conn1 (open-conn)
              conn2 (open-conn)]
    (xt/execute-tx conn1 [[:put-docs :docs {:xt/id :foo}]])

    (t/is (= [{:xt/id :foo}] (xt/q conn2 "SELECT _id FROM docs")))
    (t/is (= [{:xt/id :foo}] (xt/q conn1 "SELECT _id FROM docs")))))

(deftest status-over-flight-sql
  (with-open [conn (open-conn)]
    (xt/execute-tx conn [[:put-docs :docs {:xt/id :foo}]])

    (let [status (xt/status conn)]
      (t/is (map? status))
      (t/is (contains? status :latest-completed-txs)))))

(deftest mixed-type-column-round-trips
  (with-open [conn (open-conn)]
    (xt/execute-tx conn [[:put-docs :docs {:xt/id 1, :v "str"}]
                         [:put-docs :docs {:xt/id 2, :v 42}]])

    (t/is (= [{:xt/id 1, :v "str"} {:xt/id 2, :v 42}]
             (xt/q conn "SELECT _id, v FROM docs ORDER BY _id")))))
