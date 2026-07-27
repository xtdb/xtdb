(ns xtdb.api-adbc-test
  "Exercises `xtdb.api` over the in-process ADBC path — an explicit `Xtdb.Connection` obtained from
  the node — plus a guard that a plain JDBC connection stays on the pgwire path."
  (:require [clojure.test :as t :refer [deftest]]
            [next.jdbc :as jdbc]
            [xtdb.api :as xt]
            [xtdb.serde :as serde]
            [xtdb.test-util :as tu :refer [*node*]]
            [xtdb.time :as time]))

(t/use-fixtures :each tu/with-mock-clock tu/with-node)

(deftest execute-tx-and-query
  (with-open [conn (xt/open-adbc-conn *node*)]
    (t/is (= (serde/->TxKey 0 (time/->instant #inst "2020-01-01"))
             (xt/execute-tx conn [[:put-docs :docs {:xt/id :foo, :n 1}]]))
          "execute-tx over an Xtdb.Connection returns the tx-key")

    (t/is (= [{:xt/id :foo, :n 1}]
             (xt/q conn "SELECT _id, n FROM docs"))
          "the connection reads its own write (read-your-writes)")

    (t/is (= {:tx-id 1} (xt/submit-tx conn [[:put-docs :docs {:xt/id :bar, :n 2}]]))
          "submit-tx returns the tx-id")

    (t/is (= #{{:xt/id :foo, :n 1} {:xt/id :bar, :n 2}}
             (set (xt/q conn "SELECT _id, n FROM docs"))))))

(deftest client-op-conversions
  (with-open [conn (xt/open-adbc-conn *node*)]
    (xt/execute-tx conn [[:put-docs :docs {:xt/id :a, :n 1}]
                         [:put-docs :docs {:xt/id :b, :n 2}]
                         [:put-docs :docs {:xt/id :c, :n 3}]])

    (xt/execute-tx conn [[:patch-docs :docs {:xt/id :a, :n 10}]
                         [:delete-docs :docs :b]
                         [:sql "INSERT INTO docs (_id, n) VALUES ('d', ?)" [4]]])

    (t/is (= #{{:xt/id :a, :n 10} {:xt/id :c, :n 3} {:xt/id "d", :n 4}}
             (set (xt/q conn "SELECT _id, n FROM docs")))
          "patch updated :a, delete removed :b, raw sql inserted the string id 'd'")

    (xt/execute-tx conn [[:erase-docs :docs :a]])
    (t/is (= #{{:xt/id :c, :n 3} {:xt/id "d", :n 4}}
             (set (xt/q conn "SELECT _id, n FROM docs")))
          "erase removed :a")))

(deftest multi-row-batches
  ;; each of these converts to a single TxOp.Sql carrying a multi-row batch
  (with-open [conn (xt/open-adbc-conn *node*)]
    (xt/execute-tx conn [[:sql "INSERT INTO docs (_id, n) VALUES (?, ?)" [1 10] [2 20] [3 30]]])
    (t/is (= #{{:xt/id 1, :n 10} {:xt/id 2, :n 20} {:xt/id 3, :n 30}}
             (set (xt/q conn "SELECT _id, n FROM docs")))
          "a batched :sql insert applies every arg-row")

    (xt/execute-tx conn [[:delete-docs :docs 1 2]])
    (t/is (= #{{:xt/id 3, :n 30}}
             (set (xt/q conn "SELECT _id, n FROM docs")))
          "delete-docs with several ids removes them all")))

(deftest query-args-and-plan-q
  (with-open [conn (xt/open-adbc-conn *node*)]
    (xt/execute-tx conn [[:put-docs :docs {:xt/id :foo, :n 1}]
                         [:put-docs :docs {:xt/id :bar, :n 2}]])

    (t/is (= [{:xt/id :foo, :n 1}]
             (xt/q conn ["SELECT _id, n FROM docs WHERE _id = ?" :foo]))
          "positional args bind on the in-process path")

    (t/is (= [1 2]
             (sort (into [] (map :n) (xt/plan-q conn "SELECT n FROM docs"))))
          "plan-q streams rows over the in-process path")))

(deftest submit-tx-then-query-sees-write
  (with-open [conn (xt/open-adbc-conn *node*)]
    (xt/submit-tx conn [[:put-docs :docs {:xt/id :foo, :n 1}]])
    (t/is (= [{:xt/id :foo, :n 1}]
             (xt/q conn "SELECT _id, n FROM docs"))
          "an async submit-tx is awaited by a subsequent read on the same connection")))

(deftest invalid-system-time-is-an-anomaly
  (with-open [conn (xt/open-adbc-conn *node*)]
    (t/is (anomalous? [:incorrect nil]
                      (xt/submit-tx conn [[:put-docs :docs {:xt/id 1}]] {:system-time "foo"}))
          "a bad system-time surfaces as an :incorrect anomaly, not a raw parse exception")))

(deftest status-over-in-process
  (with-open [conn (xt/open-adbc-conn *node*)]
    (xt/execute-tx conn [[:put-docs :docs {:xt/id :foo}]])
    (let [status (xt/status conn)]
      (t/is (map? status))
      (t/is (contains? status :latest-completed-txs)))))

(deftest node-routes-database-opt
  ;; the in-process node must route :database to that db, not silently use the default — an unknown
  ;; db surfaces (rather than the write landing in xtdb)
  (t/is (anomalous? [:incorrect nil]
                    (xt/execute-tx *node* [[:put-docs :docs {:xt/id 1}]] {:database :no-such-db}))
        "an unknown :database is rejected"))

(deftest connection-rejects-database-opt
  ;; a connection is already bound to its db, so selecting one must fail rather than silently write to
  ;; the bound db — the same contract the JDBC connectables enforce
  (with-open [conn (xt/open-adbc-conn *node*)]
    (t/is (anomalous? [:incorrect :cannot-set-db]
                      (xt/execute-tx conn [[:put-docs :docs {:xt/id 1}]] {:database :some-db})))

    (t/is (anomalous? [:incorrect :cannot-set-db]
                      (xt/submit-tx conn [[:put-docs :docs {:xt/id 1}]] {:database :some-db})))

    (t/is (anomalous? [:incorrect :cannot-set-db]
                      (into [] (xt/plan-q conn "SELECT 1" {:database :some-db}))))))

(deftest jdbc-path-still-works
  ;; a plain java.sql.Connection stays on the pgwire path — the protocol dispatch must not capture it
  (with-open [conn (jdbc/get-connection *node*)]
    (xt/execute-tx conn [[:put-docs :docs {:xt/id :foo, :n 1}]])
    (t/is (= [{:xt/id :foo, :n 1}]
             (xt/q conn "SELECT _id, n FROM docs")))))
