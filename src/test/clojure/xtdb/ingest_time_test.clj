(ns xtdb.ingest-time-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.test-util :as tu]
            [xtdb.time :as time]))

(t/use-fixtures :each tu/with-mock-clock tu/with-node)

(t/deftest dml-can-backfill-behind-a-create-table
  (xt/execute-tx tu/*node* [[:sql "CREATE TABLE docs (_id, v)"]])

  (xt/execute-tx tu/*node* [[:sql "INSERT INTO docs (_id, v) VALUES (1, 'a')"]]
                 {:system-time #inst "2019-01-01"})

  (t/is (= [{:xt/id 1, :v "a", :xt/system-from (time/->zdt #inst "2019-01-01")}]
           (xt/q tu/*node* "SELECT _id, v, _system_from FROM docs"))))

(t/deftest ddl-under-an-explicit-system-time-leaves-the-dml-clock-alone
  (xt/execute-tx tu/*node* [[:sql "INSERT INTO docs (_id) VALUES (1)"]]
                 {:system-time #inst "2019-01-01"})

  (xt/execute-tx tu/*node* [[:sql "CREATE TABLE foo (_id)"]]
                 {:system-time #inst "2019-06-01"})

  (xt/execute-tx tu/*node* [[:sql "INSERT INTO docs (_id) VALUES (2)"]]
                 {:system-time #inst "2019-03-01"})

  (t/is (= #{{:xt/id 1} {:xt/id 2}}
           (set (xt/q tu/*node* "SELECT _id FROM docs")))))

(t/deftest a-rejected-system-time-leaves-the-dml-clock-alone-4603
  (xt/execute-tx tu/*node* [[:sql "INSERT INTO docs (_id) VALUES (1)"]]
                 {:system-time #inst "2019-01-01"})

  (t/is (anomalous? [:incorrect :invalid-system-time]
                    (xt/execute-tx tu/*node* [[:sql "INSERT INTO docs (_id) VALUES (2)"]]
                                   {:system-time #inst "2018-01-01"})))

  (xt/execute-tx tu/*node* [[:sql "INSERT INTO docs (_id) VALUES (3)"]]
                 {:system-time #inst "2019-02-01"})

  (t/is (= #{{:xt/id 1} {:xt/id 3}}
           (set (xt/q tu/*node* "SELECT _id FROM docs")))))

(t/deftest a-tx-has-a-system-time-iff-it-wrote-data
  (let [create-key (xt/execute-tx tu/*node* [[:sql "CREATE TABLE docs (_id)"]])
        insert-key (xt/execute-tx tu/*node* [[:sql "INSERT INTO docs (_id) VALUES (1)"]]
                                  {:system-time #inst "2019-01-01"})]
    (t/is (nil? (:system-time create-key)))
    (t/is (= (time/->instant #inst "2019-01-01") (:system-time insert-key)))
    (t/is (neg? (compare (:ingest-time create-key) (:ingest-time insert-key)))
          "ingest times increase whatever the system time")

    (t/is (= [{:xt/id 0} {:xt/id 1, :system-time (time/->zdt #inst "2019-01-01")}]
             (xt/q tu/*node* "SELECT _id, system_time FROM xt.txs ORDER BY _valid_from")))))

(t/deftest dml-that-writes-no-rows-leaves-the-dml-clock-alone
  (xt/execute-tx tu/*node* [[:sql "INSERT INTO docs (_id) VALUES (1)"]]
                 {:system-time #inst "2019-01-01"})

  (xt/execute-tx tu/*node* [[:sql "DELETE FROM docs WHERE _id = 42"]])
  (xt/execute-tx tu/*node* [])

  (xt/execute-tx tu/*node* [[:sql "INSERT INTO docs (_id) VALUES (2)"]]
                 {:system-time #inst "2019-02-01"})

  (t/is (= #{{:xt/id 1} {:xt/id 2}}
           (set (xt/q tu/*node* "SELECT _id FROM docs")))))

(t/deftest a-snapshot-token-pins-dml-and-the-txs-table
  (xt/execute-tx tu/*node* [[:sql "INSERT INTO docs (_id) VALUES (1)"]])
  (xt/execute-tx tu/*node* [[:sql "CREATE TABLE foo (_id)"]])

  (let [token (:tok (first (xt/q tu/*node* "SELECT SNAPSHOT_TOKEN tok")))]
    (xt/execute-tx tu/*node* [[:sql "CREATE TABLE bar (_id)"]])
    (xt/execute-tx tu/*node* [[:sql "INSERT INTO docs (_id) VALUES (2)"]])

    (t/is (= [{:xt/id 1}]
             (xt/q tu/*node* "SELECT _id FROM docs" {:snapshot-token token})))

    (t/is (= [{:xt/id 0} {:xt/id 1}]
             (xt/q tu/*node* "SELECT _id FROM xt.txs ORDER BY _id" {:snapshot-token token})))

    (t/is (= [{:xt/id 0} {:xt/id 1} {:xt/id 2} {:xt/id 3}]
             (xt/q tu/*node* "SELECT _id FROM xt.txs ORDER BY _id")))))

(t/deftest a-token-taken-before-any-dml-sees-no-dml
  (xt/execute-tx tu/*node* [[:sql "CREATE TABLE docs (_id)"]])

  (let [token (:tok (first (xt/q tu/*node* "SELECT SNAPSHOT_TOKEN tok")))]
    (xt/execute-tx tu/*node* [[:sql "INSERT INTO docs (_id) VALUES (1)"]])

    (t/is (= [] (xt/q tu/*node* "SELECT _id FROM docs" {:snapshot-token token})))))

(t/deftest a-ddl-only-tx-is-visible-once-awaited
  (xt/execute-tx tu/*node* [[:sql "INSERT INTO docs (_id) VALUES (1)"]])
  (xt/q tu/*node* "SELECT _id FROM docs")

  (xt/execute-tx tu/*node* [[:sql "CREATE TABLE foo (_id, bar)"]])

  (t/is (= [{:column-name "bar"}]
           (xt/q tu/*node* "SELECT column_name FROM information_schema.columns WHERE table_name = 'foo' AND column_name = 'bar'"))))

(t/deftest both-clocks-survive-a-restart
  (tu/with-tmp-dirs #{node-dir}
    (with-open [node (tu/->local-node {:node-dir node-dir})]
      (xt/execute-tx node [[:sql "INSERT INTO docs (_id) VALUES (1)"]]
                     {:system-time #inst "2019-01-01"})
      (xt/execute-tx node [[:sql "CREATE TABLE foo (_id)"]])
      (tu/flush-block! node))

    (with-open [node (tu/->local-node {:node-dir node-dir})]
      (t/is (anomalous? [:incorrect :invalid-system-time]
                        (xt/execute-tx node [[:sql "INSERT INTO docs (_id) VALUES (2)"]]
                                       {:system-time #inst "2018-01-01"})))

      (xt/execute-tx node [[:sql "INSERT INTO docs (_id) VALUES (3)"]]
                     {:system-time #inst "2019-02-01"})

      (t/is (= #{{:xt/id 1} {:xt/id 3}}
               (set (xt/q node "SELECT _id FROM docs")))))))
