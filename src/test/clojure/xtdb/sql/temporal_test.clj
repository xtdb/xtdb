(ns xtdb.sql.temporal-test
  (:require [clojure.test :refer [deftest is use-fixtures testing]]
            [xtdb.datalog :as xt.d]
            [xtdb.sql :as xt.sql]
            [xtdb.test-util :as tu]))

(use-fixtures :each tu/with-node)

(defn query-at-tx [query tx]
  (xt.sql/q tu/*node* query {:basis {:tx tx}}))

(deftest all-system-time
  (let [_tx (xt.d/submit-tx tu/*node* [[:put :foo {:xt/id :my-doc, :last_updated "tx1"}]] {:sys-time #inst "3000"})
        tx2 (xt.d/submit-tx tu/*node* [[:put :foo {:xt/id :my-doc, :last_updated "tx2"}]] {:sys-time #inst "3001"})]

    (is (= [{:last_updated "tx1"} {:last_updated "tx2"}]
           (query-at-tx
            "SELECT foo.last_updated FROM foo"
            tx2)))

    (is (= [{:last_updated "tx1"} {:last_updated "tx1"} {:last_updated "tx2"}]
           (query-at-tx
            "SELECT foo.last_updated FROM foo FOR ALL SYSTEM_TIME"
            tx2)))))

(deftest system-time-as-of
  (let [tx (xt.d/submit-tx tu/*node* [[:put :foo {:xt/id :my-doc, :last_updated "tx1"}]] {:sys-time #inst "3000"})
        tx2 (xt.d/submit-tx tu/*node* [[:put :foo {:xt/id :my-doc, :last_updated "tx2"}]] {:sys-time #inst "3001"})]

    (is (= []
           (query-at-tx
            "SELECT foo.last_updated FROM foo FOR SYSTEM_TIME AS OF TIMESTAMP '2999-01-01 00:00:00+00:00'"
            tx)))

    (is (= [{:last_updated "tx1"}]
           (query-at-tx
            "SELECT foo.last_updated FROM foo FOR SYSTEM_TIME AS OF TIMESTAMP '3000-01-01 00:00:00+00:00'"
            tx)))

    (is (= [{:last_updated "tx1"} {:last_updated "tx2"}]
           (query-at-tx
            "SELECT foo.last_updated FROM foo FOR SYSTEM_TIME AS OF TIMESTAMP '3002-01-01 00:00:00+00:00'"
            tx2)))))

(deftest system-time-from-a-to-b
  (let [tx (xt.d/submit-tx tu/*node* [[:put :foo {:xt/id :my-doc, :last_updated "tx1"}]] {:sys-time #inst "3000"})
        tx2 (xt.d/submit-tx tu/*node* [[:put :foo {:xt/id :my-doc, :last_updated "tx2"}]] {:sys-time #inst "3001"})]

    (is (= []
           (query-at-tx
            "SELECT foo.last_updated FROM foo FOR SYSTEM_TIME FROM DATE '2999-01-01' TO TIMESTAMP '3000-01-01 00:00:00+00:00'"
            tx)))

    (is (= [{:last_updated "tx1"}]
           (query-at-tx
            "SELECT foo.last_updated FROM foo FOR SYSTEM_TIME FROM DATE '2999-01-01' TO TIMESTAMP '3001-01-01 00:00:00+00:00'"
            tx)))

    (is (= [{:last_updated "tx1"} {:last_updated "tx1"} {:last_updated "tx2"}]
           (query-at-tx
            "SELECT foo.last_updated FROM foo FOR SYSTEM_TIME FROM DATE '2999-01-01' TO TIMESTAMP '3002-01-01 00:00:00+00:00'"
            tx2)))

    (is (= [{:last_updated "tx1"} {:last_updated "tx2"}]
           (query-at-tx
            "SELECT foo.last_updated FROM foo FOR SYSTEM_TIME FROM DATE '3001-01-01' TO TIMESTAMP '3002-01-01 00:00:00+00:00'"
            tx2)))))

(deftest system-time-between-a-to-b
  (let [tx (xt.d/submit-tx tu/*node* [[:put :foo {:xt/id :my-doc, :last_updated "tx1"}]] {:sys-time #inst "3000"})
        tx2 (xt.d/submit-tx tu/*node* [[:put :foo {:xt/id :my-doc, :last_updated "tx2"}]] {:sys-time #inst "3001"})]
    (is (= []
           (query-at-tx
            "SELECT foo.last_updated FROM foo FOR SYSTEM_TIME BETWEEN DATE '2998-01-01' AND TIMESTAMP '2999-01-01 00:00:00+00:00'"
            tx)))

    (is (= [{:last_updated "tx1"}]
           (query-at-tx
            "SELECT foo.last_updated FROM foo FOR SYSTEM_TIME BETWEEN DATE '2999-01-01' AND TIMESTAMP '3000-01-01 00:00:00+00:00'"
            tx))
        "second point in time is inclusive for between")

    (is (= [{:last_updated "tx1"}]
           (query-at-tx
            "SELECT foo.last_updated FROM foo FOR SYSTEM_TIME BETWEEN DATE '2999-01-01' AND TIMESTAMP '3001-01-01 00:00:00+00:00'"
            tx)))

    (is (= [{:last_updated "tx1"} {:last_updated "tx1"} {:last_updated "tx2"}]
           (query-at-tx
            "SELECT foo.last_updated FROM foo FOR SYSTEM_TIME BETWEEN DATE '2999-01-01' AND TIMESTAMP '3002-01-01 00:00:00+00:00'"
            tx2)))

    (is (= [{:last_updated "tx1"} {:last_updated "tx2"}]
           (query-at-tx
            "SELECT foo.last_updated FROM foo FOR SYSTEM_TIME BETWEEN DATE '3001-01-01' AND TIMESTAMP '3002-01-01 00:00:00+00:00'"
            tx2)))))

(deftest app-time-period-predicates
  (testing "OVERLAPS"
    (let [tx (xt.d/submit-tx tu/*node* [[:put :foo {:xt/id :my-doc, :last_updated "2000"}
                                         {:for-app-time [:in #inst "2000"]}]
                                        [:put :foo {:xt/id :my-doc, :last_updated "3000"}
                                         {:for-app-time [:in #inst "3000"]}]
                                        [:put :foo {:xt/id :some-other-doc, :last_updated "4000"}
                                         {:for-app-time [:in #inst "4000" #inst "4001"]}]])]

      (is (= [{:last_updated "2000"} {:last_updated "3000"} {:last_updated "4000"}]
             (query-at-tx
              "SELECT foo.last_updated FROM foo"
              tx)))

      (is (= [{:last_updated "2000"}]
             (query-at-tx
              "SELECT foo.last_updated FROM foo WHERE foo.APP_TIME OVERLAPS PERIOD (TIMESTAMP '2000-01-01 00:00:00', TIMESTAMP '2001-01-01 00:00:00')"
              tx)))

      (is (= [{:last_updated "3000"}]
             (query-at-tx
              "SELECT foo.last_updated FROM foo WHERE foo.APP_TIME OVERLAPS PERIOD (TIMESTAMP '3000-01-01 00:00:00', TIMESTAMP '3001-01-01 00:00:00')"
              tx)))

      (is (= [{:last_updated "3000"} {:last_updated "4000"}]
             (query-at-tx
              "SELECT foo.last_updated FROM foo WHERE foo.APP_TIME OVERLAPS PERIOD (TIMESTAMP '4000-01-01 00:00:00', TIMESTAMP '4001-01-01 00:00:00')"
              tx)))

      (is (= [{:last_updated "3000"}]
             (query-at-tx
              "SELECT foo.last_updated FROM foo WHERE foo.APP_TIME OVERLAPS PERIOD (TIMESTAMP '4002-01-01 00:00:00', TIMESTAMP '9999-01-01 00:00:00')"
              tx))))))

(deftest app-time-multiple-tables
  (let [tx (xt.d/submit-tx tu/*node* [[:put :foo {:xt/id :foo-doc, :last_updated "2001" }
                                       {:for-app-time [:in #inst "2000" #inst "2001"]}]
                                      [:put :bar {:xt/id :bar-doc, :l_updated "2003" }
                                       {:for-app-time [:in #inst "2002" #inst "2003"]}]])]

    (is (= [{:last_updated "2001"}]
           (query-at-tx
            "SELECT foo.last_updated FROM foo
             WHERE foo.APP_TIME OVERLAPS PERIOD (TIMESTAMP '1999-01-01 00:00:00', TIMESTAMP '2002-01-01 00:00:00')"
            tx)))

    (is (= [{:l_updated "2003"}]
           (query-at-tx
            "SELECT bar.l_updated FROM bar
             WHERE bar.APP_TIME OVERLAPS PERIOD (TIMESTAMP '2002-01-01 00:00:00', TIMESTAMP '2003-01-01 00:00:00')"
            tx)))

    (is (= []
           (query-at-tx
            "SELECT foo.last_updated, bar.l_updated FROM foo, bar
             WHERE foo.APP_TIME OVERLAPS PERIOD (TIMESTAMP '1999-01-01 00:00:00', TIMESTAMP '2001-01-01 00:00:00')
             AND
             bar.APP_TIME OVERLAPS PERIOD (TIMESTAMP '2000-01-01 00:00:00', TIMESTAMP '2001-01-01 00:00:00')"
            tx)))

    (is (= [{:last_updated "2001", :l_updated "2003"}]
           (query-at-tx
            "SELECT foo.last_updated, bar.l_updated FROM foo, bar
             WHERE foo.APP_TIME OVERLAPS PERIOD (TIMESTAMP '2000-01-01 00:00:00', TIMESTAMP '2001-01-01 00:00:00')
             AND
             bar.APP_TIME OVERLAPS PERIOD (TIMESTAMP '2002-01-01 00:00:00', TIMESTAMP '2003-01-01 00:00:00')"
            tx)))

    (is (= []
           (query-at-tx
            "SELECT foo.last_updated, bar.name FROM foo, bar
             WHERE foo.APP_TIME OVERLAPS bar.APP_TIME" tx)))))

(deftest app-time-joins

  (let [tx (xt.d/submit-tx tu/*node* [[:put :foo {:xt/id :bill, :name "Bill"}
                                       {:app-time-start #inst "2016"
                                        :app-time-end #inst "2019"}]
                                      [:put :bar {:xt/id :jeff, :also_name "Jeff"}
                                       {:app-time-start #inst "2018"
                                        :app-time-end #inst "2020"}]])]

    (is (= []
           (query-at-tx
            "SELECT foo.name, bar.also_name
             FROM foo, bar
             WHERE foo.APP_TIME SUCCEEDS bar.APP_TIME"
            tx)))

    (is (= [{:name "Bill" :also_name "Jeff"}]
           (query-at-tx
            "SELECT foo.name, bar.also_name
             FROM foo, bar
             WHERE foo.APPLICATION_TIME OVERLAPS bar.APP_TIME"
            tx)))))
