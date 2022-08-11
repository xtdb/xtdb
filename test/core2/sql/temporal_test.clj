(ns core2.sql.temporal-test
  (:require [clojure.test :refer [deftest is use-fixtures testing]]
            [core2.api :as c2]
            [core2.test-util :as tu]))

(use-fixtures :each tu/with-node)

(defn query-at-tx [query tx]
  (c2/sql-query tu/*node* query {:basis {:tx tx}}))

(deftest system-time-as-of
  (let [!tx (c2/submit-tx tu/*node* [[:put {:id :my-doc, :last_updated "tx1"}]] {:sys-time #inst "3000"})
        !tx2 (c2/submit-tx tu/*node* [[:put {:id :my-doc, :last_updated "tx2"}]] {:sys-time #inst "3001"})]

    (is (= []
           (query-at-tx
             "SELECT foo.last_updated FROM foo FOR SYSTEM_TIME AS OF TIMESTAMP '2999-01-01 00:00:00+00:00'"
             !tx)))

    (is (= [{:last_updated "tx1"}]
           (query-at-tx
             "SELECT foo.last_updated FROM foo FOR SYSTEM_TIME AS OF TIMESTAMP '3000-01-01 00:00:00+00:00'"
             !tx)))

    (is (= [{:last_updated "tx1"} {:last_updated "tx2"}]
           (query-at-tx
             "SELECT foo.last_updated FROM foo FOR SYSTEM_TIME AS OF TIMESTAMP '3002-01-01 00:00:00+00:00'"
             !tx2)))))

(deftest system-time-from-a-to-b
  (let [!tx (c2/submit-tx tu/*node* [[:put {:id :my-doc, :last_updated "tx1"}]] {:sys-time #inst "3000"})
        !tx2 (c2/submit-tx tu/*node* [[:put {:id :my-doc, :last_updated "tx2"}]] {:sys-time #inst "3001"})]

    (is (= []
           (query-at-tx
             "SELECT foo.last_updated FROM foo FOR SYSTEM_TIME FROM DATE '2999-01-01' TO TIMESTAMP '3000-01-01 00:00:00+00:00'"
             !tx)))

    (is (= [{:last_updated "tx1"}]
           (query-at-tx
             "SELECT foo.last_updated FROM foo FOR SYSTEM_TIME FROM DATE '2999-01-01' TO TIMESTAMP '3001-01-01 00:00:00+00:00'"
             !tx)))

    (is (= [{:last_updated "tx1"} {:last_updated "tx1"} {:last_updated "tx2"}]
             (query-at-tx
               "SELECT foo.last_updated FROM foo FOR SYSTEM_TIME FROM DATE '2999-01-01' TO TIMESTAMP '3002-01-01 00:00:00+00:00'"
               !tx2)))

    (is (= [{:last_updated "tx1"} {:last_updated "tx2"}]
             (query-at-tx
               "SELECT foo.last_updated FROM foo FOR SYSTEM_TIME FROM DATE '3001-01-01' TO TIMESTAMP '3002-01-01 00:00:00+00:00'"
               !tx2)))))

(deftest system-time-between-a-to-b

  (let [!tx (c2/submit-tx tu/*node* [[:put {:id :my-doc, :last_updated "tx1"}]] {:sys-time #inst "3000"})
        !tx2 (c2/submit-tx tu/*node* [[:put {:id :my-doc, :last_updated "tx2"}]] {:sys-time #inst "3001"})]

    (is (= []
           (query-at-tx
             "SELECT foo.last_updated FROM foo FOR SYSTEM_TIME BETWEEN DATE '2998-01-01' AND TIMESTAMP '2999-01-01 00:00:00+00:00'"
             !tx)))

    (is (= [{:last_updated "tx1"}]
           (query-at-tx
             "SELECT foo.last_updated FROM foo FOR SYSTEM_TIME BETWEEN DATE '2999-01-01' AND TIMESTAMP '3000-01-01 00:00:00+00:00'"
             !tx))
        "second point in time is inclusive for between")

    (is (= [{:last_updated "tx1"}]
           (query-at-tx
             "SELECT foo.last_updated FROM foo FOR SYSTEM_TIME BETWEEN DATE '2999-01-01' AND TIMESTAMP '3001-01-01 00:00:00+00:00'"
             !tx)))

    (is (= [{:last_updated "tx1"} {:last_updated "tx1"} {:last_updated "tx2"}]
           (query-at-tx
             "SELECT foo.last_updated FROM foo FOR SYSTEM_TIME BETWEEN DATE '2999-01-01' AND TIMESTAMP '3002-01-01 00:00:00+00:00'"
             !tx2)))

    (is (= [{:last_updated "tx1"} {:last_updated "tx2"}]
           (query-at-tx
             "SELECT foo.last_updated FROM foo FOR SYSTEM_TIME BETWEEN DATE '3001-01-01' AND TIMESTAMP '3002-01-01 00:00:00+00:00'"
             !tx2)))

    (is (= [{:last_updated "tx1"} {:last_updated "tx2"}]
           (query-at-tx
             "SELECT foo.last_updated FROM foo FOR SYSTEM_TIME BETWEEN SYMMETRIC TIMESTAMP '3002-01-01 00:00:00+00:00' AND DATE '3001-01-01'"
             !tx2))
        "SYMMETRIC flips POT1 and 2")))

(deftest app-time-period-predicates

  (testing "OVERLAPS"

    (let [!tx (c2/submit-tx tu/*node* [[:put {:id :my-doc, :last_updated "2000"}
                                        {:app-time-start #inst "2000"}]
                                       [:put {:id :my-doc, :last_updated "3000"}
                                        {:app-time-start #inst "3000"}]
                                       [:put {:id :some-other-doc, :last_updated "4000"}
                                        {:app-time-start #inst "4000"
                                         :app-time-end #inst "4001"}]])]

      (is (= [{:last_updated "2000"} {:last_updated "3000"} {:last_updated "4000"}]
             (query-at-tx
               "SELECT foo.last_updated FROM foo"
               !tx)))

      (is (= [{:last_updated "2000"}]
             (query-at-tx
               "SELECT foo.last_updated FROM foo WHERE foo.APP_TIME OVERLAPS PERIOD (TIMESTAMP '2000-01-01 00:00:00', TIMESTAMP '2001-01-01 00:00:00')"
               !tx)))

      (is (= [{:last_updated "3000"}]
             (query-at-tx
               "SELECT foo.last_updated FROM foo WHERE foo.APP_TIME OVERLAPS PERIOD (TIMESTAMP '3000-01-01 00:00:00', TIMESTAMP '3001-01-01 00:00:00')"
               !tx)))

      (is (= [{:last_updated "3000"} {:last_updated "4000"}]
             (query-at-tx
               "SELECT foo.last_updated FROM foo WHERE foo.APP_TIME OVERLAPS PERIOD (TIMESTAMP '4000-01-01 00:00:00', TIMESTAMP '4001-01-01 00:00:00')"
               !tx)))

      (is (= [{:last_updated "3000"}]
             (query-at-tx
               "SELECT foo.last_updated FROM foo WHERE foo.APP_TIME OVERLAPS PERIOD (TIMESTAMP '4002-01-01 00:00:00', TIMESTAMP '9999-01-01 00:00:00')"
               !tx))))))

(deftest app-time-multiple-tables
  (let [!tx (c2/submit-tx tu/*node* [[:put {:id :foo-doc, :last_updated "2001"}
                                      {:app-time-start #inst "2000"
                                       :app-time-end #inst "2001"}]
                                     [:put {:id :bar-doc, :l_updated "2003"}
                                      {:app-time-start #inst "2002"
                                       :app-time-end #inst "2003"}]])]

    (is (= [{:last_updated "2001"}]
           (query-at-tx
             "SELECT foo.last_updated FROM foo
             WHERE foo.APP_TIME OVERLAPS PERIOD (TIMESTAMP '1999-01-01 00:00:00', TIMESTAMP '2002-01-01 00:00:00')"
             !tx)))

    (is (= [{:l_updated "2003"}]
           (query-at-tx
             "SELECT bar.l_updated FROM bar
             WHERE bar.APP_TIME OVERLAPS PERIOD (TIMESTAMP '2002-01-01 00:00:00', TIMESTAMP '2003-01-01 00:00:00')"
             !tx)))

    (is (= []
           (query-at-tx
             "SELECT foo.last_updated, bar.l_updated FROM foo, bar
             WHERE foo.APP_TIME OVERLAPS PERIOD (TIMESTAMP '1999-01-01 00:00:00', TIMESTAMP '2001-01-01 00:00:00')
             AND
             bar.APP_TIME OVERLAPS PERIOD (TIMESTAMP '2000-01-01 00:00:00', TIMESTAMP '2001-01-01 00:00:00')"
             !tx)))

    (is (= [{:last_updated "2001", :l_updated "2003"}]
           (query-at-tx
             "SELECT foo.last_updated, bar.l_updated FROM foo, bar
             WHERE foo.APP_TIME OVERLAPS PERIOD (TIMESTAMP '2000-01-01 00:00:00', TIMESTAMP '2001-01-01 00:00:00')
             AND
             bar.APP_TIME OVERLAPS PERIOD (TIMESTAMP '2002-01-01 00:00:00', TIMESTAMP '2003-01-01 00:00:00')"
             !tx)))

    (is (= []
           (query-at-tx
             "SELECT foo.last_updated, bar.name FROM foo, bar
             WHERE foo.APP_TIME OVERLAPS bar.APP_TIME" !tx)))))

(deftest app-time-joins

  (let [!tx (c2/submit-tx tu/*node* [[:put {:id :bill, :name "Bill"}
                                      {:app-time-start #inst "2016"
                                       :app-time-end #inst "2019"}]
                                     [:put {:id :jeff, :also_name "Jeff"}
                                      {:app-time-start #inst "2018"
                                       :app-time-end #inst "2020"}]])]

    (is (= []
           (query-at-tx
             "SELECT foo.name, bar.also_name
             FROM foo, bar
             WHERE foo.APP_TIME SUCCEEDS bar.APP_TIME"
             !tx)))

    (is (= [{:name "Bill" :also_name "Jeff"}]
           (query-at-tx
             "SELECT foo.name, bar.also_name
             FROM foo, bar
             WHERE foo.APP_TIME OVERLAPS bar.APP_TIME"
             !tx)))))
