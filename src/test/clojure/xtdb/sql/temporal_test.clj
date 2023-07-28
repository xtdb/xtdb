(ns xtdb.sql.temporal-test
  (:require [clojure.test :refer [deftest is use-fixtures testing]]
            [xtdb.api :as xt]
            [xtdb.test-util :as tu]))

(use-fixtures :each tu/with-node)

(defn query-at-tx [query tx]
  (xt/q tu/*node* query {:basis {:tx tx}, :default-all-valid-time? true}))

(deftest all-system-time
  (let [_tx (xt/submit-tx tu/*node* [[:put :foo {:xt/id :my-doc, :last_updated "tx1"}]] {:system-time #inst "3000"})
        tx2 (xt/submit-tx tu/*node* [[:put :foo {:xt/id :my-doc, :last_updated "tx2"}]] {:system-time #inst "3001"})]

    (is (= [{:last_updated "tx1"} {:last_updated "tx2"}]
           (query-at-tx
            "SELECT foo.last_updated FROM foo"
            tx2)))

    (is (= [{:last_updated "tx1"} {:last_updated "tx1"} {:last_updated "tx2"}]
           (query-at-tx
            "SELECT foo.last_updated FROM foo FOR ALL SYSTEM_TIME"
            tx2)))))

(deftest system-time-as-of
  (let [tx (xt/submit-tx tu/*node* [[:put :foo {:xt/id :my-doc, :last_updated "tx1"}]] {:system-time #inst "3000"})
        tx2 (xt/submit-tx tu/*node* [[:put :foo {:xt/id :my-doc, :last_updated "tx2"}]] {:system-time #inst "3001"})]

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
  (let [tx (xt/submit-tx tu/*node* [[:put :foo {:xt/id :my-doc, :last_updated "tx1"}]] {:system-time #inst "3000"})
        tx2 (xt/submit-tx tu/*node* [[:put :foo {:xt/id :my-doc, :last_updated "tx2"}]] {:system-time #inst "3001"})]

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
  (let [tx (xt/submit-tx tu/*node* [[:put :foo {:xt/id :my-doc, :last_updated "tx1"}]] {:system-time #inst "3000"})
        tx2 (xt/submit-tx tu/*node* [[:put :foo {:xt/id :my-doc, :last_updated "tx2"}]] {:system-time #inst "3001"})]
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
    (let [tx (xt/submit-tx tu/*node* [[:put :foo {:xt/id :my-doc, :last_updated "2000"}
                                       {:for-valid-time [:in #inst "2000"]}]
                                      [:put :foo {:xt/id :my-doc, :last_updated "3000"}
                                       {:for-valid-time [:in #inst "3000"]}]
                                      [:put :foo {:xt/id :some-other-doc, :last_updated "4000"}
                                       {:for-valid-time [:in #inst "4000" #inst "4001"]}]])]

      (is (= [{:last_updated "2000"} {:last_updated "3000"} {:last_updated "4000"}]
             (query-at-tx
              "SELECT foo.last_updated FROM foo"
              tx)))

      (is (= [{:last_updated "2000"}]
             (query-at-tx
              "SELECT foo.last_updated FROM foo WHERE foo.VALID_TIME OVERLAPS PERIOD (TIMESTAMP '2000-01-01 00:00:00', TIMESTAMP '2001-01-01 00:00:00')"
              tx)))

      (is (= [{:last_updated "3000"}]
             (query-at-tx
              "SELECT foo.last_updated FROM foo WHERE foo.VALID_TIME OVERLAPS PERIOD (TIMESTAMP '3000-01-01 00:00:00', TIMESTAMP '3001-01-01 00:00:00')"
              tx)))

      (is (= [{:last_updated "3000"} {:last_updated "4000"}]
             (query-at-tx
              "SELECT foo.last_updated FROM foo WHERE foo.VALID_TIME OVERLAPS PERIOD (TIMESTAMP '4000-01-01 00:00:00', TIMESTAMP '4001-01-01 00:00:00')"
              tx)))

      (is (= [{:last_updated "3000"}]
             (query-at-tx
              "SELECT foo.last_updated FROM foo WHERE foo.VALID_TIME OVERLAPS PERIOD (TIMESTAMP '4002-01-01 00:00:00', TIMESTAMP '9999-01-01 00:00:00')"
              tx))))))

(deftest app-time-multiple-tables
  (let [tx (xt/submit-tx tu/*node* [[:put :foo {:xt/id :foo-doc, :last_updated "2001" }
                                     {:for-valid-time [:in #inst "2000" #inst "2001"]}]
                                    [:put :bar {:xt/id :bar-doc, :l_updated "2003" }
                                     {:for-valid-time [:in #inst "2002" #inst "2003"]}]])]

    (is (= [{:last_updated "2001"}]
           (query-at-tx
            "SELECT foo.last_updated FROM foo
             WHERE foo.VALID_TIME OVERLAPS PERIOD (TIMESTAMP '1999-01-01 00:00:00', TIMESTAMP '2002-01-01 00:00:00')"
            tx)))

    (is (= [{:l_updated "2003"}]
           (query-at-tx
            "SELECT bar.l_updated FROM bar
             WHERE bar.VALID_TIME OVERLAPS PERIOD (TIMESTAMP '2002-01-01 00:00:00', TIMESTAMP '2003-01-01 00:00:00')"
            tx)))

    (is (= []
           (query-at-tx
            "SELECT foo.last_updated, bar.l_updated FROM foo, bar
             WHERE foo.VALID_TIME OVERLAPS PERIOD (TIMESTAMP '1999-01-01 00:00:00', TIMESTAMP '2001-01-01 00:00:00')
             AND
             bar.VALID_TIME OVERLAPS PERIOD (TIMESTAMP '2000-01-01 00:00:00', TIMESTAMP '2001-01-01 00:00:00')"
            tx)))

    (is (= [{:last_updated "2001", :l_updated "2003"}]
           (query-at-tx
            "SELECT foo.last_updated, bar.l_updated FROM foo, bar
             WHERE foo.VALID_TIME OVERLAPS PERIOD (TIMESTAMP '2000-01-01 00:00:00', TIMESTAMP '2001-01-01 00:00:00')
             AND
             bar.VALID_TIME OVERLAPS PERIOD (TIMESTAMP '2002-01-01 00:00:00', TIMESTAMP '2003-01-01 00:00:00')"
            tx)))

    (is (= []
           (query-at-tx
            "SELECT foo.last_updated, bar.name FROM foo, bar
             WHERE foo.VALID_TIME OVERLAPS bar.VALID_TIME" tx)))))

(deftest app-time-joins
  (let [tx (xt/submit-tx tu/*node* [[:put :foo {:xt/id :bill, :name "Bill"}
                                     {:app-time-start #inst "2016"
                                      :app-time-end #inst "2019"}]
                                    [:put :bar {:xt/id :jeff, :also_name "Jeff"}
                                     {:app-time-start #inst "2018"
                                      :app-time-end #inst "2020"}]])]

    (is (= []
           (query-at-tx
            "SELECT foo.name, bar.also_name
             FROM foo, bar
             WHERE foo.VALID_TIME SUCCEEDS bar.VALID_TIME"
            tx)))

    (is (= [{:name "Bill" :also_name "Jeff"}]
           (query-at-tx
            "SELECT foo.name, bar.also_name
             FROM foo, bar
             WHERE foo.VALID_TIME OVERLAPS bar.VALID_TIME"
            tx)))))

(deftest test-inconsistent-valid-time-range-2494
  (xt/submit-tx tu/*node* [[:sql "INSERT INTO xt_docs (xt$id, xt$valid_to) VALUES (1, DATE '2011-01-01')"]])
  (is (= [{:tx-id 0, :committed? false}]
         (xt/q tu/*node* '{:find [tx-id committed?]
                           :where [($ :xt/txs {:xt/id tx-id,
                                               :xt/committed? committed?})]})))
  (xt/submit-tx tu/*node* [[:sql "INSERT INTO xt_docs (xt$id) VALUES (2)"]])
  (xt/submit-tx tu/*node* [[:sql ["DELETE FROM xt_docs FOR PORTION OF VALID_TIME FROM NULL TO ? WHERE xt_docs.xt$id = 2"
                                  #inst "2011"]]])
  (is (= [{:tx-id 0, :committed? false}
          {:tx-id 1, :committed? true}
          {:tx-id 2, :committed? false}]
         (xt/q tu/*node* '{:find [tx-id committed?]
                           :where [($ :xt/txs {:xt/id tx-id,
                                               :xt/committed? committed?})]
                           :order-by [[tx-id]]})))
  (xt/submit-tx tu/*node* [[:sql "INSERT INTO xt_docs (xt$id) VALUES (3)"]])
  (xt/submit-tx tu/*node* [[:sql ["UPDATE xt_docs FOR PORTION OF VALID_TIME FROM NULL TO ? SET foo = 'bar' WHERE xt_docs.xt$id = 3"
                                  #inst "2011"]]])
  (is (= [{:committed? false}]
         (xt/q tu/*node* '{:find [committed?]
                           :where [($ :xt/txs {:xt/id 4,
                                               :xt/committed? committed?})]}))))
