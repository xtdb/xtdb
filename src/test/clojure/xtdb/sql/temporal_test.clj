(ns xtdb.sql.temporal-test
  (:require [clojure.test :refer [deftest is use-fixtures testing]]
            [xtdb.api :as xt]
            [xtdb.test-util :as tu]))

(use-fixtures :each tu/with-node)

(defn query-at-tx [query tx]
  (xt/q tu/*node* query {:basis {:at-tx tx}, :default-all-valid-time? true}))

(deftest all-system-time
  (let [_tx (xt/submit-tx tu/*node* [[:put-docs :foo {:xt/id :my-doc, :last-updated "tx1"}]] {:system-time #inst "3000"})
        tx2 (xt/submit-tx tu/*node* [[:put-docs :foo {:xt/id :my-doc, :last-updated "tx2"}]] {:system-time #inst "3001"})]

    (is (= {{:last-updated "tx1"} 1, {:last-updated "tx2"}, 1}
           (frequencies (query-at-tx "SELECT foo.last_updated FROM foo" tx2))))

    (is (= {{:last-updated "tx1"} 2, {:last-updated "tx2"} 1}
           (frequencies
            (query-at-tx
             "SELECT foo.last_updated FROM foo FOR ALL SYSTEM_TIME"
             tx2))))

    (is (= {{:last-updated "tx1"} 2, {:last-updated "tx2"} 1}
           (frequencies
            (query-at-tx
             "SELECT foo.last_updated FROM foo FOR SYSTEM_TIME ALL"
             tx2))))))

(deftest system-time-as-of
  (let [tx (xt/submit-tx tu/*node* [[:put-docs :foo {:xt/id :my-doc, :last-updated "tx1"}]] {:system-time #inst "3000"})
        tx2 (xt/submit-tx tu/*node* [[:put-docs :foo {:xt/id :my-doc, :last-updated "tx2"}]] {:system-time #inst "3001"})]

    (is (= []
           (query-at-tx
            "SELECT foo.last_updated FROM foo FOR SYSTEM_TIME AS OF TIMESTAMP '2999-01-01 00:00:00+00:00'"
            tx)))

    (is (= [{:last-updated "tx1"}]
           (query-at-tx
            "SELECT foo.last_updated FROM foo FOR SYSTEM_TIME AS OF TIMESTAMP '3000-01-01 00:00:00+00:00'"
            tx)))

    (is (= #{{:last-updated "tx1"} {:last-updated "tx2"}}
           (set (query-at-tx
                 "SELECT foo.last_updated FROM foo FOR SYSTEM_TIME AS OF TIMESTAMP '3002-01-01 00:00:00+00:00'"
                 tx2))))))

(deftest system-time-from-a-to-b
  (let [tx (xt/submit-tx tu/*node* [[:put-docs :foo {:xt/id :my-doc, :last-updated "tx1"}]] {:system-time #inst "3000"})
        tx2 (xt/submit-tx tu/*node* [[:put-docs :foo {:xt/id :my-doc, :last-updated "tx2"}]] {:system-time #inst "3001"})]

    (is (= []
           (query-at-tx
            "SELECT foo.last_updated FROM foo FOR SYSTEM_TIME FROM DATE '2999-01-01' TO TIMESTAMP '3000-01-01 00:00:00+00:00'"
            tx)))

    (is (= [{:last-updated "tx1"}]
           (query-at-tx
            "SELECT foo.last_updated FROM foo FOR SYSTEM_TIME FROM DATE '2999-01-01' TO TIMESTAMP '3001-01-01 00:00:00+00:00'"
            tx)))

    (is (= [{:last-updated "tx1"} {:last-updated "tx1"} {:last-updated "tx2"}]
           (query-at-tx
            "SELECT foo.last_updated FROM foo FOR SYSTEM_TIME FROM DATE '2999-01-01' TO TIMESTAMP '3002-01-01 00:00:00+00:00'
             ORDER BY last_updated"
            tx2)))

    (is (= #{{:last-updated "tx1"} {:last-updated "tx2"}}
           (set (query-at-tx
                 "SELECT foo.last_updated FROM foo FOR SYSTEM_TIME FROM DATE '3001-01-01' TO TIMESTAMP '3002-01-01 00:00:00+00:00'"
                 tx2))))))

(deftest system-time-between-a-to-b
  (let [tx (xt/submit-tx tu/*node* [[:put-docs :foo {:xt/id :my-doc, :last-updated "tx1"}]] {:system-time #inst "3000"})
        tx2 (xt/submit-tx tu/*node* [[:put-docs :foo {:xt/id :my-doc, :last-updated "tx2"}]] {:system-time #inst "3001"})]
    (is (= []
           (query-at-tx
            "SELECT foo.last_updated FROM foo FOR SYSTEM_TIME BETWEEN DATE '2998-01-01' AND TIMESTAMP '2999-01-01 00:00:00+00:00'"
            tx)))

    (is (= [{:last-updated "tx1"}]
           (query-at-tx
            "SELECT foo.last_updated FROM foo FOR SYSTEM_TIME BETWEEN DATE '2999-01-01' AND TIMESTAMP '3000-01-01 00:00:00+00:00'"
            tx))
        "second point in time is inclusive for between")

    (is (= [{:last-updated "tx1"}]
           (query-at-tx
            "SELECT foo.last_updated FROM foo FOR SYSTEM_TIME BETWEEN DATE '2999-01-01' AND TIMESTAMP '3001-01-01 00:00:00+00:00'"
            tx)))

    (is (= [{:last-updated "tx1"} {:last-updated "tx1"} {:last-updated "tx2"}]
           (query-at-tx
            "SELECT foo.last_updated FROM foo FOR SYSTEM_TIME BETWEEN DATE '2999-01-01' AND TIMESTAMP '3002-01-01 00:00:00+00:00'
             ORDER BY last_updated"
            tx2)))

    (is (= #{{:last-updated "tx1"} {:last-updated "tx2"}}
           (set (query-at-tx
                 "SELECT foo.last_updated FROM foo FOR SYSTEM_TIME BETWEEN DATE '3001-01-01' AND TIMESTAMP '3002-01-01 00:00:00+00:00'"
                 tx2))))))

(deftest app-time-period-predicates
  (testing "OVERLAPS"
    (let [tx (xt/submit-tx tu/*node* [[:put-docs {:into :foo, :valid-from #inst "2000"}
                                       {:xt/id :my-doc, :last-updated "2000"}]

                                      [:put-docs {:into :foo, :valid-from #inst "3000"}
                                       {:xt/id :my-doc, :last-updated "3000"}]

                                      [:put-docs {:into :foo, :valid-from #inst "4000", :valid-to #inst "4001"}
                                       {:xt/id :some-other-doc, :last-updated "4000"}]])]

      (is (= [{:last-updated "2000"} {:last-updated "3000"} {:last-updated "4000"}]
             (query-at-tx
              "SELECT foo.last_updated FROM foo ORDER BY last_updated"
              tx)))

      (is (= [{:last-updated "2000"}]
             (query-at-tx
              "SELECT foo.last_updated FROM foo WHERE foo.VALID_TIME OVERLAPS PERIOD (TIMESTAMP '2000-01-01 00:00:00', TIMESTAMP '2001-01-01 00:00:00')"
              tx)))

      (is (= [{:last-updated "3000"}]
             (query-at-tx
              "SELECT foo.last_updated FROM foo WHERE foo.VALID_TIME OVERLAPS PERIOD (TIMESTAMP '3000-01-01 00:00:00', TIMESTAMP '3001-01-01 00:00:00')"
              tx)))

      (is (= [{:last-updated "3000"} {:last-updated "4000"}]
             (query-at-tx
              "SELECT foo.last_updated FROM foo WHERE foo.VALID_TIME OVERLAPS PERIOD (TIMESTAMP '4000-01-01 00:00:00', TIMESTAMP '4001-01-01 00:00:00')"
              tx)))

      (is (= [{:last-updated "3000"}]
             (query-at-tx
              "SELECT foo.last_updated FROM foo WHERE foo.VALID_TIME OVERLAPS PERIOD (TIMESTAMP '4002-01-01 00:00:00', TIMESTAMP '9999-01-01 00:00:00')"
              tx))))))

(deftest app-time-multiple-tables
  (let [tx (xt/submit-tx tu/*node* [[:put-docs {:into :foo, :valid-from #inst "2000", :valid-to #inst "2001"}
                                     {:xt/id :foo-doc, :last-updated "2001" }]

                                    [:put-docs {:into :bar, :valid-from #inst "2002", :valid-to #inst "2003"}
                                     {:xt/id :bar-doc, :l_updated "2003" :name "test"}]])]

    (is (= [{:last-updated "2001"}]
           (query-at-tx
            "SELECT foo.last_updated FROM foo
             WHERE foo.VALID_TIME OVERLAPS PERIOD (TIMESTAMP '1999-01-01 00:00:00', TIMESTAMP '2002-01-01 00:00:00')"
            tx)))

    (is (= [{:l-updated "2003"}]
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

    (is (= [{:last-updated "2001", :l-updated "2003"}]
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
  (let [tx (xt/submit-tx tu/*node* [[:put-docs {:into :foo, :valid-from #inst "2016", :valid-to  #inst "2019"}
                                     {:xt/id :bill, :name "Bill"}]
                                    [:put-docs {:into :bar, :valid-from #inst "2018", :valid-to  #inst "2020"}
                                     {:xt/id :jeff, :also_name "Jeff"}]])]

    (is (= []
           (query-at-tx
            "SELECT foo.name, bar.also_name
             FROM foo, bar
             WHERE foo.VALID_TIME SUCCEEDS bar.VALID_TIME"
            tx)))

    (is (= [{:name "Bill" :also-name "Jeff"}]
           (query-at-tx
            "SELECT foo.name, bar.also_name
             FROM foo, bar
             WHERE foo.VALID_TIME OVERLAPS bar.VALID_TIME"
            tx)))))

(deftest test-inconsistent-valid-time-range-2494
  (xt/submit-tx tu/*node* [[:sql "INSERT INTO xt_docs (xt$id, xt$valid_to) VALUES (1, DATE '2011-01-01')"]])
  (is (= [{:tx-id 0, :committed? false}]
         (xt/q tu/*node* '(from :xt/txs [{:xt/id tx-id, :xt/committed? committed?}]))))
  (xt/submit-tx tu/*node* [[:sql "INSERT INTO xt_docs (xt$id) VALUES (2)"]])
  (xt/submit-tx tu/*node* [[:sql "DELETE FROM xt_docs FOR PORTION OF VALID_TIME FROM NULL TO ? WHERE xt_docs.xt$id = 2"
                            [#inst "2011"]]])
  ;; TODO what do we want to do about NULL here? atm it works like 'start of time'
  (is (= #{{:tx-id 0, :committed? false}
           {:tx-id 1, :committed? true}
           {:tx-id 2, :committed? true}}
         (set (xt/q tu/*node* '(from :xt/txs [{:xt/id tx-id, :xt/committed? committed?}])))))
  (xt/submit-tx tu/*node* [[:sql "INSERT INTO xt_docs (xt$id) VALUES (3)"]])
  (xt/submit-tx tu/*node* [[:sql "UPDATE xt_docs FOR PORTION OF VALID_TIME FROM NULL TO ? SET foo = 'bar' WHERE xt_docs.xt$id = 3"
                            [#inst "2011"]]])
  (is (= [{:committed? true}]
         (xt/q tu/*node* '(from :xt/txs [{:xt/id 4, :xt/committed? committed?}])))))
