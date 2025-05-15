(ns xtdb.sql.temporal-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.test-util :as tu]
            [xtdb.time :as time]))

(t/use-fixtures :each tu/with-mock-clock tu/with-node)

(defn query-at [query {:keys [system-time]}]
  (xt/q tu/*node* query {:snapshot-time system-time}))

(t/deftest all-system-time
  (xt/submit-tx tu/*node* [[:put-docs :foo {:xt/id :my-doc, :last-updated "tx1"}]] {:system-time #inst "3000"})
  (xt/submit-tx tu/*node* [[:put-docs :foo {:xt/id :my-doc, :last-updated "tx2"}]] {:system-time #inst "3001"})

  (t/is (= {{:last-updated "tx1"} 1, {:last-updated "tx2"}, 1}
           (frequencies (xt/q tu/*node* "SELECT foo.last_updated FROM foo FOR ALL VALID_TIME"))))

  (t/is (= {{:last-updated "tx1"} 2, {:last-updated "tx2"} 1}
           (frequencies
            (xt/q tu/*node* "SELECT foo.last_updated FROM foo FOR ALL SYSTEM_TIME FOR ALL VALID_TIME"))))

  (t/is (= {{:last-updated "tx1"} 2, {:last-updated "tx2"} 1}
           (frequencies (xt/q tu/*node* "SELECT foo.last_updated FROM foo FOR SYSTEM_TIME ALL FOR VALID_TIME ALL")))))

(t/deftest system-time-as-of
  (let [tx (xt/execute-tx tu/*node* [[:put-docs :foo {:xt/id :my-doc, :last-updated "tx1"}]] {:system-time #inst "3000"})
        tx2 (xt/execute-tx tu/*node* [[:put-docs :foo {:xt/id :my-doc, :last-updated "tx2"}]] {:system-time #inst "3001"})]

    (t/is (= []
             (query-at
              "SELECT foo.last_updated FROM foo FOR ALL VALID_TIME FOR SYSTEM_TIME AS OF TIMESTAMP '2999-01-01 00:00:00+00:00'"
              tx)))

    (t/is (= [{:last-updated "tx1"}]
             (query-at
              "SELECT foo.last_updated FROM foo FOR ALL VALID_TIME FOR SYSTEM_TIME AS OF TIMESTAMP '3000-01-01 00:00:00+00:00'"
              tx)))

    (t/is (= #{{:last-updated "tx1"} {:last-updated "tx2"}}
             (set (query-at
                   "SELECT foo.last_updated FROM foo FOR ALL VALID_TIME FOR SYSTEM_TIME AS OF TIMESTAMP '3002-01-01 00:00:00+00:00'"
                   tx2))))))

(t/deftest system-time-from-a-to-b
  (let [tx (xt/execute-tx tu/*node* [[:put-docs :foo {:xt/id :my-doc, :last-updated "tx1"}]] {:system-time #inst "3000"})
        tx2 (xt/execute-tx tu/*node* [[:put-docs :foo {:xt/id :my-doc, :last-updated "tx2"}]] {:system-time #inst "3001"})]

    (t/is (= []
             (query-at
              "SELECT foo.last_updated FROM foo FOR SYSTEM_TIME FROM DATE '2999-01-01' TO TIMESTAMP '3000-01-01 00:00:00+00:00'"
              tx)))

    (t/is (= [{:last-updated "tx1"}]
             (query-at
              "SETTING DEFAULT VALID_TIME ALL
             SELECT foo.last_updated FROM foo FOR SYSTEM_TIME FROM DATE '2999-01-01' TO TIMESTAMP '3001-01-01 00:00:00+00:00'"
              tx)))

    (t/is (= [{:last-updated "tx1"} {:last-updated "tx1"} {:last-updated "tx2"}]
             (query-at
              "SETTING DEFAULT VALID_TIME ALL
             SELECT foo.last_updated FROM foo FOR SYSTEM_TIME FROM DATE '2999-01-01' TO TIMESTAMP '3002-01-01 00:00:00+00:00'
             ORDER BY last_updated"
              tx2)))

    (t/is (= #{{:last-updated "tx1"} {:last-updated "tx2"}}
             (set (query-at
                   "SETTING DEFAULT VALID_TIME ALL
                  SELECT foo.last_updated FROM foo FOR SYSTEM_TIME FROM DATE '3001-01-01' TO TIMESTAMP '3002-01-01 00:00:00+00:00'"
                   tx2))))))

(t/deftest system-time-between-a-to-b
  (let [tx (xt/execute-tx tu/*node* [[:put-docs :foo {:xt/id :my-doc, :last-updated "tx1"}]] {:system-time #inst "3000"})
        tx2 (xt/execute-tx tu/*node* [[:put-docs :foo {:xt/id :my-doc, :last-updated "tx2"}]] {:system-time #inst "3001"})]
    (t/is (= []
             (query-at
              "SELECT foo.last_updated FROM foo FOR SYSTEM_TIME BETWEEN DATE '2998-01-01' AND TIMESTAMP '2999-01-01 00:00:00+00:00'"
              tx)))

    (t/is (= [{:last-updated "tx1"}]
             (query-at
              "SETTING DEFAULT VALID_TIME ALL
             SELECT foo.last_updated FROM foo FOR SYSTEM_TIME BETWEEN DATE '2999-01-01' AND TIMESTAMP '3000-01-01 00:00:00+00:00'"
              tx))
          "second point in time is inclusive for between")

    (t/is (= [{:last-updated "tx1"}]
             (query-at
              "SETTING DEFAULT VALID_TIME ALL
             SELECT foo.last_updated FROM foo FOR SYSTEM_TIME BETWEEN DATE '2999-01-01' AND TIMESTAMP '3001-01-01 00:00:00+00:00'"
              tx)))

    (t/is (= [{:last-updated "tx1"} {:last-updated "tx1"} {:last-updated "tx2"}]
             (query-at
              "SETTING DEFAULT VALID_TIME ALL
             SELECT foo.last_updated FROM foo FOR SYSTEM_TIME BETWEEN DATE '2999-01-01' AND TIMESTAMP '3002-01-01 00:00:00+00:00'
             ORDER BY last_updated"
              tx2)))

    (t/is (= #{{:last-updated "tx1"} {:last-updated "tx2"}}
             (set (query-at
                   "SETTING DEFAULT VALID_TIME ALL
                  SELECT foo.last_updated FROM foo FOR SYSTEM_TIME BETWEEN DATE '3001-01-01' AND TIMESTAMP '3002-01-01 00:00:00+00:00'"
                   tx2))))))

(t/deftest app-time-period-predicates
  (t/testing "OVERLAPS"
    (let [tx (xt/execute-tx tu/*node* [[:put-docs {:into :foo, :valid-from #inst "2000"}
                                        {:xt/id :my-doc, :last-updated "2000"}]

                                       [:put-docs {:into :foo, :valid-from #inst "3000"}
                                        {:xt/id :my-doc, :last-updated "3000"}]

                                       [:put-docs {:into :foo, :valid-from #inst "4000", :valid-to #inst "4001"}
                                        {:xt/id :some-other-doc, :last-updated "4000"}]])]

      (t/is (= [{:last-updated "2000"} {:last-updated "3000"} {:last-updated "4000"}]
               (query-at
                "SELECT foo.last_updated FROM foo FOR ALL VALID_TIME ORDER BY last_updated"
                tx)))

      (t/is (= [{:last-updated "2000"}]
               (query-at
                "SELECT foo.last_updated FROM foo FOR ALL VALID_TIME WHERE foo._VALID_TIME OVERLAPS PERIOD (TIMESTAMP '2000-01-01 00:00:00', TIMESTAMP '2001-01-01 00:00:00')"
                tx)))

      (t/is (= [{:last-updated "3000"}]
               (query-at
                "SELECT foo.last_updated FROM foo FOR ALL VALID_TIME WHERE foo._VALID_TIME OVERLAPS PERIOD (TIMESTAMP '3000-01-01 00:00:00', TIMESTAMP '3001-01-01 00:00:00')"
                tx)))

      (t/is (= [{:last-updated "3000"} {:last-updated "4000"}]
               (query-at
                "SELECT foo.last_updated FROM foo FOR ALL VALID_TIME WHERE foo._VALID_TIME OVERLAPS PERIOD (TIMESTAMP '4000-01-01 00:00:00', TIMESTAMP '4001-01-01 00:00:00')"
                tx)))

      (t/is (= [{:last-updated "3000"}]
               (query-at
                "SELECT foo.last_updated FROM foo FOR ALL VALID_TIME WHERE foo._VALID_TIME OVERLAPS PERIOD (TIMESTAMP '4002-01-01 00:00:00', TIMESTAMP '9999-01-01 00:00:00')"
                tx)))

      (t/testing "#3935"
        (t/is (= [{:last-updated "2000"}]
                 (query-at "SELECT last_updated FROM foo FOR ALL VALID_TIME WHERE foo._VALID_TIME CONTAINS TIMESTAMP '2500-01-01T00:00:00'" tx)))

        (t/is (= [{:last-updated "3000"}]
                 (query-at "SELECT last_updated FROM foo FOR ALL VALID_TIME WHERE foo._VALID_TIME CONTAINS DATE '3500-01-01'" tx)))))))

(t/deftest app-time-multiple-tables
  (let [tx (xt/execute-tx tu/*node* [[:put-docs {:into :foo, :valid-from #inst "2000", :valid-to #inst "2001"}
                                      {:xt/id :foo-doc, :last-updated "2001" }]

                                     [:put-docs {:into :bar, :valid-from #inst "2002", :valid-to #inst "2003"}
                                      {:xt/id :bar-doc, :l_updated "2003" :name "test"}]])]

    (t/is (= [{:last-updated "2001"}]
             (query-at
              "SELECT foo.last_updated FROM foo FOR ALL VALID_TIME
             WHERE foo._VALID_TIME OVERLAPS PERIOD (TIMESTAMP '1999-01-01 00:00:00Z', TIMESTAMP '2002-01-01 00:00:00Z')"
              tx)))

    (t/is (= [{:l-updated "2003"}]
             (query-at
              "SELECT bar.l_updated FROM bar FOR ALL VALID_TIME
             WHERE bar._VALID_TIME OVERLAPS PERIOD (TIMESTAMP '2002-01-01 00:00:00Z', TIMESTAMP '2003-01-01 00:00:00Z')"
              tx)))

    (t/is (= []
             (query-at
              "SELECT foo.last_updated, bar.l_updated FROM foo, bar
             WHERE foo._VALID_TIME OVERLAPS PERIOD (TIMESTAMP '1999-01-01 00:00:00Z', TIMESTAMP '2001-01-01 00:00:00Z')
             AND
             bar._VALID_TIME OVERLAPS PERIOD (TIMESTAMP '2000-01-01 00:00:00Z', TIMESTAMP '2001-01-01 00:00:00Z')"
              tx)))

    (t/is (= [{:last-updated "2001", :l-updated "2003"}]
             (query-at
              "SETTING DEFAULT VALID_TIME ALL
             SELECT foo.last_updated, bar.l_updated FROM foo, bar
             WHERE foo._VALID_TIME OVERLAPS PERIOD (TIMESTAMP '2000-01-01 00:00:00Z', TIMESTAMP '2001-01-01 00:00:00Z')
               AND bar._VALID_TIME OVERLAPS PERIOD (TIMESTAMP '2002-01-01 00:00:00Z', TIMESTAMP '2003-01-01 00:00:00Z')"
              tx)))

    (t/is (= []
             (query-at
              "SELECT foo.last_updated, bar.name FROM foo, bar
             WHERE foo._VALID_TIME OVERLAPS bar._VALID_TIME" tx)))))

(t/deftest app-time-joins
  (let [tx (xt/execute-tx tu/*node* [[:put-docs {:into :foo, :valid-from #inst "2016", :valid-to #inst "2019"}
                                      {:xt/id :bill, :name "Bill"}]
                                     [:put-docs {:into :bar, :valid-from #inst "2018", :valid-to #inst "2020"}
                                      {:xt/id :jeff, :also_name "Jeff"}]])]

    (t/is (= []
             (query-at
              "SELECT foo.name, bar.also_name
             FROM foo, bar
             WHERE foo._VALID_TIME SUCCEEDS bar._VALID_TIME"
              tx)))

    (t/is (= [{:name "Bill" :also-name "Jeff"}]
             (query-at
              "SETTING DEFAULT VALID_TIME ALL
             SELECT foo.name, bar.also_name
             FROM foo, bar
             WHERE foo._VALID_TIME OVERLAPS bar._VALID_TIME"
              tx)))))

(t/deftest test-inconsistent-valid-time-range-2494
  (xt/submit-tx tu/*node* [[:sql "INSERT INTO xt_docs (_id, _valid_to) VALUES (1, DATE '2011-01-01')"]])
  (t/is (= [{:tx-id 0, :committed? false}]
           (xt/q tu/*node* '(from :xt/txs [{:xt/id tx-id, :committed committed?}]))))
  (xt/submit-tx tu/*node* [[:sql "INSERT INTO xt_docs (_id) VALUES (2)"]])
  (xt/submit-tx tu/*node* [[:sql "DELETE FROM xt_docs FOR PORTION OF VALID_TIME FROM NULL TO ? WHERE xt_docs._id = 2"
                            [#inst "2011"]]])
  ;; TODO what do we want to do about NULL here? atm it works like 'start of time'
  (t/is (= #{{:tx-id 0, :committed? false}
             {:tx-id 1, :committed? true}
             {:tx-id 2, :committed? true}}
           (set (xt/q tu/*node* '(from :xt/txs [{:xt/id tx-id, :committed committed?}])))))
  (xt/submit-tx tu/*node* [[:sql "INSERT INTO xt_docs (_id) VALUES (3)"]])
  (xt/submit-tx tu/*node* [[:sql "UPDATE xt_docs FOR PORTION OF VALID_TIME FROM NULL TO ? SET foo = 'bar' WHERE xt_docs._id = 3"
                            [#inst "2011"]]])
  (t/is (= [{:committed? true}]
           (xt/q tu/*node* '(from :xt/txs [{:xt/id 4, :committed committed?}])))))


(t/deftest test-period-intersection-3493
  (t/is (= [{:intersection
             #xt/tstz-range [#xt/zoned-date-time "2021-01-01T00:00Z"
                             #xt/zoned-date-time "2022-01-01T00:00Z"]}]
           (xt/q tu/*node* ["SELECT PERIOD(?, ?) * PERIOD(?, ?) AS intersection"

                            (time/->zdt #inst "2020-01-01")
                            (time/->zdt #inst "2022-01-01")
                            (time/->zdt #inst "2021-01-01")
                            (time/->zdt #inst "2023-01-01")])))

  (t/is (= [{:variadic
             #xt/tstz-range [#xt/zoned-date-time "2022-01-01T00:00Z"
                             #xt/zoned-date-time "2024-01-01T00:00Z"]}]
           (xt/q tu/*node* ["SELECT PERIOD(?, ?) * PERIOD(?, ?) * PERIOD(?, ?) AS variadic"

                            (time/->zdt #inst "2020-01-01")
                            (time/->zdt #inst "2024-01-01")
                            (time/->zdt #inst "2021-01-01")
                            (time/->zdt #inst "2025-01-01")
                            (time/->zdt #inst "2022-01-01")
                            (time/->zdt #inst "2026-01-01")]))
        "variadic")

  (xt/submit-tx tu/*node* [[:put-docs :foo {:xt/id 1}]])
  (xt/submit-tx tu/*node* [[:put-docs :bar {:xt/id 1}]])
  (xt/submit-tx tu/*node* [[:delete-docs :foo 1]])

  (t/is (= [{:xt/id 1,
             :xt/valid-time #xt/tstz-range [#xt/zoned-date-time "2020-01-01T00:00Z"
                                            #xt/zoned-date-time "2020-01-03T00:00Z"]}]
           (xt/q tu/*node* "SELECT _id, _valid_time FROM foo FOR ALL VALID_TIME")))

  (t/is (= [{:xt/id 1,
             :xt/valid-time #xt/tstz-range [#xt/zoned-date-time "2020-01-02T00:00Z" nil]}]
           (xt/q tu/*node* "SELECT _id, _valid_time FROM bar FOR ALL VALID_TIME")))

  (t/is (= [{:xt/id 1,
             :intersection
             #xt/tstz-range [#xt/zoned-date-time "2020-01-02T00:00:00Z"
                             #xt/zoned-date-time "2020-01-03T00:00:00Z"]}]
           (xt/q tu/*node* "SETTING DEFAULT VALID_TIME ALL
                            SELECT _id, foo._valid_time * bar._valid_time AS intersection
                            FROM foo JOIN bar USING (_id)
                            WHERE foo._valid_time OVERLAPS bar._valid_time"))))

(t/deftest update-delete-and-patch-explicit-null-behaviour
  (xt/execute-tx tu/*node* [[:sql "INSERT INTO users RECORDS {_id: ?, _valid_from: ?, _valid_to: ?}" [1 #inst "2010" #inst "2040"]]
                            ])

  (xt/execute-tx tu/*node* [[:sql "UPDATE users FOR PORTION OF VALID_TIME FROM NULL TO ? SET foo = 1 WHERE _id = 1"
                             [#inst "2020"]]])

  (xt/execute-tx tu/*node* [[:sql "UPDATE users FOR PORTION OF VALID_TIME FROM ? TO NULL SET foo = 2 WHERE _id = 1"
                             [#inst "2030"]]])

  (t/is (= #{{:xt/id 1, :foo 2, :xt/valid-from #xt/zdt "2030-01-01T00:00Z[UTC]", :xt/valid-to #xt/zdt "2040-01-01T00:00Z[UTC]"}
             {:xt/id 1, :foo 1, :xt/valid-from #xt/zdt "2010-01-01T00:00Z[UTC]", :xt/valid-to #xt/zdt "2020-01-01T00:00Z[UTC]"}
             {:xt/id 1, :xt/valid-from #xt/zdt "2020-01-01T00:00Z[UTC]", :xt/valid-to #xt/zdt "2030-01-01T00:00Z[UTC]"}}
           (set (xt/q tu/*node* "SELECT *, _valid_from, _valid_to FROM users FOR ALL VALID_TIME WHERE _id = 1 "))))


  (xt/execute-tx tu/*node* [[:sql "PATCH INTO users FOR PORTION OF VALID_TIME FROM NULL TO NULL RECORDS {_id: 1, foo: 3}"]])


  (t/is (= [{:xt/id 1,
             :foo 3,
             :xt/valid-from #xt/zdt "-290308-12-21T19:59:05.224192Z[UTC]",
             :xt/valid-to #xt/zdt "2010-01-01T00:00Z[UTC]"}
            {:xt/id 1,
             :foo 3,
             :xt/valid-from #xt/zdt "2010-01-01T00:00Z[UTC]",
             :xt/valid-to #xt/zdt "2020-01-01T00:00Z[UTC]"}
            {:xt/id 1,
             :foo 3,
             :xt/valid-from #xt/zdt "2020-01-01T00:00Z[UTC]",
             :xt/valid-to #xt/zdt "2030-01-01T00:00Z[UTC]"}
            {:xt/id 1,
             :foo 3,
             :xt/valid-from #xt/zdt "2030-01-01T00:00Z[UTC]",
             :xt/valid-to #xt/zdt "2040-01-01T00:00Z[UTC]"}
            {:xt/id 1, :foo 3, :xt/valid-from #xt/zdt "2040-01-01T00:00Z[UTC]"}]
           (xt/q tu/*node* "SELECT *, _valid_from, _valid_to FROM users FOR ALL VALID_TIME WHERE _id = 1 ORDER BY _valid_from")))

  (xt/execute-tx tu/*node* ["DELETE FROM users FOR PORTION OF VALID_TIME FROM NULL TO NULL WHERE _id = 1"])

  (t/is (= [] (xt/q tu/*node* "SELECT *, _valid_from, _valid_to FROM users FOR ALL VALID_TIME WHERE _id = 1 "))))
