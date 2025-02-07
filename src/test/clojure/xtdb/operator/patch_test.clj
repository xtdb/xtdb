(ns xtdb.operator.patch-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.test-util :as tu]
            [xtdb.time :as time]
            [xtdb.types :as types]
            [xtdb.compactor :as c]))

(t/use-fixtures :each tu/with-allocator tu/with-mock-clock tu/with-node)

(t/deftest test-patch-3879
  (xt/submit-tx tu/*node* [[:put-docs :foo {:xt/id 1, :a 1, :b 2}]])
  (xt/submit-tx tu/*node* [[:patch-docs :foo
                            {:xt/id 1, :c 3}
                            {:xt/id 2, :a 4, :b 5}]])

  (t/is (= [{:xt/id 1, :a 1, :b 2,
             :xt/valid-from (time/->zdt #inst "2020-01-01"),
             :xt/valid-to (time/->zdt #inst "2020-01-02")}
            {:xt/id 1, :a 1, :b 2, :c 3,
             :xt/valid-from (time/->zdt #inst "2020-01-02")}
            {:xt/id 2, :a 4, :b 5,
             :xt/valid-from (time/->zdt #inst "2020-01-02")}]
           (xt/q tu/*node* "SELECT *, _valid_from, _valid_to FROM foo FOR ALL VALID_TIME ORDER BY _id, _valid_from"))))

(t/deftest test-patch-operator
  (letfn [(test [data]
            (->> (tu/query-ra
                  [:patch-gaps {:valid-from (time/->instant #inst "2020-01-02")
                                :valid-to (time/->instant #inst "2020-01-04")}
                   [::tu/pages {'_iid :uuid
                                 '_valid_from types/temporal-col-type
                                 '_valid_to types/nullable-temporal-type
                                 'doc :i64}
                    [(map (partial zipmap [:_iid :_valid_from :_valid_to :doc]) data)]]])
                 (mapv (juxt :xt/iid :xt/valid-from :xt/valid-to :doc))))]

    (let [id #uuid "05ec7111-0ba9-450c-9ef1-0a6e93f65bc3"]
      (t/is (= [[id (time/->zdt #inst "2020-01-02") (time/->zdt #inst "2020-01-04") 1]]
               (test [[id (time/->instant #inst "2020-01-01") nil 1]])))

      (t/is (= [[id (time/->zdt #inst "2020-01-02") (time/->zdt #inst "2020-01-04") 1]]
               (test [[id (time/->instant #inst "2020-01-02") (time/->zdt #inst "2020-01-04") 1]]))
            "exact match")

      (t/is (= [[id (time/->zdt #inst "2020-01-02") (time/->zdt #inst "2020-01-03") nil]
                [id (time/->zdt #inst "2020-01-03") (time/->zdt #inst "2020-01-04") 1]]
               (test [[id (time/->instant #inst "2020-01-03") nil 1]]))
            "adds blank before")

      (t/is (= [[id (time/->zdt #inst "2020-01-02") (time/->zdt #inst "2020-01-03") nil]
                [id (time/->zdt #inst "2020-01-03") (time/->zdt #inst "2020-01-04") 1]]
               (test [[id (time/->instant #inst "2020-01-03") (time/->instant #inst "2020-01-04") 1]]))
            "finishes")

      (t/is (= [[id (time/->zdt #inst "2020-01-02") (time/->zdt #inst "2020-01-03") 1]
                [id (time/->zdt #inst "2020-01-03") (time/->zdt #inst "2020-01-04") nil]]
               (test [[id (time/->instant #inst "2020-01-01") (time/->instant #inst "2020-01-03") 1]]))
            "adds blank after")

      (t/is (= [[id (time/->zdt #inst "2020-01-02") (time/->zdt #inst "2020-01-03") 1]
                [id (time/->zdt #inst "2020-01-03") (time/->zdt #inst "2020-01-04") nil]]
               (test [[id (time/->instant #inst "2020-01-02") (time/->instant #inst "2020-01-03") 1]]))
            "starts")

      (t/is (= [[id (time/->zdt #inst "2020-01-02") (time/->zdt #inst "2020-01-03") nil]
                [id (time/->zdt #inst "2020-01-03") (time/->zdt #inst "2020-01-03T12") 1]
                [id (time/->zdt #inst "2020-01-03T12") (time/->zdt #inst "2020-01-04") nil]]
               (test [[id (time/->instant #inst "2020-01-03") (time/->instant #inst "2020-01-03T12") 1]]))
            "adds blank both sides")

      (t/is (= [[id (time/->zdt #inst "2020-01-02") (time/->zdt #inst "2020-01-02T12") 1]
                [id (time/->zdt #inst "2020-01-02T12") (time/->zdt #inst "2020-01-03T12") nil]
                [id (time/->zdt #inst "2020-01-03T12") (time/->zdt #inst "2020-01-04") 2]]
               (test [[id (time/->instant #inst "2020-01-01") (time/->instant #inst "2020-01-02T12") 1]
                      [id (time/->instant #inst "2020-01-03T12") (time/->instant #inst "2020-01-05") 2]]))
            "adds blank between")

      (let [id2 #uuid "416e77bf-ac95-4e13-a1e8-c3bb301c0292"]
        (t/is (= [[id (time/->zdt #inst "2020-01-02") (time/->zdt #inst "2020-01-02T12") 1]
                  [id (time/->zdt #inst "2020-01-02T12") (time/->zdt #inst "2020-01-04") nil]
                  [id2 (time/->zdt #inst "2020-01-02") (time/->zdt #inst "2020-01-03") nil]
                  [id2 (time/->zdt #inst "2020-01-03") (time/->zdt #inst "2020-01-04") 2]]
                 (test [[id (time/->instant #inst "2020-01-01") (time/->instant #inst "2020-01-02T12") 1]
                        [id2 (time/->instant #inst "2020-01-03") nil 2]])))))))

(t/deftest test-patch-sql
  (letfn [(q [sql]
            (->> (xt/q tu/*node* sql)
                 (map (juxt #(select-keys % [:xt/id :a :b :c :tmp]) :xt/valid-from :xt/valid-to))))]

    (xt/submit-tx tu/*node* ["INSERT INTO foo RECORDS {_id: 1, a: 1, b: 2}"])
    (xt/execute-tx tu/*node* ["PATCH INTO foo RECORDS {_id: 1, c: 3}, {_id: 2, a: 4, b: 5}"])

    (t/is (= [[{:xt/id 1, :a 1, :b 2} (time/->zdt #inst "2020-01-01") (time/->zdt #inst "2020-01-02")]
              [{:xt/id 1, :a 1, :b 2, :c 3} (time/->zdt #inst "2020-01-02") nil]
              [{:xt/id 2, :a 4, :b 5} (time/->zdt #inst "2020-01-02") nil]]
             (q "SELECT *, _valid_from, _valid_to FROM foo FOR ALL VALID_TIME ORDER BY _id, _valid_from")))

    (t/testing "for portion of valid_time"
      (xt/submit-tx tu/*node* ["INSERT INTO bar RECORDS {_id: 1, a: 1, b: 2}"])
      (xt/execute-tx tu/*node* ["PATCH INTO bar FOR VALID_TIME FROM DATE '2020-01-05' TO DATE '2020-01-07' RECORDS {_id: 1, tmp: 'hi!'}"])
      (xt/execute-tx tu/*node* [["PATCH INTO bar FOR VALID_TIME FROM ? RECORDS {_id: 2, a: 6, b: 8}" #inst "2020-01-08"]])

      (t/is (= [[{:xt/id 1, :a 1, :b 2} (time/->zdt #inst "2020-01-03") (time/->zdt #inst "2020-01-05")]
                [{:xt/id 1, :a 1, :b 2, :tmp "hi!"} (time/->zdt #inst "2020-01-05") (time/->zdt #inst "2020-01-07")]
                [{:xt/id 1, :a 1, :b 2} (time/->zdt #inst "2020-01-07") nil]
                [{:xt/id 2, :a 6, :b 8} (time/->zdt #inst "2020-01-08") nil]]
               (q "SELECT *, _valid_from, _valid_to FROM bar FOR ALL VALID_TIME ORDER BY _id, _valid_from"))))))

(t/deftest patch-with-forbidden-columns-fails-4120
  (xt/submit-tx tu/*node* [[:sql "INSERT INTO docs RECORDS {_id: 1}"]])

  (t/testing "patching with forbidden columns directly"
    (xt/execute-tx tu/*node* ["PATCH INTO docs RECORDS {_id: 1,
                                                        _valid_from: TIMESTAMP '2020-01-01 00:00:00+00:00',
                                                        _valid_to: TIMESTAMP '2030-01-01 00:00:00+00:00'}"])

    (t/is (= [{:xt/id 1,
               :committed false,
               :error
               #xt/illegal-arg [:xtdb/sql-error "Errors planning SQL statement:
  - Cannot PATCH (_valid_from _valid_to) column" {:errors [{:col (_valid_from _valid_to)}]}],
               :system-time #xt/zoned-date-time "2020-01-02T00:00Z[UTC]"}]
             (xt/q tu/*node* "SELECT * FROM xt.txs ORDER BY _id DESC LIMIT 1"))))

  (t/testing "patching with forbidden columns in parameters"
    (xt/submit-tx tu/*node* [[:sql "PATCH INTO docs RECORDS ? "
                              [{:_id 1 :_valid_from (time/->zdt #inst "2022") :_valid_to (time/->zdt #inst "2028")}]]])

    (t/is (= [{:xt/id 2,
               :committed false,
               :error
               #xt/illegal-arg [:xtdb/sql-error "Errors planning SQL statement:
  - Cannot PATCH (_valid_from _valid_to) column" {:errors [{:col (_valid_from _valid_to)}]}],
               :system-time #xt/zoned-date-time "2020-01-03T00:00Z[UTC]"}]
             (xt/q tu/*node* "SELECT * FROM xt.txs ORDER BY _id DESC LIMIT 1"))))

  (t/is (= [{:xt/id 1, :xt/valid-from #xt/zoned-date-time "2020-01-01T00:00Z[UTC]"}]
           (xt/q tu/*node* "SELECT *,_valid_from, _valid_to FROM docs")))

  (t/testing "works after compaction"
    (tu/finish-block! tu/*node*)
    (c/compact-all! tu/*node*)

    (t/is (= [{:xt/id 1, :xt/valid-from #xt/zoned-date-time "2020-01-01T00:00Z[UTC]"}]
             (xt/q tu/*node* "SELECT *,_valid_from, _valid_to FROM docs")))))
