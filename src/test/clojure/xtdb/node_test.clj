(ns xtdb.node-test
  (:require [clojure.test :as t :refer [deftest]]
            [xtdb.api :as xt]
            [xtdb.node :as xtn]
            [xtdb.test-util :as tu]
            [xtdb.time :as time])
  (:import [xtdb.tx Ops]
           (xtdb.query Expr Query OutSpec)))

(t/use-fixtures :each tu/with-mock-clock tu/with-node)

(t/deftest test-multi-value-insert-423
  (letfn [(expected [tt]
            {[(time/->zdt #inst "2024-01-01") (time/->zdt #inst "2025-01-01"), tt nil]
             "Happy 2024!"

             [(time/->zdt #inst "2025-01-01") (time/->zdt #inst "2026-01-01"), tt nil]
             "Happy 2025!",

             [(time/->zdt #inst "2026-01-01") nil, tt nil]
             "Happy 2026!"})

          (q [table]
            (->> (xt/q tu/*node* (format "
SELECT p.xt$id, p.text,
       p.xt$valid_from, p.xt$valid_to,
       p.xt$system_from, p.xt$system_to
FROM %s FOR ALL SYSTEM_TIME AS p"
                                         table)
                       {:default-all-valid-time? true})
                 (into {} (map (juxt (juxt :xt$valid_from :xt$valid_to
                                           :xt$system_from :xt$system_to)
                                     :text)))))]

    (xt/submit-tx tu/*node* [(xt/sql-op "
INSERT INTO posts (xt$id, text, xt$valid_from)
VALUES (1, 'Happy 2024!', DATE '2024-01-01'),
       (1, 'Happy 2025!', DATE '2025-01-01'),
       (1, 'Happy 2026!', DATE '2026-01-01')")])

    (t/is (= (expected (time/->zdt #inst "2020-01-01"))
             (q "posts")))

    (xt/submit-tx tu/*node* [(xt/sql-op "INSERT INTO posts2 (xt$id, text, xt$valid_from) VALUES (1, 'Happy 2024!', DATE '2024-01-01')")
                             (xt/sql-op "INSERT INTO posts2 (xt$id, text, xt$valid_from) VALUES (1, 'Happy 2025!', DATE '2025-01-01')")
                             (xt/sql-op "INSERT INTO posts2 (xt$id, text, xt$valid_from) VALUES (1, 'Happy 2026!', DATE '2026-01-01')")])

    (t/is (= (expected (time/->zdt #inst "2020-01-02"))
             (q "posts2")))))

(t/deftest test-dml-sees-in-tx-docs
  (xt/submit-tx tu/*node* [(xt/sql-op "INSERT INTO foo (xt$id, v) VALUES ('foo', 0)")
                           (xt/sql-op "UPDATE foo SET v = 1")])

  (t/is (= [{:xt$id "foo", :v 1}]
           (xt/q tu/*node* "SELECT foo.xt$id, foo.v FROM foo"))))

(t/deftest test-delete-without-search-315
  (letfn [(q []
            (xt/q tu/*node* "SELECT foo.xt$id, foo.xt$valid_from, foo.xt$valid_to FROM foo"
                  {:default-all-valid-time? true}))]
    (xt/submit-tx tu/*node* [(xt/sql-op "INSERT INTO foo (xt$id) VALUES ('foo')")])

    (t/is (= [{:xt$id "foo",
               :xt$valid_from (time/->zdt #inst "2020")
               :xt$valid_to nil}]
             (q)))

    (xt/submit-tx tu/*node* [(xt/sql-op "DELETE FROM foo")])

    (t/is (= [{:xt$id "foo"
               :xt$valid_from (time/->zdt #inst "2020")
               :xt$valid_to (time/->zdt #inst "2020-01-02")}]
             (q)))

    (xt/submit-tx tu/*node* [(xt/sql-op "DELETE FROM foo")]
                  {:default-all-valid-time? true})

    (t/is (= [] (q)))))

(t/deftest test-update-set-field-from-param-328
  (xt/submit-tx tu/*node* [(-> (xt/sql-op "INSERT INTO users (xt$id, first_name, last_name) VALUES (?, ?, ?)")
                               (xt/with-op-args ["susan", "Susan", "Smith"]))])

  (xt/submit-tx tu/*node* [(-> (xt/sql-op "UPDATE users FOR PORTION OF VALID_TIME FROM ? TO NULL AS u SET first_name = ? WHERE u.xt$id = ?")
                               (xt/with-op-args [#inst "2021", "sue", "susan"]))])

  (t/is (= #{["Susan" "Smith", (time/->zdt #inst "2020") (time/->zdt #inst "2021")]
             ["sue" "Smith", (time/->zdt #inst "2021") nil]}
           (->> (xt/q tu/*node* "SELECT u.first_name, u.last_name, u.xt$valid_from, u.xt$valid_to FROM users u"
                      {:default-all-valid-time? true})
                (into #{} (map (juxt :first_name :last_name :xt$valid_from :xt$valid_to)))))))

(t/deftest test-can-submit-same-id-into-multiple-tables-338
  (let [tx1 (xt/submit-tx tu/*node* [(xt/sql-op "INSERT INTO t1 (xt$id, foo) VALUES ('thing', 't1-foo')")
                                     (xt/sql-op "INSERT INTO t2 (xt$id, foo) VALUES ('thing', 't2-foo')")])
        tx2 (xt/submit-tx tu/*node* [(xt/sql-op "UPDATE t2 SET foo = 't2-foo-v2' WHERE t2.xt$id = 'thing'")])]

    (t/is (= [{:xt$id "thing", :foo "t1-foo"}]
             (xt/q tu/*node* "SELECT t1.xt$id, t1.foo FROM t1"
                   {:basis {:at-tx tx1}})))

    (t/is (= [{:xt$id "thing", :foo "t1-foo"}]
             (xt/q tu/*node* "SELECT t1.xt$id, t1.foo FROM t1"
                   {:basis {:at-tx tx2}})))

    (t/is (= [{:xt$id "thing", :foo "t2-foo"}]
             (xt/q tu/*node* "SELECT t2.xt$id, t2.foo FROM t2"
                   {:basis {:at-tx tx1}})))

    (t/is (= [{:xt$id "thing", :foo "t2-foo-v2"}]
             (xt/q tu/*node* "SELECT t2.xt$id, t2.foo FROM t2"
                   {:basis {:at-tx tx2}, :default-all-valid-time? false})))))

(t/deftest test-put-delete-with-implicit-tables-338
  (letfn [(foos []
            (->> (for [[tk tn] {:xt "docs"
                                :t1 "explicit_table1"
                                :t2 "explicit_table2"}]
                   [tk (->> (xt/q tu/*node* (format "SELECT t.xt$id, t.v FROM %s t WHERE t.xt$valid_to = END_OF_TIME" tn))
                            (into #{} (map :v)))])
                 (into {})))]

    (xt/submit-tx tu/*node*
                  [(xt/put :docs {:xt/id :foo, :v "implicit table"})
                   (xt/put :explicit_table1 {:xt/id :foo, :v "explicit table 1"})
                   (xt/put :explicit_table2 {:xt/id :foo, :v "explicit table 2"})])

    (t/is (= {:xt #{"implicit table"}, :t1 #{"explicit table 1"}, :t2 #{"explicit table 2"}}
             (foos)))

    (xt/submit-tx tu/*node* [(xt/delete :docs :foo)])

    (t/is (= {:xt #{}, :t1 #{"explicit table 1"}, :t2 #{"explicit table 2"}}
             (foos)))

    (xt/submit-tx tu/*node* [(xt/delete :explicit_table1 :foo)])

    (t/is (= {:xt #{}, :t1 #{}, :t2 #{"explicit table 2"}}
             (foos)))))

(t/deftest test-array-element-reference-is-one-based-336
  (xt/submit-tx tu/*node* [(xt/sql-op "INSERT INTO foo (xt$id, arr) VALUES ('foo', ARRAY[9, 8, 7, 6])")])

  (t/is (= [{:xt$id "foo", :arr [9 8 7 6], :fst 9, :snd 8, :lst 6}]
           (xt/q tu/*node* "SELECT foo.xt$id, foo.arr, foo.arr[1] AS fst, foo.arr[2] AS snd, foo.arr[4] AS lst FROM foo"))))

(t/deftest test-interval-literal-cce-271
  (t/is (= [{:a #xt/interval-ym "P12M"}]
           (xt/q tu/*node* "select a.a from (values (1 year)) a (a)"))))

(t/deftest test-overrides-range
  (xt/submit-tx tu/*node* [(xt/sql-op "
INSERT INTO foo (xt$id, v, xt$valid_from, xt$valid_to)
VALUES (1, 1, DATE '1998-01-01', DATE '2000-01-01')")])

  (xt/submit-tx tu/*node* [(xt/sql-op "
INSERT INTO foo (xt$id, v, xt$valid_from, xt$valid_to)
VALUES (1, 2, DATE '1997-01-01', DATE '2001-01-01')")])

  (t/is (= #{{:xt$id 1, :v 1,
              :xt$valid_from (time/->zdt #inst "1998")
              :xt$valid_to (time/->zdt #inst "2000")
              :xt$system_from (time/->zdt #inst "2020-01-01")
              :xt$system_to (time/->zdt #inst "2020-01-02")}
             {:xt$id 1, :v 2,
              :xt$valid_from (time/->zdt #inst "1997")
              :xt$valid_to (time/->zdt #inst "2001")
              :xt$system_from (time/->zdt #inst "2020-01-02")
              :xt$system_to nil}}

           (set (xt/q tu/*node* "
SELECT foo.xt$id, foo.v,
       foo.xt$valid_from, foo.xt$valid_to,
       foo.xt$system_from, foo.xt$system_to
FROM foo FOR ALL SYSTEM_TIME FOR ALL VALID_TIME")))))

(t/deftest test-current-timestamp-in-temporal-constraint-409
  (xt/submit-tx tu/*node* [(xt/sql-op "
INSERT INTO foo (xt$id, v)
VALUES (1, 1)")])

  (t/is (= [{:xt$id 1, :v 1,
             :xt$valid_from (time/->zdt #inst "2020")
             :xt$valid_to nil}]
           (xt/q tu/*node* "SELECT foo.xt$id, foo.v, foo.xt$valid_from, foo.xt$valid_to FROM foo")))

  (t/is (= []
           (xt/q tu/*node* "
SELECT foo.xt$id, foo.v, foo.xt$valid_from, foo.xt$valid_to
FROM foo FOR VALID_TIME AS OF DATE '1999-01-01'"
                 {:basis {:current-time (time/->instant #inst "1999")}})))

  (t/is (= []
           (xt/q tu/*node* "
SELECT foo.xt$id, foo.v, foo.xt$valid_from, foo.xt$valid_to
FROM foo FOR VALID_TIME AS OF CURRENT_TIMESTAMP"
                 {:basis {:current-time (time/->instant #inst "1999")}}))))

(t/deftest test-repeated-row-id-scan-bug-also-409
  (xt/submit-tx tu/*node* [(xt/sql-op "INSERT INTO foo (xt$id, v) VALUES (1, 1)")])

  (letfn [(q1 [opts]
            (xt/q tu/*node* "
SELECT foo.xt$id, foo.v, foo.xt$valid_from, foo.xt$valid_to
FROM foo
ORDER BY foo.xt$valid_from"
                  opts))
          (q2 [opts]
            (frequencies
             (xt/q tu/*node* "SELECT foo.xt$id, foo.v FROM foo" opts)))]

    (let [tx1 (xt/submit-tx tu/*node* [(xt/sql-op "
UPDATE foo
FOR PORTION OF VALID_TIME FROM DATE '2022-01-01' TO DATE '2024-01-01'
SET v = 2
WHERE foo.xt$id = 1")])

          tx2 (xt/submit-tx tu/*node* [(xt/sql-op "
DELETE FROM foo
FOR PORTION OF VALID_TIME FROM DATE '2023-01-01' TO DATE '2025-01-01'
WHERE foo.xt$id = 1")])]

      (t/is (= [{:xt$id 1, :v 1
                 :xt$valid_from (time/->zdt #inst "2020")
                 :xt$valid_to (time/->zdt #inst "2022")}
                {:xt$id 1, :v 2
                 :xt$valid_from (time/->zdt #inst "2022")
                 :xt$valid_to (time/->zdt #inst "2024")}
                {:xt$id 1, :v 1
                 :xt$valid_from (time/->zdt #inst "2024")
                 :xt$valid_to nil}]

               (q1 {:basis {:at-tx tx1}, :default-all-valid-time? true})))

      (t/is (= {{:xt$id 1, :v 1} 2, {:xt$id 1, :v 2} 1}
               (q2 {:basis {:at-tx tx1}, :default-all-valid-time? true})))

      (t/is (= [{:xt$id 1, :v 1
                 :xt$valid_from (time/->zdt #inst "2020")
                 :xt$valid_to (time/->zdt #inst "2022")}
                {:xt$id 1, :v 2
                 :xt$valid_from (time/->zdt #inst "2022")
                 :xt$valid_to (time/->zdt #inst "2023")}
                {:xt$id 1, :v 1
                 :xt$valid_from (time/->zdt #inst "2025")
                 :xt$valid_to nil}]

               (q1 {:basis {:at-tx tx2}, :default-all-valid-time? true})))

      (t/is (= [{:xt$id 1, :v 1
                 :xt$valid_from (time/->zdt #inst "2025")
                 :xt$valid_to nil}]

               (q1 {:basis {:at-tx tx2, :current-time (time/->instant #inst "2026")}
                    :default-all-valid-time? false})))

      (t/is (= {{:xt$id 1, :v 1} 2, {:xt$id 1, :v 2} 1}
               (q2 {:basis {:at-tx tx2}, :default-all-valid-time? true}))))))

(t/deftest test-error-handling-inserting-strings-into-app-time-cols-397
  (xt/submit-tx tu/*node* [(xt/sql-op "INSERT INTO foo (xt$id, xt$valid_from) VALUES (1, '2018-01-01')")])

  ;; TODO check the rollback error when it's available, #401
  (t/is (= [] (xt/q tu/*node* "SELECT foo.xt$id FROM foo"))))

(t/deftest test-vector-type-mismatch-245
  (t/is (= [{:a [4 "2"]}]
           (xt/q tu/*node* "SELECT a.a FROM (VALUES (ARRAY [4, '2'])) a (a)")))

  (t/is (= [{:a [["hello"] "world"]}]
           (xt/q tu/*node* "SELECT a.a FROM (VALUES (ARRAY [['hello'], 'world'])) a (a)"))))

#_ ;TODO
(t/deftest test-double-quoted-col-refs
  (xt/submit-tx tu/*node* [(xt/sql-op "INSERT INTO foo (xt$id, \"kebab-case-col\") VALUES (1, 'kebab-case-value')")])
  (t/is (= [{:xt$id 1, :kebab-case-col "kebab-case-value"}]
           (xt/q tu/*node* "SELECT foo.xt$id, foo.\"kebab-case-col\" FROM foo WHERE foo.\"kebab-case-col\" = 'kebab-case-value'")))

  (xt/submit-tx tu/*node* [(xt/sql-op "UPDATE foo SET \"kebab-case-col\" = 'CamelCaseValue' WHERE foo.\"kebab-case-col\" = 'kebab-case-value'")]
                {:default-all-valid-time? true})
  (t/is (= [{:xt$id 1, :kebab-case-col "CamelCaseValue"}]
           (xt/q tu/*node* "SELECT foo.xt$id, foo.\"kebab-case-col\" FROM foo")))

  (xt/submit-tx tu/*node* [(xt/sql-op "DELETE FROM foo WHERE foo.\"kebab-case-col\" = 'CamelCaseValue'")]
                {:default-all-valid-time? true})
  (t/is (= []
           (xt/q tu/*node* "SELECT foo.xt$id, foo.\"kebab-case-col\" FROM foo"))))

(t/deftest test-select-left-join-2302
  (xt/submit-tx tu/*node* [(xt/sql-op "INSERT INTO foo (xt$id, x) VALUES (1, 1), (2, 2)")
                           (xt/sql-op "INSERT INTO bar (xt$id, x) VALUES (1, 1), (2, 3)")])

  (t/is (= [{:foo 1, :x 1}, {:foo 2, :x 2}]
           (xt/q tu/*node* "SELECT foo.xt$id foo, foo.x FROM foo LEFT JOIN bar USING (xt$id, x)")))

  #_ ; FIXME #2302
  (t/is (= []
           (xt/q tu/*node* "SELECT foo.xt$id foo, foo.x FROM foo LEFT JOIN bar USING (xt$id) WHERE foo.x = bar.x"))))

(t/deftest test-c1-importer-abort-op
  (xt/submit-tx tu/*node* [(xt/put :docs {:xt/id :foo})])

  (xt/submit-tx tu/*node* [(xt/put :docs {:xt/id :bar})
                           Ops/ABORT
                           (xt/put :docs {:xt/id :baz})])
  (t/is (= [{:id :foo}]
           (xt/q tu/*node* '(from :docs [{:xt/id id}])))))

(t/deftest test-list-round-trip-2342
  (xt/submit-tx tu/*node* [(xt/sql-op "INSERT INTO t3(xt$id, data) VALUES (1, [2, 3])")
                           (xt/sql-op "INSERT INTO t3(xt$id, data) VALUES (2, [6, 7])")])

  (t/is (= {{:data [2 3], :data:1 [2 3]} 1
            {:data [2 3], :data:1 [6 7]} 1
            {:data [6 7], :data:1 [2 3]} 1
            {:data [6 7], :data:1 [6 7]} 1}
           (frequencies (xt/q tu/*node* "SELECT t3.data, t2.data FROM t3, t3 AS t2")))))

(t/deftest test-mutable-data-buffer-bug
  (xt/submit-tx tu/*node* [(xt/sql-op "INSERT INTO t1(xt$id) VALUES(1)")])

  (t/is (= [{:$column_1$ [{:foo 5} {:foo 5}]}]
           (xt/q tu/*node* "SELECT ARRAY [OBJECT('foo': 5), OBJECT('foo': 5)] FROM t1"))))

(t/deftest test-differing-length-lists-441
  (xt/submit-tx tu/*node* [(xt/sql-op "INSERT INTO t1(xt$id, data) VALUES (1, [2, 3])")
                           (xt/sql-op "INSERT INTO t1(xt$id, data) VALUES (2, [5, 6, 7])")])

  (t/is (= #{{:data [2 3]} {:data [5 6 7]}}
           (set (xt/q tu/*node* "SELECT t1.data FROM t1"))))

  (xt/submit-tx tu/*node* [(xt/sql-op "INSERT INTO t2(xt$id, data) VALUES (1, [2, 3])")
                           (xt/sql-op "INSERT INTO t2(xt$id, data) VALUES (2, ['dog', 'cat'])")])

  (t/is (= #{{:data [2 3]} {:data ["dog" "cat"]}}
           (set (xt/q tu/*node* "SELECT t2.data FROM t2")))))

(t/deftest test-cross-join-ioobe-2343
  (xt/submit-tx tu/*node* [(xt/sql-op "
INSERT INTO t2(xt$id, data)
VALUES(2, OBJECT ('foo': OBJECT('bibble': false), 'bar': OBJECT('baz': 1002)))")

                           (xt/sql-op "
INSERT INTO t1(xt$id, data)
VALUES(1, OBJECT ('foo': OBJECT('bibble': true), 'bar': OBJECT('baz': 1001)))")])

  (t/is (= [{:t2d {:foo {:bibble false}, :bar {:baz 1002}},
             :t1d {:foo {:bibble true}, :bar {:baz 1001}}}]
           (xt/q tu/*node* "SELECT t2.data t2d, t1.data t1d FROM t2, t1"))))

(t/deftest test-txs-table-485
  (tu/with-log-level 'xtdb.indexer :error
    (xt/submit-tx tu/*node* [(xt/put :docs {:xt/id :foo})])
    (xt/submit-tx tu/*node* [Ops/ABORT])
    (xt/submit-tx tu/*node* [(xt/put :docs {:xt/id :bar})])
    (xt/submit-tx tu/*node* [(xt/put-fn :tx-fn-fail
                                        '(fn []
                                           (throw (Exception. "boom"))))
                             (xt/call :tx-fn-fail)])

    (t/is (= #{{:tx-id 0, :tx-time (time/->zdt #inst "2020-01-01"), :committed? true}
               {:tx-id 1, :tx-time (time/->zdt #inst "2020-01-02"), :committed? false}
               {:tx-id 2, :tx-time (time/->zdt #inst "2020-01-03"), :committed? true}
               {:tx-id 3, :tx-time (time/->zdt #inst "2020-01-04"), :committed? false}}
             (set (xt/q tu/*node*
                        '(from :xt/txs [{:xt/id tx-id, :xt/tx-time tx-time, :xt/committed? committed?}])))))

    (t/is (= [{:committed? false}]
             (xt/q tu/*node*
                   '(from :xt/txs [{:xt/id $tx-id, :xt/committed? committed?}])
                   {:args {:tx-id 1}})))

    (t/is (thrown-with-msg?
           RuntimeException
           #":xtdb\.call/error-evaluating-tx-fn"

           (throw (-> (xt/q tu/*node*
                            '(from :xt/txs [{:xt/id $tx-id, :xt/error err}])
                            {:args {:tx-id 3}})
                      first
                      :err))))))

(t/deftest test-indexer-cleans-up-aborted-transactions-2489
  (t/testing "INSERT"
    (xt/submit-tx tu/*node*
                  [(xt/sql-op "INSERT INTO docs (xt$id, xt$valid_from, xt$valid_to)
                               VALUES (1, DATE '2010-01-01', DATE '2020-01-01'),
                                      (1, DATE '2030-01-01', DATE '2020-01-01')")])

    (t/is (= [{:committed? false}]
             (xt/q tu/*node*
                   '(from :xt/txs [{:xt/id $tx-id, :xt/committed? committed?}])
                   {:args {:tx-id 0}})))))

(t/deftest test-nulling-valid-time-columns-2504
  (xt/submit-tx tu/*node* [(-> (xt/sql-op "INSERT INTO docs (xt$id, xt$valid_from, xt$valid_to) VALUES (1, NULL, ?), (2, ?, NULL), (3, NULL, NULL)")
                               (xt/with-op-args [#inst "3000" , #inst "3000"]))])
  (t/is (= #{{:id 1, :vf (time/->zdt #inst "2020"), :vt (time/->zdt #inst "3000")}
             {:id 2, :vf (time/->zdt #inst "3000"), :vt nil}
             {:id 3, :vf (time/->zdt #inst "2020"), :vt nil}}
           (set (xt/q tu/*node* '(from :docs {:bind [{:xt/id id, :xt/valid-from vf, :xt/valid-to vt}]
                                              :for-valid-time :all-time}))))))

(deftest test-select-star
  (xt/submit-tx tu/*node*
                [(xt/sql-op "INSERT INTO foo (xt$id, a) VALUES (1, 1)")
                 (xt/sql-op "INSERT INTO foo (xt$id, b) VALUES (2, 2)")
                 (xt/sql-op "INSERT INTO bar (xt$id, a) VALUES (1, 3)")
                 (xt/sql-op "INSERT INTO bar (xt$id, b) VALUES (2, 4)")])

  (t/is (= #{{:a 1, :xt$id 1} {:b 2, :xt$id 2}}
           (set (xt/q tu/*node* "SELECT * FROM foo"))))

  (t/is (=
         #{{:a 1, :xt$id 1, :a:1 3, :xt$id:1 1}
           {:a 1, :xt$id 1, :b:1 4, :xt$id:1 2}
           {:b 2, :xt$id 2, :a:1 3, :xt$id:1 1}
           {:b 2, :xt$id 2, :b:1 4, :xt$id:1 2}}
         (set (xt/q tu/*node* "SELECT * FROM foo, bar"))))

  (t/is (=
         #{{:xt$id 1, :a 3, :xt$id:1 1, :a:1 1}
           {:xt$id 2, :b 4, :xt$id:1 1, :a:1 1}
           {:xt$id 1, :a 3, :xt$id:1 2, :b:1 2}
           {:xt$id 2, :b 4, :xt$id:1 2, :b:1 2}}
         (set (xt/q tu/*node* "SELECT bar.*, foo.* FROM foo, bar"))))

  (t/is (=
         #{{:a 1, :xt$id 1, :a:1 3, :xt$id:1 1}
           {:a 1, :xt$id 1, :b:1 4, :xt$id:1 2}
           {:b 2, :xt$id 2, :a:1 3, :xt$id:1 1}
           {:b 2, :xt$id 2, :b:1 4, :xt$id:1 2}}
         (set (xt/q tu/*node* "SELECT * FROM (SELECT * FROM foo, bar) AS baz"))))

  (xt/submit-tx tu/*node*
                [(xt/sql-op "INSERT INTO bing (SELECT * FROM foo)")])

  (t/is (= #{{:a 1, :xt$id 1} {:b 2, :xt$id 2}}
           (set (xt/q tu/*node* "SELECT * FROM bing")))))

(deftest test-scan-all-table-col-names
  (t/testing "testing scan.allTableColNames combines table info from both live and past chunks"
    (-> (xt/submit-tx tu/*node* [(xt/put :foo {:xt/id "foo1" :a 1})
                                 (xt/put :bar {:xt/id "bar1"})
                                 (xt/put :bar {:xt/id "bar2" :b 2})])
        (tu/then-await-tx tu/*node*))

    (tu/finish-chunk! tu/*node*)

    (xt/submit-tx tu/*node* [(xt/put :foo {:xt/id "foo2" :c 3})
                             (xt/put :baz {:xt/id "foo1" :a 4})])

    (t/is (= #{{:a 1, :xt$id "foo1"} {:xt$id "foo2", :c 3}}
             (set (xt/q tu/*node* "SELECT * FROM foo"))))
    (t/is (= #{{:xt$id "bar1"} {:b 2, :xt$id "bar2"}}
             (set (xt/q tu/*node* "SELECT * FROM bar"))))
    (t/is (= #{{:a 4, :xt$id "foo1"}}
             (set (xt/q tu/*node* "SELECT * FROM baz"))))))

(deftest test-erase-after-delete-2607
  (t/testing "general case"
    (xt/submit-tx tu/*node* [(xt/sql-op "INSERT INTO foo (xt$id, bar) VALUES (1, 1)")])
    (xt/submit-tx tu/*node* [(xt/sql-op "DELETE FROM foo WHERE foo.xt$id = 1")])
    (xt/submit-tx tu/*node* [(xt/sql-op "ERASE FROM foo WHERE foo.xt$id = 1")])
    (t/is (= [] (xt/q tu/*node* "SELECT * FROM foo FOR ALL VALID_TIME")))
    (t/is (= [] (xt/q tu/*node* "SELECT * FROM foo FOR ALL SYSTEM_TIME"))))
  (t/testing "zero width case"
    (xt/submit-tx tu/*node* [(xt/sql-op "INSERT INTO foo (xt$id, bar) VALUES (2, 1)")
                             (xt/sql-op "DELETE FROM foo WHERE foo.xt$id = 2")])
    (xt/submit-tx tu/*node* [(xt/sql-op "ERASE FROM foo WHERE foo.xt$id = 2")])
    (t/is (= [] (xt/q tu/*node* "SELECT * FROM foo FOR ALL VALID_TIME")))
    ;; TODO if it doesn't show up in valid-time it won't get deleted
    #_(t/is (= [] (xt/q tu/*node* "SELECT * FROM foo FOR ALL SYSTEM_TIME")))))

(t/deftest test-explain-plan-sql
  (t/is (= '[{:plan [:rename {x1 xt$id, x2 foo}
                     [:project [x1 x2]
                      [:rename {xt$id x1, foo x2, a x3, b x4}
                       [:select (= (+ a b) 12)
                        [:scan {:table users} [xt$id foo a b]]]]]]}]

           (xt/q tu/*node*
                 "SELECT u.xt$id, u.foo FROM users u WHERE u.a + u.b = 12"
                 {:explain? true}))))

(t/deftest test-normalising-nested-cols-2483
  (xt/submit-tx tu/*node* [(xt/put :docs {:xt/id 1 :foo {:a/b "foo"}})])
  (t/is (= [{:foo {:a$b "foo"}}] (xt/q tu/*node* "SELECT docs.foo FROM docs")))
  (t/is (= [{:a$b "foo"}] (xt/q tu/*node* "SELECT docs.foo.a$b FROM docs"))))

(t/deftest non-namespaced-keys-for-structs-2418
  (xt/submit-tx tu/*node* [(xt/sql-op "INSERT INTO foo(xt$id, bar) VALUES (1, OBJECT('c$d': 'bar'))")])
  (t/is (= [{:bar {:c/d "bar"}}]
           (xt/q tu/*node* '(from :foo [bar])))))

(t/deftest large-xt-stars-2484
  (letfn [(rand-str [l]
            (apply str (repeatedly l #(rand-nth "abcdefghijklmnopqrstuvwxyz0123456789"))))]
    (with-open [node (xtn/start-node {})]
      (let [docs (for [id (range 100)]
                   (let [data (repeatedly 10 #(rand-str 10))]
                     (-> (zipmap (map keyword data) data)
                         (assoc :xt/id id))))]

        (xt/submit-tx node (for [doc docs]
                             (xt/put :docs doc)))

        #_ ; FIXME #2923
        (t/is (= (set docs)
                 (->> (xt/q node '{:find [e] :where [(match :docs {:xt/* e})]})
                      (into #{} (map :e)))))))))

(t/deftest non-existant-column-no-nil-rows-2898
  (xt/submit-tx tu/*node* [(xt/sql-op "INSERT INTO foo(xt$id, bar) VALUES (1, 2)")])
  (t/is (= [{}]
           (xt/q tu/*node* "SELECT foo.not_a_column FROM foo"))))

(t/deftest test-nested-field-normalisation
  (xt/submit-tx tu/*node* [(xt/sql-op "INSERT INTO t1(xt$id, data) VALUES(1, OBJECT ('field-name1': OBJECT('field-name2': true),
                                                                                'field/name3': OBJECT('baz': -4113466)))")])

  (t/is (= [{:data {:field$name3 {:baz -4113466}, :field_name1 {:field_name2 true}}}]
           (xt/q tu/*node* "SELECT t1.data FROM t1"))
        "testing insert worked")

  (t/is (= [{:field_name2 true, :baz nil}]
           (xt/q tu/*node* "SELECT t2.field_name1.field_name2, t2.field$name4.baz
                            FROM (SELECT t1.data.field_name1, t1.data.field$name4 FROM t1) AS t2"))
        "testing insert worked"))

(t/deftest test-get-field-on-duv-with-struct-2425
  (xt/submit-tx tu/*node* [(xt/sql-op "INSERT INTO t2(xt$id, data) VALUES(1, 'bar')")
                           (xt/sql-op "INSERT INTO t2(xt$id, data) VALUES(2, OBJECT('foo': 2))")])

  (t/is (= [{:foo 2} {:foo nil}]
           (xt/q tu/*node* "SELECT t2.data.foo FROM t2"))))

(t/deftest distinct-null-2535
  (xt/submit-tx tu/*node* [(xt/sql-op "INSERT INTO t1(xt$id, foo) VALUES(1, NULL)")
                           (xt/sql-op "INSERT INTO t1(xt$id, foo) VALUES(2, NULL)")])

  (t/is (= [{:nil nil}]
           (xt/q tu/*node* "SELECT DISTINCT NULL AS nil FROM t1")))

  (t/is (= [{:foo nil}]
           (xt/q tu/*node* "SELECT DISTINCT t1.foo FROM t1"))))

;; TODO move this to api-test once #2937 is in
(t/deftest throw-on-unknown-query-type
  (t/is (thrown-with-msg?
         xtdb.IllegalArgumentException
         #"Illegal argument: ':unknown-query-type'"
         (xt/q tu/*node* (Object.)))))


(t/deftest test-array-agg-2946
  (xt/submit-tx tu/*node*
                [(xt/put :track {:xt/id :track-1 :name "foo1" :composer "bar" :album :album-1})
                 (xt/put :track {:xt/id :track-2 :name "foo2" :composer "bar" :album :album-1})
                 (xt/put :album  {:xt/id :album-1 :name "foo-album"})])


  (t/is (= [{:name "foo-album", :tracks ["foo1" "foo2"]}]
           (xt/q tu/*node*
                 "SELECT a.name, ARRAY_AGG(t.name) AS tracks FROM track AS t, album AS a
                  WHERE t.album = a.xt$id
                  GROUP BY a.name"))
        "array-agg")

  #_ ;;TODO parser needs to be adpated #2948
  (t/is (= [{:name "foo-album", :tracks ["bar"]}]
           (xt/q tu/*node*
                 "SELECT a.name, ARRAY_AGG(DISTINCT t.composer) AS composer FROM track AS t, album AS a
                  WHERE t.album = a.xt$id
                  GROUP BY a.name"))
        "array-agg distinct"))
