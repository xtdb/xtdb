(ns xtdb.node-test
  (:require [clojure.test :as t]
            [xtdb.datalog :as xt.d]
            [xtdb.sql :as xt.sql]
            [xtdb.test-util :as tu]
            [xtdb.util :as util]))

(t/use-fixtures :each tu/with-mock-clock tu/with-node)

(t/deftest test-multi-value-insert-423
  (letfn [(expected [tt]
            {[(util/->zdt #inst "2024-01-01") (util/->zdt util/end-of-time), tt tt]
             "Happy 2024!",

             ;; weird? zero-width sys-time so won't normally show up
             [(util/->zdt #inst "2024-01-01") (util/->zdt #inst "2026-01-01"), tt tt]
             "Happy 2024!",

             [(util/->zdt #inst "2024-01-01") (util/->zdt #inst "2025-01-01"), tt (util/->zdt util/end-of-time)]
             "Happy 2024!"

             [(util/->zdt #inst "2025-01-01") (util/->zdt util/end-of-time), tt tt]
             "Happy 2025!",

             [(util/->zdt #inst "2025-01-01") (util/->zdt #inst "2026-01-01"), tt (util/->zdt util/end-of-time)]
             "Happy 2025!",

             [(util/->zdt #inst "2026-01-01") (util/->zdt util/end-of-time), tt (util/->zdt util/end-of-time)]
             "Happy 2026!"})

          (q [table]
            (->> (xt.sql/q tu/*node* (format "
SELECT p.id, p.text,
       p.application_time_start, p.application_time_end,
       p.system_time_start, p.system_time_end
FROM %s FOR ALL SYSTEM_TIME AS p"
                                             table))
                 (into {} (map (juxt (juxt :application_time_start :application_time_end
                                           :system_time_start :system_time_end)
                                     :text)))))]

    (xt.sql/submit-tx tu/*node* [[:sql "
INSERT INTO posts (id, text, application_time_start)
VALUES (1, 'Happy 2024!', DATE '2024-01-01'),
       (1, 'Happy 2025!', DATE '2025-01-01'),
       (1, 'Happy 2026!', DATE '2026-01-01')"]])

    (t/is (= (expected (util/->zdt #inst "2020-01-01"))
             (q "posts")))

    (xt.sql/submit-tx tu/*node* [[:sql "INSERT INTO posts2 (id, text, application_time_start) VALUES (1, 'Happy 2024!', DATE '2024-01-01')"]
                                 [:sql "INSERT INTO posts2 (id, text, application_time_start) VALUES (1, 'Happy 2025!', DATE '2025-01-01')"]
                                 [:sql "INSERT INTO posts2 (id, text, application_time_start) VALUES (1, 'Happy 2026!', DATE '2026-01-01')"]])

    (t/is (= (expected (util/->zdt #inst "2020-01-02"))
             (q "posts2")))))

(t/deftest test-dml-sees-in-tx-docs
  (xt.sql/submit-tx tu/*node* [[:sql "INSERT INTO foo (id, v) VALUES ('foo', 0)"]
                               [:sql "UPDATE foo SET v = 1"]])

  (t/is (= [{:id "foo", :v 1}]
           (xt.sql/q tu/*node* "SELECT foo.id, foo.v FROM foo"))))

(t/deftest test-delete-without-search-315
  (letfn [(q []
            (xt.sql/q tu/*node* "SELECT foo.id, foo.application_time_start, foo.application_time_end FROM foo"))]
    (xt.sql/submit-tx tu/*node* [[:sql "INSERT INTO foo (id) VALUES ('foo')"]])

    (t/is (= [{:id "foo",
               :application_time_start (util/->zdt #inst "2020")
               :application_time_end (util/->zdt util/end-of-time)}]
             (q)))

    (xt.sql/submit-tx tu/*node* [[:sql "DELETE FROM foo"]]
                      {:app-time-as-of-now? true})

    (t/is (= [{:id "foo"
               :application_time_start (util/->zdt #inst "2020")
               :application_time_end (util/->zdt #inst "2020-01-02")}]
             (q)))

    (xt.sql/submit-tx tu/*node* [[:sql "DELETE FROM foo"]])

    (t/is (= [] (q)))))

(t/deftest test-update-set-field-from-param-328
  (xt.sql/submit-tx tu/*node* [[:sql "INSERT INTO users (id, first_name, last_name) VALUES (?, ?, ?)"
                                [["susan", "Susan", "Smith"]]]])

  (xt.sql/submit-tx tu/*node* [[:sql "UPDATE users FOR PORTION OF APP_TIME FROM ? TO ? AS u SET first_name = ? WHERE u.id = ?"
                                [[#inst "2021", util/end-of-time, "sue", "susan"]]]])

  (t/is (= #{["Susan" "Smith", (util/->zdt #inst "2020") (util/->zdt #inst "2021")]
             ["sue" "Smith", (util/->zdt #inst "2021") (util/->zdt util/end-of-time)]}
           (->> (xt.sql/q tu/*node* "SELECT u.first_name, u.last_name, u.application_time_start, u.application_time_end FROM users u")
                (into #{} (map (juxt :first_name :last_name :application_time_start :application_time_end)))))))

(t/deftest test-can-submit-same-id-into-multiple-tables-338
  (let [tx1 (xt.sql/submit-tx tu/*node* [[:sql "INSERT INTO t1 (id, foo) VALUES ('thing', 't1-foo')"]
                                         [:sql "INSERT INTO t2 (id, foo) VALUES ('thing', 't2-foo')"]])
        tx2 (xt.sql/submit-tx tu/*node* [[:sql "UPDATE t2 SET foo = 't2-foo-v2' WHERE t2.id = 'thing'"]])]

    (t/is (= [{:id "thing", :foo "t1-foo"}]
             (xt.sql/q tu/*node* "SELECT t1.id, t1.foo FROM t1"
                       {:basis {:tx tx1}})))

    (t/is (= [{:id "thing", :foo "t1-foo"}]
             (xt.sql/q tu/*node* "SELECT t1.id, t1.foo FROM t1"
                       {:basis {:tx tx2}})))

    (t/is (= [{:id "thing", :foo "t2-foo"}]
             (xt.sql/q tu/*node* "SELECT t2.id, t2.foo FROM t2"
                       {:basis {:tx tx1}})))

    (t/is (= [{:id "thing", :foo "t2-foo-v2"}]
             (xt.sql/q tu/*node* "SELECT t2.id, t2.foo FROM t2"
                       {:basis {:tx tx2}})))))

(t/deftest test-put-delete-with-implicit-tables-338
  (letfn [(foos []
            (->> (for [[tk tn] {:xt "xt_docs"
                                :t1 "explicit_table1"
                                :t2 "explicit_table2"}]
                   [tk (->> (xt.sql/q tu/*node* (format "SELECT t.id, t.v FROM %s t WHERE t.application_time_end = END_OF_TIME" tn))
                            (into #{} (map :v)))])
                 (into {})))]

    (xt.d/submit-tx tu/*node*
                    [[:put :xt_docs {:id :foo, :v "implicit table"}]
                     [:put :explicit_table1 {:id :foo, :v "explicit table 1"}]
                     [:put :explicit_table2 {:id :foo, :v "explicit table 2"}]])

    (t/is (= {:xt #{"implicit table"}, :t1 #{"explicit table 1"}, :t2 #{"explicit table 2"}}
             (foos)))

    (xt.d/submit-tx tu/*node* [[:delete :xt_docs :foo]])

    (t/is (= {:xt #{}, :t1 #{"explicit table 1"}, :t2 #{"explicit table 2"}}
             (foos)))

    (xt.d/submit-tx tu/*node* [[:delete :explicit_table1 :foo]])

    (t/is (= {:xt #{}, :t1 #{}, :t2 #{"explicit table 2"}}
             (foos)))))

(t/deftest test-array-element-reference-is-one-based-336
  (xt.sql/submit-tx tu/*node* [[:sql "INSERT INTO foo (id, arr) VALUES ('foo', ARRAY[9, 8, 7, 6])"]])

  (t/is (= [{:id "foo", :arr [9 8 7 6], :fst 9, :snd 8, :lst 6}]
           (xt.sql/q tu/*node* "SELECT foo.id, foo.arr, foo.arr[1] AS fst, foo.arr[2] AS snd, foo.arr[4] AS lst FROM foo"))))

(t/deftest test-interval-literal-cce-271
  (t/is (= [{:a #xt/interval-ym "P12M"}]
           (xt.sql/q tu/*node* "select a.a from (values (1 year)) a (a)"))))

(t/deftest test-overrides-range
  (xt.sql/submit-tx tu/*node* [[:sql "
INSERT INTO foo (id, v, application_time_start, application_time_end)
VALUES (1, 1, DATE '1998-01-01', DATE '2000-01-01')"]])

  (xt.sql/submit-tx tu/*node* [[:sql "
INSERT INTO foo (id, v, application_time_start, application_time_end)
VALUES (1, 2, DATE '1997-01-01', DATE '2001-01-01')"]])

  (t/is (= [{:id 1, :v 1,
             :application_time_start (util/->zdt #inst "1998")
             :application_time_end (util/->zdt #inst "2000")
             :system_time_start (util/->zdt #inst "2020-01-01")
             :system_time_end (util/->zdt #inst "2020-01-02")}
            {:id 1, :v 2,
             :application_time_start (util/->zdt #inst "1997")
             :application_time_end (util/->zdt #inst "2001")
             :system_time_start (util/->zdt #inst "2020-01-02")
             :system_time_end (util/->zdt util/end-of-time)}]

           (xt.sql/q tu/*node* "
SELECT foo.id, foo.v,
       foo.application_time_start, foo.application_time_end,
       foo.system_time_start, foo.system_time_end
FROM foo FOR ALL SYSTEM_TIME FOR ALL APPLICATION_TIME"))))

(t/deftest test-current-timestamp-in-temporal-constraint-409
  (xt.sql/submit-tx tu/*node* [[:sql "
INSERT INTO foo (id, v)
VALUES (1, 1)"]])

  (t/is (= [{:id 1, :v 1,
             :application_time_start (util/->zdt #inst "2020")
             :application_time_end (util/->zdt util/end-of-time)}]
           (xt.sql/q tu/*node* "SELECT foo.id, foo.v, foo.application_time_start, foo.application_time_end FROM foo")))

  (t/is (= []
           (xt.sql/q tu/*node* "
SELECT foo.id, foo.v, foo.application_time_start, foo.application_time_end
FROM foo FOR APPLICATION_TIME AS OF DATE '1999-01-01'"
                     {:basis {:current-time (util/->instant #inst "1999")}})))

  (t/is (= []
           (xt.sql/q tu/*node* "
SELECT foo.id, foo.v, foo.application_time_start, foo.application_time_end
FROM foo FOR APPLICATION_TIME AS OF CURRENT_TIMESTAMP"
                     {:basis {:current-time (util/->instant #inst "1999")}}))))

(t/deftest test-repeated-row-id-scan-bug-also-409
  (xt.sql/submit-tx tu/*node* [[:sql "INSERT INTO foo (id, v) VALUES (1, 1)"]])

  (let [tx1 (xt.sql/submit-tx tu/*node* [[:sql "
UPDATE foo
FOR PORTION OF APP_TIME FROM DATE '2022-01-01' TO DATE '2024-01-01'
SET v = 2
WHERE foo.id = 1"]])

        tx2 (xt.sql/submit-tx tu/*node* [[:sql "
DELETE FROM foo
FOR PORTION OF APP_TIME FROM DATE '2023-01-01' TO DATE '2025-01-01'
WHERE foo.id = 1"]])]

    (letfn [(q1 [opts]
              (xt.sql/q tu/*node* "
SELECT foo.id, foo.v, foo.application_time_start, foo.application_time_end
FROM foo
ORDER BY foo.application_time_start"
                        opts))
            (q2 [opts]
              (frequencies
               (xt.sql/q tu/*node* "SELECT foo.id, foo.v FROM foo" opts)))]

      (t/is (= [{:id 1, :v 1
                 :application_time_start (util/->zdt #inst "2020")
                 :application_time_end (util/->zdt #inst "2022")}
                {:id 1, :v 2
                 :application_time_start (util/->zdt #inst "2022")
                 :application_time_end (util/->zdt #inst "2024")}
                {:id 1, :v 1
                 :application_time_start (util/->zdt #inst "2024")
                 :application_time_end (util/->zdt util/end-of-time)}]

               (q1 {:basis {:tx tx1}})))

      (t/is (= {{:id 1, :v 1} 2, {:id 1, :v 2} 1}
               (q2 {:basis {:tx tx1}})))

      (t/is (= [{:id 1, :v 1
                 :application_time_start (util/->zdt #inst "2020")
                 :application_time_end (util/->zdt #inst "2022")}
                {:id 1, :v 2
                 :application_time_start (util/->zdt #inst "2022")
                 :application_time_end (util/->zdt #inst "2023")}
                {:id 1, :v 1
                 :application_time_start (util/->zdt #inst "2025")
                 :application_time_end (util/->zdt util/end-of-time)}]

               (q1 {:basis {:tx tx2}})))

      (t/is (= [{:id 1, :v 1
                 :application_time_start (util/->zdt #inst "2025")
                 :application_time_end (util/->zdt util/end-of-time)}]

               (q1 {:basis {:tx tx2, :current-time (util/->instant #inst "2026")}
                    :app-time-as-of-now? true})))

      (t/is (= {{:id 1, :v 1} 2, {:id 1, :v 2} 1}
               (q2 {:basis {:tx tx2}}))))))

(t/deftest test-error-handling-inserting-strings-into-app-time-cols-397
  (xt.sql/submit-tx tu/*node* [[:sql "INSERT INTO foo (id, application_time_start) VALUES (1, '2018-01-01')"]])

  ;; TODO check the rollback error when it's available, #401
  (t/is (= [] (xt.sql/q tu/*node* "SELECT foo.id FROM foo"))))

(t/deftest test-vector-type-mismatch-245
  (t/is (= [{:a [4 "2"]}]
           (xt.sql/q tu/*node* "SELECT a.a FROM (VALUES (ARRAY [4, '2'])) a (a)")))

  (t/is (= [{:a [["hello"] "world"]}]
           (xt.sql/q tu/*node* "SELECT a.a FROM (VALUES (ARRAY [['hello'], 'world'])) a (a)"))))

(t/deftest test-double-quoted-col-refs
  (xt.sql/submit-tx tu/*node* [[:sql "INSERT INTO foo (id, \"kebab-case-col\") VALUES (1, 'kebab-case-value')"]])

  (t/is (= [{:id 1, :kebab-case-col "kebab-case-value"}]
           (xt.sql/q tu/*node* "SELECT foo.id, foo.\"kebab-case-col\" FROM foo WHERE foo.\"kebab-case-col\" = 'kebab-case-value'"))))

(t/deftest test-select-left-join-471
  (xt.sql/submit-tx tu/*node* [[:sql "INSERT INTO foo (id, x) VALUES (1, 1), (2, 2)"]
                               [:sql "INSERT INTO bar (id, x) VALUES (1, 1), (2, 3)"]])

  (t/is (= [{:foo 1, :x 1}, {:foo 2, :x 2}]
           (xt.sql/q tu/*node* "SELECT foo.id foo, foo.x FROM foo LEFT JOIN bar USING (id, x)")))

  #_ ; FIXME #471
  (t/is (= []
           (xt.sql/q tu/*node* "SELECT foo.id foo, foo.x FROM foo LEFT JOIN bar USING (id) WHERE foo.x = bar.x"))))

(t/deftest test-c1-importer-abort-op
  (xt.d/submit-tx tu/*node* [[:put :xt_docs {:id :foo}]])

  (xt.d/submit-tx tu/*node* [[:put :xt_docs {:id :bar}]
                             [:abort]
                             [:put :xt_docs {:id :baz}]])
  (t/is (= [{:id :foo}]
           (xt.d/q tu/*node*
                   '{:find [id]
                     :where [(match :xt_docs [id])
                             [id :id]]}))))

(t/deftest test-list-round-trip-546
  (xt.sql/submit-tx tu/*node* [[:sql "INSERT INTO t3(id, data) VALUES (1, [2, 3])"]
                               [:sql "INSERT INTO t3(id, data) VALUES (2, [6, 7])"]])
  #_ ; FIXME #546
  (t/is (= [{:data [2 3], :data_1 [2 3]}
            {:data [2 3], :data_1 [6 7]}
            {:data [6 7], :data_1 [2 3]}
            {:data [6 7], :data_1 [6 7]}]
           (xt.sql/q tu/*node* "SELECT t3.data, t2.data FROM t3, t3 AS t2"))))

(t/deftest test-mutable-data-buffer-bug
  (xt.sql/submit-tx tu/*node* [[:sql "INSERT INTO t1(id) VALUES(1)"]])

  (t/is (= [{:$column_1$ [{:foo 5} {:foo 5}]}]
           (xt.sql/q tu/*node* "SELECT ARRAY [OBJECT('foo': 5), OBJECT('foo': 5)] FROM t1"))))

(t/deftest test-differing-length-lists-441
  (xt.sql/submit-tx tu/*node* [[:sql "INSERT INTO t1(id, data) VALUES (1, [2, 3])"]
                               [:sql "INSERT INTO t1(id, data) VALUES (2, [5, 6, 7])"]])

  (t/is (= [{:data [2 3]} {:data [5 6 7]}]
           (xt.sql/q tu/*node* "SELECT t1.data FROM t1")))

  (xt.sql/submit-tx tu/*node* [[:sql "INSERT INTO t2(id, data) VALUES (1, [2, 3])"]
                               [:sql "INSERT INTO t2(id, data) VALUES (2, ['dog', 'cat'])"]])

  (t/is (= [{:data [2 3]} {:data ["dog" "cat"]}]
           (xt.sql/q tu/*node* "SELECT t2.data FROM t2"))))

(t/deftest test-cross-join-ioobe-547
  (xt.sql/submit-tx tu/*node* [[:sql "
INSERT INTO t2(id, data)
VALUES(2, OBJECT ('foo': OBJECT('bibble': true), 'bar': OBJECT('baz': 1001)))"]

                               [:sql "
INSERT INTO t1(id, data)
VALUES(1, OBJECT ('foo': OBJECT('bibble': true), 'bar': OBJECT('baz': 1001)))"]])

  #_ ; FIXME
  (t/is (= [{:t2d {:bibble true}, :t1d {:baz 1001}}]
           (xt.sql/q tu/*node* "SELECT t2.data t2d, t1.data t1d FROM t2, t1"))))
