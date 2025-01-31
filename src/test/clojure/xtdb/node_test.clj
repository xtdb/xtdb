(ns xtdb.node-test
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :as t :refer [deftest]]
            [next.jdbc :as jdbc]
            [xtdb.api :as xt]
            [xtdb.logging :as logging]
            [xtdb.node :as xtn]
            [xtdb.node.impl] ;;TODO probably move internal methods to main node interface
            [xtdb.protocols :as xtp]
            [xtdb.query :as query]
            [xtdb.serde :as serde]
            [xtdb.test-util :as tu]
            [xtdb.time :as time]
            [xtdb.util :as util])
  (:import [java.time ZonedDateTime]
           [xtdb.api ServerConfig Xtdb$Config]
           [xtdb.query IQuerySource]
           xtdb.types.RegClass))

(t/use-fixtures :each tu/with-allocator tu/with-mock-clock tu/with-node)

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
SELECT p._id, p.body,
       p._valid_from, p._valid_to,
       p._system_from, p._system_to
FROM %s FOR ALL SYSTEM_TIME FOR ALL VALID_TIME AS p"
                                         table))
                 (into {} (map (juxt (juxt :xt/valid-from :xt/valid-to
                                           :xt/system-from :xt/system-to)
                                     :body)))))]

    (xt/submit-tx tu/*node* [[:sql "
INSERT INTO posts (_id, body, _valid_from)
VALUES (1, 'Happy 2024!', DATE '2024-01-01'),
       (1, 'Happy 2025!', DATE '2025-01-01'),
       (1, 'Happy 2026!', DATE '2026-01-01')"]])

    (t/is (= (expected (time/->zdt #inst "2020-01-01"))
             (q "posts")))

    (xt/submit-tx tu/*node* [[:sql "INSERT INTO posts2 (_id, body, _valid_from) VALUES (1, 'Happy 2024!', DATE '2024-01-01')"]
                             [:sql "INSERT INTO posts2 (_id, body, _valid_from) VALUES (1, 'Happy 2025!', DATE '2025-01-01')"]
                             [:sql "INSERT INTO posts2 (_id, body, _valid_from) VALUES (1, 'Happy 2026!', DATE '2026-01-01')"]])

    (t/is (= (expected (time/->zdt #inst "2020-01-02"))
             (q "posts2")))))

(t/deftest test-dml-sees-in-tx-docs
  (xt/submit-tx tu/*node* [[:sql "INSERT INTO foo (_id, v) VALUES ('foo', 0)"]
                           [:sql "UPDATE foo SET v = 1"]])

  (t/is (= [{:xt/id "foo", :v 1}]
           (xt/q tu/*node* "SELECT foo._id, foo.v FROM foo"))))

(t/deftest test-delete-without-search-315
  (letfn [(q []
            (xt/q tu/*node* "SELECT foo._id, foo._valid_from, foo._valid_to FROM foo FOR ALL VALID_TIME"))]
    (xt/submit-tx tu/*node* [[:sql "INSERT INTO foo (_id) VALUES ('foo')"]])

    (t/is (= [{:xt/id "foo",
               :xt/valid-from (time/->zdt #inst "2020")}]
             (q)))

    (xt/submit-tx tu/*node* [[:sql "DELETE FROM foo"]])

    (t/is (= [{:xt/id "foo"
               :xt/valid-from (time/->zdt #inst "2020")
               :xt/valid-to (time/->zdt #inst "2020-01-02")}]
             (q)))

    (xt/submit-tx tu/*node* [[:sql "DELETE FROM foo FOR ALL VALID_TIME"]])

    (t/is (= [] (q)))))

(t/deftest test-update-set-field-from-param-328
  (xt/submit-tx tu/*node* [[:sql "INSERT INTO users (_id, first_name, last_name) VALUES (?, ?, ?)"
                            ["susan", "Susan", "Smith"]]])

  (xt/submit-tx tu/*node* [[:sql "UPDATE users FOR PORTION OF VALID_TIME FROM ? TO NULL AS u SET first_name = ? WHERE u._id = ?"
                            [#inst "2021", "sue", "susan"]]])

  (t/is (= #{["Susan" "Smith", (time/->zdt #inst "2020") (time/->zdt #inst "2021")]
             ["sue" "Smith", (time/->zdt #inst "2021") nil]}
           (->> (xt/q tu/*node* "SELECT u.first_name, u.last_name, u._valid_from, u._valid_to FROM users FOR ALL VALID_TIME u")
                (into #{} (map (juxt :first-name :last-name :xt/valid-from :xt/valid-to)))))))

(t/deftest test-can-submit-same-id-into-multiple-tables-338
  (xt/submit-tx tu/*node* [[:sql "INSERT INTO t1 (_id, foo) VALUES ('thing', 't1-foo')"]
                           [:sql "INSERT INTO t2 (_id, foo) VALUES ('thing', 't2-foo')"]])
  (xt/submit-tx tu/*node* [[:sql "UPDATE t2 SET foo = 't2-foo-v2' WHERE t2._id = 'thing'"]])

  (t/is (= [{:xt/id "thing", :foo "t1-foo"}]
           (xt/q tu/*node* "SELECT t1._id, t1.foo FROM t1"
                 {:snapshot-time #inst "2020"})))

  (t/is (= [{:xt/id "thing", :foo "t1-foo"}]
           (xt/q tu/*node* "SELECT t1._id, t1.foo FROM t1"
                 {:snapshot-time #inst "2020-01-02"})))

  (t/is (= [{:xt/id "thing", :foo "t2-foo"}]
           (xt/q tu/*node* "SELECT t2._id, t2.foo FROM t2"
                 {:snapshot-time #inst "2020"})))

  (t/is (= [{:xt/id "thing", :foo "t2-foo-v2"}]
           (xt/q tu/*node* "SELECT t2._id, t2.foo FROM t2"
                 {:snapshot-time #inst "2020-01-02"}))))

(t/deftest ensure-snapshot-time-has-been-indexed
  (t/is (thrown-with-msg? IllegalArgumentException
                          #"snapshot-time \(.+?\) is after the latest completed tx \(nil\)"
                          (xt/q tu/*node* "SELECT 1"
                                {:snapshot-time #inst "2020"})))

  (xt/submit-tx tu/*node* [[:sql "INSERT INTO foo (_id) VALUES (1)"]])

  (t/is (= [{:xt/id 1}]
           (xt/q tu/*node* "SELECT * FROM foo"
                 {:snapshot-time #inst "2020"})))

  (t/is (thrown-with-msg? IllegalArgumentException
                          #"snapshot-time \(.+?\) is after the latest completed tx \(#xt/tx-key \{.+?\}\)"
                          (xt/q tu/*node* "SELECT * FROM foo"
                                {:snapshot-time #inst "2021"}))))

(t/deftest test-put-delete-with-implicit-tables-338
  (letfn [(foos []
            (->> (for [[tk tn] {:xt "docs"
                                :t1 "explicit_table1"
                                :t2 "explicit_table2"}]
                   [tk (->> (xt/q tu/*node* (format "SELECT t._id, t.v FROM %s t WHERE t._valid_to IS NULL" tn))
                            (into #{} (map :v)))])
                 (into {})))]

    (xt/submit-tx tu/*node*
                  [[:put-docs :docs {:xt/id :foo, :v "implicit table"}]
                   [:put-docs :explicit_table1 {:xt/id :foo, :v "explicit table 1"}]
                   [:put-docs :explicit_table2 {:xt/id :foo, :v "explicit table 2"}]])

    (t/is (= {:xt #{"implicit table"}, :t1 #{"explicit table 1"}, :t2 #{"explicit table 2"}}
             (foos)))

    (xt/submit-tx tu/*node* [[:delete-docs :docs :foo]])

    (t/is (= {:xt #{}, :t1 #{"explicit table 1"}, :t2 #{"explicit table 2"}}
             (foos)))

    (xt/submit-tx tu/*node* [[:delete-docs :explicit_table1 :foo]])

    (t/is (= {:xt #{}, :t1 #{}, :t2 #{"explicit table 2"}}
             (foos)))))

(t/deftest test-array-element-reference-is-one-based-336
  (xt/submit-tx tu/*node* [[:sql "INSERT INTO foo (_id, arr) VALUES ('foo', ARRAY[9, 8, 7, 6])"]])

  (t/is (= [{:xt/id "foo", :arr [9 8 7 6], :fst 9, :snd 8, :lst 6}]
           (xt/q tu/*node* "SELECT foo._id, foo.arr, foo.arr[1] AS fst, foo.arr[2] AS snd, foo.arr[4] AS lst FROM foo"))))

(t/deftest test-interval-literal-cce-271
  (t/is (= [{:a #xt/interval-ym "P12M"}]
           (xt/q tu/*node* "select a.a from (values (INTERVAL '1' YEAR)) a (a)"))))

(t/deftest test-overrides-range
  (xt/submit-tx tu/*node* [[:sql "
INSERT INTO foo (_id, v, _valid_from, _valid_to)
VALUES (1, 1, DATE '1998-01-01', DATE '2000-01-01')"]])

  (xt/submit-tx tu/*node* [[:sql "
INSERT INTO foo (_id, v, _valid_from, _valid_to)
VALUES (1, 2, DATE '1997-01-01', DATE '2001-01-01')"]])

  (t/is (= #{{:xt/id 1, :v 1,
              :xt/valid-from (time/->zdt #inst "1998")
              :xt/valid-to (time/->zdt #inst "2000")
              :xt/system-from (time/->zdt #inst "2020-01-01")
              :xt/system-to (time/->zdt #inst "2020-01-02")}
             {:xt/id 1, :v 2,
              :xt/valid-from (time/->zdt #inst "1997")
              :xt/valid-to (time/->zdt #inst "2001")
              :xt/system-from (time/->zdt #inst "2020-01-02")}}

           (set (xt/q tu/*node* "
SELECT foo._id, foo.v,
       foo._valid_from, foo._valid_to,
       foo._system_from, foo._system_to
FROM foo FOR ALL SYSTEM_TIME FOR ALL VALID_TIME")))))

(t/deftest test-current-timestamp-in-temporal-constraint-409
  (xt/submit-tx tu/*node* [[:sql "
INSERT INTO foo (_id, v)
VALUES (1, 1)"]])

  (t/is (= [{:xt/id 1, :v 1,
             :xt/valid-from (time/->zdt #inst "2020")}]
           (xt/q tu/*node* "SELECT foo._id, foo.v, foo._valid_from, foo._valid_to FROM foo")))

  (t/is (= []
           (xt/q tu/*node* "
SELECT foo._id, foo.v, foo._valid_from, foo._valid_to
FROM foo FOR VALID_TIME AS OF DATE '1999-01-01'"
                 {:current-time (time/->instant #inst "1999")})))

  (t/is (= []
           (xt/q tu/*node* "
SELECT foo._id, foo.v, foo._valid_from, foo._valid_to
FROM foo FOR VALID_TIME AS OF CURRENT_TIMESTAMP"
                 {:current-time (time/->instant #inst "1999")}))))

(t/deftest test-repeated-row-id-scan-bug-also-409
  (xt/submit-tx tu/*node* [[:sql "INSERT INTO foo (_id, v) VALUES (1, 1)"]])

  (letfn [(q1 [opts]
            (xt/q tu/*node* "
SELECT foo._id, foo.v, foo._valid_from, foo._valid_to
FROM foo FOR ALL VALID_TIME
ORDER BY foo._valid_from"
                  opts))
          (q1-now [opts]
            (xt/q tu/*node* "
SELECT foo._id, foo.v, foo._valid_from, foo._valid_to
FROM foo
ORDER BY foo._valid_from"
                  opts))
          (q2 [opts]
            (frequencies
             (xt/q tu/*node* "SELECT foo._id, foo.v FROM foo FOR ALL VALID_TIME" opts)))]

    (xt/submit-tx tu/*node* [[:sql "
UPDATE foo
FOR PORTION OF VALID_TIME FROM DATE '2022-01-01' TO DATE '2024-01-01'
SET v = 2
WHERE foo._id = 1"]])

    (xt/submit-tx tu/*node* [[:sql "
DELETE FROM foo
FOR PORTION OF VALID_TIME FROM DATE '2023-01-01' TO DATE '2025-01-01'
WHERE foo._id = 1"]])

    (t/is (= [{:xt/id 1, :v 1
               :xt/valid-from (time/->zdt #inst "2020")
               :xt/valid-to (time/->zdt #inst "2022")}
              {:xt/id 1, :v 2
               :xt/valid-from (time/->zdt #inst "2022")
               :xt/valid-to (time/->zdt #inst "2024")}
              {:xt/id 1, :v 1
               :xt/valid-from (time/->zdt #inst "2024")}]

             (q1 {:snapshot-time #inst "2020-01-02"})))

    (t/is (= {{:xt/id 1, :v 1} 2, {:xt/id 1, :v 2} 1}
             (q2 {:snapshot-time #inst "2020-01-02"})))

    (t/is (= [{:xt/id 1, :v 1
               :xt/valid-from (time/->zdt #inst "2020")
               :xt/valid-to (time/->zdt #inst "2022")}
              {:xt/id 1, :v 2
               :xt/valid-from (time/->zdt #inst "2022")
               :xt/valid-to (time/->zdt #inst "2023")}
              {:xt/id 1, :v 1
               :xt/valid-from (time/->zdt #inst "2025")}]

             (q1 {:snapshot-time #inst "2020-01-03"})))

    (t/is (= [{:xt/id 1, :v 1
               :xt/valid-from (time/->zdt #inst "2025")}]

             (q1-now {:snapshot-time #inst "2020-01-03", :current-time (time/->instant #inst "2026")})))

    (t/is (= {{:xt/id 1, :v 1} 2, {:xt/id 1, :v 2} 1}
             (q2 {:snapshot-time #inst "2020-01-03"})))))

(t/deftest test-error-handling-inserting-strings-into-app-time-cols-397
  (t/is (= (serde/->tx-aborted 0 (time/->instant #inst "2020-01-01")
                               #xt/runtime-err [:xtdb.expression/invalid-temporal-string "String '2018-01-01' has invalid format for type timestamp with timezone" {}])
           (xt/execute-tx tu/*node* [[:sql "INSERT INTO foo (_id, _valid_from) VALUES (1, '2018-01-01')"]])))

  ;; TODO check the rollback error when it's available, #401
  (t/is (= [] (xt/q tu/*node* "SELECT foo._id FROM foo"))))

(t/deftest test-vector-type-mismatch-245
  (t/is (= [{:a [4 "2"]}]
           (xt/q tu/*node* "SELECT a.a FROM (VALUES (ARRAY [4, '2'])) a (a)")))

  (t/is (= [{:a [["hello"] "world"]}]
           (xt/q tu/*node* "SELECT a.a FROM (VALUES (ARRAY [['hello'], 'world'])) a (a)"))))

(t/deftest test-double-quoted-col-refs
  (xt/submit-tx tu/*node* [[:sql "INSERT INTO foo (_id, \"kebab-case-col\") VALUES (1, 'kebab-case-value')"]])
  (t/is (= [{:xt/id 1, :kebab-case-col "kebab-case-value"}]
           (xt/q tu/*node* "SELECT foo._id, foo.\"kebab-case-col\" FROM foo WHERE foo.\"kebab-case-col\" = 'kebab-case-value'")))

  (xt/submit-tx tu/*node* [[:sql "UPDATE foo SET \"kebab-case-col\" = 'CamelCaseValue' WHERE foo.\"kebab-case-col\" = 'kebab-case-value'"]])
  (t/is (= [{:xt/id 1, :kebab-case-col "CamelCaseValue"}]
           (xt/q tu/*node* "SELECT foo._id, foo.\"kebab-case-col\" FROM foo")))

  (xt/submit-tx tu/*node* [[:sql "DELETE FROM foo WHERE foo.\"kebab-case-col\" = 'CamelCaseValue'"]])
  (t/is (= []
           (xt/q tu/*node* "SELECT foo._id, foo.\"kebab-case-col\" FROM foo"))))

(t/deftest test-select-left-join-2302
  (xt/submit-tx tu/*node* [[:sql "INSERT INTO foo (_id, x) VALUES (1, 1), (2, 2)"]
                           [:sql "INSERT INTO bar (_id, x) VALUES (1, 1), (2, 3)"]])

  (t/is (= [{:foo 1, :x 1}, {:foo 2, :x 2}]
           (xt/q tu/*node* "SELECT foo._id foo, foo.x FROM foo LEFT JOIN bar USING (_id, x)")))

  #_ ; FIXME #2302
  (t/is (= []
           (xt/q tu/*node* "SELECT foo._id foo, foo.x FROM foo LEFT JOIN bar USING (_id) WHERE foo.x = bar.x"))))

(t/deftest test-c1-importer-abort-op
  (xt/submit-tx tu/*node* [[:put-docs :docs {:xt/id :foo}]])

  (xt/submit-tx tu/*node* [[:put-docs :docs {:xt/id :bar}]
                           [:abort]
                           [:put-docs :docs {:xt/id :baz}]])
  (t/is (= [{:id :foo}]
           (xt/q tu/*node* '(from :docs [{:xt/id id}])))))

(t/deftest test-list-round-trip-2342
  (xt/submit-tx tu/*node* [[:sql "INSERT INTO t3(_id, data) VALUES (1, [2, 3])"]
                           [:sql "INSERT INTO t3(_id, data) VALUES (2, [6, 7])"]])

  (t/is (= {{:t3-data [2 3], :t2-data [2 3]} 1
            {:t3-data [2 3], :t2-data [6 7]} 1
            {:t3-data [6 7], :t2-data [2 3]} 1
            {:t3-data [6 7], :t2-data [6 7]} 1}
           (frequencies (xt/q tu/*node* "SELECT t3.data AS t3_data, t2.data AS t2_data FROM t3, t3 AS t2")))))

(t/deftest test-mutable-data-buffer-bug
  (xt/submit-tx tu/*node* [[:sql "INSERT INTO t1(_id) VALUES(1)"]])

  (t/is (= [{:col [{:foo 5} {:foo 5}]}]
           (xt/q tu/*node* "SELECT ARRAY [OBJECT(foo: 5), OBJECT(foo: 5)] AS col FROM t1"))))

(t/deftest test-differing-length-lists-441
  (xt/submit-tx tu/*node* [[:sql "INSERT INTO t1(_id, data) VALUES (1, [2, 3])"]
                           [:sql "INSERT INTO t1(_id, data) VALUES (2, [5, 6, 7])"]])

  (t/is (= #{{:data [2 3]} {:data [5 6 7]}}
           (set (xt/q tu/*node* "SELECT t1.data FROM t1"))))

  (xt/submit-tx tu/*node* [[:sql "INSERT INTO t2(_id, data) VALUES (1, [2, 3])"]
                           [:sql "INSERT INTO t2(_id, data) VALUES (2, ['dog', 'cat'])"]])

  (t/is (= #{{:data [2 3]} {:data ["dog" "cat"]}}
           (set (xt/q tu/*node* "SELECT t2.data FROM t2")))))

(t/deftest test-cross-join-ioobe-2343
  (xt/submit-tx tu/*node* [[:sql "
INSERT INTO t2(_id, data)
VALUES(2, OBJECT (foo: OBJECT(bibble: false), bar: OBJECT(baz: 1002)))"]

                           [:sql "
INSERT INTO t1(_id, data)
VALUES(1, OBJECT (foo: OBJECT(bibble: true), bar: OBJECT(baz: 1001)))"]])

  (t/is (= [{:t2d {:foo {:bibble false}, :bar {:baz 1002}},
             :t1d {:foo {:bibble true}, :bar {:baz 1001}}}]
           (xt/q tu/*node* "SELECT t2.data t2d, t1.data t1d FROM t2, t1"))))

(t/deftest test-txs-table-485
  (logging/with-log-level 'xtdb.indexer :error
    (t/is (= (serde/->tx-committed 0 #xt/instant "2020-01-01T00:00:00Z")
             (xt/execute-tx tu/*node* [[:put-docs :docs {:xt/id :foo}]])))

    (t/is (= (serde/->tx-aborted 1 #xt/instant "2020-01-02T00:00:00Z" nil)
             (xt/execute-tx tu/*node* [[:abort]])))

    (t/is (= (serde/->tx-committed 2 #xt/instant "2020-01-03T00:00:00Z")
             (xt/execute-tx tu/*node* [[:put-docs :docs {:xt/id :bar}]])))

    (t/is (= (serde/->tx-aborted 3 #xt/instant "2020-01-04T00:00:00Z"
                                 #xt/runtime-err [:xtdb.call/error-evaluating-tx-fn
                                                  "Runtime error: 'xtdb.call/error-evaluating-tx-fn'"
                                                  {:fn-id :tx-fn-fail, :args []}])
             (-> (xt/execute-tx tu/*node* [[:put-fn :tx-fn-fail
                                            '(fn []
                                               (throw (Exception. "boom")))]
                                           [:call :tx-fn-fail]])

                 ;; can't compare `:cause`, because Exceptions are identity-equal
                 (update :error (comp read-string pr-str)))))

    (t/is (= #{{:tx-id 0, :system-time (time/->zdt #inst "2020-01-01"), :committed? true}
               {:tx-id 1, :system-time (time/->zdt #inst "2020-01-02"), :committed? false}
               {:tx-id 2, :system-time (time/->zdt #inst "2020-01-03"), :committed? true}
               {:tx-id 3, :system-time (time/->zdt #inst "2020-01-04"), :committed? false}}
             (set (xt/q tu/*node*
                        '(from :xt/txs [{:xt/id tx-id, :system-time system-time, :committed committed?}])))))

    (t/is (= [{:committed? false}]
             (xt/q tu/*node*
                   '(from :xt/txs [{:xt/id $tx-id, :committed committed?}])
                   {:args {:tx-id 1}})))

    (t/is (thrown-with-msg?
           RuntimeException
           #"xtdb\.call/error-evaluating-tx-fn"

           (throw (-> (xt/q tu/*node*
                            '(from :xt/txs [{:xt/id $tx-id, :error err}])
                            {:args {:tx-id 3}})
                      first
                      :err))))))

(t/deftest test-indexer-cleans-up-aborted-transactions-2489
  (t/testing "INSERT"
    (t/is (= (serde/->tx-aborted 0 #xt/instant "2020-01-01T00:00:00Z"
                                 #xt/runtime-err [:xtdb.indexer/invalid-valid-times
                                                  "Runtime error: 'xtdb.indexer/invalid-valid-times'"
                                                  {:valid-from #xt/instant "2030-01-01T00:00:00Z"
                                                   :valid-to #xt/instant "2020-01-01T00:00:00Z"}])
             (xt/execute-tx tu/*node*
                            [[:sql "INSERT INTO docs (_id, _valid_from, _valid_to)
                               VALUES (1, DATE '2010-01-01', DATE '2020-01-01'),
                                      (1, DATE '2030-01-01', DATE '2020-01-01')"]])))

    (t/is (= [{:committed? false}]
             (xt/q tu/*node*
                   '(from :xt/txs [{:xt/id $tx-id, :committed committed?}])
                   {:args {:tx-id 0}})))))

(t/deftest test-nulling-valid-time-columns-2504
  (xt/submit-tx tu/*node* [[:sql "INSERT INTO docs (_id, _valid_from, _valid_to) VALUES (1, NULL, ?), (2, ?, NULL), (3, NULL, NULL)"
                            [#inst "3000" , #inst "3000"]]])
  (t/is (= #{{:id 1, :vf (time/->zdt #inst "2020"), :vt (time/->zdt #inst "3000")}
             {:id 2, :vf (time/->zdt #inst "3000")}
             {:id 3, :vf (time/->zdt #inst "2020")}}
           (set (xt/q tu/*node* '(from :docs {:bind [{:xt/id id, :xt/valid-from vf, :xt/valid-to vt}]
                                              :for-valid-time :all-time}))))))

(deftest test-select-star
  (xt/submit-tx tu/*node*
                [[:sql "INSERT INTO foo (_id, a) VALUES (1, 1)"]
                 [:sql "INSERT INTO foo (_id, b) VALUES (2, 2)"]
                 [:sql "INSERT INTO bar (_id, c) VALUES (1, 3)"]
                 [:sql "INSERT INTO bar (_id, d) VALUES (2, 4)"]])

  (t/is (= #{{:a 1, :xt/id 1} {:b 2, :xt/id 2}}
           (set (xt/q tu/*node* "SELECT * FROM foo"))))

  (t/is (= #{{:a 1, :xt/id 1} {:b 2, :xt/id 2}}
           (set (xt/q tu/*node* "FROM foo"))))

  (t/is (= #{{:a 1 :c 3} {:a 1 :d 4} {:b 2 :c 3} {:b 2 :d 4}}
           (set (xt/q tu/*node* "SELECT * EXCLUDE (_id) FROM foo, bar"))))

  ;; Thrown due to duplicate column projection in the query on `_id`
  (t/is (thrown-with-msg?
         IllegalArgumentException
         #"Duplicate column projection: _id"
         (xt/q tu/*node* "FROM foo, bar")))

  (t/is (thrown-with-msg?
         IllegalArgumentException
         #"Duplicate column projection: _id"
         (xt/q tu/*node* "SELECT bar.*, foo.* FROM foo, bar")))

  (t/is (= #{{:a 1 :c 3} {:a 1 :d 4} {:b 2 :c 3} {:b 2 :d 4}}
           (set (xt/q tu/*node* "SELECT bar.* EXCLUDE (_id), foo.* EXCLUDE (_id) FROM foo, bar"))))

  (t/is (= #{{:a 1 :c 3} {:a 1 :d 4} {:b 2 :c 3} {:b 2 :d 4}}
           (set (xt/q tu/*node* "SELECT * FROM (SELECT * EXCLUDE (_id) FROM foo, bar) AS baz"))))

  (t/is (= #{{:a 1 :c 3} {:a 1 :d 4} {:b 2 :c 3} {:b 2 :d 4}}
           (set (xt/q tu/*node* "FROM (SELECT * EXCLUDE (_id) FROM foo, bar) AS baz"))))

  (t/is (= #{{:xt/id 1 :a 1 :c 3} {:xt/id 1 :b 2 :c 3} {:xt/id 2 :a 1 :d 4} {:xt/id 2 :b 2 :d 4}}
           (set (xt/q tu/*node* "SELECT bar.*, foo.a, foo.b FROM foo, bar"))))

  (t/is (= #{{} {:b 2}}
           (set (xt/q tu/*node* "SELECT * EXCLUDE (_id, a) FROM foo"))))

  (t/is (= #{{:xt/id 1 :new 1} {:xt/id 2 :b 2}}
           (set (xt/q tu/*node* "SELECT * RENAME a AS new FROM foo"))))

  (t/is (= #{{:xt/id 1 :new 1} {:xt/id 2 :new2 2}}
           (set (xt/q tu/*node* "SELECT * RENAME (a AS new, b AS new2)  FROM foo"))))

  (t/is (= #{{} {:new 1}}
           (set (xt/q tu/*node* "SELECT * EXCLUDE (_id, b) RENAME a AS new FROM foo"))))

  (xt/submit-tx tu/*node*
                [[:sql "INSERT INTO bing (SELECT * FROM foo)"]])

  (t/is (= #{{:a 1, :xt/id 1} {:b 2, :xt/id 2}}
           (set (xt/q tu/*node* "SELECT * FROM bing"))))

  (t/is (= #{{:a 1, :xt/id 1} {:b 2, :xt/id 2}}
           (set (xt/q tu/*node* "FROM bing")))))

(deftest test-scan-all-table-col-names
  (t/testing "testing scan.allTableColNames combines table info from both live and past chunks"
    (-> (xt/submit-tx tu/*node* [[:put-docs :foo {:xt/id "foo1" :a 1}]
                                 [:put-docs :bar {:xt/id "bar1"}]
                                 [:put-docs :bar {:xt/id "bar2" :b 2}]])
        (tu/then-await-tx tu/*node*))

    (tu/finish-chunk! tu/*node*)

    (xt/submit-tx tu/*node* [[:put-docs :foo {:xt/id "foo2" :c 3}]
                             [:put-docs :baz {:xt/id "foo1" :a 4}]])

    (t/is (= #{{:xt/id "foo2", :c 3}}
             (set (xt/q tu/*node* "SELECT * FROM foo WHERE foo.c = 3"))))
    (t/is (= #{{:xt/id "foo2", :c 3}}
             (set (xt/q tu/*node* "FROM foo WHERE foo.c = 3"))))

    (t/is (= #{{:xt/id "bar1"} {:b 2, :xt/id "bar2"}}
             (set (xt/q tu/*node* "SELECT * FROM bar"))))
    (t/is (= #{{:xt/id "bar1"} {:b 2, :xt/id "bar2"}}
             (set (xt/q tu/*node* "FROM bar"))))

    (t/is (= #{{:a 4, :xt/id "foo1"}}
             (set (xt/q tu/*node* "SELECT * FROM baz"))))
    (t/is (= #{{:a 4, :xt/id "foo1"}}
             (set (xt/q tu/*node* "FROM baz"))))))

(deftest test-erase-after-delete-2607
  (t/testing "general case"
    (xt/submit-tx tu/*node* [[:sql "INSERT INTO foo (_id, bar) VALUES (1, 1)"]])
    (xt/submit-tx tu/*node* [[:sql "DELETE FROM foo WHERE foo._id = 1"]])
    (xt/submit-tx tu/*node* [[:sql "ERASE FROM foo WHERE foo._id = 1"]])
    (t/is (= [] (xt/q tu/*node* "SELECT * FROM foo FOR ALL VALID_TIME")))
    (t/is (= [] (xt/q tu/*node* "SELECT * FROM foo FOR ALL SYSTEM_TIME"))))
  (t/testing "zero width case"
    (xt/submit-tx tu/*node* [[:sql "INSERT INTO foo (_id, bar) VALUES (2, 1)"]
                             [:sql "DELETE FROM foo WHERE foo._id = 2"]])
    (xt/submit-tx tu/*node* [[:sql "ERASE FROM foo WHERE foo._id = 2"]])
    (t/is (= [] (xt/q tu/*node* "SELECT * FROM foo FOR ALL VALID_TIME")))
    ;; TODO if it doesn't show up in valid-time it won't get deleted
    #_(t/is (= [] (xt/q tu/*node* "SELECT * FROM foo FOR ALL SYSTEM_TIME")))))

(t/deftest test-explain-plan-sql
  (xt/execute-tx tu/*node* [[:sql "INSERT INTO users (_id, foo, a, b) VALUES (1, 2, 3, 4)"]])

  (t/is (= [{:plan (str/trim "
[:project
 [{_id u.1/_id} {foo u.1/foo}]
 [:rename
  u.1
  [:select (= (+ a b) 12) [:scan {:table public/users} [b foo a _id]]]]]
")}]
           (-> (xt/q tu/*node*
                     "SELECT u._id, u.foo FROM users u WHERE u.a + u.b = 12"
                     {:explain? true})
               (update-in [0 :plan] str/trim)))))

(t/deftest test-normalising-nested-cols-2483
  (xt/submit-tx tu/*node* [[:put-docs :docs {:xt/id 1 :foo {:a/b "foo"}}]])
  (t/is (= [{:foo {:a/b "foo"}}] (xt/q tu/*node* "SELECT docs.foo FROM docs")))
  (t/is (= [{:a/b "foo"}] (xt/q tu/*node* "SELECT (docs.foo).a$b FROM docs"))))

(t/deftest non-namespaced-keys-for-structs-2418
  (xt/submit-tx tu/*node* [[:sql "INSERT INTO foo(_id, bar) VALUES (1, OBJECT(c$d: 'bar'))"]])
  (t/is (= [{:bar {:c/d "bar"}}]
           (xt/q tu/*node* '(from :foo [bar])))))

(t/deftest large-xt-stars-2484
  (letfn [(rand-str [l]
            (apply str (repeatedly l #(rand-nth "abcdefghijklmnopqrstuvwxyz0123456789"))))]
    (let [docs (for [id (range 100)]
                 (let [data (repeatedly 10 #(rand-str 10))]
                   (-> (zipmap (map keyword data) data)
                       (assoc :xt/id id))))]

      (xt/submit-tx tu/*node* (for [doc docs]
                                [:put-docs :docs doc]))

      #_ ; FIXME #2923
      (t/is (= (set docs)
               (->> (xt/q node '{:find [e] :where [(match :docs {:xt/* e})]})
                    (into #{} (map :e))))))))

(t/deftest non-existant-column-no-nil-rows-2898
  (xt/submit-tx tu/*node* [[:sql "INSERT INTO foo(_id, bar) VALUES (1, 2)"]])
  (t/is (= [{}]
           (xt/q tu/*node* "SELECT foo.not_a_column FROM foo"))))

(t/deftest test-nested-field-normalisation
  (jdbc/execute! tu/*node* ["INSERT INTO t1(_id, data)
                             VALUES(1, {field_name1: {field_name2: true},
                                        field_name3: {baz: -4113466}})"])

  (t/is (= [{:data {:field-name3 {:baz -4113466}, :field-name1 {:field-name2 true}}}]
           (jdbc/execute! tu/*conn* ["SELECT t1.data FROM t1"]
                          tu/jdbc-qopts))
        "testing insert worked")

  (t/is (= [{:field-name2 true}]
           (xt/q tu/*node* "SELECT (t2.field_name1).field_name2, t2.field$name4.baz
                            FROM (SELECT (t1.data).field_name1, (t1.data).field$name4 FROM t1) AS t2"))
        "testing insert worked")

  (t/is (= [{:field-name2 true}]
           (jdbc/execute! tu/*conn*
                          ["SELECT (t2.field_name1).field_name2, t2.field$name4.baz
                            FROM (SELECT (t1.data).field_name1, (t1.data).field$name4 FROM t1) AS t2"]
                          tu/jdbc-qopts))
        "testing insert worked (JDBC)"))

(t/deftest test-get-field-on-duv-with-struct-2425
  (xt/submit-tx tu/*node* [[:sql "INSERT INTO t2(_id, data) VALUES(1, 'bar')"]
                           [:sql "INSERT INTO t2(_id, data) VALUES(2, OBJECT(foo: 2))"]])

  (t/is (= [{:foo 2} {}]
           (xt/q tu/*node* "SELECT (t2.data).foo FROM t2"))))

(t/deftest distinct-null-2535
  (xt/submit-tx tu/*node* [[:sql "INSERT INTO t1(_id, foo) VALUES(1, NULL)"]
                           [:sql "INSERT INTO t1(_id, foo) VALUES(2, NULL)"]])

  (t/is (= [{}]
           (xt/q tu/*node* "SELECT DISTINCT NULL AS nil FROM t1")))

  (t/is (= [{}]
           (xt/q tu/*node* "SELECT DISTINCT t1.foo FROM t1"))))

;; TODO move this to api-test once #2937 is in
(t/deftest throw-on-unknown-query-type
  (t/is (thrown-with-msg?
         xtdb.IllegalArgumentException
         #"Illegal argument: 'unknown-query-type'"
         (xt/q tu/*node* (Object.)))))


(t/deftest test-array-agg-2946
  (xt/submit-tx tu/*node*
                [[:put-docs :track {:xt/id :track-1 :name "foo1" :composer "bar" :album :album-1}]
                 [:put-docs :track {:xt/id :track-2 :name "foo2" :composer "bar" :album :album-1}]
                 [:put-docs :album {:xt/id :album-1 :name "foo-album"}]])


  (t/is (= [{:name "foo-album", :tracks ["foo1" "foo2"]}]
           (xt/q tu/*node*
                 "SELECT a.name, ARRAY_AGG(t.name) AS tracks
                  FROM track AS t, album AS a
                  WHERE t.album = a._id
                  GROUP BY a.name"))
        "array-agg")

  #_ ;;TODO parser needs to be adpated #2948
  (t/is (= [{:name "foo-album", :tracks ["bar"]}]
           (xt/q tu/*node*
                 "SELECT a.name, ARRAY_AGG(DISTINCT t.composer) AS composer
                  FROM track AS t, album AS a
                  WHERE t.album = a._id
                  GROUP BY a.name"))
        "array-agg distinct"))

(t/deftest start-node-from-non-map-config
  (t/testing "directly using Xtdb$Config"
    (let [config-object (doto (Xtdb$Config.)
                          (.setServer (ServerConfig. 0 42 nil)))]
      (with-open [node (xtn/start-node config-object)]
        (t/is node)
        (xt/submit-tx node [[:put-docs :docs {:xt/id :foo, :inst #inst "2021"}]])
        (t/is (= [{:e :foo, :inst (time/->zdt #inst "2021")}]
                 (xt/q node '(from :docs [{:xt/id e} inst])))))))

  (t/testing "using file based YAML config"
    (let [config-file (io/resource "test-config.yaml")]
      (with-open [node (xtn/start-node (io/file config-file))]
        (t/is node)
        (xt/submit-tx node [[:put-docs :docs {:xt/id :foo, :inst #inst "2021"}]])
        (t/is (= [{:e :foo, :inst (time/->zdt #inst "2021")}]
                 (xt/q node '(from :docs [{:xt/id e} inst]))))))))

(deftest assert-exists-on-empty-tables-3061
  (t/is (= (serde/->tx-aborted 0 #xt/instant "2020-01-01T00:00:00Z"
                                 #xt/runtime-err [:xtdb/assert-failed "Assert failed" {}])
           (xt/execute-tx tu/*node* [[:assert-exists '(from :users [{:xt/id :john}])]])))

  (t/is (= [{:xt/id 0, :committed? false,
             :error #xt/runtime-err [:xtdb/assert-failed "Assert failed" {}]}]
           (xt/q tu/*node*
                 '(from :xt/txs [xt/id {:committed committed?} error])))

        "assert fails on empty table")

  (t/is (= (serde/->tx-aborted 1 #xt/instant "2020-01-02T00:00:00Z"
                               #xt/runtime-err [:xtdb/assert-failed "Assert failed" {}])

           (xt/execute-tx tu/*node* [[:put-docs :users {:xt/id :not-john}]
                                     [:assert-exists '(from :users [{:xt/id :john}])]])))

  (t/is (= [{:xt/id 1,
             :committed? false,
             :error #xt/runtime-err [:xtdb/assert-failed "Assert failed" {}]}]
           (xt/q tu/*node*
                 '(from :xt/txs [{:xt/id 1, :committed committed?} xt/id error])))
        "when the table has an (non-matching) entry the assert also fails"))

(defn- random-maps [n]
  (let [nb-ks 5
        ks [:foo :bar :baz :toto :fufu]]
    (->> (repeatedly n #(zipmap ks (map str (repeatedly nb-ks random-uuid))))
         (map-indexed #(assoc %2 :xt/id %1)))))

(deftest resources-are-correctly-closed-on-interrupt-2800
  (let [node-dir (.toPath (io/file "target/incoming-puts-dont-cause-memory-leak"))
        node-opts {:node-dir node-dir
                   :rows-per-chunk 16}]
    (util/delete-dir node-dir)
    (dotimes [_ 5]
      (util/with-open [node (tu/->local-node node-opts)]
        (doseq [tx (->> (random-maps 32)
                        (map #(vector :put-docs :docs %))
                        (partition-all 16))]
          (xt/submit-tx node tx))))))

(deftest test-plan-query-cache
  (let [query-src ^IQuerySource (tu/component :xtdb.query/query-source)
        wm-src (tu/component :xtdb/indexer)
        pq1 (.planQuery query-src "SELECT 1" wm-src {})
        pq2 (.planQuery query-src "SELECT 1" wm-src {:explain? true})
        pq3 (.planQuery query-src "SELECT 1" wm-src {})]

    ;;could add explicit test for all query options that are relevant to planning

    (t/is (identical? pq1 pq3)
          "duplicate query with matching options is returned from cache")

    (t/is (not (identical? pq1 pq2))
          "different relevant query options returns new query")

     (let [tx-id (xt/submit-tx tu/*node* [[:put-docs :docs {:xt/id 1 :foo 2}]])]

       (tu/then-await-tx tx-id tu/*node*)

       (let [pq4 (.planQuery query-src "SELECT 1" wm-src {:after-tx-id tx-id})]

         (t/is (not (identical? pq1 pq4))
               "changing table-info causes previous cache-hits to miss")))))

(deftest test-prepared-statements
  (-> (xt/submit-tx tu/*node* [[:put-docs :foo {:xt/id 1 :a "one" :b 2}]
                               [:put-docs :unrelated-table {:xt/id 1 :a "a-string"}]])
      (tu/then-await-tx tu/*node* #xt/duration "PT2S"))

  (let [pq (xtp/prepare-sql tu/*node* "SELECT foo.*, ? FROM foo" {:param-types [:i64]})
        column-fields [#xt.arrow/field ["_id" #xt.arrow/field-type [#xt.arrow/type :i64 false]]
                       #xt.arrow/field ["a" #xt.arrow/field-type [#xt.arrow/type :utf8 false]]
                       #xt.arrow/field ["b" #xt.arrow/field-type [#xt.arrow/type :i64 false]]]]

    (t/is (= (conj column-fields
                   #xt.arrow/field ["_column_2" #xt.arrow/field-type [#xt.arrow/type :i64 true]])
             (.columnFields pq))
          "param type is assumed to be nullable")

    (with-open [bq (.bind pq {:args (tu/open-args [42])})
                cursor (.openCursor bq)]

      (t/is (= (conj column-fields
                     #xt.arrow/field ["_column_2" #xt.arrow/field-type [#xt.arrow/type :i64 false]])
               (.columnFields bq))
            "now param value has been supplied we know its type is non-null")

      (t/is (= [[{:xt/id 1, :a "one", :b 2, :xt/column-2 42}]]
               (tu/<-cursor cursor))))

    (t/testing "preparedQuery rebound with different param types"
      (with-open [bq (.bind pq {:args (tu/open-args ["fish"])})
                  cursor (.openCursor bq)]

        (t/is (= (conj column-fields
                       #xt.arrow/field ["_column_2" #xt.arrow/field-type [#xt.arrow/type :utf8 false]])
                 (.columnFields bq))
              "now param value has been supplied we know its type is non-null")

        (t/is (= [[{:xt/id 1, :a "one", :b 2, :xt/column-2 "fish"}]]
                 (tu/<-cursor cursor)))))

    (t/testing "statement invalidation"

      (with-open [bq (.bind pq {:args (tu/open-args [42])})]

        (let [res [[{:xt/id 2, :a "two", :b 3, :xt/column-2 42}
                    {:xt/id 1, :a "one", :b 2, :xt/column-2 42}]]]

          (t/testing "relevant schema unchanged since preparing query"

            (let [tx (xt/submit-tx tu/*node* [[:put-docs :foo {:xt/id 2 :a "two" :b 3}]])]
              (tu/then-await-tx tx tu/*node*))

            (with-open [cursor (.openCursor bq)]

              (t/is (= res (tu/<-cursor cursor)))))

          (t/testing "irrelevant schema changed since preparing query"

            (let [tx (xt/submit-tx tu/*node* [[:put-docs :unrelated-table {:xt/id 2 :a 222}]])]
              (tu/then-await-tx tx tu/*node*))

            (with-open [cursor (.openCursor bq)]

              (t/is (= res (tu/<-cursor cursor))))))

        (t/testing "relevant schema changed since preparing query"

          (let [tx (xt/submit-tx tu/*node* [[:put-docs :foo {:xt/id 3 :a 1 :b 4}]])]
            (tu/then-await-tx tx tu/*node*))

          (t/is (thrown-with-msg? xtdb.RuntimeException
                                  #"Relevant table schema has changed since preparing query, please prepare again"
                                  (.openCursor bq))))))))

(deftest test-prepared-statements-default-tz
  (t/testing "default-tz supplied at prepare"
    (let [ptz #xt/zone "America/New_York"
          pq (xtp/prepare-sql tu/*node* "SELECT CURRENT_TIMESTAMP x" {:default-tz ptz})]

      (t/testing "and not at bind"
        (with-open [bq (.bind pq {})
                    cursor (.openCursor bq)]

          (t/is (= ptz (.getZone ^ZonedDateTime (:x (ffirst (tu/<-cursor cursor))))))))

      (t/testing "and and also at bind"
        (let [tz #xt/zone "Asia/Bangkok"]
          (with-open [bq (.bind pq {:default-tz tz})
                      cursor (.openCursor bq)]

            (t/is (= tz (.getZone ^ZonedDateTime (:x (ffirst (tu/<-cursor cursor)))))))))))

  (t/testing "default-tz not supplied at prepare"
    (let [pq (xtp/prepare-sql tu/*node* "SELECT CURRENT_TIMESTAMP x" {})]

      (t/testing "and not at bind"
        (with-open [bq (.bind pq {})
                    cursor (.openCursor bq)]

          (t/is (= #xt/zone "Z" (.getZone ^ZonedDateTime (:x (ffirst (tu/<-cursor cursor))))))))

      (t/testing "but at bind"
        (let [tz #xt/zone "Asia/Bangkok"]
          (with-open [bq (.bind pq {:default-tz tz})
                      cursor (.openCursor bq)]

            (t/is (= tz (.getZone ^ZonedDateTime (:x (ffirst (tu/<-cursor cursor))))))))))))

(deftest test-default-param-types
  (let [pq (xtp/prepare-sql tu/*node* "SELECT ? v" {:param-types nil})]
    (t/is (= [#xt.arrow/field ["?_0" #xt.arrow/field-type [#xt.arrow/type :utf8 true]]]
             (.paramFields pq))
          "unspecified param-types are assumed to be utf8")

     (t/testing "preparedQuery rebound with args matching the assumed type"

       (with-open [bq (.bind pq {:args (tu/open-args ["42"])})
                   cursor (.openCursor bq)]

                   (t/is (= [[{:v "42"}]]
                            (tu/<-cursor cursor))))

       (t/testing "or can be rebound with a different type"

         (with-open [bq (.bind pq {:args (tu/open-args [44])})
                     cursor (.openCursor bq)]

           (t/is (= [[{:v 44}]]
                    (tu/<-cursor cursor))))))))

(deftest test-schema-ee-entry-points
  ;;test is using regclass to verify that expected EE entrypoints have access
  ;;to the new schema variable
  (xt/submit-tx tu/*node* [[:put-docs :bar {:xt/id 1, :col 904292726}]
                           [:put-docs :bar {:xt/id 2, :col 1111}]])


  (t/is (= [{:v (RegClass. 904292726)}]
           (tu/query-ra
            '[:project [{v (cast "bar" :regclass)}]
              [:table [{}]]]
            {:node tu/*node*}))
        "project")

  (t/is (= [{:v 904292726}]
           (tu/query-ra
            '[:select (= (cast "bar" :regclass) v)
              [:table [{v 904292726} {v 111}]]]
            {:node tu/*node*}))
        "select")

  (t/is (= [{:col 904292726}]
           (tu/query-ra
            '[:scan {:table public/bar}
              [{col (= col (cast "bar" :regclass))}]]
            {:node tu/*node*}))
        "scan pred"))

(t/deftest copes-with-log-time-going-backwards-3864
  (with-open [node (xtn/start-node {:log [:in-memory {:instant-src (tu/->mock-clock [#inst "2020" #inst "2019" #inst "2021"])}]})]
    (t/is (= 0 (xt/submit-tx node [[:put-docs :foo {:xt/id 1, :version 0}]])))
    (t/is (= 1 (xt/submit-tx node [[:put-docs :foo {:xt/id 1, :version 1}]])))
    (t/is (= 2 (xt/submit-tx node [[:put-docs :foo {:xt/id 1, :version 2}]])))

    (t/is (= [{:xt/id 0, :system-time #xt/zoned-date-time "2020-01-01T00:00Z[UTC]"}
              {:xt/id 1, :system-time #xt/zoned-date-time "2020-01-01T00:00:00.000001Z[UTC]"}
              {:xt/id 2, :system-time #xt/zoned-date-time "2021-01-01T00:00Z[UTC]"}]
             (xt/q node "SELECT _id, system_time FROM xt.txs ORDER BY _id")))))

(t/deftest startup-error-doesnt-output-integrant-system
  (t/is (thrown-with-msg? IllegalArgumentException
                          #"Port value out of range: 99999"
                          (xtn/start-node {:server {:port 99999}}))))

(t/deftest test-ts-tz-zone-first-wins-3723
  (xt/execute-tx tu/*node*
                 [[:put-docs :docs
                   {:xt/id 1 :ts #xt/zoned-date-time "2021-10-21T12:00+00:00[America/Los_Angeles]"}
                   {:xt/id 2 :ts #xt/zoned-date-time "2021-10-21T12:00+04:00"}
                   {:xt/id 3 :ts #xt/offset-date-time "2021-10-21T12:00+02:00"}]])

  (t/is (= [{:xt/id 1, :tz -7} {:xt/id 2, :tz 4} {:xt/id 3, :tz 2}]
           (xt/q tu/*node* "SELECT _id, EXTRACT(TIMEZONE_HOUR FROM ts) as tz from docs ORDER BY _id"))))
