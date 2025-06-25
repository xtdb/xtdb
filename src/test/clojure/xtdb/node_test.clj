(ns xtdb.node-test
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.test :as t :refer [deftest]]
            [next.jdbc :as jdbc]
            [xtdb.api :as xt]
            [xtdb.compactor :as c]
            [xtdb.error :as err]
            [xtdb.logging :as logging]
            [xtdb.next.jdbc :as xt-jdbc]
            [xtdb.node :as xtn]
            [xtdb.node.impl] ;;TODO probably move internal methods to main node interface
            [xtdb.protocols :as xtp]
            [xtdb.query :as query]
            [xtdb.serde :as serde]
            [xtdb.test-util :as tu]
            [xtdb.time :as time]
            [xtdb.util :as util])
  (:import [java.time ZoneId ZonedDateTime]
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
VALUES (1, 'Happy 2024!', TIMESTAMP '2024-01-01Z'),
       (1, 'Happy 2025!', TIMESTAMP '2025-01-01Z'),
       (1, 'Happy 2026!', TIMESTAMP '2026-01-01Z')"]])

    (t/is (= (expected (time/->zdt #inst "2020-01-01"))
             (q "posts")))

    (xt/submit-tx tu/*node* [[:sql "INSERT INTO posts2 (_id, body, _valid_from) VALUES (1, 'Happy 2024!', TIMESTAMP '2024-01-01Z')"]
                             [:sql "INSERT INTO posts2 (_id, body, _valid_from) VALUES (1, 'Happy 2025!', TIMESTAMP '2025-01-01Z')"]
                             [:sql "INSERT INTO posts2 (_id, body, _valid_from) VALUES (1, 'Happy 2026!', TIMESTAMP '2026-01-01Z')"]])

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
  (t/is (anomalous? [:incorrect nil
                     #"snapshot-time \(.+?\) is after the latest completed tx \(nil\)"]
                    (xt/q tu/*node* "SELECT 1"
                          {:snapshot-time #inst "2020"})))

  (xt/submit-tx tu/*node* [[:sql "INSERT INTO foo (_id) VALUES (1)"]])

  (t/is (= [{:xt/id 1}]
           (xt/q tu/*node* "SELECT * FROM foo"
                 {:snapshot-time #inst "2020"})))

  (t/is (anomalous? [:incorrect nil
                     #"snapshot-time \(.+?\) is after the latest completed tx \(#xt/tx-key \{.+?\}\)"]
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
  (t/is (= [{:a #xt/interval "P1Y"}]
           (xt/q tu/*node* "select a.a from (values (INTERVAL '1' YEAR)) a (a)"))))

(t/deftest test-overrides-range
  (xt/submit-tx tu/*node* [[:sql "
INSERT INTO foo (_id, v, _valid_from, _valid_to)
VALUES (1, 1, TIMESTAMP '1998-01-01Z', TIMESTAMP '2000-01-01Z')"]])

  (xt/submit-tx tu/*node* [[:sql "
INSERT INTO foo (_id, v, _valid_from, _valid_to)
VALUES (1, 2, TIMESTAMP '1997-01-01Z', TIMESTAMP '2001-01-01Z')"]])

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
FOR PORTION OF VALID_TIME FROM TIMESTAMP '2022-01-01Z' TO TIMESTAMP '2024-01-01Z'
SET v = 2
WHERE foo._id = 1"]])

    (xt/submit-tx tu/*node* [[:sql "
DELETE FROM foo
FOR PORTION OF VALID_TIME FROM TIMESTAMP '2023-01-01Z' TO TIMESTAMP '2025-01-01Z'
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
  ;; this is now supported
  (xt/execute-tx tu/*node* [[:sql "INSERT INTO foo (_id, _valid_from) VALUES (1, '2018-01-01')"]]
                 {:default-tz #xt/zone "Asia/Tokyo"})

  (t/is (= [{:xt/id 1, :xt/valid-from (.withZoneSameInstant #xt/zdt "2018-01-01+09:00[Asia/Tokyo]" (ZoneId/of "UTC"))}]
           (xt/q tu/*node* "SELECT _id, _valid_from FROM foo")))

  (t/is (anomalous? [:incorrect ::err/date-time-parse #"Text 'nope-01-01' could not be parsed"]
                    (xt/execute-tx tu/*node* [[:sql "INSERT INTO foo (_id, _valid_from) VALUES (1, 'nope-01-01')"]]))))

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
    (t/is (= (serde/->TxKey 0 #xt/instant "2020-01-01T00:00:00Z")
             (xt/execute-tx tu/*node* [[:put-docs :docs {:xt/id :foo}]])))

    (t/is (anomalous? [:conflict] (xt/execute-tx tu/*node* [["ASSERT 1 = 2"]])))

    (t/is (= (serde/->TxKey 2 #xt/instant "2020-01-03T00:00:00Z")
             (xt/execute-tx tu/*node* [[:put-docs :docs {:xt/id :bar}]])))

    (t/is (anomalous? [:conflict :xtdb/assert-failed "boom"]
                      (xt/execute-tx tu/*node* ["ASSERT 1 = 2, 'boom'"])))

    (t/is (= #{{:tx-id 0, :system-time (time/->zdt #inst "2020-01-01"), :committed? true}
               {:tx-id 1, :system-time (time/->zdt #inst "2020-01-02"), :committed? false}
               {:tx-id 2, :system-time (time/->zdt #inst "2020-01-03"), :committed? true}
               {:tx-id 3, :system-time (time/->zdt #inst "2020-01-04"), :committed? false}}
             (set (xt/q tu/*node*
                        '(from :xt/txs [{:xt/id tx-id, :system-time system-time, :committed committed?}])))))

    (t/is (= [{:committed? false}]
             (xt/q tu/*node*
                   ['#(from :xt/txs [{:xt/id %, :committed committed?}]) 1])))

    (t/is (= [{:err #xt/error [:conflict :xtdb/assert-failed "boom"
                               {:sql "ASSERT 1 = 2, 'boom'", :tx-op-idx 0, :arg-idx 0
                                :tx-key #xt/tx-key {:tx-id 3, :system-time #xt/instant "2020-01-04T00:00:00Z"}}]}]
             (xt/q tu/*node* ['#(from :xt/txs [{:xt/id %, :error err}]) 3])))))

(t/deftest test-indexer-cleans-up-aborted-transactions-2489
  (t/testing "INSERT"
    (t/is (anomalous? [:incorrect nil
                       #"Errors planning SQL statement:\s*- Cannot parse date:"]
                      (xt/execute-tx tu/*node*
                                     [[:sql "INSERT INTO docs (_id, _valid_from, _valid_to)
                                                  VALUES (1, DATE '2010-01-01', DATE '2020-01-01'),
                                                         (1, DATE '2030- 01-01', DATE '2020-01-01')"]])))

    (t/is (= [{:committed? false}]
             (xt/q tu/*node*
                   ['#(from :xt/txs [{:xt/id %, :committed committed?}]) 0])))))

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
  (t/is (anomalous? [:incorrect nil
                     #"Duplicate column projection: _id"]
                    (xt/q tu/*node* "FROM foo, bar")))

  (t/is (anomalous? [:incorrect nil
                     #"Duplicate column projection: _id"]
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
  (t/testing "testing scan.allTableColNames combines table info from both live and past blocks"
    (-> (xt/submit-tx tu/*node* [[:put-docs :foo {:xt/id "foo1" :a 1}]
                                 [:put-docs :bar {:xt/id "bar1"}]
                                 [:put-docs :bar {:xt/id "bar2" :b 2}]])
        (tu/then-await-tx tu/*node*))

    (tu/finish-block! tu/*node*)

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
  [:select (= (+ a b) 12) [:scan {:table public/users} [a _id foo b]]]]]
")}]
           (-> (xt/q tu/*node*
                     "EXPLAIN SELECT u._id, u.foo FROM users u WHERE u.a + u.b = 12")
               (update-in [0 :plan] str/trim))))

  (t/is (= [{:plan (str/trim "
[:project
 [{_id u.1/_id} {foo u.1/foo}]
 [:rename
  u.1
  [:select (= (+ a b) 12) [:scan {:table public/users} [a _id foo b]]]]]
")}]
           (-> (xt/q tu/*node*
                     "EXPLAIN SELECT u._id, u.foo FROM users u WHERE u.a + u.b = 12")
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
           (jdbc/execute! tu/*node* ["SELECT t1.data FROM t1"]
                          {:builder-fn xt-jdbc/builder-fn}))
        "testing insert worked")

  (t/is (= [{:field-name2 true}]
           (xt/q tu/*node* "SELECT (t2.field_name1).field_name2, t2.field$name4.baz
                            FROM (SELECT (t1.data).field_name1, (t1.data).field$name4 FROM t1) AS t2"))
        "testing insert worked")

  (t/is (= [{:field-name2 true}]
           (jdbc/execute! tu/*node*
                          ["SELECT (t2.field_name1).field_name2, t2.field$name4.baz
                            FROM (SELECT (t1.data).field_name1, (t1.data).field$name4 FROM t1) AS t2"]
                          {:builder-fn xt-jdbc/builder-fn}))
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
  (t/is (anomalous? [:incorrect nil "Unknown query type"]
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
                          (.setServer (ServerConfig. nil 0 -1 42 nil)))]
      (with-open [node (xtn/start-node config-object)]
        (t/is node)
        (xt/submit-tx node [[:put-docs :docs {:xt/id :foo, :inst #inst "2021"}]])
        (t/is (= [{:e :foo, :inst (time/->zdt #inst "2021")}]
                 (xt/q node '(from :docs [{:xt/id e} inst])))))))

  (t/testing "using file based YAML config"
    (let [config-file (io/resource "test-config.yaml")]
      (with-open [node (xtn/start-node (io/file config-file))]
        (xt/submit-tx node [[:put-docs :docs {:xt/id :foo, :inst #inst "2021"}]])
        (t/is (= [{:e :foo, :inst (time/->zdt #inst "2021")}]
                 (xt/q node '(from :docs [{:xt/id e} inst]))))))))

(defn- random-maps [n]
  (let [nb-ks 5
        ks [:foo :bar :baz :toto :fufu]]
    (->> (repeatedly n #(zipmap ks (map str (repeatedly nb-ks random-uuid))))
         (map-indexed #(assoc %2 :xt/id %1)))))

(deftest resources-are-correctly-closed-on-interrupt-2800
  (let [node-dir (.toPath (io/file "target/incoming-puts-dont-cause-memory-leak"))
        node-opts {:node-dir node-dir
                   :rows-per-block 16}]
    (util/delete-dir node-dir)
    (dotimes [_ 5]
      (util/with-open [node (tu/->local-node node-opts)]
        (doseq [tx (->> (random-maps 32)
                        (map #(vector :put-docs :docs %))
                        (partition-all 16))]
          (xt/submit-tx node tx))))))

(deftest test-plan-query-cache
  (let [query-src ^IQuerySource (tu/component :xtdb.query/query-source)
        pq1 (.planQuery query-src "SELECT 1" {})
        pq2 (.planQuery query-src "SELECT 1" {:explain? true})
        pq3 (.planQuery query-src "SELECT 1" {})]

    ;;could add explicit test for all query options that are relevant to planning

    (t/is (identical? pq1 pq3)
          "duplicate query with matching options is returned from cache")

    (t/is (not (identical? pq1 pq2))
          "different relevant query options returns new query")

     (let [tx-id (xt/submit-tx tu/*node* [[:put-docs :docs {:xt/id 1 :foo 2}]])]

       (tu/then-await-tx tx-id tu/*node*)

       (let [pq4 (.planQuery query-src "SELECT 1" {:after-tx-id tx-id})]

         (t/is (not (identical? pq1 pq4))
               "changing table-info causes previous cache-hits to miss")))))

(deftest test-prepared-statements
  (-> (xt/submit-tx tu/*node* [[:put-docs :foo {:xt/id 1 :a "one" :b 2}]
                               [:put-docs :unrelated-table {:xt/id 1 :a "a-string"}]])
      (tu/then-await-tx tu/*node* #xt/duration "PT2S"))

  (let [pq (xtp/prepare-sql tu/*node* "SELECT foo.*, ? FROM foo" {})
        column-fields [#xt.arrow/field ["_id" #xt.arrow/field-type [#xt.arrow/type :i64 false]]
                       #xt.arrow/field ["a" #xt.arrow/field-type [#xt.arrow/type :utf8 false]]
                       #xt.arrow/field ["b" #xt.arrow/field-type [#xt.arrow/type :i64 false]]]]

    (t/is (= (conj column-fields #xt.arrow/field ["_column_2" #xt.arrow/field-type [#xt.arrow/type :i64 false]])
             (.getColumnFields pq [#xt.arrow/field ["?_0" #xt.arrow/field-type [#xt.arrow/type :i64 false]]]))
          "param type is assumed to be nullable")

    (with-open [cursor (.openQuery pq {:args (tu/open-args [42])})]

      (t/is (= (conj column-fields
                     #xt.arrow/field ["_column_2" #xt.arrow/field-type [#xt.arrow/type :i64 false]])
               (.getResultFields cursor))
            "now param value has been supplied we know its type is non-null")

      (t/is (= [[{:xt/id 1, :a "one", :b 2, :xt/column-2 42}]]
               (tu/<-cursor cursor))))

    (t/testing "preparedQuery rebound with different param types"
      (with-open [cursor (.openQuery pq {:args (tu/open-args ["fish"])})]

        (t/is (= (conj column-fields
                       #xt.arrow/field ["_column_2" #xt.arrow/field-type [#xt.arrow/type :utf8 false]])
                 (.getResultFields cursor))
              "now param value has been supplied we know its type is non-null")

        (t/is (= [[{:xt/id 1, :a "one", :b 2, :xt/column-2 "fish"}]]
                 (tu/<-cursor cursor)))))

    (t/testing "statement invalidation"
      (with-open [args (tu/open-args [42])]
        (let [res [[{:xt/id 2, :a "two", :b 3, :xt/column-2 42}
                    {:xt/id 1, :a "one", :b 2, :xt/column-2 42}]]]

          (t/testing "relevant schema unchanged since preparing query"
            (let [tx (xt/submit-tx tu/*node* [[:put-docs :foo {:xt/id 2 :a "two" :b 3}]])]
              (tu/then-await-tx tx tu/*node*))

            (with-open [cursor (.openQuery pq {:args args, :close-args? false})]
              (t/is (= res (tu/<-cursor cursor)))))

          (t/testing "irrelevant schema changed since preparing query"

            (let [tx (xt/submit-tx tu/*node* [[:put-docs :unrelated-table {:xt/id 2 :a 222}]])]
              (tu/then-await-tx tx tu/*node*))

            (with-open [cursor (.openQuery pq {:args args, :close-args? false})]
              (t/is (= res (tu/<-cursor cursor)))))

          (t/testing "a -> union, but prepared query is still fine outside of pgwire"
            (let [tx (xt/submit-tx tu/*node* [[:put-docs :foo {:xt/id 3 :a 1 :b 4}]])]
              (tu/then-await-tx tx tu/*node*))

            (with-open [cursor (.openQuery pq {:args args, :close-args? false})]
              (t/is (= [[{:xt/id 2, :a "two", :b 3, :xt/column-2 42}
                         {:xt/id 1, :a "one", :b 2, :xt/column-2 42}
                         {:xt/id 3, :a 1, :b 4, :xt/column-2 42}]]
                       (tu/<-cursor cursor))))))))))

(deftest test-prepared-statements-default-tz
  (t/testing "default-tz supplied at prepare"
    (let [ptz #xt/zone "America/New_York"
          pq (xtp/prepare-sql tu/*node* "SELECT CURRENT_TIMESTAMP x" {:default-tz ptz})]

      (t/testing "and not at bind"
        (with-open [cursor (.openQuery pq {})]
          (t/is (= ptz (.getZone ^ZonedDateTime (:x (ffirst (tu/<-cursor cursor))))))))

      (t/testing "and and also at bind"
        (let [tz #xt/zone "Asia/Bangkok"]
          (with-open [cursor (.openQuery pq {:default-tz tz})]
            (t/is (= tz (.getZone ^ZonedDateTime (:x (ffirst (tu/<-cursor cursor)))))))))))

  (t/testing "default-tz not supplied at prepare"
    (let [pq (xtp/prepare-sql tu/*node* "SELECT CURRENT_TIMESTAMP x" {})]

      (t/testing "and not at open"
        (with-open [cursor (.openQuery pq {})]
          (t/is (= #xt/zone "Z" (.getZone ^ZonedDateTime (:x (ffirst (tu/<-cursor cursor))))))))

      (t/testing "but at bind"
        (let [tz #xt/zone "Asia/Bangkok"]
          (with-open [cursor (.openQuery pq {:default-tz tz})]
            (t/is (= tz (.getZone ^ZonedDateTime (:x (ffirst (tu/<-cursor cursor))))))))))))

(deftest test-default-param-types
  (let [pq (xtp/prepare-sql tu/*node* "SELECT ? v" {:param-types nil})]
    (t/testing "preparedQuery rebound with args matching the assumed type"

      (with-open [cursor (.openQuery pq {:args (tu/open-args ["42"])})]
        (t/is (= [[{:v "42"}]]
                 (tu/<-cursor cursor))))

      (t/testing "or can be rebound with a different type"
        (with-open [cursor (.openQuery pq {:args (tu/open-args [44])})]
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

    (t/is (= [{:xt/id 0, :system-time #xt/zoned-date-time "2020-01-01Z[UTC]"}
              {:xt/id 1, :system-time #xt/zoned-date-time "2020-01-01T00:00:00.000001Z[UTC]"}
              {:xt/id 2, :system-time #xt/zoned-date-time "2021-01-01Z[UTC]"}]
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

(t/deftest test-skip-txes
  (let [!skiptxid (atom nil)]
    (util/with-tmp-dirs #{path}
      (t/testing "node with no txes skipped:"
        (with-open [node (xtn/start-node {:log [:local {:path (str path "/log")}]
                                          :storage [:local {:path (str path "/storage")}]})]
          (t/testing "Send three transactions"
            (xt/execute-tx node [[:put-docs :xt_docs {:xt/id :foo}]])
            (let [{:keys [tx-id]} (xt/execute-tx node [[:put-docs :xt_docs {:xt/id :bar}]])]
              (reset! !skiptxid tx-id))
            (xt/execute-tx node [[:put-docs :xt_docs {:xt/id :baz}]]))

          (t/testing "Can query three back out"
            (t/is (= (set [{:xt/id :foo} {:xt/id :bar} {:xt/id :baz}])
                     (set (xt/q node "SELECT * from xt_docs")))))))

      (t/testing "node with txs to skip"
        (with-open [node (xtn/start-node {:log [:local {:path (str path "/log")}]
                                          :storage [:local {:path (str path "/storage")}]
                                          :indexer {:skip-txs [@!skiptxid]}})]
          (tu/then-await-tx node)
          (t/testing "Can query two back out - skipped one"
            (t/is (= (set [{:xt/id :foo} {:xt/id :baz}])
                     (set (xt/q node "SELECT * from xt_docs")))))

          ;; Call finish-block! to write files
          (tu/finish-block! node)))

      (t/testing "node can remove 'txs to skip' after block finished"
        (with-open [node (xtn/start-node {:log [:local {:path (str path "/log")}]
                                          :storage [:local {:path (str path "/storage")}]})]
          (t/testing "Only two results are returned"
            (t/is (= (set [{:xt/id :foo} {:xt/id :baz}])
                     (set (xt/q node "SELECT * from xt_docs"))))))))))


(t/deftest test-skip-txes-latest-submitted-tx-id
  (let [!skiptxid (atom nil)]
    (util/with-tmp-dirs #{path}
      (t/testing "node with no txes skipped:"
        (with-open [node (xtn/start-node {:log [:local {:path (str path "/log")}]
                                          :storage [:local {:path (str path "/storage")}]})]
          (t/testing "Send two transactions"
            (xt/execute-tx node [[:put-docs :xt_docs {:xt/id :foo}]])
            (let [{:keys [tx-id]} (xt/execute-tx node [[:put-docs :xt_docs {:xt/id :bar}]])]
              (reset! !skiptxid tx-id)))

          (t/is (= @!skiptxid (:tx-id (:latest-completed-tx (xt/status node)))))))

      (t/testing "node with txs to skip"
        (with-open [node (xtn/start-node {:log [:local {:path (str path "/log")}]
                                          :storage [:local {:path (str path "/storage")}]
                                          :indexer {:skip-txs [@!skiptxid]}
                                          :compactor {:threads 0}})]

          (tu/then-await-tx node)
          (t/testing "Can query one back out - skipped one"
            (t/is (= (set [{:xt/id :foo}]) (set (xt/q node "SELECT * from xt_docs")))))

          (t/testing "Latest submitted tx id should be the one that was skipped"
            (t/is (= @!skiptxid (:tx-id (:latest-completed-tx (xt/status node))))))

          ;; Call finish-block! to write files
          (tu/finish-block! node)))

      (t/testing "node can remove 'txs to skip' after block finished"
        (with-open [node (xtn/start-node {:log [:local {:path (str path "/log")}]
                                          :storage [:local {:path (str path "/storage")}]
                                          :compactor {:threads 0}})]

          (t/testing "Latest submitted tx id should still be the one that was skipped"
            (t/is (= @!skiptxid (:tx-id (:latest-completed-tx (xt/status node))))))

          (t/testing "Can send a new transaction after skipping one"
            (xt/execute-tx node [[:put-docs :xt_docs {:xt/id :baz}]])
            (t/is (= (set [{:xt/id :foo} {:xt/id :baz}])
                     (set (xt/q node "SELECT * from xt_docs"))))))))))

(t/deftest null-to-duv-promotion-halts-ingestion-4153
  (xt/submit-tx tu/*node* ["INSERT INTO docs RECORDS {_id: 1, a: 1, b: 1.5}"])
  (xt/execute-tx tu/*node* ["INSERT INTO docs RECORDS {_id: 2, a: 2, b: 1}"])
  (tu/finish-block! tu/*node*)
  ;; on disk, b :: Union(f64,i64)

  (xt/submit-tx tu/*node* ["INSERT INTO docs RECORDS {_id: 3, b: null}"])
  ;; we don't read the disk here, so live index b :: null

  (xt/submit-tx tu/*node* ["UPDATE docs SET a = 0.1 WHERE _id = 2"])
  ;; this one promotes the null to DUV(f64,i64) but doesn't set the id=3 null correctly
  ;; setting b here instead fixes the issue because it promotes correctly before needing to read the null value out

  (xt/submit-tx tu/*node* ["UPDATE docs SET a = 0.1 WHERE _id = 3"])
  ;; we try to copy b out of the live index -> boom.
  ;; setting b here, again, fixes the issue because it doesn't need to read the null value out

  (t/is (= [{:xt/id 1, :a 1, :b 1.5} {:xt/id 2, :b 1, :a 0.1} {:xt/id 3, :a 0.1}]
           (xt/q tu/*node* ["SELECT * FROM docs ORDER BY _id"]))))

(deftest check-explicitly-for-l0-content-metadat-4185
  (with-open [node (xtn/start-node {:compactor {:threads 0}})]
    (xt/execute-tx node [[:put-docs :docs {:xt/id 1 :a #inst "2000-01-01"}]] )
    (tu/finish-block! node)

    (t/is (= [{:xt/id 1}]
             (xt/q node ["SELECT _id FROM docs WHERE a < ?" #xt/date "2500-01-01"])))))

(t/deftest able-to-override-valid-time-in-put-docs
  (xt/execute-tx tu/*node* [[:put-docs :docs
                             {:xt/id 1, :xt/valid-from #inst "2000-01-01"}
                             {:xt/id 1, :a 1}
                             {:xt/id 2, :xt/valid-from #inst "2000-01-01", :xt/valid-to #inst "2000-01-02"}]])

  (t/is (= [{:xt/id 1, :xt/valid-from (time/->zdt #inst "2000-01-01"), :xt/valid-to (time/->zdt #inst "2020-01-01")}
            {:xt/id 1, :a 1, :xt/valid-from (time/->zdt #inst "2020-01-01")}
            {:xt/id 2, :xt/valid-from (time/->zdt #inst "2000-01-01"), :xt/valid-to (time/->zdt #inst "2000-01-02")}]
           (xt/q tu/*node* "SELECT *, _valid_from, _valid_to FROM docs FOR ALL VALID_TIME ORDER BY _id, _valid_from")))

  (xt/execute-tx tu/*node* [[:put-docs {:into :docs2, :valid-from #inst "2025-01-01"}
                             {:xt/id 1, :xt/valid-from #inst "2000-01-01"}
                             {:xt/id 1, :a 1}
                             {:xt/id 2, :xt/valid-from #inst "2000-01-01", :xt/valid-to #inst "2000-01-02"}]])

  (t/is (= [{:xt/id 1, :xt/valid-from (time/->zdt #inst "2000-01-01"), :xt/valid-to (time/->zdt #inst "2025-01-01")}
            {:xt/id 1, :a 1, :xt/valid-from (time/->zdt #inst "2025-01-01")}
            {:xt/id 2, :xt/valid-from (time/->zdt #inst "2000-01-01"), :xt/valid-to (time/->zdt #inst "2000-01-02")}]
           (xt/q tu/*node* "SELECT *, _valid_from, _valid_to FROM docs2 FOR ALL VALID_TIME ORDER BY _id, _valid_from"))))

(t/deftest test-finish-block-bug
  (util/with-tmp-dirs #{path}
    (with-open [node (xtn/start-node {:log [:local {:path (.resolve path "log")}]
                                      :storage [:local {:path (.resolve path "objects")}]
                                      :compactor {:threads 0}})]
      (xt/execute-tx node [[:put-docs :docs {:xt/id :foo}]])
      (xt/execute-tx node [[:put-docs :docs {:xt/id :foo}]])
      (xt/execute-tx node [[:put-docs :docs {:xt/id :foo}]])
      (tu/finish-block! node)

      (t/is (= [{:xt/id :foo}]
               (xt/q node "SELECT * FROM docs"))))

    (with-open [node (xtn/start-node {:log [:local {:path (.resolve path "log")}]
                                      :storage [:local {:path (.resolve path "objects")}]
                                      :compactor {:threads 0}})]

      ;; FAILED - returned {:xt/id :foo} three times
      (t/is (= [{:xt/id :foo}]
               (xt/q node "SELECT * FROM docs")))

      (xt/execute-tx node [[:put-docs :docs {:xt/id :foo}]])

      ;; SUCCEEDED - returned {:xt/id :foo} once
      (t/is (= [{:xt/id :foo}]
               (xt/q node "SELECT * FROM docs"))))))

(t/deftest ingestion-stopped-on-null-col-ref-4259
  (t/is (anomalous? [:incorrect nil
                     #"Invalid ID type"]
                    (xt/execute-tx tu/*node* ["INSERT INTO docs RECORDS {_id: \"1\"}"]))))

(t/deftest test-field-mismatch-4271
  (xt/execute-tx tu/*node* ["INSERT INTO docs RECORDS {_id: 1, a: 1.5}"])

  (tu/finish-block! tu/*node*)

  (xt/execute-tx tu/*node* ["INSERT INTO docs RECORDS {_id: 2, a: 3, b: [2, 3, 4], c: {d: 1}}"])

  (xt/execute-tx tu/*node* ["UPDATE docs SET a = 'hello' WHERE _id = 1"])

  (t/is (= [{:xt/id 1, :a 1.5,
             :xt/valid-from (time/->zdt #inst "2020-01-01")
             :xt/valid-to (time/->zdt #inst "2020-01-03")}
            {:xt/id 1, :a "hello",
             :xt/valid-from (time/->zdt #inst "2020-01-03")}
            {:xt/id 2, :a 3, :b [2 3 4], :c {:d 1}
             :xt/valid-from (time/->zdt #inst "2020-01-02")}]

           (xt/q tu/*node* ["SELECT *, _valid_from, _valid_to
                             FROM docs FOR ALL VALID_TIME
                             ORDER BY _id, _valid_from"]))))

(t/deftest pushdown-bloom-col-renaming-at-cursor-build-time-4279
  (with-open [node (xtn/start-node)]
    (xt/execute-tx node ["INSERT INTO foo (_id) VALUES ('foo1')"
                         "INSERT INTO bar (_id, foo) VALUES ('bar1', 'foo1')"])
    (tu/finish-block! node)

    (c/compact-all! node #xt/duration "PT1S")

    (t/is (= [{:foo "foo1", :bar "bar1"}]
             (tu/query-ra '[:project [{bar _id} {foo foo/_id}]
                            [:semi-join [{_id vals/_column_1}]
                             [:rename {bar/_id _id}
                              [:join [{foo/_id bar/foo}]
                               [:rename foo [:scan {:table public/foo} [_id]]]
                               [:rename bar [:scan {:table public/bar} [_id foo]]]]]
                             [:rename vals
                              [:table [_column_1]
                               [{:_column_1 "bar1"}]]]]]
                          {:node node})))

    (t/is (= [{:xt/id "bar1"}]
             (xt/q node "FROM foo
                         JOIN bar ON bar.foo = foo._id
                         SELECT bar._id AS _id,
                         WHERE _id IN ('bar1')")))))

(t/deftest ^:integration log-backpressure-doesnt-halt-indexer-4285
  (doseq [batch (partition-all 1000 (range 400000))]
    (xt/submit-tx tu/*node* [(into [:put-docs :docs] (map (fn [idx] {:xt/id idx})) batch)]))
  (t/is (= [{:v 1 }] (xt/q tu/*node* "SELECT 1 AS v"))))

(deftest pushdown-blooms-not-working-for-l0
  (binding [c/*ignore-signal-block?* true]
    (xt/execute-tx tu/*node* [[:put-docs :xt-docs {:xt/id :foo, :col 1}]
                              [:put-docs :xt-docs {:xt/id :bar, :col 2}]])
    (tu/finish-block! tu/*node*)
    (xt/execute-tx tu/*node* [[:put-docs :xt-docs {:xt/id :toto, :col 3}]])
    (tu/finish-block! tu/*node*))

  (t/is (= [{:xt/id :toto, :col 3}]
           (tu/query-ra
            '[:join [{col col}]
              [:scan {:table public/xt_docs} [_id {col (= col 3)}]]
              [:scan {:table public/xt_docs} [_id col]]]
            {:node tu/*node*}))))

(deftest test-decimal-support
  ;; different precisions same transactions
  (xt/execute-tx tu/*node* [[:put-docs :table
                             {:xt/id 1 :data 0.1M}
                             {:xt/id 2 :data 24580955505371094.000001M}]])
  (xt/execute-tx tu/*node* [[:put-docs :table {:xt/id 3 :data 1000000000000000000000000000000000000000000000000M}]])
  ;; different bitwidth
  (xt/execute-tx tu/*node* [[:put-docs :table
                             {:xt/id 4 :data (/ 1.0 1000000000000000000000000000000000000M)}
                             {:xt/id 5 :data 61954235709850086879078532699846656405640394575840079131296.39935M}]])

  (let [res [{:xt/id 1, :data 0.1M}
             {:xt/id 2, :data 24580955505371094.000001M}
             {:xt/id 3 :data 1000000000000000000000000000000000000000000000000M}
             {:xt/id 4 :data (/ 1.0 1000000000000000000000000000000000000M)}
             {:xt/id 5 :data 61954235709850086879078532699846656405640394575840079131296.39935M}]]

    (t/is (= res (xt/q tu/*node* "SELECT * FROM table ORDER BY _id")))

    (tu/finish-block! tu/*node*)
    (c/compact-all! tu/*node* nil)

    (t/is (= res (xt/q tu/*node* "SELECT * FROM table ORDER BY _id")))))

