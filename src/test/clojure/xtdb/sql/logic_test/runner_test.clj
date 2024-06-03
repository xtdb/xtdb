(ns xtdb.sql.logic-test.runner-test
  (:require [clojure.test :as t]
            [xtdb.sql.logic-test.runner :as slt]
            [xtdb.sql.logic-test.xtdb-engine :as xtdb-engine]
            [xtdb.sql.plan :as plan]
            [xtdb.test-util :as tu]))

(t/use-fixtures :each tu/with-node)

(t/deftest test-sql-logic-test-parser
  (t/is (= 6 (count (slt/parse-script
                     "# full line comment

statement ok
CREATE TABLE t1(a INTEGER, b INTEGER, c INTEGER, d INTEGER, e INTEGER)

statement ok
INSERT INTO t1(e,c,b,d,a) VALUES(103,102,100,101,104) # end of line comment

halt

hash-threshold 32

query I nosort
SELECT CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END
  FROM t1
 ORDER BY 1
----
30 values hashing to 3c13dee48d9356ae19af2515e05e6b54

skipif postgresql
query III rowsort label-xyzzy
SELECT a x, b y, c z FROM t1
")))))

(t/deftest test-sql-logic-test-execution
  (let [records (slt/parse-script
                 "
statement ok
CREATE TABLE t1(a INTEGER, b INTEGER, c INTEGER, d INTEGER, e INTEGER)

statement ok
INSERT INTO t1(e,c,b,d,a) VALUES(103,102,100,101,104)

query I nosort
SELECT t1.a FROM t1
----
104
")]



    (t/is (= 3 (count records)))
    (t/is (= {:t1 '[a b c d e]} (:tables (:db-engine (slt/execute-records tu/*node* records)))))))

(t/deftest test-sql-logic-test-completion
  (let [script "statement ok
CREATE TABLE t1(a INTEGER, b INTEGER, c INTEGER, d INTEGER, e INTEGER)

statement ok
INSERT INTO t1(e,c,b,d,a) VALUES(103,102,100,101,104)

query I nosort
SELECT t1.a FROM t1
----
104

skipif xtdb
query I nosort
SELECT t1.b FROM t1
----
foo

"
        records (slt/parse-script script)]

    (binding [slt/*opts* {:script-mode :completion :query-limit 2}]
      (t/is (= script
               (with-out-str
                 (slt/execute-records tu/*node* records)))))))

(t/deftest test-parse-create-table
  (t/is (= '{:type :create-table
             :table-name :t1
             :columns [a b]}
           (xtdb-engine/parse-create-table "CREATE TABLE t1(a INTEGER, b INTEGER)")))

  (t/is (= '{:type :create-table
             :table-name :t2
             :columns [a b c]}

           (xtdb-engine/parse-create-table "
CREATE TABLE t2(
  a INTEGER,
  b VARCHAR(30),
  c INTEGER
)"))))

(t/deftest test-parse-create-view
  (t/is (= {:type :create-view
            :view-name :view_1
            :as "SELECT pk, col0 FROM tab0 WHERE col0 = 49"}
           (xtdb-engine/parse-create-view "CREATE VIEW view_1 AS SELECT pk, col0 FROM tab0 WHERE col0 = 49"))))

(t/deftest test-skip-statement?
  (t/is (false? (xtdb-engine/skip-statement? "INSERT INTO t1(e,c,b,d,a) VALUES(103,102,100,101,104)")))
  (t/is (true? (xtdb-engine/skip-statement? "CREATE INDEX t1i0 ON t1(a1,b1,c1,d1,e1,x1)")))
  (t/is (true? (xtdb-engine/skip-statement? "
CREATE UNIQUE INDEX t1i0 ON t1(
  a1,
  b1
)"))))

(t/deftest test-insert->doc
  (let [tables {:t1 '[a b c d e]}]
    (letfn [(sql->ops [sql-insert-string]
              (-> (.accept (plan/parse-statement sql-insert-string)
                           (xtdb-engine/->InsertOpsVisitor (assoc tu/*node* :tables tables) sql-insert-string))
                  (update-in [0 2] dissoc :xt/id)))]
      (t/is (= [[:put-docs :t1 {:e 103 :c 102 :b 100 :d 101 :a 104}]]
               (sql->ops "INSERT INTO t1(e,c,b,d,a) VALUES(103,102,100,101,104)")))

      (t/is (= [[:put-docs :t1 {:a nil :b -102 :c true :d "101" :e 104.5}]]
               (sql->ops "INSERT INTO t1 VALUES(NULL,-102,TRUE,'101',104.5)"))))))
