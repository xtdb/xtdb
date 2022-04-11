(ns core2.sql.logic-test.runner-test
  (:require [clojure.test :as t]
            [clojure.java.io :as io]
            [core2.sql :as sql]
            [core2.sql.logic-test.runner :as slt]
            [core2.sql.logic-test.xtdb-engine :as xtdb-engine]
            [core2.test-util :as tu]))

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
    (t/is (= {"t1" ["a" "b" "c" "d" "e"]} (:tables (slt/execute-records tu/*node* records))))))

(t/deftest test-parse-create-table
  (t/is (= {:type :create-table
            :table-name "t1"
            :columns ["a" "b"]}
           (xtdb-engine/parse-create-table "CREATE TABLE t1(a INTEGER, b INTEGER)")))

  (t/is (= {:type :create-table
            :table-name "t2"
            :columns ["a" "b" "c"]}

           (xtdb-engine/parse-create-table "
CREATE TABLE t2(
  a INTEGER,
  b VARCHAR(30),
  c INTEGER
)"))))

(t/deftest test-parse-create-view
  (t/is (= {:type :create-view
            :view-name "view_1"
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

(t/deftest test-normalize-query
  (let [tables {"t1" ["a" "b" "c" "d" "e"]}
        query "SELECT a+b*2+c*3+d*4+e*5, (a+b+c+d+e)/5 FROM t1 ORDER BY 1,2"
        expected "SELECT t1.a+t1.b*2+t1.c*3+t1.d*4+t1.e*5 AS col1, (t1.a+t1.b+t1.c+t1.d+t1.e)/5 AS col2 FROM t1 ORDER BY col1,col2"]
    (t/is (= (sql/parse expected :query_expression)
             (xtdb-engine/normalize-query tables (sql/parse query :query_expression))))))

(t/deftest test-insert->doc
  (let [tables {"t1" ["a" "b" "c" "d" "e"]}]
    (t/is (= [{:e 103 :c 102 :b 100 :d 101 :a 104 :_table "t1"}]
             (xtdb-engine/insert->docs tables (sql/parse "INSERT INTO t1(e,c,b,d,a) VALUES(103,102,100,101,104)" :insert_statement))))
    (t/is (= [{:a nil :b -102 :c true :d "101" :e 104.5 :_table "t1"}]
             (xtdb-engine/insert->docs tables (sql/parse "INSERT INTO t1 VALUES(NULL,-102,TRUE,'101',104.5)" :insert_statement))))))

(comment
  (dotimes [n 5]
    (time
     (let [f (format "core2/sql/logic_test/select%d.test" (inc n))
           records (slt/parse-script (slurp (io/resource f)))]
       (println f (count records))
       (doseq [{:keys [type statement query] :as record} records
               :let [input (case type
                             :statement statement
                             :query query
                             nil)]
               :when input
               :let [tree (sql/parse-sql2011 input :start :directly_executable_statement)]]
         (if-let [failure (instaparse.core/get-failure tree)]
           (println (or (xtdb-engine/parse-create-table input) failure))
           (print ".")))
       (println)))))
