(ns core2.sql.logic-test.runner-test
  (:require [clojure.test :as t]
            [clojure.java.io :as io]
            [core2.sql.parser :as p]
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

(t/deftest test-sql-logic-test-completion
  (let [script  "statement ok
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
        expected "SELECT t1.a+t1.b*2+t1.c*3+t1.d*4+t1.e*5 AS col__1, (t1.a+t1.b+t1.c+t1.d+t1.e)/5 AS col__2 FROM t1 AS t1(a, b, c, d, e) ORDER BY col__1,col__2"]
    (t/is (= (p/parse expected :query_expression)
             (xtdb-engine/normalize-query tables (p/parse query :query_expression)))))

  (let [tables {"tab0" ["col0" "col1" "col2"]
                "tab1" ["col0" "col1" "col2"]
                "tab2" ["col0" "col1" "col2"]}]
    (let [query "SELECT cor0.col2 col1 FROM tab0 AS cor0 GROUP BY col2, cor0.col1"
          expected "SELECT cor0.col2 col1 FROM tab0 AS cor0(col0, col1, col2) GROUP BY cor0.col2, cor0.col1"]
      (t/is (= (p/parse expected :query_expression)
               (xtdb-engine/normalize-query tables (p/parse query :query_expression)))))

    (let [query "SELECT DISTINCT col0 FROM tab0"
          expected "SELECT DISTINCT tab0.col0 AS col__1 FROM tab0 AS tab0(col0, col1, col2)"]
      (t/is (= (p/parse expected :query_expression)
               (xtdb-engine/normalize-query tables (p/parse query :query_expression)))))

    (let [query "SELECT DISTINCT - ( + col1 ) + - 51 AS col0 FROM tab1 AS cor0 GROUP BY col1"
          expected "SELECT DISTINCT - ( + cor0.col1 ) + - 51 AS col0 FROM tab1 AS cor0(col0, col1, col2) GROUP BY cor0.col1"]
      (t/is (= (p/parse expected :query_expression)
               (xtdb-engine/normalize-query tables (p/parse query :query_expression)))))))

(t/deftest test-insert->doc
  (let [tables {"t1" ["a" "b" "c" "d" "e"]}]
    (t/is (= [{:e 103 :c 102 :b 100 :d 101 :a 104 :_table "t1"}]
             (xtdb-engine/insert->docs tables (p/parse "INSERT INTO t1(e,c,b,d,a) VALUES(103,102,100,101,104)" :insert_statement))))
    (t/is (= [{:a nil :b -102 :c true :d "101" :e 104.5 :_table "t1"}]
             (xtdb-engine/insert->docs tables (p/parse "INSERT INTO t1 VALUES(NULL,-102,TRUE,'101',104.5)" :insert_statement))))))

(comment

  (time
   (let [tables {"t1" ["a" "b" "c" "d" "e"]}
         query "SELECT a+b*2+c*3+d*4+e*5, (a+b+c+d+e)/5 FROM t1 ORDER BY 1,2"
         tree (p/parse query :query_expression)
         tree (xtdb-engine/normalize-query tables tree)]
     (dotimes [_ 1000]
       (core2.sql.plan/plan-query tree))))

  (doseq [f (->> (file-seq (io/file (io/resource "core2/sql/logic_test/sqlite_test/")))
                 (filter #(clojure.string/ends-with? % ".test"))
                 (sort))]
    (time
     (let [records (slt/parse-script (slurp f))
           failures (atom 0)]
       (println f (count records))
       (doseq [{:keys [type statement query] :as record} records
               :let [input (case type
                             :statement statement
                             :query (xtdb-engine/preprocess-query query)
                             nil)]
               :when (and input
                          (not (slt/skip-record? "xtdb" record))
                          (not (xtdb-engine/skip-statement? input)))
               :let [tree (p/sql-parser input :directly_executable_statement)]]
         (if (p/failure? tree)
           (do (when-not (xtdb-engine/parse-create-table input)
                 (when (= 1 (swap! failures inc))
                   (println (p/failure->str tree))))
               #_(println (or (xtdb-engine/parse-create-table input) (p/failure->str tree))))
           #_(print ".")))
       (println "failures: " @failures)
       (println)))))
