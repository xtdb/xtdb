(ns core2.sql.annotate-test
  (:require [clojure.test :as t]
            [core2.sql :as sql]))

(t/deftest test-annotate-query-scopes
  (let [tree (sql/parse "WITH RECURSIVE foo AS (SELECT 1 FROM foo AS bar)
SELECT t1.d-t1.e
  FROM t1, foo AS baz
 WHERE EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b)
   AND t1.a>t1.b
 ORDER BY 1")]
    (t/is (= [{:id 1
               :ctes {"foo" [{:query-name "foo" :id 2}]}
               :tables {"t1" [{:table-or-query-name "t1" :correlation-name "t1" :id 5}]
                        "baz" [{:table-or-query-name "foo" :correlation-name "baz" :id 6 :cte-id 2}]}
               :columns #{{:identifiers ["t1" "d"] :table-id 5 :qualified? true}
                          {:identifiers ["t1" "e"] :table-id 5 :qualified? true}
                          {:identifiers ["t1" "a"] :table-id 5 :qualified? true}
                          {:identifiers ["t1" "b"] :table-id 5 :qualified? true}}}
              {:id 3
               :parent-id 1
               :ctes {}
               :tables {"bar" [{:table-or-query-name "foo" :correlation-name "bar" :id 4 :cte-id 2}]}
               :columns #{}}
              {:id 7
               :parent-id 1
               :ctes {}
               :tables {"x" [{:table-or-query-name "t1" :correlation-name "x" :id 8}]}
               :columns #{{:identifiers ["x" "b"] :table-id 8 :qualified? true}
                          {:identifiers ["t1" "b"] :table-id 5 :qualified? true}}}]
             (:scopes (sql/analyze-query tree))))))

(t/deftest test-scope-rules
  (t/is (re-find #"XTDB requires fully-qualified columns: a at line 1, column 8"
                 (first (:errs (sql/analyze-query (sql/parse "SELECT a FROM foo"))))))

  (t/is (re-find #"Table not in scope: bar at line 1, column 8"
                 (first (:errs (sql/analyze-query (sql/parse "SELECT bar.a FROM foo"))))))
  (t/is (re-find #"Table not in scope: bar"
                 (first (:errs (sql/analyze-query (sql/parse "SELECT bar.a FROM bar AS foo"))))))

  (t/is (empty? (:errs (sql/analyze-query (sql/parse "SELECT bar.a FROM bar")))))
  (t/is (empty? (:errs (sql/analyze-query (sql/parse "SELECT bar.a FROM foo AS bar")))))

  (t/is (empty? (:errs (sql/analyze-query (sql/parse "SELECT t1.b FROM t1 WHERE EXISTS (SELECT x.b FROM t1 AS x WHERE x.b < t1.b)")))))

  (t/is (empty? (:errs (sql/analyze-query (sql/parse "SELECT t1.b FROM t1, LATERAL (SELECT x.b FROM t1 AS x WHERE x.b < t1.b) AS t2")))))
  (t/is (re-find #"Table not in scope: t1"
                 (first (:errs (sql/analyze-query (sql/parse "SELECT * FROM t1, (SELECT x.b FROM t1 AS x WHERE x.b < t1.b) AS t2"))))))
  (t/is (re-find #"Table not in scope: t2"
                 (first (:errs (sql/analyze-query (sql/parse "SELECT * FROM t1, LATERAL (SELECT x.b FROM t1 AS x WHERE x.b < t1.b) AS t2, (SELECT x.b FROM t1 AS x WHERE x.b < t2.b) AS t3"))))))

  (t/is (empty? (:errs (sql/analyze-query (sql/parse "SELECT t1.b FROM t1 JOIN t2 USING (x)")))))
  (t/is (empty? (:errs (sql/analyze-query (sql/parse "SELECT t1.b FROM t1 JOIN t2 ON (t1.x = t2.y)")))))
  (t/is (empty? (:errs (sql/analyze-query (sql/parse "SELECT * FROM foo, LATERAL (SELECT t1.b FROM t1 JOIN t2 ON (t1.x = t2.y AND t1.x = foo.x)) AS t2")))))
  (t/is (empty? (:errs (sql/analyze-query (sql/parse "SELECT * FROM foo WHERE foo.x = (SELECT t1.b FROM t1 JOIN t2 ON (t1.x = t2.y AND t1.x = foo.x))")))))
  (t/is (empty? (:errs (sql/analyze-query (sql/parse "SELECT t1.b FROM foo, t1 JOIN t2 ON (t1.x = foo.y)")))))
  (t/is (empty? (:errs (sql/analyze-query (sql/parse "SELECT t1.b FROM bar, (t1 NATURAL JOIN t2) JOIN t3 ON (t1.x = t3.y)")))))

  (t/is (re-find #"Table not in scope: foo"
                 (first (:errs (sql/analyze-query (sql/parse "SELECT * FROM (SELECT t1.b FROM t1 JOIN t2 ON (t1.x = t2.y AND t1.x = foo.x)) AS t2"))))))
  (t/is (re-find #"Table not in scope: t1"
                 (first (:errs (sql/analyze-query (sql/parse "SELECT t2.b FROM (SELECT x.b FROM t1 AS x WHERE x.b < t1.b) AS t2, t1"))))))
  (t/is (re-find #"Table not in scope: bar"
                 (first (:errs (sql/analyze-query (sql/parse "SELECT bar.a FROM foo WHERE EXISTS (SELECT bar.b FROM bar WHERE foo.a < bar.b)"))))))
  (t/is (re-find #"Table not in scope: foo"
                 (first (:errs (sql/analyze-query (sql/parse "SELECT * FROM foo AS baz WHERE EXISTS (SELECT bar.b FROM bar WHERE foo.a < bar.b)"))))))

  (t/is (re-find #"Table variable duplicated: baz"
                 (first (:errs (sql/analyze-query (sql/parse "SELECT * FROM foo AS baz, baz"))))))
  (t/is (re-find #"CTE query name duplicated: foo"
                 (first (:errs (sql/analyze-query (sql/parse "WITH foo AS (SELECT 1 FROM foo), foo AS (SELECT 1 FROM foo) SELECT * FROM foo"))))))
  (t/is (re-find #"Column name duplicated: foo"
                 (first (:errs (sql/analyze-query (sql/parse "SELECT * FROM (SELECT 1, 2 FROM foo) AS bar (foo, foo)")))))))
