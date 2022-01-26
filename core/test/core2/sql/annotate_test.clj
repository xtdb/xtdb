(ns core2.sql.annotate-test
  (:require [clojure.test :as t]
            [core2.sql :as sql]))

(t/deftest test-annotate-query-scopes
  (let [tree (sql/parse "WITH RECURSIVE foo AS (SELECT 1 FROM foo AS bar)
SELECT t1.d-t1.e, SUM(t1.a)
  FROM t1, foo AS baz
 WHERE EXISTS (SELECT 1 FROM t1 AS x WHERE x.b < t1.c AND x.b > (SELECT t1.b FROM t2 WHERE t1.b = t2.b))
   AND t1.a > t1.b
 GROUP BY t1.d
 ORDER BY 1")]
    (t/is (= [{:id 1
               :ctes {"foo" {:query-name "foo" :id 2}}
               :tables {"t1" {:table-or-query-name "t1" :correlation-name "t1" :id 5 :scope-id 1
                              :projection #{["t1" "a"] ["t1" "b"] ["t1" "c"]  ["t1" "d"] ["t1" "e"]}}
                        "baz" {:table-or-query-name "foo" :correlation-name "baz" :id 6 :scope-id 1 :cte-id 2 :projection #{}}}
               :columns #{{:identifiers ["t1" "d"] :table-id 5 :table-scope-id 1 :scope-id 1 :qualified? true :type :group-invariant}
                          {:identifiers ["t1" "d"] :table-id 5 :table-scope-id 1 :scope-id 1 :qualified? true :type :ordinary}
                          {:identifiers ["t1" "e"] :table-id 5 :table-scope-id 1 :scope-id 1 :qualified? true :type :invalid-group-invariant}
                          {:identifiers ["t1" "a"] :table-id 5 :table-scope-id 1 :scope-id 1 :qualified? true :type :ordinary}
                          {:identifiers ["t1" "a"] :table-id 5 :table-scope-id 1 :scope-id 1 :qualified? true :type :within-group-varying}
                          {:identifiers ["t1" "b"] :table-id 5 :table-scope-id 1 :scope-id 1 :qualified? true :type :ordinary}}
               :nested-used-columns #{{:identifiers ["t1" "b"]  :table-id 5 :table-scope-id 1 :scope-id 9 :qualified? true :type :outer}
                                      {:identifiers ["t1" "c"]  :table-id 5 :table-scope-id 1 :scope-id 7 :qualified? true :type :outer}}
               :dependent-columns #{}}
              {:id 3
               :parent-id 1
               :ctes {}
               :tables {"bar" {:table-or-query-name "foo" :correlation-name "bar" :id 4 :scope-id 3 :cte-id 2 :projection #{}}}
               :columns #{}
               :nested-used-columns #{}
               :dependent-columns #{}}
              {:id 7
               :parent-id 1
               :ctes {}
               :tables {"x" {:table-or-query-name "t1" :correlation-name "x" :id 8 :scope-id 7 :projection #{["x" "b"]}}}
               :columns #{{:identifiers ["x" "b"] :table-id 8 :table-scope-id 7 :scope-id 7 :qualified? true :type :ordinary}
                          {:identifiers ["t1" "c"] :table-id 5 :table-scope-id 1 :scope-id 7 :qualified? true :type :outer}}
               :nested-used-columns #{}
               :dependent-columns #{{:identifiers ["t1" "c"] :table-id 5 :table-scope-id 1 :scope-id 7 :qualified? true :type :outer}
                                    {:identifiers ["t1" "b"] :table-id 5 :table-scope-id 1 :scope-id 9 :qualified? true :type :outer}}}
              {:id 9
               :parent-id 7
               :ctes {}
               :tables {"t2" {:table-or-query-name "t2" :correlation-name "t2" :id 10 :scope-id 9 :projection #{["t2" "b"]}}}
               :columns #{{:identifiers ["t1" "b"] :table-id 5 :table-scope-id 1 :scope-id 9 :qualified? true :type :outer}
                          {:identifiers ["t2" "b"] :table-id 10 :table-scope-id 9 :scope-id 9 :qualified? true :type :ordinary}}
               :nested-used-columns #{}
               :dependent-columns #{{:identifiers ["t1" "b"] :table-id 5 :table-scope-id 1 :scope-id 9 :qualified? true :type :outer}}}]
             (:scopes (sql/analyze-query tree))))))

(t/deftest test-parsing-errors-are-reported
  (t/is (re-find #"Parse error at line 1, column 1:\nSELEC\n" (first (:errs (sql/analyze-query (sql/parse "SELEC")))))))

(t/deftest test-scope-rules
  (t/is (re-find #"XTDB requires fully-qualified columns: a at line 1, column 8"
                 (first (:errs (sql/analyze-query (sql/parse "SELECT a FROM foo"))))))

  (t/is (re-find #"Table not in scope: bar at line 1, column 8"
                 (first (:errs (sql/analyze-query (sql/parse "SELECT bar.a FROM foo"))))))
  (t/is (re-find #"Table not in scope: bar"
                 (first (:errs (sql/analyze-query (sql/parse "SELECT bar.a FROM bar AS foo"))))))

  (t/is (empty? (:errs (sql/analyze-query (sql/parse "SELECT bar.a FROM bar")))))
  (t/is (empty? (:errs (sql/analyze-query (sql/parse "SELECT bar.a FROM foo AS bar")))))

  (t/is (empty? (:errs (sql/analyze-query (sql/parse "SELECT bar.a FROM foo AS bar ORDER BY bar.y")))))
  (t/is (empty? (:errs (sql/analyze-query (sql/parse "SELECT bar.a AS a FROM foo AS bar ORDER BY a")))))
  (t/is (empty? (:errs (sql/analyze-query (sql/parse "SELECT bar.x, bar.a + 1 FROM foo AS bar ORDER BY bar.a + 1")))))

  (t/is (re-find #"Table not in scope: baz"
                 (first (:errs (sql/analyze-query (sql/parse "SELECT bar.a FROM foo AS bar ORDER BY baz.y"))))))

  (t/is (empty? (:errs (sql/analyze-query (sql/parse "SELECT t1.b FROM t1 WHERE EXISTS (SELECT x.b FROM t1 AS x WHERE x.b < t1.b)")))))

  (t/is (empty? (:errs (sql/analyze-query (sql/parse "SELECT t1.a FROM t1 WHERE EXISTS (SELECT t1.a, COUNT(x.a) FROM t1 AS x WHERE x.b < t1.b GROUP BY x.a HAVING x.a = 1) GROUP BY t1.a")))))

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
                 (first (:errs (sql/analyze-query (sql/parse "SELECT * FROM foo AS baz WHERE EXISTS (SELECT bar.b FROM bar WHERE foo.a < bar.b)")))))))

(t/deftest test-variable-duplication
  (t/is (re-find #"Table variable duplicated: baz"
                 (first (:errs (sql/analyze-query (sql/parse "SELECT * FROM foo AS baz, baz"))))))
  (t/is (re-find #"CTE query name duplicated: foo"
                 (first (:errs (sql/analyze-query (sql/parse "WITH foo AS (SELECT 1 FROM foo), foo AS (SELECT 1 FROM foo) SELECT * FROM foo"))))))
  (t/is (re-find #"Column name duplicated: foo"
                 (first (:errs (sql/analyze-query (sql/parse "SELECT * FROM (SELECT 1, 2 FROM foo) AS bar (foo, foo)")))))))

(t/deftest test-grouping-columns
  (t/is (re-find #"Column reference is not a grouping column: t1.a"
                 (first (:errs (sql/analyze-query (sql/parse "SELECT t1.a FROM t1 GROUP BY t1.b"))))))
  (t/is (re-find #"Column reference is not a grouping column: t1.a"
                 (first (:errs (sql/analyze-query (sql/parse "SELECT t1.b FROM t1 GROUP BY t1.b HAVING t1.a"))))))
  (t/is (empty? (:errs (sql/analyze-query (sql/parse "SELECT t1.b, COUNT(t1.a) FROM t1 GROUP BY t1.b")))))
  (t/is (re-find #"Column reference is not a grouping column: t1.a"
                 (first (:errs (sql/analyze-query (sql/parse "SELECT t1.a, COUNT(t1.b) FROM t1"))))))

  (t/is (re-find #"Outer column reference is not an outer grouping column: t1.b"
                 (first (:errs (sql/analyze-query (sql/parse "SELECT t1.b FROM t1 WHERE 1 = (SELECT t1.b, COUNT(*) FROM t2)"))))))
  (t/is (re-find #"Within group varying column reference is an outer column: t1.b"
                 (first (:errs (sql/analyze-query (sql/parse "SELECT t1.b FROM t1 WHERE 1 = (SELECT COUNT(t1.b) FROM t2)"))))))
  (t/is (empty? (:errs (sql/analyze-query (sql/parse "SELECT t1.b FROM t1 WHERE 1 = (SELECT t1.b, COUNT(t2.a) FROM t2) GROUP BY t1.b"))))))

(t/deftest test-clauses-not-allowed-to-contain-aggregates-or-queries
  (t/is (re-find #"Aggregate functions cannot contain aggregate functions"
                 (first (:errs (sql/analyze-query (sql/parse "SELECT COUNT(SUM(t1.b)) FROM t1"))))))
  (t/is (re-find #"Aggregate functions cannot contain nested queries"
                 (first (:errs (sql/analyze-query (sql/parse "SELECT COUNT((SELECT 1 FROM foo)) FROM t1"))))))

  (t/is (re-find #"Sort specifications cannot contain aggregate functions"
                 (first (:errs (sql/analyze-query (sql/parse "SELECT * FROM t1 ORDER BY COUNT(t1.a)"))))))
  (t/is (re-find #"Sort specifications cannot contain nested queries"
                 (first (:errs (sql/analyze-query (sql/parse "SELECT * FROM t1 ORDER BY (SELECT 1 FROM foo)"))))))

  (t/is (re-find #"WHERE clause cannot contain aggregate functions"
                 (first (:errs (sql/analyze-query (sql/parse "SELECT * FROM t1 WHERE COUNT(t1.a)"))))))
  (t/is (empty? (:errs (sql/analyze-query (sql/parse "SELECT * FROM t1 WHERE 1 = (SELECT COUNT(t1.a) FROM t1)"))))))

(t/deftest test-fetch-and-offset-type
  (t/is (re-find #"Fetch first row count must be an integer"
                 (first (:errs (sql/analyze-query (sql/parse "SELECT * FROM t1 FETCH FIRST 'foo' ROWS ONLY"))))))
  (t/is (empty? (:errs (sql/analyze-query (sql/parse "SELECT * FROM t1 FETCH FIRST 1 ROWS ONLY")))))
  (t/is (empty? (:errs (sql/analyze-query (sql/parse "SELECT * FROM t1 FETCH FIRST :foo ROWS ONLY")))))

  (t/is (re-find #"Offset row count must be an integer"
                 (first (:errs (sql/analyze-query (sql/parse "SELECT * FROM t1 OFFSET 'foo' ROWS"))))))
  (t/is (empty? (:errs (sql/analyze-query (sql/parse "SELECT * FROM t1 OFFSET 1 ROWS")))))
  (t/is (empty? (:errs (sql/analyze-query (sql/parse "SELECT * FROM t1 OFFSET :foo ROWS"))))))
