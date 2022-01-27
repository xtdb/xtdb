(ns core2.sql.annotate-test
  (:require [clojure.test :as t]
            [core2.sql :as sql]))

(t/deftest test-annotate-query-scopes
  (let [tree (sql/parse "WITH RECURSIVE foo AS (SELECT 1 FROM foo AS bar)
SELECT t1.d-t1.e, SUM(t1.a)
  FROM t1, foo AS baz
 WHERE EXISTS (SELECT 1 FROM t1 AS x WHERE x.b < t1.c AND x.b > (SELECT t1.b FROM t2 WHERE t1.b = t2.b))
   AND t1.a > t1.b
 GROUP BY t1.d, t1.e
 ORDER BY 1")]
    (t/is (= {:errs []
              :scopes
              [{:id 1
                :ctes {"foo" {:query-name "foo" :id 2}}
                :tables {"t1" {:table-or-query-name "t1" :correlation-name "t1" :id 5 :scope-id 1
                               :projection #{["t1" "a"] ["t1" "b"] ["t1" "c"]  ["t1" "d"] ["t1" "e"]}}
                         "baz" {:table-or-query-name "foo" :correlation-name "baz" :id 6 :scope-id 1 :cte-id 2 :projection #{}}}
                :columns #{{:identifiers ["t1" "d"] :table-id 5 :table-scope-id 1 :scope-id 1 :qualified? true :type :group-invariant}
                           {:identifiers ["t1" "d"] :table-id 5 :table-scope-id 1 :scope-id 1 :qualified? true :type :ordinary}
                           {:identifiers ["t1" "e"] :table-id 5 :table-scope-id 1 :scope-id 1 :qualified? true :type :group-invariant}
                           {:identifiers ["t1" "e"] :table-id 5 :table-scope-id 1 :scope-id 1 :qualified? true :type :ordinary}
                           {:identifiers ["t1" "a"] :table-id 5 :table-scope-id 1 :scope-id 1 :qualified? true :type :ordinary}
                           {:identifiers ["t1" "a"] :table-id 5 :table-scope-id 1 :scope-id 1 :qualified? true :type :within-group-varying}
                           {:identifiers ["t1" "b"] :table-id 5 :table-scope-id 1 :scope-id 1 :qualified? true :type :ordinary}}
                :dependent-columns #{}}
               {:id 3
                :parent-id 1
                :ctes {}
                :tables {"bar" {:table-or-query-name "foo" :correlation-name "bar" :id 4 :scope-id 3 :cte-id 2 :projection #{}}}
                :columns #{}
                :dependent-columns #{}}
               {:id 7
                :parent-id 1
                :ctes {}
                :tables {"x" {:table-or-query-name "t1" :correlation-name "x" :id 8 :scope-id 7 :projection #{["x" "b"]}}}
                :columns #{{:identifiers ["x" "b"] :table-id 8 :table-scope-id 7 :scope-id 7 :qualified? true :type :ordinary}
                           {:identifiers ["t1" "c"] :table-id 5 :table-scope-id 1 :scope-id 7 :qualified? true :type :outer}}
                :dependent-columns #{{:identifiers ["t1" "c"] :table-id 5 :table-scope-id 1 :scope-id 7 :qualified? true :type :outer}
                                     {:identifiers ["t1" "b"] :table-id 5 :table-scope-id 1 :scope-id 9 :qualified? true :type :outer}}}
               {:id 9
                :parent-id 7
                :ctes {}
                :tables {"t2" {:table-or-query-name "t2" :correlation-name "t2" :id 10 :scope-id 9 :projection #{["t2" "b"]}}}
                :columns #{{:identifiers ["t1" "b"] :table-id 5 :table-scope-id 1 :scope-id 9 :qualified? true :type :outer}
                           {:identifiers ["t2" "b"] :table-id 10 :table-scope-id 9 :scope-id 9 :qualified? true :type :ordinary}}
                :dependent-columns #{{:identifiers ["t1" "b"] :table-id 5 :table-scope-id 1 :scope-id 9 :qualified? true :type :outer}}}]}
             (sql/analyze-query tree)))))

(defn- invalid? [re q]
  (let [[err :as errs] (:errs (sql/analyze-query (sql/parse q)))
        err (or err "")]
    (t/is (= 1 (count errs)) (pr-str errs))
    (t/is (re-find re err))))

(defn- valid? [q]
  (let [errs (:errs (sql/analyze-query (sql/parse q)))]
    (t/is (empty? errs))))

(t/deftest test-parsing-errors-are-reported
  (invalid? #"Parse error at line 1, column 1:\nSELEC\n"
            "SELEC"))

(t/deftest test-scope-rules
  (invalid? #"XTDB requires fully-qualified columns: a at line 1, column 8"
            "SELECT a FROM foo")
  (invalid? #"Table not in scope: bar at line 1, column 8"
            "SELECT bar.a FROM foo")

  (invalid? #"Table not in scope: bar"
            "SELECT bar.a FROM bar AS foo")

  (valid? "SELECT bar.a FROM bar")
  (valid? "SELECT bar.a FROM foo AS bar")

  (valid? "SELECT bar.a FROM foo AS bar ORDER BY bar.y")
  (valid? "SELECT bar.a AS a FROM foo AS bar ORDER BY a")
  (valid? "SELECT bar.x, bar.a + 1 FROM foo AS bar ORDER BY bar.a + 1")

  (invalid? #"Table not in scope: baz"
            "SELECT bar.a FROM foo AS bar ORDER BY baz.y")

  (valid? "SELECT t1.b FROM t1 WHERE EXISTS (SELECT x.b FROM t1 AS x WHERE x.b < t1.b)")
  (valid? "SELECT t1.a FROM t1 WHERE EXISTS (SELECT t1.a, COUNT(x.a) FROM t1 AS x WHERE x.b < t1.b GROUP BY x.a HAVING x.a = 1) GROUP BY t1.a")

  (valid? "SELECT t1.b FROM t1, LATERAL (SELECT x.b FROM t1 AS x WHERE x.b < t1.b) AS t2")

  (invalid? #"Table not in scope: t1"
            "SELECT * FROM t1, (SELECT x.b FROM t1 AS x WHERE x.b < t1.b) AS t2")
  (invalid? #"Table not in scope: t2"
            "SELECT * FROM t1, LATERAL (SELECT x.b FROM t1 AS x WHERE x.b < t1.b) AS t2, (SELECT x.b FROM t1 AS x WHERE x.b < t2.b) AS t3")

  (valid? "SELECT t1.b FROM t1 JOIN t2 USING (x)")
  (valid? "SELECT t1.b FROM t1 JOIN t2 ON (t1.x = t2.y)")
  (valid? "SELECT * FROM foo, LATERAL (SELECT t1.b FROM t1 JOIN t2 ON (t1.x = t2.y AND t1.x = foo.x)) AS t2")
  (valid? "SELECT * FROM foo WHERE foo.x = (SELECT t1.b FROM t1 JOIN t2 ON (t1.x = t2.y AND t1.x = foo.x))")
  (valid? "SELECT t1.b FROM foo, t1 JOIN t2 ON (t1.x = foo.y)")
  (valid? "SELECT t1.b FROM bar, (t1 NATURAL JOIN t2) JOIN t3 ON (t1.x = t3.y)")

  (invalid? #"Table not in scope: foo"
            "SELECT * FROM (SELECT t1.b FROM t1 JOIN t2 ON (t1.x = t2.y AND t1.x = foo.x)) AS t2")
  (invalid? #"Table not in scope: t1"
            "SELECT t2.b FROM (SELECT x.b FROM t1 AS x WHERE x.b < t1.b) AS t2, t1")
  (invalid? #"Table not in scope: bar"
            "SELECT bar.a FROM foo WHERE EXISTS (SELECT bar.b FROM bar WHERE foo.a < bar.b)")
  (invalid? #"Table not in scope: foo"
            "SELECT * FROM foo AS baz WHERE EXISTS (SELECT bar.b FROM bar WHERE foo.a < bar.b)"))

(t/deftest test-variable-duplication
  (invalid? #"Table variable duplicated: baz"
            "SELECT * FROM foo AS baz, baz")
  (invalid? #"CTE query name duplicated: foo"
            "WITH foo AS (SELECT 1 FROM foo), foo AS (SELECT 1 FROM foo) SELECT * FROM foo")
  (invalid? #"Column name duplicated: foo"
            "SELECT * FROM (SELECT 1, 2 FROM foo) AS bar (foo, foo)"))

(t/deftest test-grouping-columns
  (invalid? #"Column reference is not a grouping column: t1.a"
            "SELECT t1.a FROM t1 GROUP BY t1.b")
  (invalid? #"Column reference is not a grouping column: t1.a"
            "SELECT t1.b FROM t1 GROUP BY t1.b HAVING t1.a")
  (valid? "SELECT t1.b, COUNT(t1.a) FROM t1 GROUP BY t1.b")
  (invalid? #"Column reference is not a grouping column: t1.a"
            "SELECT t1.a, COUNT(t1.b) FROM t1")

  (invalid? #"Outer column reference is not an outer grouping column: t1.b"
            "SELECT t1.b FROM t1 WHERE (1, 1) = (SELECT t1.b, COUNT(*) FROM t2)")
  (invalid? #"Within group varying column reference is an outer column: t1.b"
            "SELECT t1.b FROM t1 WHERE 1 = (SELECT COUNT(t1.b) FROM t2)")
  (valid? "SELECT t1.b FROM t1 WHERE 1 = (SELECT t1.b, COUNT(t2.a) FROM t2) GROUP BY t1.b"))

(t/deftest test-clauses-not-allowed-to-contain-aggregates-or-queries
  (invalid? #"Aggregate functions cannot contain aggregate functions"
            "SELECT COUNT(SUM(t1.b)) FROM t1")
  (invalid? #"Aggregate functions cannot contain nested queries"
            "SELECT COUNT((SELECT 1 FROM foo)) FROM t1")

  (invalid? #"Sort specifications cannot contain aggregate functions"
            "SELECT * FROM t1 ORDER BY COUNT(t1.a)")
  (invalid? #"Sort specifications cannot contain nested queries"
            "SELECT * FROM t1 ORDER BY (SELECT 1 FROM foo)")

  (invalid? #"WHERE clause cannot contain aggregate functions"
            "SELECT * FROM t1 WHERE COUNT(t1.a)")
  (valid? "SELECT * FROM t1 WHERE 1 = (SELECT COUNT(t1.a) FROM t1)"))

(t/deftest test-fetch-and-offset-type
  (invalid? #"Fetch first row count must be an integer"
            "SELECT * FROM t1 FETCH FIRST 'foo' ROWS ONLY")
  (valid? "SELECT * FROM t1 FETCH FIRST 1 ROWS ONLY")
  (valid? "SELECT * FROM t1 FETCH FIRST :foo ROWS ONLY")

  (invalid? #"Offset row count must be an integer"
            "SELECT * FROM t1 OFFSET 'foo' ROWS")
  (valid? "SELECT * FROM t1 OFFSET 1 ROWS")
  (valid? "SELECT * FROM t1 OFFSET :foo ROWS"))
