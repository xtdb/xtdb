(ns core2.sql.annotate-test
  (:require [clojure.test :as t]
            [core2.sql :as sql]))

(t/deftest test-annotate-query-scopes
  (let [tree (sql/parse "WITH RECURSIVE foo AS (SELECT 1 FROM foo AS bar)
SELECT t1.d-t1.e AS a, SUM(t1.a) AS b
  FROM t1, foo AS baz
 WHERE EXISTS (SELECT 1 FROM t1 AS x WHERE x.b < t1.c AND x.b > (SELECT t1.b FROM (SELECT 1 FROM boz) AS t2 WHERE t1.b = t2.b))
   AND t1.a > t1.b
 GROUP BY t1.d, t1.e
 ORDER BY b, t1.c")]
    (t/is (= {:scopes
              [{:id 1,
                :type :query-expression,
                :dependent-columns #{},
                :projected-columns
                [{:index 0, :identifier "a"} {:index 1, :identifier "b"}],
                :ctes
                {"foo"
                 {:query-name "foo", :id 2, :scope-id 1, :subquery-scope-id 3}},
                :order-by-indexes [1 nil]}
               {:id 3,
                :type :query-expression,
                :dependent-columns #{},
                :projected-columns [{:index 0}],
                :parent-id 1,
                :ctes {}}
               {:id 4,
                :type :query-specification,
                :dependent-columns #{},
                :projected-columns [{:index 0}],
                :parent-id 3,
                :tables
                {"bar"
                 {:correlation-name "bar",
                  :id 5,
                  :scope-id 4,
                  :table-or-query-name "foo",
                  :cte-id 2,
                  :cte-scope-id 1,
                  :used-columns #{}}},
                :columns #{}}
               {:id 6,
                :type :query-specification,
                :dependent-columns #{},
                :projected-columns
                [{:index 0, :identifier "a"} {:index 1, :identifier "b"}],
                :parent-id 1,
                :tables
                {"t1"
                 {:correlation-name "t1",
                  :id 7,
                  :scope-id 6,
                  :table-or-query-name "t1",
                  :used-columns
                  #{["t1" "e"] ["t1" "a"] ["t1" "b"] ["t1" "c"] ["t1" "d"]}},
                 "baz"
                 {:correlation-name "baz",
                  :id 8,
                  :scope-id 6,
                  :table-or-query-name "foo",
                  :cte-id 2,
                  :cte-scope-id 1,
                  :used-columns #{}}},
                :columns
                #{{:identifiers ["t1" "e"],
                   :type :group-invariant,
                   :scope-id 6,
                   :table-id 7,
                   :table-scope-id 6}
                  {:identifiers ["t1" "b"],
                   :type :ordinary,
                   :scope-id 6,
                   :table-id 7,
                   :table-scope-id 6}
                  {:identifiers ["t1" "e"],
                   :type :ordinary,
                   :scope-id 6,
                   :table-id 7,
                   :table-scope-id 6}
                  {:identifiers ["t1" "d"],
                   :type :ordinary,
                   :scope-id 6,
                   :table-id 7,
                   :table-scope-id 6}
                  {:identifiers ["t1" "d"],
                   :type :group-invariant,
                   :scope-id 6,
                   :table-id 7,
                   :table-scope-id 6}
                  {:identifiers ["t1" "a"],
                   :type :ordinary,
                   :scope-id 6,
                   :table-id 7,
                   :table-scope-id 6}
                  {:identifiers ["t1" "a"],
                   :type :within-group-varying,
                   :scope-id 6,
                   :table-id 7,
                   :table-scope-id 6}},
                :grouping-columns [["t1" "d"] ["t1" "e"]]}
               {:id 9,
                :type :query-expression,
                :dependent-columns
                #{{:identifiers ["t1" "c"],
                   :type :outer,
                   :scope-id 10,
                   :table-id 7,
                   :table-scope-id 6}
                  {:identifiers ["t1" "b"],
                   :type :outer,
                   :scope-id 13,
                   :table-id 7,
                   :table-scope-id 6}},
                :projected-columns [{:index 0}],
                :parent-id 6,
                :ctes {}}
               {:id 10,
                :type :query-specification,
                :dependent-columns
                #{{:identifiers ["t1" "c"],
                   :type :outer,
                   :scope-id 10,
                   :table-id 7,
                   :table-scope-id 6}
                  {:identifiers ["t1" "b"],
                   :type :outer,
                   :scope-id 13,
                   :table-id 7,
                   :table-scope-id 6}},
                :projected-columns [{:index 0}],
                :parent-id 9,
                :tables
                {"x"
                 {:correlation-name "x",
                  :id 11,
                  :scope-id 10,
                  :table-or-query-name "t1",
                  :used-columns #{["x" "b"]}}},
                :columns
                #{{:identifiers ["x" "b"],
                   :type :ordinary,
                   :scope-id 10,
                   :table-id 11,
                   :table-scope-id 10}
                  {:identifiers ["t1" "c"],
                   :type :outer,
                   :scope-id 10,
                   :table-id 7,
                   :table-scope-id 6}}}
               {:id 12,
                :type :query-expression,
                :dependent-columns
                #{{:identifiers ["t1" "b"],
                   :type :outer,
                   :scope-id 13,
                   :table-id 7,
                   :table-scope-id 6}},
                :projected-columns [{:index 0, :identifier "b", :qualified-column ["t1" "b"]}],
                :parent-id 10,
                :ctes {}}
               {:id 13,
                :type :query-specification,
                :dependent-columns
                #{{:identifiers ["t1" "b"],
                   :type :outer,
                   :scope-id 13,
                   :table-id 7,
                   :table-scope-id 6}},
                :projected-columns [{:index 0, :identifier "b", :qualified-column ["t1" "b"]}],
                :parent-id 12,
                :tables
                {"t2"
                 {:correlation-name "t2",
                  :id 14,
                  :scope-id 13,
                  :subquery-scope-id 15,
                  :used-columns #{["t2" "b"]}}},
                :columns
                #{{:identifiers ["t2" "b"],
                   :type :ordinary,
                   :scope-id 13,
                   :table-id 14,
                   :table-scope-id 13}
                  {:identifiers ["t1" "b"],
                   :type :outer,
                   :scope-id 13,
                   :table-id 7,
                   :table-scope-id 6}}}
               {:id 15,
                :type :query-expression,
                :dependent-columns #{},
                :projected-columns [{:index 0}],
                :parent-id 13,
                :ctes {}}
               {:id 16,
                :type :query-specification,
                :dependent-columns #{},
                :projected-columns [{:index 0}],
                :parent-id 15,
                :tables
                {"boz"
                 {:correlation-name "boz",
                  :id 17,
                  :scope-id 16,
                  :table-or-query-name "boz",
                  :used-columns #{}}},
                :columns #{}}],
              :errs []}
             (sql/analyze-query tree)))))

(defmacro ^:private invalid? [re q]
  `(let [[err# :as errs#] (:errs (sql/analyze-query (sql/parse ~q)))
         err# (or err# "")]
     (t/is (= 1 (count errs#)) (pr-str errs#))
     (t/is (re-find ~re err#))))

(defmacro ^:private valid? [q]
  `(let [{errs# :errs scopes# :scopes} (sql/analyze-query (sql/parse ~q))]
     (t/is (empty? errs#))
     scopes#))

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
  (valid? "SELECT t1.b FROM bar, (t1 INNER JOIN t2 USING (x)) JOIN t3 ON (t1.x = t3.y)")

  (invalid? #"Table not in scope: foo"
            "SELECT * FROM (SELECT t1.b FROM t1 JOIN t2 ON (t1.x = t2.y AND t1.x = foo.x)) AS t2")
  (invalid? #"Table not in scope: t1"
            "SELECT t2.b FROM (SELECT x.b FROM t1 AS x WHERE x.b < t1.b) AS t2, t1")
  (invalid? #"Table not in scope: bar"
            "SELECT bar.a FROM foo WHERE EXISTS (SELECT bar.b FROM bar WHERE foo.a < bar.b)")
  (invalid? #"Table not in scope: foo"
            "SELECT 1 FROM foo AS baz WHERE EXISTS (SELECT bar.b FROM bar WHERE foo.a < bar.b)"))

(t/deftest test-variable-duplication
  (invalid? #"Table variable duplicated: baz"
            "SELECT 1 FROM foo AS baz, baz")
  (invalid? #"CTE query name duplicated: foo"
            "WITH foo AS (SELECT 1 FROM foo), foo AS (SELECT 1 FROM foo) SELECT 1 FROM foo")
  (invalid? #"Column name duplicated: foo"
            "SELECT 1 FROM (SELECT 1, 2 FROM foo) AS bar (foo, foo)"))

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
            "SELECT 1 FROM t1 ORDER BY COUNT(t1.a)")
  (invalid? #"Sort specifications cannot contain nested queries"
            "SELECT 1 FROM t1 ORDER BY (SELECT 1 FROM foo)")

  (invalid? #"WHERE clause cannot contain aggregate functions"
            "SELECT 1 FROM t1 WHERE COUNT(t1.a)")
  (valid? "SELECT 1 FROM t1 WHERE 1 = (SELECT COUNT(t1.a) FROM t1)"))

(t/deftest test-fetch-and-offset-type
  (invalid? #"Fetch first row count must be an integer"
            "SELECT 1 FROM t1 FETCH FIRST 'foo' ROWS ONLY")
  (valid? "SELECT 1 FROM t1 FETCH FIRST 1 ROWS ONLY")
  (valid? "SELECT 1 FROM t1 FETCH FIRST :foo ROWS ONLY")

  (invalid? #"Offset row count must be an integer"
            "SELECT 1 FROM t1 OFFSET 'foo' ROWS")
  (valid? "SELECT 1 FROM t1 OFFSET 1 ROWS")
  (valid? "SELECT 1 FROM t1 OFFSET :foo ROWS"))

(t/deftest test-projection
  (t/is (= [[{:index 0, :identifier "b", :qualified-column ["t1" "b"]}]
            [{:index 0, :identifier "b", :qualified-column ["t1" "b"]}]
            [{:index 0, :identifier "b", :qualified-column ["t2" "b"]}]]
           (->> (valid? "SELECT t1.b FROM t1 UNION SELECT t2.b FROM t2")
                (map :projected-columns))))

  (t/is (= [[{:index 0, :identifier "b", :qualified-column ["t1" "b"]}]
            [{:index 0, :identifier "b", :qualified-column ["t1" "b"]}]
            [{:index 0, :identifier "a", :qualified-column ["t2" "a"]}]]
           (->> (valid? "SELECT t1.b FROM t1 EXCEPT SELECT t2.a FROM t2")
                (map :projected-columns))))

  (invalid? #"INTERSECT does not have corresponding columns"
            "SELECT t1.b FROM t1 INTERSECT CORRESPONDING SELECT t2.a FROM t2")

  (t/is (= [[{:index 0, :identifier "b", :qualified-column ["t1" "b"]}]
            [{:index 0, :identifier "b", :qualified-column ["t1" "b"]}]
            [{:index 0, :identifier "a", :qualified-column ["t2" "a"]} {:index 1, :identifier "b", :qualified-column ["t2" "b"]}]]
           (->> (valid? "SELECT t1.b FROM t1 EXCEPT CORRESPONDING SELECT t2.a, t2.b FROM t2")
                (map :projected-columns))))

  (t/is (= [[{:index 0, :identifier "b" :qualified-column ["t1" "b"]}]
            [{:index 0, :identifier "b" :qualified-column ["t1" "b"]}]
            [{:index 0, :identifier "a" :qualified-column ["t2" "a"]} {:index 1, :identifier "b"  :qualified-column ["t2" "b"]}]]
           (->> (valid? "SELECT t1.b FROM t1 EXCEPT CORRESPONDING BY (b) SELECT t2.a, t2.b FROM t2")
                (map :projected-columns))))

  (invalid? #"INTERSECT does not have corresponding columns"
            "SELECT t1.b FROM t1 INTERSECT CORRESPONDING BY (c) SELECT t2.b FROM t2")
  (invalid? #"UNION requires tables to have same degree"
            "SELECT t1.b FROM t1 UNION SELECT t2.b, t2.c FROM t2")

  (t/is (= [[{:index 0} {:index 1} {:index 2}]]
           (->> (valid? "VALUES (1, 2, 3), (4, 5, 6)")
                (map :projected-columns))))

  (t/is (= [[{:index 0}]]
           (->> (valid? "VALUES 1, 2")
                (map :projected-columns))))

  (t/is (= [[{:index 0}]]
           (->> (valid? "VALUES 1, (2)")
                (map :projected-columns))))

  (invalid? #"VALUES requires rows to have same degree"
            "VALUES (1, 2), (3, 4, 5)")

  (valid? "VALUES (1, 2), (SELECT t1.a, t1.b FROM t1)")
  (valid? "VALUES 1, (SELECT t1.a FROM t1)")
  (invalid? #"VALUES requires rows to have same degree"
            "VALUES (1, 2), (SELECT t1.a FROM t1)")
  (invalid? #"VALUES requires rows to have same degree"
            "VALUES (1), (SELECT t1.a, t1.b FROM t1)")

  (t/is (= [[{:index 0 :identifier "a" :qualified-column ["x" "a"]}]
            [{:index 0 :identifier "a" :qualified-column ["x" "a"]}]
            [{:index 0 :identifier "a" :qualified-column ["t1" "a"]} {:index 1 :identifier "b" :qualified-column ["t1" "b"]}]
            [{:index 0 :identifier "a" :qualified-column ["t1" "a"]} {:index 1 :identifier "b" :qualified-column ["t1" "b"]}]]
           (->> (valid? "SELECT x.a FROM (SELECT t1.a, t1.b FROM t1) AS x (a, b)")
                (map :projected-columns))))
  (t/is (= [[{:index 0 :identifier "a" :qualified-column ["x" "a"]}]
            [{:index 0 :identifier "a" :qualified-column ["x" "a"]}]
            [{:index 0}]]
           (->> (valid? "SELECT x.a FROM (VALUES (1)) AS x (a)")
                (map :projected-columns))))
  (t/is (= [[{:index 0 :identifier "a"}]
            [{:index 0 :identifier "a"}]]
           (->> (valid? "SELECT :a FROM x")
                (map :projected-columns))))

  (invalid? #"Derived columns has to have same degree as table"
            "SELECT x.a FROM (SELECT t1.a, t1.b FROM t1) AS x (a)")
  (invalid? #"Derived columns has to have same degree as table"
            "SELECT x.a FROM LATERAL (SELECT t1.a FROM t1) AS x (a, b)")
  (invalid? #"Derived columns has to have same degree as table"
            "SELECT x.a FROM (VALUES (1, 2)) AS x (a)")
  (valid? "SELECT x.a FROM y AS x (a, b)")
  (valid? "SELECT x.a FROM y, UNNEST(y.a) AS x (a)")

  (t/is (= [[{:index 0 :identifier "a" :qualified-column ["x" "a"]} {:index 1 :identifier "b" :qualified-column ["x" "b"]}]
            [{:index 0 :identifier "a" :qualified-column ["x" "a"]} {:index 1 :identifier "b" :qualified-column ["x" "b"]}]]
           (->> (valid? "SELECT * FROM x WHERE x.a = x.b")
                (map :projected-columns))))

  (t/is (= [[{:index 0 :identifier "a" :qualified-column ["x" "a"]} {:index 1 :identifier "b" :qualified-column ["x" "b"]}]
            [{:index 0 :identifier "a" :qualified-column ["x" "a"]} {:index 1 :identifier "b" :qualified-column ["x" "b"]}]
            [{:index 0 :identifier "a" :qualified-column ["y" "a"]} {:index 1 :identifier "b" :qualified-column ["y" "b"]}]
            [{:index 0 :identifier "a" :qualified-column ["y" "a"]} {:index 1 :identifier "b" :qualified-column ["y" "b"]}]]
           (->> (valid? "SELECT * FROM (SELECT y.a, y.b FROM y) AS x")
                (map :projected-columns))))

  (t/is (= [[{:index 0 :identifier "c" :qualified-column ["x" "c"]} {:index 1 :identifier "d" :qualified-column ["x" "d"]}]
            [{:index 0 :identifier "c" :qualified-column ["x" "c"]} {:index 1 :identifier "d" :qualified-column ["x" "d"]}]
            [{:index 0 :identifier "a" :qualified-column ["y" "a"]} {:index 1 :identifier "b" :qualified-column ["y" "b"]}]
            [{:index 0 :identifier "a" :qualified-column ["y" "a"]} {:index 1 :identifier "b" :qualified-column ["y" "b"]}]]
           (->> (valid? "SELECT * FROM (SELECT y.a, y.b FROM y) AS x (c, d)")
                (map :projected-columns))))

  (t/is (= [[{:index 0 :identifier "a" :qualified-column ["x" "a"]} {:index 1 :identifier "b" :qualified-column ["z" "b"]}]
            [{:index 0 :identifier "a" :qualified-column ["x" "a"]} {:index 1 :identifier "b" :qualified-column ["z" "b"]}]
            [{:index 0 :identifier "a" :qualified-column ["y" "a"]}]
            [{:index 0 :identifier "a" :qualified-column ["y" "a"]}]]
           (->> (valid? "SELECT * FROM (SELECT y.a FROM y WHERE y.z = FALSE) AS x, z WHERE z.b = TRUE")
                (map :projected-columns))))

  (t/is (= [[{:index 0 :identifier "b" :qualified-column ["x" "b"]} {:index 1 :identifier "b" :qualified-column ["z" "b"]}]
            [{:index 0 :identifier "b" :qualified-column ["x" "b"]} {:index 1 :identifier "b" :qualified-column ["z" "b"]}]
            [{:index 0 :identifier "b" :qualified-column ["y" "b"]}]
            [{:index 0 :identifier "b" :qualified-column ["y" "b"]}]
            [{:index 0 :identifier "b" :qualified-column ["y" "b"]}]
            [{:index 0 :identifier "b" :qualified-column ["y" "b"]}]]
           (->> (valid? "SELECT * FROM (SELECT y.b FROM y) AS x, (SELECT y.b FROM y) AS z")
                (map :projected-columns))))

  (t/is (= [[{:index 0 :identifier "b" :qualified-column ["x" "b"]} {:index 1 :identifier "z" :qualified-column ["x" "z"]}]
            [{:index 0 :identifier "b" :qualified-column ["x" "b"]} {:index 1 :identifier "z" :qualified-column ["x" "z"]}]
            [{:index 0 :identifier "b" :qualified-column ["y" "b"]}]
            [{:index 0 :identifier "b" :qualified-column ["y" "b"]}]
            [{:index 0 :identifier "a" :qualified-column ["y" "a"]}]
            [{:index 0 :identifier "a" :qualified-column ["y" "a"]}]]
           (->> (valid? "SELECT x.* FROM (SELECT y.b FROM y) AS x, (SELECT y.a FROM y) AS z WHERE x.z IS NULL")
                (map :projected-columns))))

  (t/is (= [[{:index 0 :identifier "b" :qualified-column ["x" "b"]}
             {:index 1 :identifier "z" :qualified-column ["x" "z"]}
             {:index 2 :identifier "a" :qualified-column ["z" "a"]}]
            [{:index 0 :identifier "b" :qualified-column ["x" "b"]}
             {:index 1 :identifier "z" :qualified-column ["x" "z"]}
             {:index 2 :identifier "a" :qualified-column ["z" "a"]}]
            [{:index 0 :identifier "b" :qualified-column ["y" "b"]}]
            [{:index 0 :identifier "b" :qualified-column ["y" "b"]}]
            [{:index 0 :identifier "a" :qualified-column ["y" "a"]}]
            [{:index 0 :identifier "a" :qualified-column ["y" "a"]}]]
           (->> (valid? "SELECT x.*, z.* FROM (SELECT y.b FROM y) AS x, (SELECT y.a FROM y) AS z WHERE x.z IS NULL")
                (map :projected-columns))))

  (t/is (= [[{:index 0 :identifier "a" :qualified-column ["x" "a"]}]
            [{:index 0 :identifier "a" :qualified-column ["x" "a"]}]]
           (->> (valid? "SELECT * FROM x WHERE x.a = x.b GROUP BY x.a")
                (map :projected-columns))))

  (t/is (= [[{:index 0 :identifier "a" :qualified-column ["x" "a"]}
             {:index 1 :identifier "c" :qualified-column ["x" "c"]}
             {:index 2}]
            [{:index 0 :identifier "a" :qualified-column ["x" "a"]}
             {:index 1 :identifier "c" :qualified-column ["x" "c"]}
             {:index 2}]]
           (->> (valid? "SELECT x.*, COUNT(x.b) FROM x WHERE x.a = x.b GROUP BY x.a, x.c")
                (map :projected-columns))))

  (invalid? #"Query does not select any columns"
            "SELECT * FROM foo")
  (invalid? #"Table not in scope: baz"
            "SELECT foo.x, baz.* FROM foo")

  (t/is (= [[{:index 0 :identifier "a" :qualified-column ["x" "a"]}]
            [{:index 0 :identifier "a" :qualified-column ["x" "a"]}]]
           (->> (valid? "SELECT x.a, x.* FROM x WHERE x.a IS NOT NULL")
                (map :projected-columns))))

  (t/is (= [[{:index 0 :identifier "a" :qualified-column ["x" "a"]}
             {:index 1 :identifier "a" :qualified-column ["x" "a"]}]
            [{:index 0 :identifier "a" :qualified-column ["x" "a"]}
             {:index 1 :identifier "a" :qualified-column ["x" "a"]}]]
           (->> (valid? "SELECT x.a, x.a FROM x WHERE x.a IS NOT NULL")
                (map :projected-columns))))

  (t/is (= [[{:index 0 :identifier "a" :qualified-column ["x" "a"]}
             {:index 1 :identifier "a" :qualified-column ["y" "a"]}]
            [{:index 0 :identifier "a" :qualified-column ["x" "a"]}
             {:index 1 :identifier "a" :qualified-column ["y" "a"]}]]
           (->> (valid? "SELECT * FROM x, y WHERE x.a = y.a")
                (map :projected-columns))))

  (t/is (= [[{:index 0 :identifier "a" :qualified-column ["foo" "a"]}
             {:index 1 :identifier "b" :qualified-column ["foo" "b"]}]
            [{:index 0 :identifier "a" :qualified-column ["x" "a"]}
             {:index 1 :identifier "b" :qualified-column ["x" "b"]}]
            [{:index 0 :identifier "a" :qualified-column ["x" "a"]}
             {:index 1 :identifier "b" :qualified-column ["x" "b"]}]
            [{:index 0 :identifier "a" :qualified-column ["foo" "a"]}
             {:index 1 :identifier "b" :qualified-column ["foo" "b"]}]]
           (->> (valid? "WITH foo AS (SELECT * FROM x WHERE x.a = x.b) SELECT * FROM foo")
                (map :projected-columns)))))
