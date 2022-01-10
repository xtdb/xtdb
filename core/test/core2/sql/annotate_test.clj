(ns core2.sql.annotate-test
  (:require [clojure.test :as t]
            [core2.sql :as sql]))

(t/deftest test-annotate-query-scopes
  (let [tree (sql/parse "WITH foo AS (SELECT 1 FROM bar)
SELECT t1.d-t1.e
  FROM t1, foo AS baz
 WHERE EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b)
   AND t1.a>t1.b
 ORDER BY 1" :query_expression)
        scopes (->> (sql/annotate-tree tree)
                    (tree-seq vector? seq)
                    (keep (comp :core2.sql/scope meta)))]
    (t/is (= [{:with {"foo" {:query-name "foo" :id 2}}
               :tables {"t1" {:table-or-query-name "t1" :correlation-name "t1" :id 3}
                        "baz" {:table-or-query-name "foo" :correlation-name "baz" :id 4 :with-id 2}}
               :columns #{{:identifiers ["t1" "d"] :table-id 3}
                          {:identifiers ["t1" "e"] :table-id 3}
                          {:identifiers ["t1" "a"] :table-id 3}
                          {:identifiers ["t1" "b"] :table-id 3}}}
              {:with {}
               :tables {"bar" {:table-or-query-name "bar" :correlation-name "bar" :id 1}}
               :columns #{}}
              {:with {}
               :tables {"x" {:table-or-query-name "t1" :correlation-name "x" :id 5}}
               :columns #{{:identifiers ["x" "b"] :table-id 5}
                          {:identifiers ["t1" "b"] :table-id 3}}}]
             scopes))))

(t/deftest test-scope-rules
  (t/is (thrown-with-msg? IllegalArgumentException #"XTDB requires fully-qualified columns: a at line 1, column 8"
                          (sql/annotate-tree (sql/parse "SELECT a FROM foo"))))

  (t/is (thrown-with-msg? IllegalArgumentException #"Table not in scope: bar at line 1, column 8"
                          (sql/annotate-tree (sql/parse "SELECT bar.a FROM foo"))))
  (t/is (thrown-with-msg? IllegalArgumentException #"Table not in scope: bar"
                          (sql/annotate-tree (sql/parse "SELECT bar.a FROM bar AS foo"))))

  (t/is (some? (sql/annotate-tree (sql/parse "SELECT bar.a FROM bar"))))
  (t/is (some? (sql/annotate-tree (sql/parse "SELECT bar.a FROM foo AS bar"))))

  (t/is (some? (sql/annotate-tree (sql/parse "SELECT t1.b FROM t1 WHERE EXISTS (SELECT x.b FROM t1 AS x WHERE x.b < t1.b)"))))

  (t/is (some? (sql/annotate-tree (sql/parse "SELECT t1.b FROM t1, LATERAL (SELECT x.b FROM t1 AS x WHERE x.b < t1.b) AS t2"))))
  (t/is (thrown-with-msg? IllegalArgumentException #"Table not in scope: t1"
                          (sql/annotate-tree (sql/parse "SELECT * FROM t1, (SELECT x.b FROM t1 AS x WHERE x.b < t1.b) AS t2"))))
  (t/is (thrown-with-msg? IllegalArgumentException #"Table not in scope: t2"
                          (sql/annotate-tree (sql/parse "SELECT * FROM t1, LATERAL (SELECT x.b FROM t1 AS x WHERE x.b < t1.b) AS t2, (SELECT x.b FROM t1 AS x WHERE x.b < t2.b) AS t3"))))

  (t/is (some? (sql/annotate-tree (sql/parse "SELECT t1.b FROM t1 JOIN t2 USING (x)"))))
  (t/is (some? (sql/annotate-tree (sql/parse "SELECT t1.b FROM t1 JOIN t2 ON (t1.x = t2.y)"))))
  (t/is (some? (sql/annotate-tree (sql/parse "SELECT * FROM foo, LATERAL (SELECT t1.b FROM t1 JOIN t2 ON (t1.x = t2.y AND t1.x = foo.x)) AS t2"))))
  (t/is (some? (sql/annotate-tree (sql/parse "SELECT * FROM foo WHERE foo.x = (SELECT t1.b FROM t1 JOIN t2 ON (t1.x = t2.y AND t1.x = foo.x))"))))
  (t/is (thrown-with-msg? IllegalArgumentException #"Table not in scope: foo"
                          (sql/annotate-tree (sql/parse "SELECT * FROM (SELECT t1.b FROM t1 JOIN t2 ON (t1.x = t2.y AND t1.x = foo.x)) AS t2"))))

  (t/is (thrown-with-msg? IllegalArgumentException #"Table not in scope: foo"
                          (sql/annotate-tree (sql/parse "SELECT t1.b FROM foo, t1 JOIN t2 ON (t1.x = foo.y)"))))

  (t/is (thrown-with-msg? IllegalArgumentException #"Table not in scope: t1"
                          (sql/annotate-tree (sql/parse "SELECT t2.b FROM (SELECT x.b FROM t1 AS x WHERE x.b < t1.b) AS t2, t1"))))
  (t/is (thrown-with-msg? IllegalArgumentException #"Table not in scope: bar"
                          (sql/annotate-tree (sql/parse "SELECT bar.a FROM foo WHERE EXISTS (SELECT bar.b FROM bar WHERE foo.a < bar.b)"))))
  (t/is (thrown-with-msg? IllegalArgumentException #"Table not in scope: foo"
                          (sql/annotate-tree (sql/parse "SELECT * FROM foo AS baz WHERE EXISTS (SELECT bar.b FROM bar WHERE foo.a < bar.b)")))))
