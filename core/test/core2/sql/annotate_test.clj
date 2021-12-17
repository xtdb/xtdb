(ns core2.sql.annotate-test
  (:require [clojure.test :as t]
            [core2.sql :as sql]))

(t/deftest test-annotate-query-scopes
  (let [tree (sql/parse "WITH foo AS (SELECT 1 FROM bar)
SELECT t1.d-t1.e
  FROM t1
 WHERE EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b)
   AND t1.a>t1.b
 ORDER BY 1" :query_expression)
        scopes (->> (sql/annotate-tree tree)
                    (tree-seq vector? seq)
                    (keep (comp :core2.sql/scope meta)))]
    (t/is (= [{:tables #{{:table-or-query-name "t1"}}
               :columns #{["t1" "e"] ["t1" "a"] ["t1" "b"] ["t1" "d"]}
               :with #{"foo"}
               :correlated-columns #{}}
              {:tables #{{:table-or-query-name "bar"}},
               :columns #{},
               :with #{},
               :correlated-columns #{}}
              {:tables #{{:table-or-query-name "t1" :correlation-name "x"}}
               :columns #{["x" "b"] ["t1" "b"]}
               :with #{}
               :correlated-columns #{["t1" "b"]}}]
             scopes))))

(t/deftest test-scope-rules
  (t/is (thrown-with-msg? IllegalArgumentException #"XTDB requires fully-qualified columns: a"
                          (sql/annotate-tree (sql/parse "SELECT a FROM foo"))))

  (t/is (thrown-with-msg? IllegalArgumentException #"Table not in scope: bar"
                          (sql/annotate-tree (sql/parse "SELECT bar.a FROM foo"))))
  (t/is (thrown-with-msg? IllegalArgumentException #"Table not in scope: bar"
                          (sql/annotate-tree (sql/parse "SELECT bar.a FROM bar AS foo"))))

  (t/is (some? (sql/annotate-tree (sql/parse "SELECT bar.a FROM bar"))))
  (t/is (some? (sql/annotate-tree (sql/parse "SELECT bar.a FROM foo AS bar"))))

  (t/is (some? (sql/annotate-tree (sql/parse "SELECT t1.b FROM t1 WHERE EXISTS (SELECT x.b FROM t1 AS x WHERE x.b < t1.b)"))))
  (t/is (some? (sql/annotate-tree (sql/parse "SELECT t1.b FROM t1, (SELECT x.b FROM t1 AS x WHERE x.b < t1.b) AS t2"))))

  (t/is (thrown-with-msg? IllegalArgumentException #"Table not in scope: t1"
                          (sql/annotate-tree (sql/parse "SELECT t2.b FROM (SELECT x.b FROM t1 AS x WHERE x.b < t1.b) AS t2, t1"))))
  (t/is (thrown-with-msg? IllegalArgumentException #"Table not in scope: bar"
                          (sql/annotate-tree (sql/parse "SELECT bar.a FROM foo WHERE EXISTS (SELECT bar.b FROM bar WHERE foo.a < bar.b)"))))
  (t/is (thrown-with-msg? IllegalArgumentException #"Table not in scope: foo"
                          (sql/annotate-tree (sql/parse "SELECT * FROM foo AS baz WHERE EXISTS (SELECT bar.b FROM bar WHERE foo.a < bar.b)")))))
