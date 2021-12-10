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
