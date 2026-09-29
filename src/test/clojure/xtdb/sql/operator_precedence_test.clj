(ns xtdb.sql.operator-precedence-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.test-util :as tu]))

(t/use-fixtures :each tu/with-node)

(defn- q1 [sql]
  (-> (xt/q tu/*node* (str "SELECT " sql " AS r")) first :r))

(t/deftest multiplication-binds-tighter-than-addition
  (t/is (= 14 (q1 "2 + 3 * 4")))
  (t/is (= -6 (q1 "-2 * 3"))))

(t/deftest bitwise-operators-share-a-level-and-associate-left
  (t/is (= 1 (q1 "5 | 3 & 1")))
  (t/is (= 0 (q1 "2 << 1 & 1")))
  (t/is (= 4 (q1 "1 << 1 + 1"))))

(t/deftest element-access-binds-tighter-than-arithmetic
  (t/is (= 2 (q1 "{a: 1} -> 'a' + 1")))
  (t/is (= "1" (q1 "{a: {b: 1}} -> 'a' ->> 'b'"))))

(t/deftest predicates-bind-tighter-than-comparisons
  (t/is (true? (q1 "true = 'abc' LIKE 'a%'")))
  (t/is (true? (q1 "true = 'abc' LIKE_REGEX 'b'")))
  (t/is (true? (q1 "true = 'abc' ~ 'b'")))
  (t/is (true? (q1 "true = 1 IN (1, 2)")))
  (t/is (true? (q1 "true = 2 BETWEEN 1 AND 3")))
  (t/is (true? (q1 "true = PERIOD(TIMESTAMP '2020-01-01Z', TIMESTAMP '2021-01-01Z') CONTAINS TIMESTAMP '2020-06-01Z'"))))

(t/deftest comparisons-bind-tighter-than-is
  (t/is (true? (q1 "1 = 2 IS FALSE")))
  (t/is (true? (q1 "1 = NULL IS NULL")))
  (t/is (true? (q1 "1 = 2 IS DISTINCT FROM true"))))

(t/deftest boolean-operators-bind-loosest
  (t/is (true? (q1 "NOT 1 = 2")))
  (t/is (true? (q1 "true OR false AND false")))
  (t/is (false? (q1 "2 BETWEEN 1 AND 3 AND false")))
  (t/is (false? (q1 "'abc' LIKE 'a%' AND false"))))
