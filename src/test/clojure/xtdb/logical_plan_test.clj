(ns xtdb.logical-plan-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.test-util :as tu]))

(t/use-fixtures :each tu/with-mock-clock tu/with-node)

(t/deftest test-count-star-rule-9
  (t/testing "count-star should be rewritten to count(dep-col) where dep col is a projected inner col of value 1")
    (xt/execute-tx tu/*node* [[:put-docs :t1 {:xt/id 1 :x 1}]
                              [:put-docs :t2 {:xt/id 2 :y 2}]])

    (t/is (= [{:t1-count 10, :y 2}]
             (xt/q tu/*node* "SELECT (SELECT (10 + count(*) + count(*)) FROM t1 WHERE t1.x = t2.y ) AS t1_count, t2.y FROM t2"))))
