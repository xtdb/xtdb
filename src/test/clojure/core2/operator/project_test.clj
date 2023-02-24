(ns core2.operator.project-test
  (:require [clojure.test :as t]
            [core2.test-util :as tu]))

(t/use-fixtures :each tu/with-allocator)

(t/deftest test-project
  (t/is (= {:col-types '{a :i64, c :i64}
            :res [[{:a 12, :c 22}, {:a 0, :c 15}]
                  [{:a 100, :c 183}]]}
           (tu/query-ra [:project '[a {c (+ a b)}]
                         [::tu/blocks
                          [[{:a 12, :b 10}
                            {:a 0, :b 15}]
                           [{:a 100, :b 83}]]]]
                        {:preserve-blocks? true
                         :with-col-types? true})))

  (t/testing "param"
    (t/is (= [[{:a 12, :b 52}, {:a 0, :b 57}]
              [{:a 100, :b 125}]]
             (tu/query-ra [:project '[a {b (+ b ?p)}]
                           [::tu/blocks
                            [[{:a 12, :b 10}
                              {:a 0, :b 15}]
                             [{:a 100, :b 83}]]]]
                          {:params '{?p 42}
                           :preserve-blocks? true})))))

(t/deftest test-project-row-number
  (t/is (= {:col-types '{a :i64, $row-num :i64}
            :res [[{:a 12, :$row-num 1}, {:a 0, :$row-num 2}]
                  [{:a 100, :$row-num 3}]]}
           (tu/query-ra [:project '[a, {$row-num (row-number)}]
                         [::tu/blocks
                          [[{:a 12, :b 10}
                            {:a 0, :b 15}]
                           [{:a 100, :b 83}]]]]
                        {:preserve-blocks? true
                         :with-col-types? true}))))

(t/deftest test-identity-projection-not-closed
  (t/is (= [{:a nil, :b 12} {:a nil, :b 2} {:a 12, :b nil} {:a 0, :b nil}]
           (tu/query-ra
             '[:project [a b]
               [:full-outer-join [false]
                [:table [{:a 12}, {:a 0}]]
                [:table [{:b 12}, {:b 2}]]]]
             {}))))
