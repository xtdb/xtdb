(ns xtdb.operator.select-test
  (:require [clojure.test :as t :refer [deftest]]
            [xtdb.test-util :as tu]))

(t/use-fixtures :each tu/with-allocator)

(t/deftest test-select
  (t/is (= [[{:a 12, :b 10}]
            [{:a 100, :b 83}]]
           (tu/query-ra [:select '(> a b)
                         [::tu/blocks
                          [[{:a 12, :b 10}
                            {:a 0, :b 15}]
                           [{:a 100, :b 83}]
                           [{:a 83, :b 100}]]]]
                        {:preserve-blocks? true})))

  (t/testing "param"
    (t/is (= [[{:a 100, :b 83}]
              [{:a 83, :b 100}]]
             (tu/query-ra [:select '(> a ?b)
                           [::tu/blocks
                            [[{:a 12, :b 10}
                              {:a 0, :b 15}]
                             [{:a 100, :b 83}]
                             [{:a 83, :b 100}]]]]
                          {:params {'?b 50}
                           :preserve-blocks? true})))))

(deftest test-no-column-relation
  (t/is (= [{}]
           (tu/query-ra '[:select true
                          [:table [{}]]])))
  (t/is (= []
           (tu/query-ra
            '[:select false
              [:table [{}]]])))

  (t/is (= [{} {} {}]
           (tu/query-ra '[:select (= ?ap_n 2)
                          [:table [{} {} {}]]]
                        {:params {'?ap_n 2}}))))
