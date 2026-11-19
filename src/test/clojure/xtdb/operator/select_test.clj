(ns xtdb.operator.select-test
  (:require [clojure.test :as t :refer [deftest]]
            [xtdb.test-util :as tu]))

(t/use-fixtures :each tu/with-allocator)

(t/deftest test-select
  (t/is (= [[{:a 12, :b 10}]
            [{:a 100, :b 83}]]
           (tu/query-ra [:select {:predicate '(> a b)}
                         [::tu/pages
                          [[{:a 12, :b 10}
                            {:a 0, :b 15}]
                           [{:a 100, :b 83}]
                           [{:a 83, :b 100}]]]]
                        {:preserve-pages? true})))

  (t/testing "param"
    (t/is (= [[{:a 100, :b 83}]
              [{:a 83, :b 100}]]
             (tu/query-ra [:select {:predicate '(> a ?b)}
                           [::tu/pages
                            [[{:a 12, :b 10}
                              {:a 0, :b 15}]
                             [{:a 100, :b 83}]
                             [{:a 83, :b 100}]]]]
                          {:args {:b 50}
                           :preserve-pages? true})))))

(deftest test-no-column-relation
  (t/is (= [{}]
           (tu/query-ra '[:select {:predicate true}
                          [:table {:rows [{}]}]])))
  (t/is (= []
           (tu/query-ra
            '[:select {:predicate false}
              [:table {:rows [{}]}]])))

  (t/is (= [{} {} {}]
           (tu/query-ra '[:select {:predicate (== ?ap_n 2)}
                          [:table {:rows [{} {} {}]}]]
                        {:args {:ap-n 2}}))))

(deftest test-select-null-predicate-is-unselected
  (t/is (= [{:a 1, :b 0} {:a 4, :b 2}]
           (tu/query-ra [:select {:predicate '(> a b)}
                         [::tu/pages
                          [[{:a 1, :b 0}
                            {:a nil, :b 1}
                            {:a 3, :b nil}
                            {:a 4, :b 2}
                            {:a 0, :b 5}]]]]))))

(deftest test-select-across-word-boundaries
  (let [selected #{0 62 63 64 65 127 128 191 199}
        rows (for [a (range 200)] {:a a, :sel (contains? selected a)})]
    (t/is (= (filter :sel rows)
             (tu/query-ra [:select {:predicate 'sel}
                           [::tu/pages [rows]]])))))

(deftest test-stacked-selects
  (let [rows (for [a (range 200)] {:a a, :b (* 2 a), :sixth (zero? (mod a 6))})]
    (t/is (= (filter (fn [{:keys [a b sixth]}] (and (< b 300) sixth (> a 20))) rows)
             (tu/query-ra [:select '{:predicate (> a 20)}
                           [:select '{:predicate sixth}
                            [:select '{:predicate (< b 300)}
                             [::tu/pages [rows]]]]])))))
