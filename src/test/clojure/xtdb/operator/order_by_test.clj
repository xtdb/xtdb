(ns xtdb.operator.order-by-test
  (:require [clojure.test :as t]
            [xtdb.operator.order-by :as order-by]
            [xtdb.test-util :as tu]))

(t/use-fixtures :each tu/with-allocator)

(t/deftest test-order-by
  (t/is (= [[{:a 0, :b 15}
             {:a 12, :b 10}
             {:a 83, :b 100}
             {:a 100, :b 83}]]
           (tu/query-ra [:order-by '[[a]]
                         [::tu/pages
                          [[{:a 12, :b 10}
                            {:a 0, :b 15}]
                           [{:a 100, :b 83}]
                           [{:a 83, :b 100}]]]]
                        {:preserve-pages? true})))

  (t/is (= [{:a 0, :b 15}
            {:a 12.4, :b 10}
            {:a 83.0, :b 100}
            {:a 100, :b 83}]
           (tu/query-ra '[:order-by [[a]]
                          [:table [{:a 12.4, :b 10}
                                   {:a 0, :b 15}
                                   {:a 100, :b 83}
                                   {:a 83.0, :b 100}]]]
                        {}))
        "mixed numeric types")

  (t/is (= []
           (tu/query-ra '[:order-by [[a]]
                          [::tu/pages
                           [[] []]]]
                        {}))
        "empty batches"))

(t/deftest test-order-by-with-nulls
  (let [table-with-nil [{:a 12.4, :b 10}, {:a nil, :b 15}, {:a 100, :b 83}, {:a 83.0, :b 100}]]
    (t/is (= [{:b 15}, {:a 12.4, :b 10}, {:a 83.0, :b 100}, {:a 100, :b 83}]
             (tu/query-ra '[:order-by [[a {:null-ordering :nulls-first}]]
                            [:table ?table]]
                          {:args {:table table-with-nil}}))
          "nulls first")

    (t/is (= [{:a 12.4, :b 10}, {:a 83.0, :b 100}, {:a 100, :b 83}, {:b 15}]
             (tu/query-ra '[:order-by [[a {:null-ordering :nulls-last}]]
                            [:table ?table]]
                          {:args {:table table-with-nil}}))
          "nulls last")

    (t/is (= [{:a 12.4, :b 10}, {:a 83.0, :b 100}, {:a 100, :b 83}, {:b 15}]
             (tu/query-ra '[:order-by [[a]]
                            [:table ?table]]
                          {:args {:table table-with-nil}}))
          "default nulls last")))

(t/deftest test-order-by-spill
  (binding [order-by/*block-size* 10]
    (let [data (map-indexed (fn [i d] {:a d :b i}) (repeatedly 1000 #(rand-int 1000000)))
          batches (mapv vec (partition-all 13 data))
          sorted (sort-by (juxt :a :b) data)]
      (t/is (= sorted
               (tu/query-ra [:order-by '[[a] [b]]
                             [::tu/pages batches]]
                            {}))
            "spilling to disk"))))
