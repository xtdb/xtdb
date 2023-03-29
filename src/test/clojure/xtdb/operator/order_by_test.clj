(ns xtdb.operator.order-by-test
  (:require [clojure.test :as t]
            [xtdb.test-util :as tu]))

(t/use-fixtures :each tu/with-allocator)

(t/deftest test-order-by
  (t/is (= [[{:a 0, :b 15}
             {:a 12, :b 10}
             {:a 83, :b 100}
             {:a 100, :b 83}]]
           (tu/query-ra [:order-by '[[a]]
                         [::tu/blocks
                          [[{:a 12, :b 10}
                            {:a 0, :b 15}]
                           [{:a 100, :b 83}]
                           [{:a 83, :b 100}]]]]
                        {:preserve-blocks? true})))

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

  (let [table-with-nil [{:a 12.4, :b 10}, {:a nil, :b 15}, {:a 100, :b 83}, {:a 83.0, :b 100}]]
    (t/is (= [{:a nil, :b 15}, {:a 12.4, :b 10}, {:a 83.0, :b 100}, {:a 100, :b 83}]
             (tu/query-ra '[:order-by [[a {:null-ordering :nulls-first}]]
                            [:table ?table]]
                          {:table-args {'?table table-with-nil}}))
          "nulls first")

    (t/is (= [{:a 12.4, :b 10}, {:a 83.0, :b 100}, {:a 100, :b 83}, {:a nil, :b 15}]
             (tu/query-ra '[:order-by [[a {:null-ordering :nulls-last}]]
                            [:table ?table]]
                          {:table-args {'?table table-with-nil}}))
          "nulls last")

    (t/is (= [{:a 12.4, :b 10}, {:a 83.0, :b 100}, {:a 100, :b 83}, {:a nil, :b 15}]
             (tu/query-ra '[:order-by [[a]]
                            [:table ?table]]
                          {:table-args {'?table table-with-nil}}))
          "default nulls last")))
