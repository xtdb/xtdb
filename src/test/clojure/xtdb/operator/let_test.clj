(ns xtdb.operator.let-test
  (:require [clojure.test :as t]
            [xtdb.test-util :as tu]))

(t/use-fixtures :each tu/with-allocator)

(t/deftest test-let
  (t/is (= [{:a 4}]
           (tu/query-ra '[:let [Foo [::tu/pages
                                     [[{:a 12}, {:a 0}]
                                      [{:a 12}, {:a 100}]]]]
                          [:table [{:a 4}]]]))
        "unused let")

  (t/is (= [[{:a 12}, {:a 0}]
            [{:a 12}, {:a 100}]]
           (tu/query-ra '[:let [Foo [::tu/pages
                                     [[{:a 12}, {:a 0}]
                                      [{:a 12}, {:a 100}]]]]
                          [:relation Foo {:col-names [a]}]]
                        {:preserve-pages? true}))
        "normal usage")

  (t/is (= [{:a 1 :b 1}]
           (tu/query-ra '[:let [X [:table ?x]]
                          [:let [Y [:join [{a b}]
                                    [:relation X {:col-names [a]}]
                                    [:table ?y]]]
                           [:let [X [:relation Y {:col-names [a b]}]]
                            [:relation X {:col-names [a b]}]]]]

                        {:args {:x [{:a 1}]
                                :y [{:b 1}]}}))
        "can see & override earlier assignments")

  (t/is (= [[{:a 12}, {:a 0}]
            [{:a 12}, {:a 100}]
            [{:a 12}, {:a 0}]
            [{:a 12}, {:a 100}]]
           (tu/query-ra '[:let [Foo [::tu/pages
                                     [[{:a 12}, {:a 0}]
                                      [{:a 12}, {:a 100}]]]]
                          [:union-all
                           [:relation Foo {:col-names [a]}]
                           [:relation Foo {:col-names [a]}]]]
                        {:preserve-pages? true}))
        "can use it multiple times")

  (t/is (= [{:a 0} {:a 0}
            {:a 12} {:a 12} {:a 12} {:a 12}
            {:a 100} {:a 100}]
           (tu/query-ra '[:let [Foo [::tu/pages
                                     [[{:a 12}, {:a 0}]
                                      [{:a 12}, {:a 100}]]]]
                          [:order-by [[a]]
                           [:union-all
                            [:relation Foo {:col-names [a]}]
                            [:relation Foo {:col-names [a]}]]]]))
        "can pass it to other operators"))
