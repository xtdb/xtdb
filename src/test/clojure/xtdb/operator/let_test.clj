(ns xtdb.operator.let-test
  (:require [clojure.test :as t]
            [xtdb.operator.let :as let]
            [xtdb.test-util :as tu]))

(t/use-fixtures :each tu/with-allocator)

(t/deftest test-let
  (t/is (= [{:a 4}]
           (tu/query-ra '[:let {:binding-sym Foo}
                          [::tu/pages
                           [[{:a 12}, {:a 0}]
                            [{:a 12}, {:a 100}]]]
                          [:table {:rows [{:a 4}]}]]))
        "unused let")

  (t/is (= [[{:a 12}, {:a 0}]
            [{:a 12}, {:a 100}]]
           (tu/query-ra '[:let {:binding-sym Foo}
                          [::tu/pages
                           [[{:a 12}, {:a 0}]
                            [{:a 12}, {:a 100}]]]
                          [:relation {:cte-id Foo :col-names [a]}]]
                        {:preserve-pages? true}))
        "normal usage")

  (t/is (= [{:a 1 :b 1}]
           (tu/query-ra '[:let {:binding-sym X}
                          [:table {:param ?x}]
                          [:let {:binding-sym Y}
                           [:join {:conditions [{a b}]}
                            [:relation {:cte-id X :col-names [a]}]
                            [:table {:param ?y}]]
                           [:let {:binding-sym X}
                            [:relation {:cte-id Y :col-names [a b]}]
                            [:relation {:cte-id X :col-names [a b]}]]]]

                        {:args {:x [{:a 1}]
                                :y [{:b 1}]}}))
        "can see & override earlier assignments")

  (t/is (= [[{:a 12}, {:a 0}]
            [{:a 12}, {:a 100}]
            [{:a 12}, {:a 0}]
            [{:a 12}, {:a 100}]]
           (tu/query-ra '[:let {:binding-sym Foo}
                          [::tu/pages
                           [[{:a 12}, {:a 0}]
                            [{:a 12}, {:a 100}]]]
                          [:union-all {}
                           [:relation {:cte-id Foo :col-names [a]}]
                           [:relation {:cte-id Foo :col-names [a]}]]]
                        {:preserve-pages? true}))
        "can use it multiple times")

  (t/is (= [{:a 0} {:a 0}
            {:a 12} {:a 12} {:a 12} {:a 12}
            {:a 100} {:a 100}]
           (tu/query-ra '[:let {:binding-sym Foo}
                          [::tu/pages
                           [[{:a 12}, {:a 0}]
                            [{:a 12}, {:a 100}]]]
                          [:sort {:order-specs [[a]]}
                           [:union-all {}
                            [:relation {:cte-id Foo :col-names [a]}]
                            [:relation {:cte-id Foo :col-names [a]}]]]]))
        "can pass it to other operators"))

(t/deftest test-spilled-let
  (binding [let/*spill-threshold-rows* 3]
    (t/is (= [[{:a 12}, {:a 0}]
              [{:a 12}, {:a 100}]
              [{:a 7}, {:a "x"}]]
             (tu/query-ra '[:let {:binding-sym Foo}
                            [::tu/pages
                             [[{:a 12}, {:a 0}]
                              [{:a 12}, {:a 100}]
                              [{:a 7}, {:a "x"}]]]
                            [:relation {:cte-id Foo :col-names [a]}]]
                          {:preserve-pages? true}))
          "spilled pages round-trip, across a change of type")

    (t/is (= #{{:a 1 :b 1} {:a 2 :b 2} {:a 3 :b 3} {:a 4 :b 4}}
             (set (tu/query-ra '[:let {:binding-sym Foo}
                                 [::tu/pages [[{:a 1}, {:a 2}] [{:a 3}, {:a 4}]]]
                                 [:join {:conditions [{a b}]}
                                  [:relation {:cte-id Foo :col-names [a]}]
                                  [:rename {:columns {a b}}
                                   [:relation {:cte-id Foo :col-names [a]}]]]])))
          "self-join of a spilled CTE")

    (t/is (= [{:a 1}]
             (tu/query-ra '[:let {:binding-sym Foo}
                            [::tu/pages [[{:a 1}, {:a 2}] [{:a 3}, {:a 4}] [{:a 5}]]]
                            [:sort {:limit 1}
                             [:relation {:cte-id Foo :col-names [a]}]]]))
          "closed before a reader reaches the end of the spill")

    (t/is (anomalous? [:incorrect nil #"division by zero"]
                      (tu/query-ra '[:let {:binding-sym Foo}
                                     [:project {:projections [{b (/ 1 a)}]}
                                      [::tu/pages [[{:a 1}, {:a 2}] [{:a 3}, {:a 4}] [{:a 0}]]]]
                                     [:relation {:cte-id Foo :col-names [b]}]]))
          "fails after the spill has started")))
