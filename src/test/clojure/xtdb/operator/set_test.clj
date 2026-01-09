(ns xtdb.operator.set-test
  (:require [clojure.test :as t]
            [xtdb.test-util :as tu]))

(t/use-fixtures :each tu/with-allocator)

(t/deftest test-union-all
  (t/is (= {:types '{a #xt/type :i64, b #xt/type :i64}
            :res [#{{:a 0, :b 15}
                    {:a 12, :b 10}}
                  #{{:a 100, :b 15}}
                  #{{:a 10, :b 1}
                    {:a 15, :b 2}}
                  #{{:a 83, :b 3}}]}
           (-> (tu/query-ra [:union-all {}
                             [::tu/pages
                              [[{:a 12 :b 10}, {:a 0 :b 15}]
                               [{:a 100 :b 15}]]]
                             [::tu/pages
                              [[{:a 10 :b 1}, {:a 15 :b 2}]
                               [{:a 83 :b 3}]]]]
                            {:preserve-pages? true
                             :with-types? true})
               (update :res (partial mapv set)))))

  (t/testing "empty input and output"
    (t/is (empty? (tu/query-ra [:union-all {}
                                [::tu/pages '{a #xt/type :i64} []]
                                [::tu/pages '{a #xt/type :i64} []]])))))

(t/deftest test-union-all-empty-left-side-bug
  (t/testing "empty left side"
    (t/is (= [{:a 10} {:a 15}]
             (tu/query-ra
              [:cross-join {}
               [:table {:rows [{}]}]
               [:union-all {}
                [::tu/pages '{a #xt/type :i64} [[]]]
                [::tu/pages [[{:a 10}, {:a 15}]]]]])))))

(t/deftest test-union-all-empty-batches-both-sides
  (t/is (= [{:a 10} {:a 15}]
           (tu/query-ra
            [:union-all {}
             [::tu/pages '{a #xt/type :i64} [[] [{:a 10}]]]
             [::tu/pages '{a #xt/type :i64} [[] [{:a 15}]]]]))))

(t/deftest test-intersection
  (t/is (= {:types '{a #xt/type :i64, b #xt/type :i64}
            :res [[{:a 0, :b 15}]]}
           (tu/query-ra [:intersect {}
                         [::tu/pages
                          [[{:a 12 :b 10}, {:a 0 :b 15}]
                           [{:a 100 :b 15}]]]
                         [::tu/pages
                          [[{:a 10 :b 1}, {:a 15 :b 2}]
                           [{:a 0 :b 15}]]]]
                        {:preserve-pages? true
                         :with-types? true})))

  (t/is (= {{:a 1} 2, {:a 2} 2}
           (-> (tu/query-ra [:intersect {}
                             [::tu/pages [[{:a 1} {:a 1} {:a 1} {:a 2} {:a 2}]]]
                             [::tu/pages [[{:a 1} {:a 1} {:a 2} {:a 2} {:a 2}]]]])
               (frequencies)))
        "cardinalities - minimum of the cardinalities in each side")

  (t/testing "empty input and output"
    (t/is (empty? (tu/query-ra [:intersect {}
                                [::tu/pages '{a #xt/type :i64} []]
                                [::tu/pages '{a #xt/type :i64} []]])))

    (t/is (empty? (tu/query-ra [:intersect {}
                                [::tu/pages '{a #xt/type :i64} []]
                                [::tu/pages '{a #xt/type :i64} [[{:a 10}, {:a 15}]]]])))

    (t/is (empty? (tu/query-ra [:intersect {}
                                [::tu/pages [[{:a 10}, {:a 15}]]]
                                [::tu/pages '{a #xt/type :i64} []]])))

    (t/is (empty? (tu/query-ra [:intersect {}
                                [::tu/pages [[{:a 10}]]]
                                [::tu/pages [[{:a 20}]]]])))))

(t/deftest test-difference
  (t/is (= {:types '{a #xt/type :i64, b #xt/type :i64}
            :res [#{{:a 12, :b 10}}
                  #{{:a 100 :b 15}}]}
           (-> (tu/query-ra [:difference {}
                             [::tu/pages
                              [[{:a 12 :b 10}, {:a 0 :b 15}]
                               [{:a 100 :b 15}]]]
                             [::tu/pages
                              [[{:a 10 :b 1}, {:a 15 :b 2}]
                               [{:a 0 :b 15}]]]]
                            {:preserve-pages? true
                             :with-types? true})
               (update :res (partial mapv set)))))

  (t/is (= {{:a 1} 1}
           (-> (tu/query-ra [:difference {}
                             [::tu/pages [[{:a 1} {:a 1} {:a 1} {:a 2} {:a 2}]]]
                             [::tu/pages [[{:a 1} {:a 1} {:a 2} {:a 2} {:a 2}]]]])
               (frequencies)))
        "cardinalities - difference of the cardinalities in each side")

  (t/testing "empty input and output"
    (t/is (empty? (tu/query-ra [:difference {}
                                [::tu/pages '{a #xt/type :i64} []]
                                [::tu/pages '{a #xt/type :i64} []]])))

    (t/is (empty? (tu/query-ra [:difference {}
                                [::tu/pages '{a #xt/type :i64} []]
                                [::tu/pages '{a #xt/type :i64} [[{:a 10}, {:a 15}]]]])))

    (t/is (= [#{{:a 10} {:a 15}}]
             (->> (tu/query-ra [:difference {}
                                [::tu/pages '{a #xt/type :i64} [[{:a 10}, {:a 15}]]]
                                [::tu/pages '{a #xt/type :i64} []]]
                               {:preserve-pages? true})
                  (mapv set))))))

(t/deftest first-tuple-in-rhs-is-taken-into-account-test
  (t/is
   (= {:res [{:x1 2}], :types '{x1 #xt/type :i64}}
      (tu/query-ra '[:difference {}
                     [:table {:rows [{x1 1} {x1 2}]}]
                     [:table {:rows [{x1 1}]}]]
                   {:with-types? true}))))
