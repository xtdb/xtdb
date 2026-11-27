(ns xtdb.operator.set-test
  (:require [clojure.test :as t]
            [xtdb.test-util :as tu]))

(t/use-fixtures :each tu/with-allocator)

(t/deftest test-union-all
  (t/is (= {:types '{a #xt/type :i64, b #xt/type [:? :i64]}
            :res [{:a 12, :b 10} {:a 0} {:a 10, :b 1}]}
           (tu/query-ra [:union-all {}
                         [::tu/pages [[{:a 12 :b 10}, {:a 0 :b nil}]]]
                         [::tu/pages [[{:a 10 :b 1}]]]]
                        {:with-types? true}))))

(t/deftest test-union-all-empty-left-side-bug
  (t/is (= [{:a 10} {:a 15}]
           (tu/query-ra
            [:cross-join {}
             [:table {:rows [{}]}]
             [:union-all {}
              [::tu/pages '{a #xt/type :i64} [[]]]
              [::tu/pages [[{:a 10}, {:a 15}]]]]]))))

(t/deftest test-intersection
  (t/is (= {:types '{a #xt/type :i64, b #xt/type :i64}
            :res [{:a 0, :b 15}]}
           (tu/query-ra [:intersect {}
                         [::tu/pages [[{:a 12 :b 10}, {:a 0 :b 15}]]]
                         [::tu/pages [[{:a 10 :b 1}, {:a 0 :b 15}]]]]
                        {:with-types? true}))))

(t/deftest test-difference
  (t/is (= {:types '{x1 #xt/type :i64}
            :res [{:x1 2}]}
           (tu/query-ra '[:difference {}
                          [:table {:rows [{x1 1} {x1 2}]}]
                          [:table {:rows [{x1 1}]}]]
                        {:with-types? true}))))

(t/deftest test-intersection-matches-null-keys-6340
  (t/is (= {{:b 1} 2}
           (-> (tu/query-ra [:intersect {}
                             [::tu/pages '{a #xt/type [:? :i64], b #xt/type :i64}
                              [[{:a nil :b 1} {:a nil :b 1} {:a nil :b 1} {:a 2 :b 2}]]]
                             [::tu/pages '{a #xt/type [:? :i64], b #xt/type :i64}
                              [[{:a nil :b 1} {:a nil :b 1} {:a 3 :b 2}]]]])
               (frequencies)))
        "cardinalities - minimum of the cardinalities in each side"))

(t/deftest test-difference-matches-null-keys-6340
  (t/is (= {{:b 1} 1, {:a 2 :b 2} 1}
           (-> (tu/query-ra [:difference {}
                             [::tu/pages '{a #xt/type [:? :i64], b #xt/type :i64}
                              [[{:a nil :b 1} {:a nil :b 1} {:a nil :b 1} {:a 2 :b 2}]]]
                             [::tu/pages '{a #xt/type [:? :i64], b #xt/type :i64}
                              [[{:a nil :b 1} {:a nil :b 1} {:a 3 :b 2}]]]])
               (frequencies)))
        "cardinalities - difference of the cardinalities in each side"))

(t/deftest test-incompatible-columns
  (t/is (thrown-with-msg? xtdb.api.error.Incorrect #"union incompatible cols"
                          (tu/query-ra [:union-all {}
                                        [::tu/pages [[{:a 1}]]]
                                        [::tu/pages [[{:b 1}]]]]))))
