(ns xtdb.operator.distinct-test
  (:require [clojure.test :as t]
            [xtdb.test-util :as tu]))

(t/use-fixtures :each tu/with-allocator)

(t/deftest test-distinct
  (t/is (= [#{{:a 12, :b 10} {:a 0, :b 15}}
            #{{:a 100, :b 15}}
            #{{:a 10, :b 15}}]
           (->> (tu/query-ra [:distinct {}
                              [::tu/pages
                               [[{:a 12 :b 10}, {:a 0 :b 15}]
                                [{:a 100 :b 15} {:a 0 :b 15}]
                                [{:a 100 :b 15}]
                                [{:a 10 :b 15} {:a 10 :b 15}]]]]
                             {:preserve-pages? true})
                (mapv set))))

  (t/testing "already distinct"
    (t/is (= [#{{:a 12, :b 10} {:a 0, :b 15}}
              #{{:a 100, :b 15}}]
             (->> (tu/query-ra [:distinct {}
                                [::tu/pages
                                 [[{:a 12 :b 10}, {:a 0 :b 15}]
                                  [{:a 100 :b 15}]]]]
                               {:preserve-pages? true})
                  (mapv set)))))

  (t/testing "empty input and output"
    (t/is (empty? (tu/query-ra [:distinct {}
                                [::tu/pages '{a #xt/type :i64} []]]))))

  (t/testing "distinct null"
    (t/is (= [{}]
             (tu/query-ra [:distinct {}
                           [::tu/pages '{a #xt/type :null}
                            [[{:a nil}, {:a nil}]
                             [{:a nil}]]]])))))

(t/deftest test-distinct-lists-and-structs-holding-nulls
  (t/is (= [{:a [8 nil]}]
           (tu/query-ra [:distinct {}
                         [::tu/pages [[{:a [8 nil]}, {:a [8 nil]}]]]])))

  (t/is (= [{:a {:c 1}}]
           (tu/query-ra [:distinct {}
                         [::tu/pages [[{:a {:b nil, :c 1}}, {:a {:b nil, :c 1}}]]]])))

  (t/is (= [{:a [{:b [nil 1]}]}]
           (tu/query-ra [:distinct {}
                         [::tu/pages [[{:a [{:b [nil 1]}]}, {:a [{:b [nil 1]}]}]]]]))))
