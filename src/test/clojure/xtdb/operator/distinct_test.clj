(ns xtdb.operator.distinct-test
  (:require [clojure.test :as t]
            [xtdb.test-util :as tu]))

(t/use-fixtures :each tu/with-allocator)

(t/deftest test-distinct
  (t/is (= [#{{:a 12, :b 10} {:a 0, :b 15}}
            #{{:a 100, :b 15}}
            #{{:a 10, :b 15}}]
           (->> (tu/query-ra [:distinct
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
             (->> (tu/query-ra [:distinct
                                [::tu/pages
                                 [[{:a 12 :b 10}, {:a 0 :b 15}]
                                  [{:a 100 :b 15}]]]]
                               {:preserve-pages? true})
                  (mapv set)))))

  (t/testing "empty input and output"
    (t/is (empty? (tu/query-ra [:distinct
                                [::tu/pages '{a :i64} []]]))))

  (t/testing "distinct null"
    (t/is (= [{}]
             (tu/query-ra [:distinct
                           [::tu/pages '{a :null}
                            [[{:a nil}, {:a nil}]
                             [{:a nil}]]]])))))
