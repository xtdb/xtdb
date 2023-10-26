(ns xtdb.operator.set-test
  (:require [clojure.test :as t]
            [xtdb.test-util :as tu]))

(t/deftest test-union-all
  (t/is (= {:col-types '{a :i64, b :i64}
            :res [#{{:a 0, :b 15}
                    {:a 12, :b 10}}
                  #{{:a 100, :b 15}}
                  #{{:a 10, :b 1}
                    {:a 15, :b 2}}
                  #{{:a 83, :b 3}}]}
           (-> (tu/query-ra [:union-all
                             [::tu/blocks
                              [[{:a 12 :b 10}, {:a 0 :b 15}]
                               [{:a 100 :b 15}]]]
                             [::tu/blocks
                              [[{:a 10 :b 1}, {:a 15 :b 2}]
                               [{:a 83 :b 3}]]]]
                            {:preserve-blocks? true
                             :with-col-types? true})
               (update :res (partial mapv set)))))

  (t/testing "empty input and output"
    (t/is (empty? (tu/query-ra [:union-all
                                [::tu/blocks '{a :i64} []]
                                [::tu/blocks '{a :i64} []]])))))

(t/deftest test-union-all-empty-left-side-bug
  (t/testing "empty left side"
    (t/is (= [{:a 10} {:a 15}]
             (tu/query-ra
              [:cross-join
               [:table [{}]]
               [:union-all
                [::tu/blocks '{a :i64} [[]]]
                [::tu/blocks [[{:a 10}, {:a 15}]]]]])))))

(t/deftest test-union-all-empty-blocks-both-sides
  (t/testing "empty blocks both side"
    (t/is (= [{:a 10} {:a 15}]
             (tu/query-ra
              [:union-all
               [::tu/blocks '{a :i64} [[] [{:a 10}]]]
               [::tu/blocks '{a :i64} [[] [{:a 15}]]]])))))

(t/deftest test-intersection
  (t/is (= {:col-types '{a :i64, b :i64}
            :res [[{:a 0, :b 15}]]}
           (tu/query-ra [:intersect
                         [::tu/blocks
                          [[{:a 12 :b 10}, {:a 0 :b 15}]
                           [{:a 100 :b 15}]]]
                         [::tu/blocks
                          [[{:a 10 :b 1}, {:a 15 :b 2}]
                           [{:a 0 :b 15}]]]]
                        {:preserve-blocks? true
                         :with-col-types? true})))

  (t/is (= {{:a 1} 2, {:a 2} 2}
           (-> (tu/query-ra [:intersect
                             [::tu/blocks [[{:a 1} {:a 1} {:a 1} {:a 2} {:a 2}]]]
                             [::tu/blocks [[{:a 1} {:a 1} {:a 2} {:a 2} {:a 2}]]]])
               (frequencies)))
        "cardinalities - minimum of the cardinalities in each side")

  (t/testing "empty input and output"
    (t/is (empty? (tu/query-ra [:intersect
                                [::tu/blocks '{a :i64} []]
                                [::tu/blocks '{a :i64} []]])))

    (t/is (empty? (tu/query-ra [:intersect
                                [::tu/blocks '{a :i64} []]
                                [::tu/blocks '{a :i64} [[{:a 10}, {:a 15}]]]])))

    (t/is (empty? (tu/query-ra [:intersect
                                [::tu/blocks [[{:a 10}, {:a 15}]]]
                                [::tu/blocks '{a :i64} []]])))

    (t/is (empty? (tu/query-ra [:intersect
                                [::tu/blocks [[{:a 10}]]]
                                [::tu/blocks [[{:a 20}]]]])))))

(t/deftest test-difference
  (t/is (= {:col-types '{a :i64, b :i64}
            :res [#{{:a 12, :b 10}}
                  #{{:a 100 :b 15}}]}
           (-> (tu/query-ra [:difference
                             [::tu/blocks
                              [[{:a 12 :b 10}, {:a 0 :b 15}]
                               [{:a 100 :b 15}]]]
                             [::tu/blocks
                              [[{:a 10 :b 1}, {:a 15 :b 2}]
                               [{:a 0 :b 15}]]]]
                            {:preserve-blocks? true
                             :with-col-types? true})
               (update :res (partial mapv set)))))

  (t/is (= {{:a 1} 1}
           (-> (tu/query-ra [:difference
                             [::tu/blocks [[{:a 1} {:a 1} {:a 1} {:a 2} {:a 2}]]]
                             [::tu/blocks [[{:a 1} {:a 1} {:a 2} {:a 2} {:a 2}]]]])
               (frequencies)))
        "cardinalities - difference of the cardinalities in each side")

  (t/testing "empty input and output"
    (t/is (empty? (tu/query-ra [:difference
                                [::tu/blocks '{a :i64} []]
                                [::tu/blocks '{a :i64} []]])))

    (t/is (empty? (tu/query-ra [:difference
                                [::tu/blocks '{a :i64} []]
                                [::tu/blocks '{a :i64} [[{:a 10}, {:a 15}]]]])))

    (t/is (= [#{{:a 10} {:a 15}}]
             (->> (tu/query-ra [:difference
                                [::tu/blocks '{a :i64} [[{:a 10}, {:a 15}]]]
                                [::tu/blocks '{a :i64} []]]
                               {:preserve-blocks? true})
                  (mapv set))))))

(t/deftest test-distinct
  (t/is (= [#{{:a 12, :b 10} {:a 0, :b 15}}
            #{{:a 100, :b 15}}
            #{{:a 10, :b 15}}]
           (->> (tu/query-ra [:distinct
                              [::tu/blocks
                               [[{:a 12 :b 10}, {:a 0 :b 15}]
                                [{:a 100 :b 15} {:a 0 :b 15}]
                                [{:a 100 :b 15}]
                                [{:a 10 :b 15} {:a 10 :b 15}]]]]
                             {:preserve-blocks? true})
                (mapv set))))

  (t/testing "already distinct"
    (t/is (= [#{{:a 12, :b 10} {:a 0, :b 15}}
              #{{:a 100, :b 15}}]
             (->> (tu/query-ra [:distinct
                                [::tu/blocks
                                 [[{:a 12 :b 10}, {:a 0 :b 15}]
                                  [{:a 100 :b 15}]]]]
                               {:preserve-blocks? true})
                  (mapv set)))))

  (t/testing "empty input and output"
    (t/is (empty? (tu/query-ra [:distinct
                                [::tu/blocks '{a :i64} []]]))))

  (t/testing "distinct null"
    (t/is (= [{:a nil}]
             (tu/query-ra [:distinct
                           [::tu/blocks '{a :null}
                            [{:a nil}, {:a nil}, {:a nil}]]])))))

(t/deftest test-fixpoint
  (t/is (= [[{:a 0, :b 1}]
            [{:a 1, :b 1}]
            [{:a 2, :b 2}]
            [{:a 3, :b 6}]
            [{:a 4, :b 24}]
            [{:a 5, :b 120}]
            [{:a 6, :b 720}]
            [{:a 7, :b 5040}]
            [{:a 8, :b 40320}]]
           (tu/query-ra [:fixpoint 'R {:incremental? true}
                         [::tu/blocks [[{:a 0 :b 1}]]]
                         '[:select (<= a 8)
                           [:project [{a (+ a 1)}
                                      {b (* (+ a 1) b)}]
                            R]]]
                        {:preserve-blocks? true}))))

(t/deftest first-tuple-in-rhs-is-taken-into-account-test
  (t/is
   (= {:res [{:x1 2}], :col-types '{x1 :i64}}
      (tu/query-ra '[:difference
                     [:table [{x1 1} {x1 2}]]
                     [:table [{x1 1}]]]
                   {:with-col-types? true}))))
