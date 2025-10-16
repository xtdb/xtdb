(ns xtdb.operator.join-test
  (:require [clojure.set :as set]
            [clojure.spec.alpha :as s]
            [clojure.test :as t :refer [deftest]]
            [next.jdbc :as jdbc]
            [xtdb.api :as xt]
            [xtdb.logical-plan :as lp]
            [xtdb.node :as xtn]
            [xtdb.operator.join :as join]
            [xtdb.test-util :as tu]
            [xtdb.util :as util]))

(t/use-fixtures :each tu/with-allocator)

(t/deftest test-cross-join
  (t/is (= {:res [{{:a 12, :b 10, :c 1} 2,
                   {:a 12, :b 15, :c 2} 2,
                   {:a 0, :b 10, :c 1} 1,
                   {:a 0, :b 15, :c 2} 1}
                  {{:a 100, :b 10, :c 1} 1, {:a 100, :b 15, :c 2} 1}
                  {{:a 12, :b 83, :c 3} 2, {:a 0, :b 83, :c 3} 1}
                  {{:a 100, :b 83, :c 3} 1}]
            :col-types '{a :i64, b :i64, c :i64}}
           (-> (tu/query-ra [:cross-join
                             [::tu/pages
                              [[{:a 12}, {:a 12}, {:a 0}]
                               [{:a 100}]]]
                             [::tu/pages
                              [[{:b 10 :c 1}, {:b 15 :c 2}]
                               [{:b 83 :c 3}]]]]
                            {:with-col-types? true, :preserve-pages? true})
               (update :res #(mapv frequencies %)))))

  (t/is (= {:res []
            :col-types '{a :i64, b :i64}}
           (tu/query-ra [:cross-join
                         [::tu/pages
                          [[{:a 12}, {:a 0}]
                           [{:a 100}]]]
                         [::tu/pages '{b :i64} []]]
                        {:with-col-types? true, :preserve-pages? true}))
        "empty input and output")

  (t/is (= {:res [[{} {} {} {} {} {}]]
            :col-types {}}
           (tu/query-ra [:cross-join
                         [::tu/pages [[{} {} {}]]]
                         [::tu/pages [[{} {}]]]]
                        {:with-col-types? true, :preserve-pages? true}))
        "tables with no cols"))

(t/deftest test-equi-join
  (t/is (= {:res [#{{:a 12, :b 12}}
                  #{{:a 100, :b 100}
                    {:a 0, :b 0}}]
            :col-types '{a :i64, b :i64}}
           (-> (tu/query-ra [:join '[{a b}]
                             [::tu/pages
                              [[{:a 12}, {:a 0}]
                               [{:a 100}]]]
                             [::tu/pages
                              [[{:b 12}, {:b 2}]
                               [{:b 100} {:b 0}]]]]
                            {:with-col-types? true, :preserve-pages? true})
               (update :res (partial mapv set)))))

  (t/is (= {:res [{{:a 12} 2}
                  {{:a 100} 1, {:a 0} 1}]
            :col-types '{a :i64}}
           (-> (tu/query-ra [:join '[{a a}]
                             [::tu/pages
                              [[{:a 12}, {:a 0}]
                               [{:a 12}, {:a 100}]]]
                             [::tu/pages
                              [[{:a 12}, {:a 2}]
                               [{:a 100} {:a 0}]]]]
                            {:with-col-types? true, :preserve-pages? true})
               (update :res (partial mapv frequencies))))
        "same column name")

  (t/is (= {:res []
            :col-types '{a :i64, b :i64}}
           (tu/query-ra [:join '[{a b}]
                         [::tu/pages
                          [[{:a 12}, {:a 0}]
                           [{:a 100}]]]
                         [::tu/pages '{b :i64}
                          []]]
                        {:with-col-types? true, :preserve-pages? true}))
        "empty input")

  (t/is (= {:res []
            :col-types '{a :i64, b :i64, c :i64}}
           (tu/query-ra [:join '[{a b}]
                         [::tu/pages
                          [[{:a 12}, {:a 0}]
                           [{:a 100}]]]
                         [::tu/pages
                          [[{:b 10 :c 1}, {:b 15 :c 2}]
                           [{:b 83 :c 3}]]]]
                        {:with-col-types? true, :preserve-pages? true}))
        "empty output"))

(t/deftest test-equi-join-multi-col
  (->> "multi column"
       (t/is (= [{{:a 12, :b 42, :c 12, :d 42, :e 0} 2}
                 {{:a 12, :b 42, :c 12, :d 42, :e 0} 4
                  {:a 12, :b 42, :c 12, :d 42, :e 1} 2}]
                (->> (tu/query-ra [:join '[{a c} {b d}]
                                   [::tu/pages
                                    [[{:a 12, :b 42}
                                      {:a 12, :b 42}
                                      {:a 11, :b 44}
                                      {:a 10, :b 42}]]]
                                   [::tu/pages
                                    [[{:c 12, :d 42, :e 0}
                                      {:c 12, :d 43, :e 0}
                                      {:c 11, :d 42, :e 0}]
                                     [{:c 12, :d 42, :e 0}
                                      {:c 12, :d 42, :e 0}
                                      {:c 12, :d 42, :e 1}]]]]
                                  {:preserve-pages? true})
                     (mapv frequencies))))))

(t/deftest test-theta-inner-join
  (t/is (= [{{:a 12, :b 44, :c 12, :d 43} 1}]
           (->> (tu/query-ra [:join '[{a c} (> b d)]
                              [::tu/pages
                               [[{:a 12, :b 42}
                                 {:a 12, :b 44}
                                 {:a 10, :b 42}]]]
                              [::tu/pages
                               [[{:c 12, :d 43}
                                 {:c 11, :d 42}]]]]
                             {:preserve-pages? true})
                (mapv frequencies)))))

(t/deftest test-semi-equi-join
  (t/is (= [{{:a 12} 2} {{:a 100} 1}]
           (->> (tu/query-ra [:semi-join '[{a b}]
                              [::tu/pages
                               [[{:a 12}, {:a 12}, {:a 0}]
                                [{:a 100}]]]
                              [::tu/pages
                               [[{:b 12}, {:b 2}]
                                [{:b 100}]]]]
                             {:preserve-pages? true})
                (mapv frequencies))))

  (t/testing "empty input"
    (t/is (empty? (tu/query-ra [:semi-join '[{a b}]
                                [::tu/pages
                                 [[{:a 12}, {:a 0}]
                                  [{:a 100}]]]
                                [::tu/pages '{b :i64} []]])))

    (t/is (empty? (tu/query-ra [:semi-join '[{a b}]
                                [::tu/pages '{a :i64} []]
                                [::tu/pages
                                 [[{:b 12}, {:b 2}]
                                  [{:b 100} {:b 0}]]]])))

    (t/is (empty? (tu/query-ra [:semi-join '[{a b}]
                                [::tu/pages
                                 [[{:a 12}, {:a 0}]
                                  [{:a 100}]]]
                                [::tu/pages '{b :i64}
                                 [[]]]]))))

  (t/testing "nulls"
    (t/is (= []
             (tu/query-ra [:semi-join '[{a b}]
                           [:table [{:a nil}]]
                           [:table [{:b 12}, {:b 2}]]])))

    (t/is (= [{:a 12}]
             (tu/query-ra [:semi-join '[{a b}]
                           [:table [{:a 12}]]
                           [:table [{:b 12}, {:b nil}]]])))

    (t/is (= []
             (tu/query-ra [:semi-join '[{a b}]
                           [:table [{:a 4}]]
                           [:table [{:b 12}, {:b nil}]]]))))

  (t/is (empty? (tu/query-ra [:semi-join '[{a b}]
                              [::tu/pages
                               [[{:a 12}, {:a 0}]
                                [{:a 100}]]]
                              [::tu/pages
                               [[{:b 10 :c 1}, {:b 15 :c 2}]
                                [{:b 83 :c 3}]]]]))
        "empty output"))

(t/deftest test-semi-equi-join-multi-col
  (->> "multi column semi"
       (t/is (= [{{:a 12, :b 42} 2}]
                (->> (tu/query-ra [:semi-join '[{a c} {b d}]
                                   [::tu/pages
                                    [[{:a 12, :b 42}
                                      {:a 12, :b 42}
                                      {:a 11, :b 44}
                                      {:a 10, :b 42}]]]
                                   [::tu/pages
                                    [[{:c 12, :d 42, :e 0}
                                      {:c 12, :d 43, :e 0}
                                      {:c 11, :d 42, :e 0}]
                                     [{:c 12, :d 42, :e 0}
                                      {:c 12, :d 42, :e 0}
                                      {:c 12, :d 42, :e 1}]]]]
                                  {:preserve-pages? true})
                     (mapv frequencies))))))

(t/deftest test-theta-semi-join
  (t/is (= [{{:a 12, :b 44} 1}]
           (->> (tu/query-ra [:semi-join '[{a c} (> b d)]
                              [::tu/pages
                               [[{:a 12, :b 42}
                                 {:a 12, :b 44}
                                 {:a 10, :b 42}]]]
                              [::tu/pages
                               [[{:c 12, :d 43}
                                 {:c 12, :d 43}
                                 {:c 11, :d 42}]]]]
                             {:preserve-pages? true})
                (mapv frequencies)))))

(t/deftest test-mark-equi-join
  (t/is (= {:res [{{:a 12, :m true} 2, {:a 0, :m false} 1} {{:a 100, :m true} 1}]
            :col-types '{a :i64, m [:union #{:null :bool}]}}
           (-> (tu/query-ra [:mark-join '{m [{a b}]}
                             [::tu/pages
                              [[{:a 12}, {:a 12}, {:a 0}]
                               [{:a 100}]]]
                             [::tu/pages
                              [[{:b 12}, {:b 2}]
                               [{:b 100}]]]]
                            {:preserve-pages? true, :with-col-types? true})
               (update :res (partial mapv frequencies)))))

  (t/testing "empty input"
    (t/is (= [[{:a 12, :m false}, {:a 0, :m false}]
              [{:a 100, :m false}, {:m false}]]
             (tu/query-ra [:mark-join '{m [{a b}]}
                           [::tu/pages
                            [[{:a 12}, {:a 0}]
                             [{:a 100}, {:a nil}]]]
                           [::tu/pages '{b :i64} []]]
                          {:preserve-pages? true})))

    (t/is (empty? (tu/query-ra [:mark-join '{m [{a b}]}
                                [::tu/pages '{a :i64} []]
                                [::tu/pages
                                 [[{:b 12}, {:b 2}]
                                  [{:b 100} {:b 0}]]]]))))

  (t/is (= [[{:a 12, :m false}, {:a 0, :m false}]
            [{:a 100, :m false}]]
           (tu/query-ra [:mark-join '{m [{a b}]}
                         [::tu/pages
                          [[{:a 12}, {:a 0}]
                           [{:a 100}]]]
                         [::tu/pages
                          [[{:b 10 :c 1}, {:b 15 :c 2}]
                           [{:b 83 :c 3}]]]]
                        {:preserve-pages? true}))
        "no matches"))

(t/deftest test-mark-join-nils
  (t/is (= [{:a 12, :m true}, {:a 14, :m false}, {}]
           (tu/query-ra [:mark-join '{m [{a b}]}
                         [::tu/pages
                          [[{:a 12}, {:a 14}, {}]]]
                         [::tu/pages
                          [[{:b 12}]]]])))

  (t/is (= [{:a 12, :m true}, {:a 14} {}]
           (tu/query-ra [:mark-join '{m [{a b}]}
                         [::tu/pages
                          [[{:a 12}, {:a 14}, {:a nil}]]]
                         [::tu/pages
                          [[{:b 12}, {:b nil}]]]]))))

(t/deftest test-mark-join-theta
  (t/is (= [{:a 12, :m true}, {:a 14, :m true}, {}]
           (tu/query-ra [:mark-join '{m [(>= a b)]}
                         [::tu/pages
                          [[{:a 12}, {:a 14}, {:a nil}]]]
                         [::tu/pages
                          [[{:b 12}]]]])))

  (t/is (= [{:a 12}, {:a 14, :m true} {}]
           (tu/query-ra [:mark-join '{m [(> a b)]}
                         [::tu/pages
                          [[{:a 12}, {:a 14}, {:a nil}]]]
                         [::tu/pages
                          [[{:b 12}, {:b nil}]]]]))))

(t/deftest test-exists-as-mark-join
  ;;These are arguably not tests of mark-join functionality itself,
  ;;but prove that exist/not-exists can be expressed via mark-join (and a negation)
  (t/testing "exists"
    (t/is (= [{:a 12, :m true}, {:a 14, :m true}, {:m true}]
             (tu/query-ra [:mark-join '{m [true]}
                           [::tu/pages
                            [[{:a 12}, {:a 14}, {:a nil}]]]
                           [::tu/pages
                            [[{:b 1} {:c 2}]]]])))

    (t/is (= [{:a 12, :m true}, {:a 14, :m true}, {:m true}]
             (tu/query-ra [:mark-join '{m [true]}
                           [::tu/pages
                            [[{:a 12}, {:a 14}, {:a nil}]]]
                           [::tu/pages
                            [[{:a nil}]]]])))

    (t/is (= [{:a 12, :m false}, {:a 14, :m false}, {:m false}]
             (tu/query-ra [:mark-join '{m [true]}
                           [::tu/pages
                            [[{:a 12}, {:a 14}, {:a nil}]]]
                           [::tu/pages
                            [[]]]]))))
  (t/testing "not-exists"
    (t/is (= [{:a 12, :m false}, {:a 14, :m false}, {:m false}]
             (tu/query-ra '[:project [a {m (not m)}]
                            [:mark-join {m [true]}
                             [::tu/pages
                              [[{:a 12}, {:a 14}, {:a nil}]]]
                             [::tu/pages
                              [[{:b 1} {:c 2}]]]]])))

    (t/is (= [{:a 12, :m false}, {:a 14, :m false}, {:m false}]
             (tu/query-ra '[:project [a {m (not m)}]
                            [:mark-join {m [true]}
                             [::tu/pages
                              [[{:a 12}, {:a 14}, {:a nil}]]]
                             [::tu/pages
                              [[{:a nil}]]]]])))

    (t/is (= [{:a 12, :m true}, {:a 14, :m true}, {:m true}]
             (tu/query-ra '[:project [a {m (not m)}]
                            [:mark-join {m [true]}
                             [::tu/pages
                              [[{:a 12}, {:a 14}, {:a nil}]]]
                             [::tu/pages
                              [[]]]]])))))

(t/deftest test-left-equi-join
  (t/is (= {:res [{{:a 12, :b 12, :c 2} 1, {:a 12, :b 12, :c 0} 1, {:a 0} 1}
                  {{:a 12, :b 12, :c 2} 1, {:a 100, :b 100, :c 3} 1, {:a 12, :b 12, :c 0} 1}]
            :col-types '{a :i64, b [:union #{:null :i64}], c [:union #{:null :i64}]}}
           (-> (tu/query-ra [:left-outer-join '[{a b}]
                             [::tu/pages
                              [[{:a 12}, {:a 0}]
                               [{:a 12}, {:a 100}]]]
                             [::tu/pages
                              [[{:b 12, :c 0}, {:b 2, :c 1}]
                               [{:b 12, :c 2}, {:b 100, :c 3}]]]]
                            {:preserve-pages? true, :with-col-types? true})
               (update :res (partial mapv frequencies)))))

  (t/testing "empty input"
    (t/is (= {:res [#{{:a 12}, {:a 0}}
                    #{{:a 100}}]
              :col-types '{a :i64, b [:union #{:null :i64}]}}
             (-> (tu/query-ra [:left-outer-join '[{a b}]
                               [::tu/pages
                                [[{:a 12}, {:a 0}]
                                 [{:a 100}]]]
                               [::tu/pages '{b :i64} []]]
                              {:preserve-pages? true, :with-col-types? true})
                 (update :res (partial mapv set)))))

    (t/is (= {:res []
              :col-types '{a :i64, b [:union #{:null :i64}]}}
             (tu/query-ra [:left-outer-join '[{a b}]
                           [::tu/pages '{a :i64} []]
                           [::tu/pages
                            [[{:b 12}, {:b 2}]
                             [{:b 100} {:b 0}]]]]
                          {:preserve-pages? true, :with-col-types? true})))

    (t/is (= {:res [#{{:a 12}, {:a 0}}
                    #{{:a 100}}]
              :col-types '{a :i64, b [:union #{:null :i64}]}}
             (-> (tu/query-ra [:left-outer-join '[{a b}]
                               [::tu/pages
                                [[{:a 12}, {:a 0}]
                                 [{:a 100}]]]
                               [::tu/pages '{b :i64}
                                [[]]]]
                              {:preserve-pages? true, :with-col-types? true})
                 (update :res (partial mapv set)))))))

(t/deftest test-left-equi-join-multi-col
  (->> "multi column left"
       (t/is (= [{{:a 11, :b 44} 1
                  {:a 10, :b 42} 1
                  {:a 12, :b 42, :c 12, :d 42, :e 0} 6
                  {:a 12, :b 42, :c 12, :d 42, :e 1} 2}]
                (->> (tu/query-ra [:left-outer-join '[{a c} {b d}]
                                   [::tu/pages
                                    [[{:a 12, :b 42}
                                      {:a 12, :b 42}
                                      {:a 11, :b 44}
                                      {:a 10, :b 42}]]]
                                   [::tu/pages
                                    [[{:c 12, :d 42, :e 0}
                                      {:c 12, :d 43, :e 0}
                                      {:c 11, :d 42, :e 0}]
                                     [{:c 12, :d 42, :e 0}
                                      {:c 12, :d 42, :e 0}
                                      {:c 12, :d 42, :e 1}]]]]
                                  {:preserve-pages? true})
                     (mapv frequencies))))))

(t/deftest test-left-theta-join
  (let [left [::tu/pages
              [[{:a 12, :b 42}
                {:a 12, :b 44}
                {:a 10, :b 42}
                {:a 10, :b 42}]]]
        right [::tu/pages
               [[{:c 12, :d 43}
                 {:c 11, :d 42}]]]]

    (t/is (= [{{:a 12, :b 42} 1
               {:a 12, :b 44, :c 12, :d 43} 1
               {:a 10, :b 42} 2}]
             (->> (tu/query-ra [:left-outer-join '[{a c} (> b d)] left right]
                               {:preserve-pages? true})
                  (mapv frequencies))))

    (t/is (= [{{:a 12, :b 42} 1
               {:a 12, :b 44} 1
               {:a 10, :b 42} 2}]
             (->> (tu/query-ra [:left-outer-join '[{a c} (> b d) (= c -1)] left right]
                               {:preserve-pages? true})
                  (mapv frequencies))))

    (t/is (= [{{:a 12, :b 42} 1
               {:a 12, :b 44} 1
               {:a 10, :b 42} 2}]
             (->> (tu/query-ra [:left-outer-join '[{a c} (and (= c -1) (> b d))] left right]
                               {:preserve-pages? true})
                  (mapv frequencies))))

    (t/is (= [{{:a 12, :b 42} 1
               {:a 12, :b 44} 1
               {:a 10, :b 42} 2}]
             (->> (tu/query-ra [:left-outer-join '[{a c} {b d} (= c -1)] left right]
                               {:preserve-pages? true})
                  (mapv frequencies))))))

(t/deftest test-full-outer-join
  (t/testing "missing on both sides"
    (t/is (= {:res [{{:a 12, :b 12, :c 0} 2, {:b 2, :c 1} 1}
                    {{:a 12, :b 12, :c 2} 2, {:a 100, :b 100, :c 3} 1}
                    {{:a 0} 1}]
              :col-types '{a [:union #{:null :i64}], b [:union #{:null :i64}], c [:union #{:null :i64}]}}
             (-> (tu/query-ra [:full-outer-join '[{a b}]
                               [::tu/pages
                                [[{:a 12}, {:a 0}]
                                 [{:a 12}, {:a 100}]]]
                               [::tu/pages
                                [[{:b 12, :c 0}, {:b 2, :c 1}]
                                 [{:b 12, :c 2}, {:b 100, :c 3}]]]]
                              {:preserve-pages? true, :with-col-types? true})
                 (update :res (partial mapv frequencies)))))

    (t/is (= {:res [{{:a 12, :b 12, :c 0} 2, {:a 12, :b 12, :c 2} 2, {:b 2, :c 1} 1}
                    {{:a 100, :b 100, :c 3} 1}
                    {{:a 0} 1}]
              :col-types '{a [:union #{:null :i64}], b [:union #{:null :i64}], c [:union #{:null :i64}]}}
             (-> (tu/query-ra [:full-outer-join '[{a b}]
                               [::tu/pages
                                [[{:a 12}, {:a 0}]
                                 [{:a 12}, {:a 100}]]]
                               [::tu/pages
                                [[{:b 12, :c 0}, {:b 12, :c 2}, {:b 2, :c 1}]
                                 [{:b 100, :c 3}]]]]
                              {:preserve-pages? true, :with-col-types? true})
                 (update :res (partial mapv frequencies))))))

  (t/testing "all matched"
    (t/is (= {:res [{{:a 12, :b 12, :c 0} 2, {:a 100, :b 100, :c 3} 1}
                    {{:a 12, :b 12, :c 2} 2}]
              :col-types '{a [:union #{:null :i64}], b [:union #{:null :i64}], c [:union #{:null :i64}]}}
             (-> (tu/query-ra [:full-outer-join '[{a b}]
                               [::tu/pages
                                [[{:a 12}]
                                 [{:a 12}, {:a 100}]]]
                               [::tu/pages
                                [[{:b 12, :c 0}, {:b 100, :c 3}]
                                 [{:b 12, :c 2}]]]]
                              {:preserve-pages? true, :with-col-types? true})
                 (update :res (partial mapv frequencies)))))

    (t/is (= {:res [{{:a 12, :b 12, :c 0} 2, {:a 12, :b 12, :c 2} 2}
                    {{:a 100, :b 100, :c 3} 1}]
              :col-types '{a [:union #{:null :i64}], b [:union #{:null :i64}], c [:union #{:null :i64}]}}
             (-> (tu/query-ra [:full-outer-join '[{a b}]
                               [::tu/pages
                                [[{:a 12}]
                                 [{:a 12}, {:a 100}]]]
                               [::tu/pages
                                [[{:b 12, :c 0}, {:b 12, :c 2}]
                                 [{:b 100, :c 3}]]]]
                              {:preserve-pages? true, :with-col-types? true})
                 (update :res (partial mapv frequencies))))))

  (t/testing "empty input"
    (t/is (= {:res [{{:a 0} 1, {:a 100} 1, {:a 12} 1}]
              :col-types '{a [:union #{:null :i64}], b [:union #{:null :i64}]}}
             (-> (tu/query-ra [:full-outer-join '[{a b}]
                               [::tu/pages
                                [[{:a 12}, {:a 0}]
                                 [{:a 100}]]]
                               [::tu/pages '{b :i64} []]]
                              {:preserve-pages? true, :with-col-types? true})
                 (update :res (partial mapv frequencies)))))

    (t/is (= {:res [{{:b 12} 1, {:b 2} 1}
                    {{:b 100} 1, {:b 0} 1}]
              :col-types '{a [:union #{:null :i64}], b [:union #{:null :i64}]}}
             (-> (tu/query-ra [:full-outer-join '[{a b}]
                               [::tu/pages '{a :i64} []]
                               [::tu/pages
                                [[{:b 12}, {:b 2}]
                                 [{:b 100} {:b 0}]]]]
                              {:preserve-pages? true, :with-col-types? true})
                 (update :res (partial mapv frequencies)))))))

(t/deftest test-full-outer-equi-join-multi-col
  (->> "multi column full outer"
       (t/is (= [{{:a 12 :b 42 :c 12 :d 42 :e 0} 2
                  {:c 11 :d 42 :e 0} 1
                  {:c 12 :d 43 :e 0} 1}
                 {{:a 12 :b 42 :c 12 :d 42 :e 0} 4
                  {:a 12 :b 42 :c 12 :d 42 :e 1} 2}
                 {{:a 10 :b 42} 1
                  {:a 11 :b 44} 1}]
                (->> (tu/query-ra [:full-outer-join '[{a c} {b d}]
                                   [::tu/pages
                                    [[{:a 12, :b 42}
                                      {:a 12, :b 42}
                                      {:a 11, :b 44}
                                      {:a 10, :b 42}]]]
                                   [::tu/pages
                                    [[{:c 12, :d 42, :e 0}
                                      {:c 12, :d 43, :e 0}
                                      {:c 11, :d 42, :e 0}]
                                     [{:c 12, :d 42, :e 0}
                                      {:c 12, :d 42, :e 0}
                                      {:c 12, :d 42, :e 1}]]]]
                                  {:preserve-pages? true})
                     (mapv frequencies))))))

(t/deftest test-full-outer-join-theta
  (let [left [::tu/pages
              [[{:a 12, :b 42}
                {:a 12, :b 44}
                {:a 10, :b 42}
                {:a 10, :b 42}]]]
        right [::tu/pages
               [[{:c 12, :d 43}
                 {:c 11, :d 42}]]]]

    (t/is (= {{:a 12, :b 42, :c 12, :d 43} 1
              {:a 12, :b 44} 1
              {:a 10, :b 42} 2
              {:c 11, :d 42} 1}

             (->> (tu/query-ra [:full-outer-join '[{a c} (not (= b 44))] left right])
                  (frequencies))))

    (t/is (= {{:a 12, :b 42} 1
              {:a 12, :b 44} 1
              {:a 10, :b 42} 2
              {:c 12, :d 43} 1
              {:c 11, :d 42} 1}
             (->> (tu/query-ra [:full-outer-join '[{a c} false] left right])
                  (frequencies))))))

(t/deftest test-anti-equi-join
  (t/is (= {:res [{{:a 0} 2}]
            :col-types '{a :i64}}
           (-> (tu/query-ra [:anti-join '[{a b}]
                             [::tu/pages
                              [[{:a 12}, {:a 0}, {:a 0}]
                               [{:a 100}]]]
                             [::tu/pages
                              [[{:b 12}, {:b 2}]
                               [{:b 100}]]]]
                            {:preserve-pages? true, :with-col-types? true})
               (update :res (partial mapv frequencies)))))

  (t/testing "empty input"
    (t/is (empty? (:res (tu/query-ra [:anti-join '[{a b}]
                                      [::tu/pages '{a :i64} []]
                                      [::tu/pages
                                       [[{:b 12}, {:b 2}]
                                        [{:b 100}]]]]))))

    (t/is (= [#{{:a 12}, {:a 0}}
              #{{:a 100}}]
             (->> (tu/query-ra [:anti-join '[{a b}]
                                [::tu/pages
                                 [[{:a 12}, {:a 0}]
                                  [{:a 100}]]]
                                [::tu/pages '{b :i64} []]]
                               {:preserve-pages? true})
                  (mapv set)))))

  (t/testing "nulls"
    (t/is (= []
             (tu/query-ra [:anti-join '[{a b}]
                           [:table [{:a nil}]]
                           [:table [{:b 12}, {:b 2}]]])))

    (t/is (= []
             (tu/query-ra [:anti-join '[{a b}]
                           [:table [{:a 12}]]
                           [:table [{:b 12}, {:b nil}]]])))

    (t/is (= []
             (tu/query-ra [:anti-join '[{a b}]
                           [:table [{:a 4}]]
                           [:table [{:b 12}, {:b nil}]]]))))

  (t/is (empty? (tu/query-ra [:anti-join '[{a b}]
                              [::tu/pages
                               [[{:a 12}, {:a 2}]
                                [{:a 100}]]]
                              [::tu/pages
                               [[{:b 12}, {:b 2}]
                                [{:b 100}]]]]))
        "empty output"))

(t/deftest test-anti-equi-join-multi-col
  (->> "multi column anti"
       (t/is (= [{{:a 10 :b 42} 1
                  {:a 11 :b 44} 1}]
                (->> (tu/query-ra [:anti-join '[{a c} {b d}]
                                   [::tu/pages
                                    [[{:a 12, :b 42}
                                      {:a 12, :b 42}
                                      {:a 11, :b 44}
                                      {:a 10, :b 42}]]]
                                   [::tu/pages
                                    [[{:c 12, :d 42, :e 0}
                                      {:c 12, :d 43, :e 0}
                                      {:c 11, :d 42, :e 0}]
                                     [{:c 12, :d 42, :e 0}
                                      {:c 12, :d 42, :e 0}
                                      {:c 12, :d 42, :e 1}]]]]
                                  {:preserve-pages? true})
                     (mapv frequencies))))))

(t/deftest test-anti-join-theta
  (let [left [::tu/pages
              [[{:a 12, :b 42}
                {:a 12, :b 44}
                {:a 10, :b 42}
                {:a 10, :b 42}]]]
        right [::tu/pages
               [[{:c 12, :d 43}
                 {:c 11, :d 42}]]]]

    (t/is (= {{:a 12, :b 44} 1
              {:a 10, :b 42} 2}

             (->> (tu/query-ra [:anti-join '[{a c} (not (= b 44))] left right])
                  (frequencies))))

    (t/is (= {{:a 12, :b 42} 1
              {:a 12, :b 44} 1
              {:a 10, :b 42} 2}
             (->> (tu/query-ra [:anti-join '[{a c} false] left right])
                  (frequencies))))))

(t/deftest test-join-on-true
  (letfn [(run-join [left? right? theta-expr]
            (->> (tu/query-ra [:join [theta-expr]
                               [::tu/pages '{a :i64}
                                (if left? [[{:a 12}, {:a 0}]] [])]
                               [::tu/pages '{b :i64}
                                (if right? [[{:b 12}, {:b 2}]] [])]]
                              {:preserve-pages? true})
                 (mapv frequencies)))]
    (t/is (= []
             (run-join true false nil)))

    (t/is (= []
             (run-join true false true)))

    (t/is (= []
             (run-join true true false)))

    (t/is (= []
             (run-join true true nil)))

    (t/is (= [{{:a 12, :b 12} 1,
               {:a 12, :b 2} 1,
               {:a 0, :b 12} 1,
               {:a 0, :b 2} 1}]
             (run-join true true true)))))

(t/deftest test-loj-on-true
  (letfn [(run-loj [left? right? theta-expr]
            (->> (tu/query-ra [:left-outer-join [theta-expr]
                               [::tu/pages '{a :i64}
                                (if left? [[{:a 12}, {:a 0}]] [])]
                               [::tu/pages '{b :i64}
                                (if right? [[{:b 12}, {:b 2}]] [])]]
                              {:preserve-pages? true})
                 (mapv frequencies)))]
    (t/is (= [{{:a 12} 1,
               {:a 0} 1}]
             (run-loj true false nil)))

    (t/is (= [{{:a 12} 1,
               {:a 0} 1}]
             (run-loj true false true)))

    (t/is (= []
             (run-loj false true nil)))

    (t/is (= []
             (run-loj false true true)))

    (t/is (= [{{:a 12} 1,
               {:a 0} 1}]
             (run-loj true true false)))

    (t/is (= [{{:a 12} 1,
               {:a 0} 1}]
             (run-loj true true nil)))

    (t/is (= [{{:a 12, :b 12} 1,
               {:a 12, :b 2} 1,
               {:a 0, :b 12} 1,
               {:a 0, :b 2} 1}]
             (run-loj true true true)))))

(t/deftest test-foj-on-true
  (letfn [(run-foj [left? right? theta-expr]
            (->> (tu/query-ra [:full-outer-join [theta-expr]
                               [::tu/pages '{a :i64}
                                (if left? [[{:a 12}, {:a 0}]] [])]
                               [::tu/pages '{b :i64}
                                (if right? [[{:b 12}, {:b 2}]] [])]]
                              {:preserve-pages? true})
                 (mapv frequencies)))]
    (t/is (= [{{:a 12} 1,
               {:a 0} 1}]
             (run-foj true false nil)))

    (t/is (= [{{:a 12} 1,
               {:a 0} 1}]
             (run-foj true false true)))

    (t/is (= [{{:b 12} 1,
               {:b 2} 1}]
             (run-foj false true nil)))

    (t/is (= [{{:b 12} 1,
               {:b 2} 1}]
             (run-foj false true true)))

    (t/is (= [{{:b 12} 1,
               {:b 2} 1}
              {{:a 12} 1,
               {:a 0} 1}]
             (run-foj true true false)))

    (t/is (= [{{:b 12} 1,
               {:b 2} 1}
              {{:a 12} 1,
               {:a 0} 1}]
             (run-foj true true nil)))

    (t/is (= [{{:a 12, :b 12} 1,
               {:a 12, :b 2} 1,
               {:a 0, :b 12} 1,
               {:a 0, :b 2} 1}]
             (run-foj true true true)))))

(t/deftest test-semi-join-on-true
  (letfn [(run-semi [left? right? theta-expr]
            (->> (tu/query-ra [:semi-join [theta-expr]
                               [::tu/pages '{a :i64}
                                (if left? [[{:a 12}, {:a 0}]] [])]

                               [::tu/pages '{b :i64}
                                (if right? [[{:b 12}, {:b 2}]] [])]]
                              {:preserve-pages? true})
                 (mapv frequencies)))]
    (t/is (= [] (run-semi true false nil)))
    (t/is (= [] (run-semi true false true)))
    (t/is (= [] (run-semi false true nil)))
    (t/is (= [] (run-semi false true true)))
    (t/is (= [] (run-semi true true false)))

    (t/is (= [] (run-semi true true nil)))
    (t/is (= [{{:a 12} 1, {:a 0} 1}] (run-semi true true true)))))

(t/deftest test-anti-join-on-true
  (letfn [(run-anti [left? right? theta-expr]
            (->> (tu/query-ra [:anti-join [theta-expr]
                               [::tu/pages '{a :i64}
                                (if left? [[{:a 12}, {:a 0}]] [])]
                               [::tu/pages '{b :i64}
                                (if right? [[{:b 12}, {:b 2}]] [])]]
                              {:preserve-pages? true})
                 (mapv frequencies)))]
    (t/is (= [{{:a 12} 1, {:a 0} 1}] (run-anti true false nil)))
    (t/is (= [{{:a 12} 1, {:a 0} 1}] (run-anti true false true)))

    (t/is (= [] (run-anti false true nil)))
    (t/is (= [] (run-anti false true true)))

    (t/is (= [{{:a 12} 1, {:a 0} 1}] (run-anti true true false)))
    (t/is (= [] (run-anti true true nil)))

    (t/is (= [] (run-anti true true true)))))

(t/deftest test-equi-join-expr
  (letfn [(test-equi-join [join-specs left-vals right-vals]
            (tu/query-ra [:join join-specs
                          [::tu/pages [left-vals]]
                          [::tu/pages [right-vals]]]
                         {}))]
    (t/is (= [{:a 42, :b 42}]
             (test-equi-join '[{(+ a 1) (+ b 1)}]
                             [{:a 42}] [{:b 42} {:b 43}])))

    (t/is (= []
             (test-equi-join '[{(+ a 1) (+ b 1)}]
                             [{:a 42}] [{:b 43} {:b 44}])))
    (t/testing "either side can be a plain column ref"
      (t/is (= [{:a 42, :b 43}] (test-equi-join '[{a (- b 1)}] [{:a 42}] [{:b 43} {:b 44}])))
      (t/is (= [{:a 42, :b 43}] (test-equi-join '[{(+ a 1) b}] [{:a 42}] [{:b 43} {:b 44}]))))))

(t/deftest test-single-join
  (t/is (= [{:x 0, :y 0, :z 0} {:x 0, :y 1, :z 0} {:x 1, :y 2, :z 1}]
           (tu/query-ra '[:single-join [{x z}]
                          [::tu/pages [[{:x 0, :y 0}, {:x 0, :y 1}, {:x 1, :y 2}]]]
                          [::tu/pages [[{:z 0}, {:z 1}]]]])))

  (t/is (thrown-with-msg? RuntimeException
                          #"cardinality violation"
                          (tu/query-ra '[:single-join [{x y}]
                                         [::tu/pages [[{:x 0}, {:x 0}, {:x 1}, {:x 2}]]]
                                         [::tu/pages [[{:y 0}, {:y 1}, {:y 1}]]]]))
        "throws on cardinality > 1")

  (t/testing "empty input"
    (t/is (= []
             (tu/query-ra '[:single-join [{x y}]
                            [::tu/pages {x :i64} []]
                            [::tu/pages [[{:y 0}, {:y 1}, {:y 1}]]]])))

    (t/is (= [{:x 0}]
             (tu/query-ra '[:single-join [{x y}]
                            [::tu/pages [[{:x 0}]]]
                            [::tu/pages {y :i64} []]])))))

(t/deftest test-mega-join
  (t/is (= [{:x1 1, :x2 1, :x4 3, :x3 1}]
           (tu/query-ra
            '[:mega-join
              [{x1 x2}
               (> (+ x1 10) (+ (+ x3 2) x4))
               {x1 x3}]
              [[::tu/pages [[{:x1 1}]]]
               [::tu/pages [[{:x2 1}]]]
               [::tu/pages [[{:x3 1 :x4 3}]]]]])))

  (t/testing "disconnected relations/sub-graphs"
    (t/is (= [{:x1 1, :x2 1, :x4 3, :x3 1, :x5 5, :x6 10, :x7 10, :x8 8}]
             (tu/query-ra
              '[:mega-join
                [{x1 x2}
                 (> (+ x1 10) (+ (+ x3 2) x4))
                 {x1 x3}
                 {x6 x7}]
                [[::tu/pages [[{:x1 1}]]]
                 [::tu/pages [[{:x2 1}]]]
                 [::tu/pages [[{:x3 1 :x4 3}]]]
                 [::tu/pages [[{:x5 5}]]]
                 [::tu/pages [[{:x6 10}]]]
                 [::tu/pages [[{:x7 10}]]]
                 [::tu/pages [[{:x8 8}]]]]]))))

  (t/testing "params"
    (t/is (= [{:x1 1, :x2 1, :x4 3, :x3 1, :x5 2}]
             (tu/query-ra
              '[:apply :cross-join {x5 ?x2}
                [::tu/pages [[{:x5 2}]]]
                [:mega-join
                 [{x1 (- ?x2 1)}
                  (> (+ x1 10) (+ (+ x3 2) x4))
                  {x1 x3}]
                 [[::tu/pages [[{:x1 1}]]]
                  [::tu/pages [[{:x2 1}]]]
                  [::tu/pages [[{:x3 1 :x4 3}]]]]]])))

    (t/is (= [{:baz 10, :bar 1}]
             (tu/query-ra
              '[:mega-join
                [{bar (- ?foo 0)}]
                [[:table [{:bar 1}]]
                 [:table [{:baz 10}]]]]
              {:args {:foo 1}}))))

  (t/testing "empty input"
    (t/is (= []
             (tu/query-ra
              '[:mega-join
                [{x1 x2}
                 (> (+ x1 10) (+ (+ x3 2) x4))
                 {x1 x3}]
                [[::tu/pages {x1 :i64} []]
                 [::tu/pages [[{:x2 1}]]]
                 [::tu/pages [[{:x3 1 :x4 3}]]]]]))))

  (t/testing "symbol to symbol equi joins do not require columns to begin with x"
    (t/is (= [{:person 1, :bar "woo", :name 1, :foo 1}
              {:person 1, :bar "yay", :name 1, :foo 1}]
             (tu/query-ra
              '[:mega-join
                [{name person}
                 {person foo}]
                [[:table [{:name 1}]]
                 [:table [{:person 1}]]
                 [:table [{:foo 1 :bar "woo"}
                          {:foo 1 :bar "yay"}]]]])))

    (t/testing "Unused join conditions are added as conditions to the outermost join"
      ;;currently mega-join starts with the smallest (in terms of row count) relation
      ;;and will only add connected relations to that sub-graph. In this case we would
      ;;need to cross join the first 2 tables before joining the third.
      ;;
      ;;Instead we produce unconnected subgraphs (in this case 3) and then add the
      ;;join conditions to the outermost join.
      (t/is (= '[[0] [1] ([:pred-expr (= (+ name person) foo)]) [2]]
               (:join-order
                (lp/emit-expr
                 (s/conform ::lp/logical-plan
                            '[:mega-join
                              [(= (+ name person) foo)]
                              [[:table [{:name 1}]]
                               [:table [{:person 1}]]
                               [:table [{:foo 2 :bar "woo"}
                                        {:foo 1 :bar "yay"}]]]])
                 {}))))

      (t/is (= [{:person 1, :bar "woo", :name 1, :foo 2}]
               (tu/query-ra
                '[:mega-join
                  [(= (+ name person) foo)]
                  [[:table [{:name 1}]]
                   [:table [{:person 1}]]
                   [:table [{:foo 2 :bar "woo"}
                            {:foo 1 :bar "yay"}]]]]))))))

(t/deftest test-adjust-to-equi-condition
  (t/is
   (= '[:equi-condition {t0_n t1_n}]
      (join/adjust-to-equi-condition
       '{:cols #{t1_n t0_n},
         :condition [:pred-expr (= t1_n t0_n)],
         :condition-id 0,
         :cols-from-current-rel #{t0_n},
         :other-cols #{t1_n},
         :all-cols-present? true}))))

(deftest test-mega-join-join-order
  (t/is
   (=
    '[[2 [[:equi-condition {foo bar}]]
       3 [[:equi-condition {foo baz}]]
       0 [[:equi-condition {bar biff}]]
       1]]
    (:join-order
     (lp/emit-expr
      '{:op :mega-join,
        :conditions
        [[:equi-condition {bar biff}]
         [:equi-condition {foo baz}]
         [:equi-condition {foo bar}]],
        :relations
        [{:op ::tu/pages,
          :stats {:row-count 3}
          :pages [[{:baz 1}]]}
         {:op ::tu/pages,
          :stats nil
          :pages [[{:biff 1}]]}
         {:op ::tu/pages,
          :stats {:row-count 1}
          :pages [[{:foo 1}]]}
         {:op ::tu/pages,
          :stats {:row-count 2}
          :pages [[{:bar 1}]]}]}
      {}))))

  (t/testing "disconnected-sub-graphs"
    (t/is
     (=
      '[[1 [[:equi-condition {foo baz}]] 0] [2]]
      (:join-order
       (lp/emit-expr
        '{:op :mega-join,
          :conditions
          [[:equi-condition {foo baz}]],
          :relations
          [{:op ::tu/pages,
            :stats {:row-count 3}
            :pages [[{:baz 1}]]}
           {:op ::tu/pages,
            :stats {:row-count 1}
            :pages [[{:foo 1}]]}
           {:op ::tu/pages,
            :stats {:row-count 2}
            :pages [[{:bar 1}]]}]}
        {}))))))

(t/deftest can-join-on-lists-491
  (t/is (= [{:id [1 2], :foo 0, :bar 5}]
           (tu/query-ra
            '[:join [{id id}]
              [::tu/pages [[{:id [1 2], :foo 0}
                            {:id [1 3], :foo 1}]]]
              [::tu/pages [[{:id [1 2], :bar 5}]]]]))))

(t/deftest can-join-on-structs-491
  (t/is (= [{:id {:a 1, :b 2}, :foo 0, :bar 5}]
           (tu/query-ra
            '[:join [{id id}]
              [::tu/pages [[{:id {:a 1, :b 2}, :foo 0}
                            {:id {:a 1, :b 3}, :foo 1}]]]
              [::tu/pages [[{:id {:a 1, :b 2}, :bar 5}]]]]))))

(t/deftest test-non-joining-join-conditions
  (t/testing "non joining join conditions are added at the earliest possible point"
    (let [plan '[:mega-join
                 [(= a b) (= b 1)]
                 [[:table [{:a 1}]]
                  [:table [{:b 1}]]]]]
      (t/is (= '[[0 [[:equi-condition {a b}] [:pred-expr (= b 1)]] 1]]
               (:join-order (lp/emit-expr (s/conform ::lp/logical-plan plan) {}))))
      (t/is (= [{:a 1, :b 1}] (tu/query-ra plan))))))

(t/deftest test-join-equi-literal
  ;;FIXME expr->columns doesn't handle scalar literals in equi-join syntax
  #_(t/is (= [{:bar 5, :id 4, :foo 0}]
             (tu/query-ra
              '[:mega-join [{id 4}]
                [[::tu/pages [[{:id 4, :foo 0}]]]
                 [::tu/pages [[{:bar 5}]]]]]))))

(t/deftest test-row-table-mega-join-order
  (t/testing "row table mega-join order"
    (let [plan '[:mega-join
                 [{bar biff} {foo baz} {foo bar}]
                 [[:table [{:baz 1} {:baz 2} {:baz 2}]]
                  [:table [{:biff 1} {:biff 2} {:biff 3} {:biff 4}]]
                  [:table [{:foo 1}]]
                  [:table [{:bar 1} {:bar 2}]]]]]
      (t/is (= '[[2 [[:equi-condition {foo bar}]]
                  3 [[:equi-condition {foo baz}]]
                  0 [[:equi-condition {bar biff}]]
                  1]]
               (:join-order (lp/emit-expr (s/conform ::lp/logical-plan plan) {})))))))

(t/deftest full-outer-join-with-polymorphic-types
  (t/testing "polymorphic types on both sides - all matching"
    (t/is (= {:res [[{:a 1 :b 1} {:a "2" :b "2"}]],
              :col-types
              '{a [:union #{:utf8 :null :i64}]
                b [:union #{:utf8 :null :i64}]}}
             (tu/query-ra [:full-outer-join '[{a b}]
                           [::tu/pages
                            [[{:a 1}, {:a "2"}]]]
                           [::tu/pages
                            [[{:b 1}, {:b "2"}]]]]
                          {:preserve-pages? true, :with-col-types? true}))))

  (t/testing "polymorphic types on both sides - some unmatching"
    (t/is (= {:res [[{:b 1, :a 1} {:b "3"}] [{:a "2"}]],
              :col-types
              '{a [:union #{:utf8 :null :i64}]
                b [:union #{:utf8 :null :i64}]}}
             (tu/query-ra [:full-outer-join '[{a b}]
                           [::tu/pages
                            [[{:a 1}, {:a "2"}]]]
                           [::tu/pages
                            [[{:b 1}, {:b "3"}]]]]
                          {:preserve-pages? true, :with-col-types? true}))))

  (t/testing "polymorphic types on both sides - all unmatching"
    (t/is (= {:res [[{:b 3} {:b "4"}] [{:a 1} {:a "2"}]],
              :col-types
              '{a [:union #{:utf8 :null :i64}]
                b [:union #{:utf8 :null :i64}]}}
             (tu/query-ra [:full-outer-join '[{a b}]
                           [::tu/pages
                            [[{:a 1}, {:a "2"}]]]
                           [::tu/pages
                            [[{:b 3}, {:b "4"}]]]]
                          {:preserve-pages? true, :with-col-types? true})))))

(t/deftest test-determine-build-side
  (t/is (= :left (join/determine-build-side {:stats {:row-count 1}} {:stats {:row-count 10}} :right))
        "left side has fewer rows")

  (t/is (= :right (join/determine-build-side {:stats {:row-count 10}} {:stats {:row-count 1}} :left))
        "right side has fewer rows")

  (t/is (= :right (join/determine-build-side {:stats {:row-count 5}} {:stats {:row-count 5}} :right))
        "both sides equal, fallback to default")

  (t/is (= :left (join/determine-build-side {:stats {:row-count 5}} {:stats nil} :right))
        "right side missing stats")

  (t/is (= :right (join/determine-build-side {:stats nil} {:stats {:row-count 5}} :left))
        "left side missing stats")

  (t/is (= :left (join/determine-build-side {:stats nil} {:stats nil} :left))
        "both sides missing stats, fallback to default"))

(t/deftest test-determine-build-side-with-emitted-relations
  (let [smaller-table (lp/emit-expr (s/conform ::lp/logical-plan [:table [{:baz 1} {:baz 2} {:baz 2}]]) {})
        bigger-table (lp/emit-expr (s/conform ::lp/logical-plan [:table [{:biff 1} {:biff 2}]]) {})]

    (t/is (= :left
             (join/determine-build-side bigger-table smaller-table :right))
          "right side has fewer rows - build right")

    (t/is (= :right
             (join/determine-build-side smaller-table bigger-table :left))
          "left side has fewer rows - build left")

    (t/is (= :right
             (join/determine-build-side smaller-table smaller-table :right))
          "equal row count - fallback to default")))

(t/deftest test-left-side-built-left-outer-join
  (t/is (= [{:a 1, :b 1, :c 1} {:a 2, :b 2, :c 1} {:a 2, :b 2, :c 2}]
           (tu/query-ra
            '[:left-outer-join [{a b}]
              [:table [{:a 1} {:a 2}]]
              [:table [{:b 1 :c 1} {:b 2 :c 1} {:b 2 :c 2}]]]))
        "left side built LOJ with all matched rows")

  (t/is (= [{:a 1, :b 1, :c 1} {:a 2, :b 2, :c 1}]
           (tu/query-ra
            '[:left-outer-join [{a b}]
              [:table [{:a 1} {:a 2}]]
              [:table [{:b 1 :c 1} {:b 2 :c 1} {:b 3 :c 1}]]]))
        "left side built LOJ with some unmatched rows")

  (t/is (= [{:a 1} {:a 2}]
           (tu/query-ra
            '[:left-outer-join [{a b}]
              [:table [{:a 1} {:a 2}]]
              [:table [{:b 3} {:b 4} {:b 5}]]]))
        "left side built LOJ with all unmatched rows")

  (t/is (= (tu/query-ra '[:left-outer-join [{a b}]
                          [:table [{:a 1} {:a 2}]]
                          [:table [{:b 1} {:b 3} {:b 4}]]])
           (tu/query-ra '[:left-outer-join [{a b}]
                          [:table [{:a 1} {:a 2}]]
                          [:table [{:b 1}]]]))
        "left side built LOJ should return same results as right built LOJ (with right side having mostly unmatched rows)"))

(deftest ^:integration test-on-disk-joining
  (binding [join/*disk-join-threshold-rows* 10000]
    (with-open [node (xtn/start-node)
                conn (jdbc/get-connection node)]
      ;; fizzbuzz ... sort of
      (let [ids (range 100000)
            fizzes (into #{} (remove #(zero? (mod % 300))) ids)
            buzzes (into #{} (remove #(zero? (mod % 500))) ids)
            count-f (count fizzes)
            count-b (count buzzes)
            f-and-b (count (set/intersection fizzes buzzes))
            f-minus-b (count (set/difference fizzes buzzes))
            b-minus-f (count (set/difference buzzes fizzes))
            f-or-b (count (set/union fizzes buzzes))]

        (doseq [batch (partition-all 1000 ids)
                :let [docs (for [id batch]
                             {:xt/id id})]]
          (xt/submit-tx conn
                        [(into [:put-docs :foo]
                               (remove #(zero? (mod (:xt/id %) 300)))
                               docs)
                         (into [:put-docs :bar]
                               (remove #(zero? (mod (:xt/id %) 500)))
                               docs)]))

        (t/testing "inner"
          (let [join (set (tu/query-ra '[:join [{foo bar}]
                                         [:rename {_id foo} [:scan {:table #xt/table foo} [_id]]]
                                         [:rename {_id bar} [:scan {:table #xt/table bar} [_id]]]]
                                       {:node node}))]
            (t/is (= f-and-b (count join)))

            (t/is (true? (contains? join {:foo 99999, :bar 99999})))
            (t/is (false? (contains? join {:foo 99900, :bar 99900})))
            (t/is (false? (contains? join {:foo 99500, :bar 99500})))))

        (t/testing "LOJ"
          (let [loj (set (tu/query-ra '[:left-outer-join [{foo bar}]
                                        [:rename {_id foo}
                                         [:scan {:table #xt/table foo} [_id]]]
                                        [:rename {_id bar}
                                         [:scan {:table #xt/table bar} [_id]]]]
                                      {:node node}))
                foos (into #{} (map :foo) loj)
                bars (into #{} (map :bar) loj)]
            (t/is (= count-f (count loj)))
            (t/is (= count-f (count foos)))
            (t/is (= (inc f-and-b) (count bars)))
            (t/is (true? (contains? bars nil)))

            (t/is (= f-minus-b (count (remove :bar loj))))
            (t/is (true? (contains? loj {:foo 99999, :bar 99999})))
            (t/is (true? (contains? loj {:foo 99500})))))

        (t/testing "LOJ flipped"
          (let [loj (set (tu/query-ra '[:left-outer-join [{bar foo}]
                                        [:rename {_id bar} [:scan {:table #xt/table bar} [_id]]]
                                        [:rename {_id foo} [:scan {:table #xt/table foo} [_id]]]]
                                      {:node node}))
                foos (into #{} (map :foo) loj)
                bars (into #{} (map :bar) loj)]
            (t/is (= count-b (count loj)))
            (t/is (= (inc f-and-b) (count foos))) ; plus nil
            (t/is (= count-b (count bars)))
            (t/is (true? (contains? foos nil)))

            (t/is (= b-minus-f (count (remove :foo loj))))
            (t/is (true? (contains? loj {:foo 99999, :bar 99999})))
            (t/is (true? (contains? loj {:bar 99300})))))

        (t/testing "FOJ"
          (let [foj (set (tu/query-ra '[:full-outer-join [{foo bar}]
                                        [:rename {_id foo} [:scan {:table #xt/table foo} [_id]]]
                                        [:rename {_id bar} [:scan {:table #xt/table bar} [_id]]]]
                                      {:node node}))
                foos (into #{} (map :foo) foj)
                bars (into #{} (map :bar) foj)]
            (t/is (= f-or-b (count foj)))
            (t/is (= (inc count-f) (count foos))) ; original plus nil
            (t/is (= (inc count-b) (count bars))) ; original plus nil
            (t/is (true? (contains? foos nil)))
            (t/is (true? (contains? bars nil)))

            (t/is (= b-minus-f (count (remove :foo foj))))
            (t/is (= f-minus-b (count (remove :bar foj))))
            (t/is (true? (contains? foj {:foo 99999, :bar 99999})))
            (t/is (true? (contains? foj {:bar 99300})))
            (t/is (true? (contains? foj {:foo 99500})))

            (t/is (false? (contains? foj {:foo 1500, :bar 1500})))
            (t/is (false? (contains? foj {:foo 1500})))
            (t/is (false? (contains? foj {:bar 1500})))))))))

(t/deftest multi-iid-selector-test
  (util/with-open [node (xtn/start-node)]
    (let [[id1 id2 id3 id4] [#uuid "863f65e6-4f36-41ca-a4bf-21f4d3b77720"
                             #uuid "55db83c1-b4f6-4cf1-9e2c-d00dda241059"
                             #uuid "0eacd26e-6378-49d1-b21f-d9f55ca4e9cd"
                             #uuid "b55725fc-80e8-4f05-8561-d04df3245783"]]
      (xt/execute-tx node [[:put-docs :foo
                            {:xt/id id1, :a 1}
                            {:xt/id id2, :a 2}
                            {:xt/id id3, :a 3}]
                           [:put-docs :bar
                            {:xt/id id1, :b 1}
                            {:xt/id id2, :b 2}
                            {:xt/id id4, :b 4}]])

      (t/is (= [{:foo id1, :bar id1, :a 1, :b 1} {:foo id2, :bar id2, :a 2, :b 2}]
               (tu/query-ra '[:order-by [[a]]
                              [:join [{foo bar}]
                               [:rename {_id foo}
                                [:scan {:table #xt/table foo}
                                 [_id a]]]
                               [:rename {_id bar}
                                [:scan {:table #xt/table bar}
                                 [_id b]]]]]
                            {:node node}))))))
