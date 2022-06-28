(ns core2.operator.join-test
  (:require [clojure.test :as t]
            [core2.operator :as op]
            [core2.test-util :as tu]))

(defn- run-ra [ra-expr]
  (with-open [res (op/open-ra ra-expr)]
    {:res (tu/<-cursor res)
     :col-types (.columnTypes res)}))

(t/deftest test-cross-join
  (t/is (= {:res [{{:a 12, :b 10, :c 1} 2,
                   {:a 12, :b 15, :c 2} 2,
                   {:a 0, :b 10, :c 1} 1,
                   {:a 0, :b 15, :c 2} 1}
                  {{:a 100, :b 10, :c 1} 1, {:a 100, :b 15, :c 2} 1}
                  {{:a 12, :b 83, :c 3} 2, {:a 0, :b 83, :c 3} 1}
                  {{:a 100, :b 83, :c 3} 1}]
            :col-types '{a :i64, b :i64, c :i64}}
           (-> (run-ra [:cross-join
                        [::tu/blocks
                         [[{:a 12}, {:a 12}, {:a 0}]
                          [{:a 100}]]]
                        [::tu/blocks
                         [[{:b 10 :c 1}, {:b 15 :c 2}]
                          [{:b 83 :c 3}]]]])
               (update :res #(mapv frequencies %)))))

  (t/is (= {:res []
            :col-types '{a :i64, b :i64}}
           (run-ra [:cross-join
                         [::tu/blocks
                          [[{:a 12}, {:a 0}]
                           [{:a 100}]]]
                         [::tu/blocks '{b :i64} []]]))
        "empty input and output")

  (t/is (= {:res [[{} {} {} {} {} {}]]
            :col-types {}}
           (run-ra [:cross-join
                    [::tu/blocks [[{} {} {}]]]
                    [::tu/blocks [[{} {}]]]]))
        "tables with no cols"))

(t/deftest test-equi-join
  (t/is (= {:res [#{{:a 12, :b 12}}
                  #{{:a 100, :b 100}
                    {:a 0, :b 0}}]
            :col-types '{a :i64, b :i64}}
           (-> (run-ra [:join '[{a b}]
                        [::tu/blocks
                         [[{:a 12}, {:a 0}]
                          [{:a 100}]]]
                        [::tu/blocks
                         [[{:b 12}, {:b 2}]
                          [{:b 100} {:b 0}]]]])
               (update :res (partial mapv set)))))

  (t/is (= {:res [{{:a 12} 2}
                  {{:a 100} 1, {:a 0} 1}]
            :col-types '{a :i64}}
           (-> (run-ra [:join '[{a a}]
                        [::tu/blocks
                         [[{:a 12}, {:a 0}]
                          [{:a 12}, {:a 100}]]]
                        [::tu/blocks
                         [[{:a 12}, {:a 2}]
                          [{:a 100} {:a 0}]]]])
               (update :res (partial mapv frequencies))))
        "same column name")

  (t/is (= {:res []
            :col-types '{a :i64, b :i64}}
           (run-ra [:join '[{a b}]
                    [::tu/blocks
                     [[{:a 12}, {:a 0}]
                      [{:a 100}]]]
                    [::tu/blocks '{b :i64}
                     []]]))
        "empty input")

  (t/is (= {:res []
            :col-types '{a :i64, b :i64, c :i64}}
           (run-ra [:join '[{a b}]
                    [::tu/blocks
                     [[{:a 12}, {:a 0}]
                      [{:a 100}]]]
                    [::tu/blocks
                     [[{:b 10 :c 1}, {:b 15 :c 2}]
                      [{:b 83 :c 3}]]]]))
        "empty output"))

(t/deftest test-equi-join-multi-col
  (->> "multi column"
       (t/is (= [{{:a 12, :b 42, :c 12, :d 42, :e 0} 2}
                 {{:a 12, :b 42, :c 12, :d 42, :e 0} 4
                  {:a 12, :b 42, :c 12, :d 42, :e 1} 2}]
                (->> (run-ra [:join '[{a c} {b d}]
                              [::tu/blocks
                               [[{:a 12, :b 42}
                                 {:a 12, :b 42}
                                 {:a 11, :b 44}
                                 {:a 10, :b 42}]]]
                              [::tu/blocks
                               [[{:c 12, :d 42, :e 0}
                                 {:c 12, :d 43, :e 0}
                                 {:c 11, :d 42, :e 0}]
                                [{:c 12, :d 42, :e 0}
                                 {:c 12, :d 42, :e 0}
                                 {:c 12, :d 42, :e 1}]]]])
                     :res
                     (mapv frequencies))))))

(t/deftest test-theta-inner-join
  (t/is (= [{{:a 12, :b 44, :c 12, :d 43} 1}]
           (->> (run-ra [:join '[{a c} (> b d)]
                         [::tu/blocks
                          [[{:a 12, :b 42}
                            {:a 12, :b 44}
                            {:a 10, :b 42}]]]
                         [::tu/blocks
                          [[{:c 12, :d 43}
                            {:c 11, :d 42}]]]])
                :res
                (mapv frequencies)))))

(t/deftest test-semi-equi-join
  (t/is (= [{{:a 12} 2} {{:a 100} 1}]
           (->> (run-ra [:semi-join '[{a b}]
                         [::tu/blocks
                          [[{:a 12}, {:a 12}, {:a 0}]
                           [{:a 100}]]]
                         [::tu/blocks
                          [[{:b 12}, {:b 2}]
                           [{:b 100}]]]])
                :res
                (mapv frequencies))))

  (t/testing "empty input"
    (t/is (empty? (:res (run-ra [:semi-join '[{a b}]
                                 [::tu/blocks
                                  [[{:a 12}, {:a 0}]
                                   [{:a 100}]]]
                                 [::tu/blocks '{b :i64} []]]))))

    (t/is (empty? (:res (run-ra [:semi-join '[{a b}]
                                 [::tu/blocks '{a :i64} []]
                                 [::tu/blocks
                                  [[{:b 12}, {:b 2}]
                                   [{:b 100} {:b 0}]]]]))))

    (t/is (empty? (:res (run-ra [:semi-join '[{a b}]
                                 [::tu/blocks
                                  [[{:a 12}, {:a 0}]
                                   [{:a 100}]]]
                                 [::tu/blocks '{b :i64}
                                  [[]]]])))))

  (t/is (empty? (:res (run-ra [:semi-join '[{a b}]
                               [::tu/blocks
                                [[{:a 12}, {:a 0}]
                                 [{:a 100}]]]
                               [::tu/blocks
                                [[{:b 10 :c 1}, {:b 15 :c 2}]
                                 [{:b 83 :c 3}]]]])))
        "empty output"))

(t/deftest test-semi-equi-join-multi-col
  (->> "multi column semi"
       (t/is (= [{{:a 12, :b 42} 2}]
                (->> (run-ra [:semi-join '[{a c} {b d}]
                              [::tu/blocks
                               [[{:a 12, :b 42}
                                 {:a 12, :b 42}
                                 {:a 11, :b 44}
                                 {:a 10, :b 42}]]]
                              [::tu/blocks
                               [[{:c 12, :d 42, :e 0}
                                 {:c 12, :d 43, :e 0}
                                 {:c 11, :d 42, :e 0}]
                                [{:c 12, :d 42, :e 0}
                                 {:c 12, :d 42, :e 0}
                                 {:c 12, :d 42, :e 1}]]]])
                     :res
                     (mapv frequencies))))))

(t/deftest test-theta-semi-join
  (t/is (= [{{:a 12, :b 44} 1}]
           (->> (run-ra [:semi-join '[{a c} (> b d)]
                         [::tu/blocks
                          [[{:a 12, :b 42}
                            {:a 12, :b 44}
                            {:a 10, :b 42}]]]
                         [::tu/blocks
                          [[{:c 12, :d 43}
                            {:c 12, :d 43}
                            {:c 11, :d 42}]]]])
                :res
                (mapv frequencies)))))

(t/deftest test-left-equi-join
  (t/is (= {:res [{{:a 12, :b 12, :c 2} 1, {:a 12, :b 12, :c 0} 1, {:a 0, :b nil, :c nil} 1}
                  {{:a 12, :b 12, :c 2} 1, {:a 100, :b 100, :c 3} 1, {:a 12, :b 12, :c 0} 1}]
            :col-types '{a :i64, b [:union #{:null :i64}], c [:union #{:null :i64}]}}
           (-> (run-ra [:left-outer-join '[{a b}]
                         [::tu/blocks
                          [[{:a 12}, {:a 0}]
                           [{:a 12}, {:a 100}]]]
                         [::tu/blocks
                          [[{:b 12, :c 0}, {:b 2, :c 1}]
                           [{:b 12, :c 2}, {:b 100, :c 3}]]]])
                (update :res (partial mapv frequencies)))))

  (t/testing "empty input"
    (t/is (= {:res [#{{:a 12, :b nil}, {:a 0, :b nil}}
                    #{{:a 100, :b nil}}]
              :col-types '{a :i64, b [:union #{:null :i64}]}}
             (-> (run-ra [:left-outer-join '[{a b}]
                          [::tu/blocks
                           [[{:a 12}, {:a 0}]
                            [{:a 100}]]]
                          [::tu/blocks '{b :i64} []]])
                 (update :res (partial mapv set)))))

    (t/is (= {:res []
              :col-types '{a :i64, b [:union #{:null :i64}]}}
             (run-ra [:left-outer-join '[{a b}]
                      [::tu/blocks '{a :i64} []]
                      [::tu/blocks
                       [[{:b 12}, {:b 2}]
                        [{:b 100} {:b 0}]]]])))

    (t/is (= {:res [#{{:a 12, :b nil}, {:a 0, :b nil}}
                    #{{:a 100, :b nil}}]
              :col-types '{a :i64, b [:union #{:null :i64}]}}
             (-> (run-ra [:left-outer-join '[{a b}]
                           [::tu/blocks
                            [[{:a 12}, {:a 0}]
                             [{:a 100}]]]
                           [::tu/blocks '{b :i64}
                            [[]]]])
                  (update :res (partial mapv set)))))))

(t/deftest test-left-equi-join-multi-col
  (->> "multi column left"
       (t/is (= [{{:a 11, :b 44, :c nil, :d nil, :e nil} 1
                  {:a 10, :b 42, :c nil, :d nil, :e nil} 1
                  {:a 12, :b 42, :c 12, :d 42, :e 0} 6
                  {:a 12, :b 42, :c 12, :d 42, :e 1} 2}]
                (->> (run-ra [:left-outer-join '[{a c} {b d}]
                              [::tu/blocks
                               [[{:a 12, :b 42}
                                 {:a 12, :b 42}
                                 {:a 11, :b 44}
                                 {:a 10, :b 42}]]]
                              [::tu/blocks
                               [[{:c 12, :d 42, :e 0}
                                 {:c 12, :d 43, :e 0}
                                 {:c 11, :d 42, :e 0}]
                                [{:c 12, :d 42, :e 0}
                                 {:c 12, :d 42, :e 0}
                                 {:c 12, :d 42, :e 1}]]]])
                     :res
                     (mapv frequencies))))))

(t/deftest test-left-theta-join
  (let [left [::tu/blocks
              [[{:a 12, :b 42}
                {:a 12, :b 44}
                {:a 10, :b 42}
                {:a 10, :b 42}]]]
        right [::tu/blocks
               [[{:c 12, :d 43}
                 {:c 11, :d 42}]]]]

    (t/is (= [{{:a 12, :b 42, :c nil, :d nil} 1
               {:a 12, :b 44, :c 12, :d 43} 1
               {:a 10, :b 42, :c nil, :d nil} 2}]
             (->> (run-ra [:left-outer-join '[{a c} (> b d)] left right])
                  :res
                  (mapv frequencies))))

    (t/is (= [{{:a 12, :b 42, :c nil, :d nil} 1
               {:a 12, :b 44, :c nil, :d nil} 1
               {:a 10, :b 42, :c nil, :d nil} 2}]
             (->> (run-ra [:left-outer-join '[{a c} (> b d) (= c -1)] left right])
                  :res
                  (mapv frequencies))))

    (t/is (= [{{:a 12, :b 42, :c nil, :d nil} 1
               {:a 12, :b 44, :c nil, :d nil} 1
               {:a 10, :b 42, :c nil, :d nil} 2}]
             (->> (run-ra [:left-outer-join '[{a c} (and (= c -1) (> b d))] left right])
                  :res
                  (mapv frequencies))))

    (t/is (= [{{:a 12, :b 42, :c nil, :d nil} 1
               {:a 12, :b 44, :c nil, :d nil} 1
               {:a 10, :b 42, :c nil, :d nil} 2}]
             (->> (run-ra [:left-outer-join '[{a c} {b d} (= c -1)] left right])
                  :res
                  (mapv frequencies))))))

(t/deftest test-full-outer-join
  (t/testing "missing on both sides"
    (t/is (= {:res [{{:a 12, :b 12, :c 0} 2, {:a nil, :b 2, :c 1} 1}
                    {{:a 12, :b 12, :c 2} 2, {:a 100, :b 100, :c 3} 1}
                    {{:a 0, :b nil, :c nil} 1}]
              :col-types '{a [:union #{:null :i64}], b [:union #{:null :i64}], c [:union #{:null :i64}]}}
             (-> (run-ra [:full-outer-join '[{a b}]
                          [::tu/blocks
                           [[{:a 12}, {:a 0}]
                            [{:a 12}, {:a 100}]]]
                          [::tu/blocks
                           [[{:b 12, :c 0}, {:b 2, :c 1}]
                            [{:b 12, :c 2}, {:b 100, :c 3}]]]])
                 (update :res (partial mapv frequencies)))))

    (t/is (= {:res [{{:a 12, :b 12, :c 0} 2, {:a 12, :b 12, :c 2} 2, {:a nil, :b 2, :c 1} 1}
                    {{:a 100, :b 100, :c 3} 1}
                    {{:a 0, :b nil, :c nil} 1}]
              :col-types '{a [:union #{:null :i64}], b [:union #{:null :i64}], c [:union #{:null :i64}]}}
             (-> (run-ra [:full-outer-join '[{a b}]
                          [::tu/blocks
                           [[{:a 12}, {:a 0}]
                            [{:a 12}, {:a 100}]]]
                          [::tu/blocks
                           [[{:b 12, :c 0}, {:b 12, :c 2}, {:b 2, :c 1}]
                            [{:b 100, :c 3}]]]])
                 (update :res (partial mapv frequencies))))))

  (t/testing "all matched"
    (t/is (= {:res [{{:a 12, :b 12, :c 0} 2, {:a 100, :b 100, :c 3} 1}
                    {{:a 12, :b 12, :c 2} 2}]
              :col-types '{a [:union #{:null :i64}], b [:union #{:null :i64}], c [:union #{:null :i64}]}}
             (-> (run-ra [:full-outer-join '[{a b}]
                          [::tu/blocks
                           [[{:a 12}]
                            [{:a 12}, {:a 100}]]]
                          [::tu/blocks
                           [[{:b 12, :c 0}, {:b 100, :c 3}]
                            [{:b 12, :c 2}]]]])
                 (update :res (partial mapv frequencies)))))

    (t/is (= {:res [{{:a 12, :b 12, :c 0} 2, {:a 12, :b 12, :c 2} 2}
                    {{:a 100, :b 100, :c 3} 1}]
              :col-types '{a [:union #{:null :i64}], b [:union #{:null :i64}], c [:union #{:null :i64}]}}
             (-> (run-ra [:full-outer-join '[{a b}]
                          [::tu/blocks
                           [[{:a 12}]
                            [{:a 12}, {:a 100}]]]
                          [::tu/blocks
                           [[{:b 12, :c 0}, {:b 12, :c 2}]
                            [{:b 100, :c 3}]]]])
                 (update :res (partial mapv frequencies))))))

  (t/testing "empty input"
    (t/is (= {:res [{{:a 0, :b nil} 1, {:a 100, :b nil} 1, {:a 12, :b nil} 1}]
              :col-types '{a [:union #{:null :i64}], b [:union #{:null :i64}]}}
             (-> (run-ra [:full-outer-join '[{a b}]
                          [::tu/blocks
                           [[{:a 12}, {:a 0}]
                            [{:a 100}]]]
                          [::tu/blocks '{b :i64} []]])
                 (update :res (partial mapv frequencies)))))

    (t/is (= {:res [{{:a nil, :b 12} 1, {:a nil, :b 2} 1}
                    {{:a nil, :b 100} 1, {:a nil, :b 0} 1}]
              :col-types '{a [:union #{:null :i64}], b [:union #{:null :i64}]}}
             (-> (run-ra [:full-outer-join '[{a b}]
                          [::tu/blocks '{a :i64} []]
                          [::tu/blocks
                           [[{:b 12}, {:b 2}]
                            [{:b 100} {:b 0}]]]])
                 (update :res (partial mapv frequencies)))))))

(t/deftest test-full-outer-equi-join-multi-col
  (->> "multi column full outer"
       (t/is (= [{{:a 12 :b 42 :c 12 :d 42 :e 0} 2
                  {:a nil :b nil :c 11 :d 42 :e 0} 1
                  {:a nil :b nil :c 12 :d 43 :e 0} 1}
                 {{:a 12 :b 42 :c 12 :d 42 :e 0} 4
                  {:a 12 :b 42 :c 12 :d 42 :e 1} 2}
                 {{:a 10 :b 42 :c nil :d nil :e nil} 1
                  {:a 11 :b 44 :c nil :d nil :e nil} 1}]
                (->> (run-ra [:full-outer-join '[{a c} {b d}]
                              [::tu/blocks
                               [[{:a 12, :b 42}
                                 {:a 12, :b 42}
                                 {:a 11, :b 44}
                                 {:a 10, :b 42}]]]
                              [::tu/blocks
                               [[{:c 12, :d 42, :e 0}
                                 {:c 12, :d 43, :e 0}
                                 {:c 11, :d 42, :e 0}]
                                [{:c 12, :d 42, :e 0}
                                 {:c 12, :d 42, :e 0}
                                 {:c 12, :d 42, :e 1}]]]])
                     :res
                     (mapv frequencies))))))

(t/deftest test-full-outer-join-theta
  (let [left [::tu/blocks
              [[{:a 12, :b 42}
                {:a 12, :b 44}
                {:a 10, :b 42}
                {:a 10, :b 42}]]]
        right [::tu/blocks
               [[{:c 12, :d 43}
                 {:c 11, :d 42}]]]]

    (t/is (= {{:a 12, :b 42, :c 12, :d 43} 1
              {:a 12, :b 44, :c nil, :d nil} 1
              {:a 10, :b 42, :c nil, :d nil} 2
              {:a nil, :b nil, :c 11, :d 42} 1}

             (->> (run-ra [:full-outer-join '[{a c} (not (= b 44))] left right])
                  :res
                  (sequence cat)
                  (frequencies))))

    (t/is (= {{:a 12, :b 42, :c nil, :d nil} 1
              {:a 12, :b 44, :c nil, :d nil} 1
              {:a 10, :b 42, :c nil :d nil} 2
              {:a nil, :b nil, :c 12, :d 43} 1
              {:a nil, :b nil, :c 11, :d 42} 1}
             (->> (run-ra [:full-outer-join '[{a c} false] left right])
                  :res
                  (sequence cat)
                  (frequencies))))))

(t/deftest test-anti-equi-join
  (t/is (= {:res [{{:a 0} 2}]
            :col-types '{a :i64}}
           (-> (run-ra [:anti-join '[{a b}]
                        [::tu/blocks
                         [[{:a 12}, {:a 0}, {:a 0}]
                          [{:a 100}]]]
                        [::tu/blocks
                         [[{:b 12}, {:b 2}]
                          [{:b 100}]]]])
               (update :res (partial mapv frequencies)))))

  (t/testing "empty input"
    (t/is (empty? (:res (run-ra [:anti-join '[{a b}]
                                 [::tu/blocks '{a :i64} []]
                                 [::tu/blocks
                                  [[{:b 12}, {:b 2}]
                                   [{:b 100}]]]]))))

    (t/is (= [#{{:a 12}, {:a 0}}
              #{{:a 100}}]
             (->> (run-ra [:anti-join '[{a b}]
                           [::tu/blocks
                            [[{:a 12}, {:a 0}]
                             [{:a 100}]]]
                           [::tu/blocks '{b :i64} []]])
                  :res
                  (mapv set)))))

  (t/is (empty? (:res (run-ra [:anti-join '[{a b}]
                               [::tu/blocks
                                [[{:a 12}, {:a 2}]
                                 [{:a 100}]]]
                               [::tu/blocks
                                [[{:b 12}, {:b 2}]
                                 [{:b 100}]]]])))
        "empty output"))

(t/deftest test-anti-equi-join-multi-col
  (->> "multi column anti"
       (t/is (= [{{:a 10 :b 42} 1
                  {:a 11 :b 44} 1}]
                (->> (run-ra [:anti-join '[{a c} {b d}]
                              [::tu/blocks
                               [[{:a 12, :b 42}
                                 {:a 12, :b 42}
                                 {:a 11, :b 44}
                                 {:a 10, :b 42}]]]
                              [::tu/blocks
                               [[{:c 12, :d 42, :e 0}
                                 {:c 12, :d 43, :e 0}
                                 {:c 11, :d 42, :e 0}]
                                [{:c 12, :d 42, :e 0}
                                 {:c 12, :d 42, :e 0}
                                 {:c 12, :d 42, :e 1}]]]])
                     :res
                     (mapv frequencies))))))

(t/deftest test-anti-join-theta
  (let [left [::tu/blocks
              [[{:a 12, :b 42}
                {:a 12, :b 44}
                {:a 10, :b 42}
                {:a 10, :b 42}]]]
        right [::tu/blocks
               [[{:c 12, :d 43}
                 {:c 11, :d 42}]]]]

    (t/is (= {{:a 12, :b 44} 1
              {:a 10, :b 42} 2}

             (->> (run-ra [:anti-join '[{a c} (not (= b 44))] left right])
                  :res
                  (sequence cat)
                  (frequencies))))

    (t/is (= {{:a 12, :b 42} 1
              {:a 12, :b 44} 1
              {:a 10, :b 42} 2}
             (->> (run-ra [:anti-join '[{a c} false] left right])
                  :res
                  (sequence cat)
                  (frequencies))))))

(t/deftest test-join-on-true
  (letfn [(run-join [left? right? theta-expr]
            (->> (run-ra [:join [theta-expr]
                          [::tu/blocks '{a :i64}
                           (if left? [[{:a 12}, {:a 0}]] [])]
                          [::tu/blocks '{b :i64}
                           (if right? [[{:b 12}, {:b 2}]] [])]])
                 :res
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
            (->> (run-ra [:left-outer-join [theta-expr]
                          [::tu/blocks '{a :i64}
                           (if left? [[{:a 12}, {:a 0}]] [])]
                          [::tu/blocks '{b :i64}
                           (if right? [[{:b 12}, {:b 2}]] [])]])
                 :res
                 (mapv frequencies)))]
    (t/is (= [{{:a 12, :b nil} 1,
               {:a 0, :b nil} 1}]
             (run-loj true false nil)))

    (t/is (= [{{:a 12, :b nil} 1,
               {:a 0, :b nil} 1}]
             (run-loj true false true)))

    (t/is (= []
             (run-loj false true nil)))

    (t/is (= []
             (run-loj false true true)))

    (t/is (= [{{:a 12, :b nil} 1,
               {:a 0, :b nil} 1}]
             (run-loj true true false)))

    (t/is (= [{{:a 12, :b nil} 1,
               {:a 0, :b nil} 1}]
             (run-loj true true nil)))

    (t/is (= [{{:a 12, :b 12} 1,
               {:a 12, :b 2} 1,
               {:a 0, :b 12} 1,
               {:a 0, :b 2} 1}]
             (run-loj true true true)))))

(t/deftest test-foj-on-true
  (letfn [(run-foj [left? right? theta-expr]
            (->> (run-ra [:full-outer-join [theta-expr]
                          [::tu/blocks '{a :i64}
                           (if left? [[{:a 12}, {:a 0}]] [])]
                          [::tu/blocks '{b :i64}
                           (if right? [[{:b 12}, {:b 2}]] [])]])
                 :res
                 (mapv frequencies)))]
    (t/is (= [{{:a 12, :b nil} 1,
               {:a 0, :b nil} 1}]
             (run-foj true false nil)))

    (t/is (= [{{:a 12, :b nil} 1,
               {:a 0, :b nil} 1}]
             (run-foj true false true)))

    (t/is (= [{{:a nil, :b 12} 1,
               {:a nil, :b 2} 1}]
             (run-foj false true nil)))

    (t/is (= [{{:a nil, :b 12} 1,
               {:a nil, :b 2} 1}]
             (run-foj false true true)))

    (t/is (= [{{:a nil, :b 12} 1,
               {:a nil, :b 2} 1}
              {{:a 12, :b nil} 1,
               {:a 0, :b nil} 1}]
             (run-foj true true false)))

    (t/is (= [{{:a nil, :b 12} 1,
               {:a nil, :b 2} 1}
              {{:a 12, :b nil} 1,
               {:a 0, :b nil} 1}]
             (run-foj true true nil)))

    (t/is (= [{{:a 12, :b 12} 1,
               {:a 12, :b 2} 1,
               {:a 0, :b 12} 1,
               {:a 0, :b 2} 1}]
             (run-foj true true true)))))

(t/deftest test-semi-join-on-true
  (letfn [(run-semi [left? right? theta-expr]
            (->> (run-ra [:semi-join [theta-expr]
                          [::tu/blocks '{a :i64}
                           (if left? [[{:a 12}, {:a 0}]] [])]

                          [::tu/blocks '{b :i64}
                           (if right? [[{:b 12}, {:b 2}]] [])]])
                 :res
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
            (->> (run-ra [:anti-join [theta-expr]
                          [::tu/blocks '{a :i64}
                           (if left? [[{:a 12}, {:a 0}]] [])]
                          [::tu/blocks '{b :i64}
                           (if right? [[{:b 12}, {:b 2}]] [])]])
                 :res
                 (mapv frequencies)))]
    (t/is (= [{{:a 12} 1, {:a 0} 1}] (run-anti true false nil)))
    (t/is (= [{{:a 12} 1, {:a 0} 1}] (run-anti true false true)))

    (t/is (= [] (run-anti false true nil)))
    (t/is (= [] (run-anti false true true)))

    (t/is (= [{{:a 12} 1, {:a 0} 1}] (run-anti true true false)))
    (t/is (= [{{:a 12} 1, {:a 0} 1}] (run-anti true true nil)))

    (t/is (= [] (run-anti true true true)))))

(t/deftest test-equi-join-expr
  (letfn [(test-equi-join [join-specs left-vals right-vals]
            (op/query-ra [:join join-specs
                          [::tu/blocks [left-vals]]
                          [::tu/blocks [right-vals]]]
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
