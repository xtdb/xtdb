(ns core2.operator.join-test
  (:require [clojure.test :as t]
            [core2.operator :as op]
            [core2.operator.join :as join]
            [core2.test-util :as tu]
            [core2.types :as ty])
  (:import (org.apache.arrow.vector.types.pojo Schema)))

(def ^:private a-field (ty/->field "a" ty/bigint-type false))
(def ^:private b-field (ty/->field "b" ty/bigint-type false))
(def ^:private c-field (ty/->field "c" ty/bigint-type false))
(def ^:private d-field (ty/->field "d" ty/bigint-type false))
(def ^:private e-field (ty/->field "e" ty/bigint-type false))

(defn- run-ra [ra-expr]
  (with-open [res (op/open-ra ra-expr)]
    (tu/<-cursor res)))

(t/deftest test-cross-join
  (t/is (= [{{:a 12, :b 10, :c 1} 2,
             {:a 12, :b 15, :c 2} 2,
             {:a 0, :b 10, :c 1} 1,
             {:a 0, :b 15, :c 2} 1}
            {{:a 100, :b 10, :c 1} 1, {:a 100, :b 15, :c 2} 1}
            {{:a 12, :b 83, :c 3} 2, {:a 0, :b 83, :c 3} 1}
            {{:a 100, :b 83, :c 3} 1}]
           (->> (run-ra [:cross-join
                         [::tu/blocks (Schema. [a-field])
                          [[{:a 12}, {:a 12}, {:a 0}]
                           [{:a 100}]]]
                         [::tu/blocks (Schema. [b-field c-field])
                          [[{:b 10 :c 1}, {:b 15 :c 2}]
                           [{:b 83 :c 3}]]]])
                (mapv frequencies))))

  (t/is (empty? (run-ra [:cross-join
                         [::tu/blocks (Schema. [a-field])
                          [[{:a 12}, {:a 0}]
                           [{:a 100}]]]
                         [::tu/blocks (Schema. [b-field])
                          []]]))
        "empty input and output")

  (t/is (= [[{} {} {} {} {} {}]]
           (run-ra [:cross-join
                    [::tu/blocks (Schema. [])
                     [[{} {} {}]]]
                    [::tu/blocks (Schema. [])
                     [[{} {}]]]]))
        "tables with no cols"))

(t/deftest test-equi-join
  (t/is (= [#{{:a 12, :b 12}}
            #{{:a 100, :b 100}
              {:a 0, :b 0}}]
           (->> (run-ra [:join '[{a b}]
                         [::tu/blocks (Schema. [a-field])
                          [[{:a 12}, {:a 0}]
                           [{:a 100}]]]
                         [::tu/blocks (Schema. [b-field])
                          [[{:b 12}, {:b 2}]
                           [{:b 100} {:b 0}]]]])
                (mapv set))))

  (t/is (= [{{:a 12} 2}
            {{:a 100} 1, {:a 0} 1}]
           (->> (run-ra [:join '[{a a}]
                         [::tu/blocks (Schema. [a-field])
                          [[{:a 12}, {:a 0}]
                           [{:a 12}, {:a 100}]]]
                         [::tu/blocks (Schema. [a-field])
                          [[{:a 12}, {:a 2}]
                           [{:a 100} {:a 0}]]]])
                (mapv frequencies)))
        "same column name")

  (t/is (empty? (run-ra [:join '[{a b}]
                         [::tu/blocks (Schema. [a-field])
                          [[{:a 12}, {:a 0}]
                           [{:a 100}]]]
                         [::tu/blocks (Schema. [b-field])
                          []]]))
        "empty input")

  (t/is (empty? (run-ra [:join '[{a b}]
                         [::tu/blocks (Schema. [a-field])
                          [[{:a 12}, {:a 0}]
                           [{:a 100}]]]
                         [::tu/blocks (Schema. [b-field c-field])
                          [[{:b 10 :c 1}, {:b 15 :c 2}]
                           [{:b 83 :c 3}]]]]))
        "empty output"))

(t/deftest test-equi-join-multi-col
  (->> "multi column"
       (t/is (= [{{:a 12, :b 42, :c 12, :d 42, :e 0} 2}
                 {{:a 12, :b 42, :c 12, :d 42, :e 0} 4
                  {:a 12, :b 42, :c 12, :d 42, :e 1} 2}]
                (->> (run-ra [:join '[{a c} {b d}]
                              [::tu/blocks (Schema. [a-field b-field])
                               [[{:a 12, :b 42}
                                 {:a 12, :b 42}
                                 {:a 11, :b 44}
                                 {:a 10, :b 42}]]]
                              [::tu/blocks (Schema. [c-field d-field e-field])
                               [[{:c 12, :d 42, :e 0}
                                 {:c 12, :d 43, :e 0}
                                 {:c 11, :d 42, :e 0}]
                                [{:c 12, :d 42, :e 0}
                                 {:c 12, :d 42, :e 0}
                                 {:c 12, :d 42, :e 1}]]]])
                     (mapv frequencies))))))

(t/deftest test-theta-inner-join
  (t/is (= [{{:a 12, :b 44, :c 12, :d 43} 1}]
           (->> (run-ra [:join '[{a c} (> b d)]
                         [::tu/blocks (Schema. [a-field b-field])
                          [[{:a 12, :b 42}
                            {:a 12, :b 44}
                            {:a 10, :b 42}]]]
                         [::tu/blocks (Schema. [c-field d-field])
                          [[{:c 12, :d 43}
                            {:c 11, :d 42}]]]])
                (mapv frequencies)))))

(t/deftest test-semi-equi-join
  (t/is (= [{{:a 12} 2} {{:a 100} 1}]
           (->> (run-ra [:semi-join '{a b}
                         [::tu/blocks (Schema. [a-field])
                          [[{:a 12}, {:a 12}, {:a 0}]
                           [{:a 100}]]]
                         [::tu/blocks (Schema. [b-field])
                          [[{:b 12}, {:b 2}]
                           [{:b 100}]]]])
                (mapv frequencies))))

  (t/testing "empty input"
    (t/is (empty? (run-ra [:semi-join '[{a b}]
                           [::tu/blocks (Schema. [a-field])
                            [[{:a 12}, {:a 0}]
                             [{:a 100}]]]
                           [::tu/blocks (Schema. [b-field])
                            []]])))

    (t/is (empty? (run-ra [:semi-join '[{a b}]
                           [::tu/blocks (Schema. [a-field])
                            []]
                           [::tu/blocks (Schema. [b-field])
                            [[{:b 12}, {:b 2}]
                             [{:b 100} {:b 0}]]]])))

    (t/is (empty? (run-ra [:semi-join '[{a b}]
                           [::tu/blocks (Schema. [a-field])
                            [[{:a 12}, {:a 0}]
                             [{:a 100}]]]
                           [::tu/blocks (Schema. [b-field])
                            [[]]]]))))

  (t/is (empty? (run-ra [:semi-join '[{a b}]
                         [::tu/blocks (Schema. [a-field c-field])
                          [[{:a 12}, {:a 0}]
                           [{:a 100}]]]
                         [::tu/blocks (Schema. [b-field c-field])
                          [[{:b 10 :c 1}, {:b 15 :c 2}]
                           [{:b 83 :c 3}]]]]))
        "empty output"))

(t/deftest test-semi-equi-join-multi-col
  (->> "multi column semi"
       (t/is (= [{{:a 12, :b 42} 2}]
                (->> (run-ra [:semi-join '[{a c} {b d}]
                              [::tu/blocks (Schema. [a-field b-field])
                               [[{:a 12, :b 42}
                                 {:a 12, :b 42}
                                 {:a 11, :b 44}
                                 {:a 10, :b 42}]]]
                              [::tu/blocks (Schema. [c-field d-field])
                               [[{:c 12, :d 42, :e 0}
                                 {:c 12, :d 43, :e 0}
                                 {:c 11, :d 42, :e 0}]
                                [{:c 12, :d 42, :e 0}
                                 {:c 12, :d 42, :e 0}
                                 {:c 12, :d 42, :e 1}]]]])
                     (mapv frequencies))))))

(t/deftest test-theta-semi-join
  (t/is (= [{{:a 12, :b 44} 1}]
           (->> (run-ra [:semi-join '[{a c} (> b d)]
                         [::tu/blocks (Schema. [a-field b-field])
                          [[{:a 12, :b 42}
                            {:a 12, :b 44}
                            {:a 10, :b 42}]]]
                         [::tu/blocks (Schema. [c-field d-field])
                          [[{:c 12, :d 43}
                            {:c 12, :d 43}
                            {:c 11, :d 42}]]]])
                (mapv frequencies)))))

#_
(t/deftest test-left-equi-join
  (t/is (= [{{:a 12, :b 12, :c 2} 1, {:a 12, :b 12, :c 0} 1, {:a 0, :b nil, :c nil} 1}
            {{:a 12, :b 12, :c 2} 1, {:a 100, :b 100, :c 3} 1, {:a 12, :b 12, :c 0} 1}]
           (->> (run-join-test join/->left-outer-equi-join-cursor
                               [[{:a 12}, {:a 0}]
                                [{:a 12}, {:a 100}]]
                               [[{:b 12, :c 0}, {:b 2, :c 1}]
                                [{:b 12, :c 2}, {:b 100, :c 3}]]
                               {:right-fields [b-field c-field]})
                (mapv frequencies))))

  (t/testing "empty input"
    (t/is (= [#{{:a 12, :b nil}, {:a 0, :b nil}}
              #{{:a 100, :b nil}}]
             (->> (run-join-test join/->left-outer-equi-join-cursor
                                 [[{:a 12}, {:a 0}]
                                  [{:a 100}]]
                                 [])
                  (mapv set))))

    (t/is (empty? (run-join-test join/->left-outer-equi-join-cursor
                                 []
                                 [[{:b 12}, {:b 2}]
                                  [{:b 100} {:b 0}]])))

    (t/is (= [#{{:a 12, :b nil}, {:a 0, :b nil}}
              #{{:a 100, :b nil}}]
             (->> (run-join-test join/->left-outer-equi-join-cursor
                                 [[{:a 12}, {:a 0}]
                                  [{:a 100}]]
                                 [[]])
                  (mapv set))))))

#_
(t/deftest test-left-equi-join-multi-col
  (->> "multi column left"
       (t/is (= [{{:a 11, :b 44, :c nil, :d nil, :e nil} 1
                  {:a 10, :b 42, :c nil, :d nil, :e nil} 1
                  {:a 12, :b 42, :c 12, :d 42, :e 0} 6
                  {:a 12, :b 42, :c 12, :d 42, :e 1} 2}]
                (->> (run-join-test join/->left-outer-equi-join-cursor
                                    [[{:a 12, :b 42}
                                      {:a 12, :b 42}
                                      {:a 11, :b 44}
                                      {:a 10, :b 42}]]
                                    [[{:c 12, :d 42, :e 0}
                                      {:c 12, :d 43, :e 0}
                                      {:c 11, :d 42, :e 0}]
                                     [{:c 12, :d 42, :e 0}
                                      {:c 12, :d 42, :e 0}
                                      {:c 12, :d 42, :e 1}]]
                                    {:left-fields [a-field b-field], :right-fields [c-field d-field e-field]
                                     :left-join-cols ["a" "b"], :right-join-cols ["c" "d"]})
                     (mapv frequencies))))))

#_
(t/deftest test-left-theta-join
  (let [left [{:a 12, :b 42}
              {:a 12, :b 44}
              {:a 10, :b 42}
              {:a 10, :b 42}]
        right [{:c 12, :d 43}
               {:c 11, :d 42}]
        j (fn j [& conditions] (quick-join join/->left-outer-equi-join-cursor left right conditions))]

    (t/is (= {{:a 12, :b 42, :c nil, :d nil} 1
              {:a 12, :b 44, :c 12, :d 43} 1
              {:a 10, :b 42, :c nil, :d nil} 2}
             (j {:a :c} '(> b d))))

    (t/is (= {{:a 12, :b 42, :c nil, :d nil} 1
              {:a 12, :b 44, :c nil, :d nil} 1
              {:a 10, :b 42, :c nil, :d nil} 2}
             (j {:a :c} '(> b d) '(= c -1))))

    (t/is (= {{:a 12, :b 42, :c nil, :d nil} 1
              {:a 12, :b 44, :c nil, :d nil} 1
              {:a 10, :b 42, :c nil, :d nil} 2}
             (j {:a :c} '(and (= c -1) (> b d)))))

    (t/is (= {{:a 12, :b 42, :c nil, :d nil} 1
              {:a 12, :b 44, :c nil, :d nil} 1
              {:a 10, :b 42, :c nil, :d nil} 2}
             (j {:a :c} {:b :d} '(= c -1))))))

#_
(t/deftest test-full-outer-join
  (t/testing "missing on both sides"
    (t/is (= [{{:a 12, :b 12, :c 0} 2, {:a nil, :b 2, :c 1} 1}
              {{:a 12, :b 12, :c 2} 2, {:a 100, :b 100, :c 3} 1}
              {{:a 0, :b nil, :c nil} 1}]
             (->> (run-join-test join/->full-outer-equi-join-cursor
                                 [[{:a 12}, {:a 0}]
                                  [{:a 12}, {:a 100}]]
                                 [[{:b 12, :c 0}, {:b 2, :c 1}]
                                  [{:b 12, :c 2}, {:b 100, :c 3}]]
                                 {:right-fields [b-field c-field]})
                  (mapv frequencies))))

    (t/is (= [{{:a 12, :b 12, :c 0} 2, {:a 12, :b 12, :c 2} 2, {:a nil, :b 2, :c 1} 1}
              {{:a 100, :b 100, :c 3} 1}
              {{:a 0, :b nil, :c nil} 1}]
             (->> (run-join-test join/->full-outer-equi-join-cursor
                                 [[{:a 12}, {:a 0}]
                                  [{:a 12}, {:a 100}]]
                                 [[{:b 12, :c 0}, {:b 12, :c 2}, {:b 2, :c 1}]
                                  [{:b 100, :c 3}]]
                                 {:right-fields [b-field c-field]})
                  (mapv frequencies)))))

  (t/testing "all matched"
    (t/is (= [{{:a 12, :b 12, :c 0} 2, {:a 100, :b 100, :c 3} 1}
              {{:a 12, :b 12, :c 2} 2}]
             (->> (run-join-test join/->full-outer-equi-join-cursor
                                 [[{:a 12}]
                                  [{:a 12}, {:a 100}]]
                                 [[{:b 12, :c 0}, {:b 100, :c 3}]
                                  [{:b 12, :c 2}]]
                                 {:right-fields [b-field c-field]})
                  (mapv frequencies))))

    (t/is (= [{{:a 12, :b 12, :c 0} 2, {:a 12, :b 12, :c 2} 2}
              {{:a 100, :b 100, :c 3} 1}]
             (->> (run-join-test join/->full-outer-equi-join-cursor
                                 [[{:a 12}]
                                  [{:a 12}, {:a 100}]]
                                 [[{:b 12, :c 0}, {:b 12, :c 2}]
                                  [{:b 100, :c 3}]]
                                 {:right-fields [b-field c-field]})
                  (mapv frequencies)))))

  (t/testing "empty input"
    (t/is (= [{{:a 0, :b nil} 1, {:a 100, :b nil} 1, {:a 12, :b nil} 1}]
             (->> (run-join-test join/->full-outer-equi-join-cursor
                                 [[{:a 12}, {:a 0}]
                                  [{:a 100}]]
                                 [])
                  (mapv frequencies))))

    (t/is (= [{{:a nil, :b 12} 1, {:a nil, :b 2} 1}
              {{:a nil, :b 100} 1, {:a nil, :b 0} 1}]
             (->> (run-join-test join/->full-outer-equi-join-cursor
                                 []
                                 [[{:b 12}, {:b 2}]
                                  [{:b 100} {:b 0}]])
                  (mapv frequencies))))))

#_
(t/deftest test-full-outer-equi-join-multi-col
  (->> "multi column full outer"
       (t/is (= [{{:a 12 :b 42 :c 12 :d 42 :e 0} 2
                  {:a nil :b nil :c 11 :d 42 :e 0} 1
                  {:a nil :b nil :c 12 :d 43 :e 0} 1}
                 {{:a 12 :b 42 :c 12 :d 42 :e 0} 4
                  {:a 12 :b 42 :c 12 :d 42 :e 1} 2}
                 {{:a 10 :b 42 :c nil :d nil :e nil} 1
                  {:a 11 :b 44 :c nil :d nil :e nil} 1}]
                (->> (run-join-test join/->full-outer-equi-join-cursor
                                    [[{:a 12, :b 42}
                                      {:a 12, :b 42}
                                      {:a 11, :b 44}
                                      {:a 10, :b 42}]]
                                    [[{:c 12, :d 42, :e 0}
                                      {:c 12, :d 43, :e 0}
                                      {:c 11, :d 42, :e 0}]
                                     [{:c 12, :d 42, :e 0}
                                      {:c 12, :d 42, :e 0}
                                      {:c 12, :d 42, :e 1}]]
                                    {:left-fields [a-field b-field], :right-fields [c-field d-field e-field]
                                     :left-join-cols ["a" "b"], :right-join-cols ["c" "d"]})
                     (mapv frequencies))))))

#_
(t/deftest test-full-outer-join-theta
  (let [left [{:a 12, :b 42}
              {:a 12, :b 44}
              {:a 10, :b 42}
              {:a 10, :b 42}]
        right [{:c 12, :d 43}
               {:c 11, :d 42}]
        j (fn j [& conditions] (quick-join join/->full-outer-equi-join-cursor left right conditions))]

    (t/is (= {{:a 12, :b 42, :c 12, :d 43} 1
              {:a 12, :b 44, :c nil, :d nil} 1
              {:a 10, :b 42, :c nil, :d nil} 2
              {:a nil, :b nil, :c 11, :d 42} 1}

             (j {:a :c} '(not (= b 44)))))

    (t/is (= {{:a 12, :b 42, :c nil, :d nil} 1
              {:a 12, :b 44, :c nil, :d nil} 1
              {:a 10, :b 42, :c nil :d nil} 2
              {:a nil, :b nil, :c 12, :d 43} 1
              {:a nil, :b nil, :c 11, :d 42} 1}
             (j {:a :c} false)))))

#_
(t/deftest test-anti-equi-join
  (t/is (= [{{:a 0} 2}]
           (->> (run-join-test join/->left-anti-semi-equi-join-cursor
                               [[{:a 12}, {:a 0}, {:a 0}]
                                [{:a 100}]]
                               [[{:b 12}, {:b 2}]
                                [{:b 100}]])
                (mapv frequencies))))

  (t/testing "empty input"
    (t/is (empty? (run-join-test join/->left-anti-semi-equi-join-cursor
                                 []
                                 [[{:b 12}, {:b 2}]
                                  [{:b 100}]])))

    (t/is (= [#{{:a 12}, {:a 0}}
              #{{:a 100}}]
             (->> (run-join-test join/->left-anti-semi-equi-join-cursor
                                 [[{:a 12}, {:a 0}]
                                  [{:a 100}]]
                                 [])
                  (mapv set)))))

  (t/is (empty? (run-join-test join/->left-anti-semi-equi-join-cursor
                               [[{:a 12}, {:a 2}]
                                [{:a 100}]]
                               [[{:b 12}, {:b 2}]
                                [{:b 100}]]))
        "empty output"))

#_
(t/deftest test-anti-equi-join-multi-col
  (->> "multi column anti"
       (t/is (= [{{:a 10 :b 42} 1
                  {:a 11 :b 44} 1}]
                (->> (run-join-test join/->left-anti-semi-equi-join-cursor
                                    [[{:a 12, :b 42}
                                      {:a 12, :b 42}
                                      {:a 11, :b 44}
                                      {:a 10, :b 42}]]
                                    [[{:c 12, :d 42, :e 0}
                                      {:c 12, :d 43, :e 0}
                                      {:c 11, :d 42, :e 0}]
                                     [{:c 12, :d 42, :e 0}
                                      {:c 12, :d 42, :e 0}
                                      {:c 12, :d 42, :e 1}]]
                                    {:left-fields [a-field b-field], :right-fields [c-field d-field e-field]
                                     :left-join-cols ["a" "b"], :right-join-cols ["c" "d"]})
                     (mapv frequencies))))))

#_
(t/deftest test-anti-join-theta
  (let [left [{:a 12, :b 42}
              {:a 12, :b 44}
              {:a 10, :b 42}
              {:a 10, :b 42}]
        right [{:c 12, :d 43}
               {:c 11, :d 42}]
        j (fn j [& conditions] (quick-join join/->left-anti-semi-equi-join-cursor left right conditions))]

    (t/is (= {{:a 12, :b 44} 1
              {:a 10, :b 42} 2}

             (j {:a :c} '(not (= b 44)))))

    (t/is (= {{:a 12, :b 42} 1
              {:a 12, :b 44} 1
              {:a 10, :b 42} 2}
             (j {:a :c} false)))))

#_
(t/deftest test-join-on-true
  (letfn [(run-join [left? right? theta-expr]
            (->> (run-join-test join/->equi-join-cursor
                                (if left? [[{:a 12}, {:a 0}]] [])
                                (if right? [[{:b 12}, {:b 2}]] [])
                                {:left-join-cols [], :right-join-cols []
                                 :theta-expr theta-expr})
                 (mapv frequencies)))]
    (t/is (= []
             (run-join true false nil)))

    (t/is (= []
             (run-join true false true)))

    (t/is (= []
             (run-join true true false)))

    (t/is (= [{{:a 12, :b 12} 1,
               {:a 12, :b 2} 1,
               {:a 0, :b 12} 1,
               {:a 0, :b 2} 1}]
             (run-join true true nil)))

    (t/is (= [{{:a 12, :b 12} 1,
               {:a 12, :b 2} 1,
               {:a 0, :b 12} 1,
               {:a 0, :b 2} 1}]
             (run-join true true true)))))

#_
(t/deftest test-loj-on-true
  (letfn [(run-loj [left? right? theta-expr]
            (->> (run-join-test join/->left-outer-equi-join-cursor
                                (if left? [[{:a 12}, {:a 0}]] [])
                                (if right? [[{:b 12}, {:b 2}]] [])
                                {:left-join-cols [], :right-join-cols []
                                 :theta-expr theta-expr})
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

    (t/is (= [{{:a 12, :b 12} 1,
               {:a 12, :b 2} 1,
               {:a 0, :b 12} 1,
               {:a 0, :b 2} 1}]
             (run-loj true true nil)))

    (t/is (= [{{:a 12, :b 12} 1,
               {:a 12, :b 2} 1,
               {:a 0, :b 12} 1,
               {:a 0, :b 2} 1}]
             (run-loj true true true)))))

#_
(t/deftest test-foj-on-true
  (letfn [(run-foj [left? right? theta-expr]
            (->> (run-join-test join/->full-outer-equi-join-cursor
                                (if left? [[{:a 12}, {:a 0}]] [])
                                (if right? [[{:b 12}, {:b 2}]] [])
                                {:left-join-cols [], :right-join-cols []
                                 :theta-expr theta-expr})
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

    (t/is (= [{{:a 12, :b 12} 1,
               {:a 12, :b 2} 1,
               {:a 0, :b 12} 1,
               {:a 0, :b 2} 1}]
             (run-foj true true nil)))

    (t/is (= [{{:a 12, :b 12} 1,
               {:a 12, :b 2} 1,
               {:a 0, :b 12} 1,
               {:a 0, :b 2} 1}]
             (run-foj true true true)))))

#_
(t/deftest test-semi-join-on-true
  (letfn [(run-semi [left? right? theta-expr]
            (->> (run-join-test join/->left-semi-equi-join-cursor
                                (if left? [[{:a 12}, {:a 0}]] [])
                                (if right? [[{:b 12}, {:b 2}]] [])
                                {:left-join-cols [], :right-join-cols []
                                 :theta-expr theta-expr})
                 (mapv frequencies)))]
    (t/is (= [] (run-semi true false nil)))
    (t/is (= [] (run-semi true false true)))
    (t/is (= [] (run-semi false true nil)))
    (t/is (= [] (run-semi false true true)))
    (t/is (= [] (run-semi true true false)))

    (t/is (= [{{:a 12} 1, {:a 0} 1}] (run-semi true true nil)))
    (t/is (= [{{:a 12} 1, {:a 0} 1}] (run-semi true true true)))))

#_
(t/deftest test-anti-join-on-true
  (letfn [(run-anti [left? right? theta-expr]
            (->> (run-join-test join/->left-anti-semi-equi-join-cursor
                                (if left? [[{:a 12}, {:a 0}]] [])
                                (if right? [[{:b 12}, {:b 2}]] [])
                                {:left-join-cols [], :right-join-cols []
                                 :theta-expr theta-expr})
                 (mapv frequencies)))]
    (t/is (= [{{:a 12} 1, {:a 0} 1}] (run-anti true false nil)))
    (t/is (= [{{:a 12} 1, {:a 0} 1}] (run-anti true false true)))

    (t/is (= [] (run-anti false true nil)))
    (t/is (= [] (run-anti false true true)))

    (t/is (= [{{:a 12} 1, {:a 0} 1}] (run-anti true true false)))

    (t/is (= [] (run-anti true true nil)))
    (t/is (= [] (run-anti true true true)))))
