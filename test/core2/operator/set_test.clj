(ns core2.operator.set-test
  (:require [clojure.test :as t]
            [core2.expression :as expr]
            [core2.operator.project :as project]
            [core2.operator.select :as select]
            [core2.operator.set :as set-op]
            [core2.operator :as op]
            [core2.test-util :as tu]
            [core2.types :as ty])
  (:import org.apache.arrow.vector.types.pojo.Schema))

(t/deftest test-union-all
  (let [a-field (ty/->field "a" ty/bigint-type false)
        b-field (ty/->field "b" ty/bigint-type false)]
    (with-open [res (op/open-ra [:union-all
                                 [::tu/blocks (Schema. [a-field b-field])
                                  [[{:a 12 :b 10}, {:a 0 :b 15}]
                                   [{:a 100 :b 15}]]]
                                 [::tu/blocks (Schema. [a-field b-field])
                                  [[{:a 10 :b 1}, {:a 15 :b 2}]
                                   [{:a 83 :b 3}]]]])]

      (t/is (= [#{{:a 0, :b 15}
                  {:a 12, :b 10}}
                #{{:a 100, :b 15}}
                #{{:a 10, :b 1}
                  {:a 15, :b 2}}
                #{{:a 83, :b 3}}]
               (mapv set (tu/<-cursor res)))))

    (t/testing "empty input and output"
      (with-open [res (op/open-ra [:union-all
                                   [::tu/blocks (Schema. [a-field]) []]
                                   [::tu/blocks (Schema. [a-field]) []]])]

        (t/is (empty? (tu/<-cursor res)))))))

(t/deftest test-intersection
  (let [a-field (ty/->field "a" ty/bigint-type false)
        b-field (ty/->field "b" ty/bigint-type false)]
    (with-open [res (op/open-ra [:intersect
                                 [::tu/blocks (Schema. [a-field b-field])
                                  [[{:a 12 :b 10}, {:a 0 :b 15}]
                                   [{:a 100 :b 15}]]]
                                 [::tu/blocks (Schema. [a-field b-field])
                                  [[{:a 10 :b 1}, {:a 15 :b 2}]
                                   [{:a 0 :b 15}]]]])]

      (t/is (= [#{{:a 0, :b 15}}]
               (mapv set (tu/<-cursor res)))))

    (t/testing "empty input and output"
      (with-open [res (op/open-ra [:intersect
                                   [::tu/blocks (Schema. [a-field]) []]
                                   [::tu/blocks (Schema. [a-field]) []]])]

        (t/is (empty? (tu/<-cursor res))))

      (with-open [res (op/open-ra [:intersect
                                   [::tu/blocks (Schema. [a-field])
                                    []]
                                   [::tu/blocks (Schema. [a-field])
                                    [[{:a 10}, {:a 15}]]]])]
        (t/is (empty? (tu/<-cursor res))))

      (with-open [res (op/open-ra [:intersect
                                   [::tu/blocks (Schema. [a-field])
                                    [[{:a 10}, {:a 15}]]]
                                   [::tu/blocks (Schema. [a-field])
                                    []]])]

        (t/is (empty? (tu/<-cursor res))))

      (with-open [res (op/open-ra [:intersect
                                   [::tu/blocks (Schema. [a-field])
                                    [[{:a 10}]]]
                                   [::tu/blocks (Schema. [a-field])
                                    [[{:a 20}]]]])]
        (t/is (empty? (tu/<-cursor res)))))))

(t/deftest test-difference
  (let [a-field (ty/->field "a" ty/bigint-type false)
        b-field (ty/->field "b" ty/bigint-type false)]
    (with-open [res (op/open-ra [:difference
                                 [::tu/blocks (Schema. [a-field b-field])
                                  [[{:a 12 :b 10}, {:a 0 :b 15}]
                                   [{:a 100 :b 15}]]]
                                 [::tu/blocks (Schema. [a-field b-field])
                                  [[{:a 10 :b 1}, {:a 15 :b 2}]
                                   [{:a 0 :b 15}]]]])]
      (t/is (= [#{{:a 12, :b 10}}
                #{{:a 100 :b 15}}]
               (mapv set (tu/<-cursor res)))))

    (t/testing "empty input and output"
      (with-open [res (op/open-ra [:difference
                                   [::tu/blocks (Schema. [a-field])
                                    []]
                                   [::tu/blocks (Schema. [a-field])
                                    []]])]
        (t/is (empty? (tu/<-cursor res))))

      (with-open [res (op/open-ra [:difference
                                   [::tu/blocks (Schema. [a-field])
                                    []]
                                   [::tu/blocks (Schema. [a-field])
                                    [[{:a 10}, {:a 15}]]]])]
        (t/is (empty? (tu/<-cursor res))))

      (with-open [res (op/open-ra [:difference
                                   [::tu/blocks (Schema. [a-field]) [[{:a 10}, {:a 15}]]]
                                   [::tu/blocks (Schema. [a-field]) []]])]
        (t/is (= [#{{:a 10} {:a 15}}]
                 (mapv set (tu/<-cursor res))))))))

(t/deftest test-distinct
  (let [a-field (ty/->field "a" ty/bigint-type false)
        b-field (ty/->field "b" ty/bigint-type false)]

    (with-open [res (op/open-ra [:distinct
                                 [::tu/blocks (Schema. [a-field b-field])
                                  [[{:a 12 :b 10}, {:a 0 :b 15}]
                                   [{:a 100 :b 15} {:a 0 :b 15}]
                                   [{:a 100 :b 15}]
                                   [{:a 10 :b 15} {:a 10 :b 15}]]]])]
      (t/is (= [#{{:a 12, :b 10} {:a 0, :b 15}}
                #{{:a 100, :b 15}}
                #{{:a 10, :b 15}}]
               (mapv set (tu/<-cursor res)))))


    (t/testing "already distinct"
      (with-open [res (op/open-ra [:distinct
                                   [::tu/blocks (Schema. [a-field b-field])
                                    [[{:a 12 :b 10}, {:a 0 :b 15}]
                                     [{:a 100 :b 15}]]]])]

        (t/is (= [#{{:a 12, :b 10} {:a 0, :b 15}}
                  #{{:a 100, :b 15}}]
                 (mapv set (tu/<-cursor res))))))

    (t/testing "empty input and output"
      (with-open [res (op/open-ra [:distinct
                                   [::tu/blocks (Schema. [a-field]) []]])]
        (t/is (empty? (tu/<-cursor res)))))))

(t/deftest test-fixpoint
  (with-open [res (op/open-ra [:fixpoint 'R {:incremental? true}
                               [::tu/blocks (Schema. [(ty/->field "a" ty/bigint-type false)
                                                      (ty/->field "b" ty/bigint-type false)])
                                [[{:a 0 :b 1}]]]
                               '[:select (<= a 8)
                                 [:project [{a (+ a 1)}
                                            {b (* (+ a 1) b)}]
                                  R]]])]
    (t/is (= [[{:a 0, :b 1}]
              [{:a 1, :b 1}]
              [{:a 2, :b 2}]
              [{:a 3, :b 6}]
              [{:a 4, :b 24}]
              [{:a 5, :b 120}]
              [{:a 6, :b 720}]
              [{:a 7, :b 5040}]
              [{:a 8, :b 40320}]]
             (tu/<-cursor res)))))

(t/deftest first-tuple-in-rhs-is-taken-into-account-test
  (t/is
   (= [{:x1 2}]
      (op/query-ra
       '[:difference
         [:table [{x1 1} {x1 2}]]
         [:table [{x1 1}]]] {}))))
