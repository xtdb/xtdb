(ns core2.operator.set-test
  (:require [clojure.test :as t]
            [core2.operator.set :as set-op]
            [core2.test-util :as tu]
            [core2.types :as ty])
  (:import org.apache.arrow.vector.types.pojo.Schema
           org.apache.arrow.vector.types.Types$MinorType))

(t/use-fixtures :each tu/with-allocator)

(t/deftest test-union
  (let [a-field (ty/->field "a" (.getType Types$MinorType/BIGINT) false)
        b-field (ty/->field "b" (.getType Types$MinorType/BIGINT) false)]
    (with-open [left-cursor (tu/->cursor (Schema. [a-field b-field])
                                         [[{:a 12 :b 10}, {:a 0 :b 15}]
                                          [{:a 100 :b 15}]])
                right-cursor (tu/->cursor (Schema. [a-field b-field])
                                          [[{:a 10 :b 1}, {:a 15 :b 2}]
                                           [{:a 83 :b 3}]])
                union-cursor (set-op/->union-cursor tu/*allocator* left-cursor right-cursor)]

      (t/is (= [#{{:a 0, :b 15}
                  {:a 12, :b 10}}
                #{{:a 100, :b 15}}
                #{{:a 10, :b 1}
                  {:a 15, :b 2}}
                #{{:a 83, :b 3}}]
               (mapv set (tu/<-cursor union-cursor)))))

    (t/testing "empty input and output"
      (with-open [left-cursor (tu/->cursor (Schema. [a-field])
                                           [])
                  right-cursor (tu/->cursor (Schema. [a-field])
                                            [])
                  union-cursor (set-op/->union-cursor tu/*allocator* left-cursor right-cursor)]

        (t/is (empty? (tu/<-cursor union-cursor)))))))

(t/deftest test-intersection
  (let [a-field (ty/->field "a" (.getType Types$MinorType/BIGINT) false)
        b-field (ty/->field "b" (.getType Types$MinorType/BIGINT) false)]
    (with-open [left-cursor (tu/->cursor (Schema. [a-field b-field])
                                         [[{:a 12 :b 10}, {:a 0 :b 15}]
                                          [{:a 100 :b 15}]])
                right-cursor (tu/->cursor (Schema. [a-field b-field])
                                          [[{:a 10 :b 1}, {:a 15 :b 2}]
                                           [{:a 0 :b 15}]])
                intersection-cursor (set-op/->intersection-cursor tu/*allocator* left-cursor right-cursor)]

      (t/is (= [#{{:a 0, :b 15}}]
               (mapv set (tu/<-cursor intersection-cursor)))))

    (t/testing "empty input and output"
      (with-open [left-cursor (tu/->cursor (Schema. [a-field])
                                           [])
                  right-cursor (tu/->cursor (Schema. [a-field])
                                            [])
                  intersection-cursor (set-op/->intersection-cursor tu/*allocator* left-cursor right-cursor)]

        (t/is (empty? (tu/<-cursor intersection-cursor))))

      (with-open [left-cursor (tu/->cursor (Schema. [a-field])
                                           [])
                  right-cursor (tu/->cursor (Schema. [a-field])
                                            [[{:a 10}, {:a 15}]])
                  intersection-cursor (set-op/->intersection-cursor tu/*allocator* left-cursor right-cursor)]

        (t/is (empty? (tu/<-cursor intersection-cursor))))

      (with-open [left-cursor (tu/->cursor (Schema. [a-field])
                                           [[{:a 10}, {:a 15}]])
                  right-cursor (tu/->cursor (Schema. [a-field])
                                            [])
                  intersection-cursor (set-op/->intersection-cursor tu/*allocator* left-cursor right-cursor)]

        (t/is (empty? (tu/<-cursor intersection-cursor))))

      (with-open [left-cursor (tu/->cursor (Schema. [a-field])
                                           [[{:a 10}]])
                  right-cursor (tu/->cursor (Schema. [a-field])
                                            [[{:a 20}]])
                  intersection-cursor (set-op/->intersection-cursor tu/*allocator* left-cursor right-cursor)]

        (t/is (empty? (tu/<-cursor intersection-cursor)))))))

(t/deftest test-difference
  (let [a-field (ty/->field "a" (.getType Types$MinorType/BIGINT) false)
        b-field (ty/->field "b" (.getType Types$MinorType/BIGINT) false)]
    (with-open [left-cursor (tu/->cursor (Schema. [a-field b-field])
                                         [[{:a 12 :b 10}, {:a 0 :b 15}]
                                          [{:a 100 :b 15}]])
                right-cursor (tu/->cursor (Schema. [a-field b-field])
                                          [[{:a 10 :b 1}, {:a 15 :b 2}]
                                           [{:a 0 :b 15}]])
                difference-cursor (set-op/->difference-cursor tu/*allocator* left-cursor right-cursor)]

      (t/is (= [#{{:a 12, :b 10}}
                #{{:a 100 :b 15}}]
               (mapv set (tu/<-cursor difference-cursor)))))

    (t/testing "empty input and output"
      (with-open [left-cursor (tu/->cursor (Schema. [a-field])
                                           [])
                  right-cursor (tu/->cursor (Schema. [a-field])
                                            [])
                  difference-cursor (set-op/->difference-cursor tu/*allocator* left-cursor right-cursor)]

        (t/is (empty? (tu/<-cursor difference-cursor))))

      (with-open [left-cursor (tu/->cursor (Schema. [a-field])
                                           [])
                  right-cursor (tu/->cursor (Schema. [a-field])
                                            [[{:a 10}, {:a 15}]])
                  difference-cursor (set-op/->difference-cursor tu/*allocator* left-cursor right-cursor)]

        (t/is (empty? (tu/<-cursor difference-cursor))))

      (with-open [left-cursor (tu/->cursor (Schema. [a-field])
                                           [[{:a 10}, {:a 15}]])
                  right-cursor (tu/->cursor (Schema. [a-field])
                                            [])
                  difference-cursor (set-op/->difference-cursor tu/*allocator* left-cursor right-cursor)]

        (t/is (= [#{{:a 10} {:a 15}}] (mapv set (tu/<-cursor difference-cursor))))))))
