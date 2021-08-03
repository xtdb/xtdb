(ns core2.operator.join-test
  (:require [clojure.test :as t]
            [core2.operator.join :as join]
            [core2.test-util :as tu]
            [core2.types :as ty])
  (:import org.apache.arrow.vector.types.pojo.Schema))

(t/use-fixtures :each tu/with-allocator)

(t/deftest test-cross-join
  (let [a-field (ty/->field "a" (ty/->arrow-type :bigint) false)
        b-field (ty/->field "b" (ty/->arrow-type :bigint) false)
        c-field (ty/->field "c" (ty/->arrow-type :bigint) false)]
    (with-open [left-cursor (tu/->cursor (Schema. [a-field])
                                         [[{:a 12}, {:a 0}]
                                          [{:a 100}]])
                right-cursor (tu/->cursor (Schema. [b-field c-field])
                                          [[{:b 10 :c 1}, {:b 15 :c 2}]
                                           [{:b 83 :c 3}]])
                join-cursor (join/->cross-join-cursor tu/*allocator* left-cursor right-cursor)]

      (t/is (= [#{{:a 12, :b 10, :c 1}
                  {:a 0, :b 10, :c 1}
                  {:a 100, :b 10, :c 1}
                  {:a 12, :b 15, :c 2}
                  {:a 0, :b 15, :c 2}
                  {:a 100, :b 15, :c 2}}
                #{{:a 12, :b 83, :c 3}
                  {:a 0, :b 83, :c 3}
                  {:a 100, :b 83, :c 3}}]
               (mapv set (tu/<-cursor join-cursor)))))

    (t/testing "empty input and output"
      (with-open [left-cursor (tu/->cursor (Schema. [a-field])
                                           [[{:a 12}, {:a 0}]
                                            [{:a 100}]])
                  right-cursor (tu/->cursor (Schema. [b-field c-field])
                                            [])
                  join-cursor (join/->cross-join-cursor tu/*allocator* left-cursor right-cursor)]

        (t/is (empty? (tu/<-cursor join-cursor)))))))


(t/deftest test-equi-join
  (let [a-field (ty/->field "a" (ty/->arrow-type :bigint) false)
        b-field (ty/->field "b" (ty/->arrow-type :bigint) false)
        c-field (ty/->field "c" (ty/->arrow-type :bigint) false)]

    (with-open [left-cursor (tu/->cursor (Schema. [a-field])
                                         [[{:a 12}, {:a 0}]
                                          [{:a 100}]])
                right-cursor (tu/->cursor (Schema. [b-field])
                                          [[{:b 12}, {:b 2}]
                                           [{:b 100} {:b 0}]])
                join-cursor (join/->equi-join-cursor tu/*allocator* left-cursor "a" right-cursor "b")]

      (t/is (= [#{{:a 12, :b 12}}
                #{{:a 100, :b 100}
                  {:a 0, :b 0}}]
               (mapv set (tu/<-cursor join-cursor)))))

    (t/testing "same column name"
      (with-open [left-cursor (tu/->cursor (Schema. [a-field])
                                           [[{:a 12}, {:a 0}]
                                            [{:a 100}]])
                  right-cursor (tu/->cursor (Schema. [a-field])
                                            [[{:a 12}, {:a 2}]
                                             [{:a 100} {:a 0}]])
                  join-cursor (join/->equi-join-cursor tu/*allocator* left-cursor "a" right-cursor "a")]

        (t/is (= [#{{:a 12}}
                  #{{:a 100}
                    {:a 0}}]
                 (mapv set (tu/<-cursor join-cursor))))))

    (t/testing "empty input"
      (with-open [left-cursor (tu/->cursor (Schema. [a-field])
                                           [[{:a 12}, {:a 0}]
                                            [{:a 100}]])
                  right-cursor (tu/->cursor (Schema. [b-field])
                                            [])
                  join-cursor (join/->equi-join-cursor tu/*allocator* left-cursor "a" right-cursor "b")]

        (t/is (empty? (tu/<-cursor join-cursor))))

      (with-open [left-cursor (tu/->cursor (Schema. [a-field])
                                           [])
                  right-cursor (tu/->cursor (Schema. [b-field])
                                            [[{:b 12}, {:b 2}]
                                             [{:b 100} {:b 0}]])
                  join-cursor (join/->equi-join-cursor tu/*allocator* left-cursor "a" right-cursor "b")]

        (t/is (empty? (tu/<-cursor join-cursor))))

      (with-open [left-cursor (tu/->cursor (Schema. [a-field])
                                           [[{:a 12}, {:a 0}]
                                            [{:a 100}]])
                  right-cursor (tu/->cursor (Schema. [b-field])
                                            [[]])
                  join-cursor (join/->equi-join-cursor tu/*allocator* left-cursor "a" right-cursor "b")]

        (t/is (empty? (tu/<-cursor join-cursor)))))


    (t/testing "empty output"
      (with-open [left-cursor (tu/->cursor (Schema. [a-field])
                                           [[{:a 12}, {:a 0}]
                                            [{:a 100}]])
                  right-cursor (tu/->cursor (Schema. [b-field c-field])
                                            [[{:b 10 :c 1}, {:b 15 :c 2}]
                                             [{:b 83 :c 3}]])
                  join-cursor (join/->equi-join-cursor tu/*allocator* left-cursor "a" right-cursor "b")]

        (t/is (empty? (tu/<-cursor join-cursor)))))))

(t/deftest test-semi-equi-join
  (let [a-field (ty/->field "a" (ty/->arrow-type :bigint) false)
        b-field (ty/->field "b" (ty/->arrow-type :bigint) false)
        c-field (ty/->field "c" (ty/->arrow-type :bigint) false)]

    (with-open [left-cursor (tu/->cursor (Schema. [a-field])
                                         [[{:a 12}, {:a 0}]
                                          [{:a 100}]])
                right-cursor (tu/->cursor (Schema. [b-field])
                                          [[{:b 12}, {:b 2}]
                                           [{:b 100}]])
                join-cursor (join/->semi-equi-join-cursor tu/*allocator* left-cursor "a" right-cursor "b")]

      (t/is (= [#{{:a 12}}
                #{{:a 100}}]
               (mapv set (tu/<-cursor join-cursor)))))

    (t/testing "empty input"
      (with-open [left-cursor (tu/->cursor (Schema. [a-field])
                                           [[{:a 12}, {:a 0}]
                                            [{:a 100}]])
                  right-cursor (tu/->cursor (Schema. [b-field])
                                            [])
                  join-cursor (join/->semi-equi-join-cursor tu/*allocator* left-cursor "a" right-cursor "b")]

        (t/is (empty? (tu/<-cursor join-cursor))))

      (with-open [left-cursor (tu/->cursor (Schema. [a-field])
                                           [])
                  right-cursor (tu/->cursor (Schema. [b-field])
                                            [[{:b 12}, {:b 2}]
                                             [{:b 100} {:b 0}]])
                  join-cursor (join/->semi-equi-join-cursor tu/*allocator* left-cursor "a" right-cursor "b")]

        (t/is (empty? (tu/<-cursor join-cursor))))

      (with-open [left-cursor (tu/->cursor (Schema. [a-field])
                                           [[{:a 12}, {:a 0}]
                                            [{:a 100}]])
                  right-cursor (tu/->cursor (Schema. [b-field])
                                            [[]])
                  join-cursor (join/->semi-equi-join-cursor tu/*allocator* left-cursor "a" right-cursor "b")]

        (t/is (empty? (tu/<-cursor join-cursor)))))


    (t/testing "empty output"
      (with-open [left-cursor (tu/->cursor (Schema. [a-field])
                                           [[{:a 12}, {:a 0}]
                                            [{:a 100}]])
                  right-cursor (tu/->cursor (Schema. [b-field c-field])
                                            [[{:b 10 :c 1}, {:b 15 :c 2}]
                                             [{:b 83 :c 3}]])
                  join-cursor (join/->semi-equi-join-cursor tu/*allocator* left-cursor "a" right-cursor "b")]

        (t/is (empty? (tu/<-cursor join-cursor)))))))

(t/deftest test-anti-equi-join
  (let [a-field (ty/->field "a" (ty/->arrow-type :bigint) false)
        b-field (ty/->field "b" (ty/->arrow-type :bigint) false)]

    (with-open [left-cursor (tu/->cursor (Schema. [a-field])
                                         [[{:a 12}, {:a 0}]
                                          [{:a 100}]])
                right-cursor (tu/->cursor (Schema. [b-field])
                                          [[{:b 12}, {:b 2}]
                                           [{:b 100}]])
                join-cursor (join/->anti-equi-join-cursor tu/*allocator* left-cursor "a" right-cursor "b")]

      (t/is (= [#{{:a 0}}]
               (mapv set (tu/<-cursor join-cursor)))))

    (t/testing "empty input"
      (with-open [left-cursor (tu/->cursor (Schema. [a-field])
                                           [])
                  right-cursor (tu/->cursor (Schema. [b-field])
                                            [[{:b 12}, {:b 2}]
                                             [{:b 100}]])
                  join-cursor (join/->anti-equi-join-cursor tu/*allocator* left-cursor "a" right-cursor "b")]

        (t/is (empty? (tu/<-cursor join-cursor))))

      (with-open [left-cursor (tu/->cursor (Schema. [a-field])
                                           [[{:a 12}, {:a 0}]
                                            [{:a 100}]])
                  right-cursor (tu/->cursor (Schema. [b-field])
                                            [])
                  join-cursor (join/->anti-equi-join-cursor tu/*allocator* left-cursor "a" right-cursor "b")]

        (t/is (= [#{{:a 12}
                    {:a 0}}
                  #{{:a 100}}]
                 (mapv set (tu/<-cursor join-cursor))))))

    (t/testing "empty output"
      (with-open [left-cursor (tu/->cursor (Schema. [a-field])
                                           [[{:a 12}, {:a 2}]
                                            [{:a 100}]])
                  right-cursor (tu/->cursor (Schema. [b-field])
                                            [[{:b 12}, {:b 2}]
                                             [{:b 100}]])
                  join-cursor (join/->anti-equi-join-cursor tu/*allocator* left-cursor "a" right-cursor "b")]

        (t/is (empty? (tu/<-cursor join-cursor)))))))
