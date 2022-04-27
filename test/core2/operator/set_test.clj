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

(t/use-fixtures :each tu/with-allocator)

(t/deftest test-union-all
  (let [a-field (ty/->field "a" ty/bigint-type false)
        b-field (ty/->field "b" ty/bigint-type false)]
    (with-open [left-cursor (tu/->cursor (Schema. [a-field b-field])
                                         [[{:a 12 :b 10}, {:a 0 :b 15}]
                                          [{:a 100 :b 15}]])
                right-cursor (tu/->cursor (Schema. [a-field b-field])
                                          [[{:a 10 :b 1}, {:a 15 :b 2}]
                                           [{:a 83 :b 3}]])
                union-cursor (set-op/->union-all-cursor left-cursor right-cursor)]

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
                  union-cursor (set-op/->union-all-cursor left-cursor right-cursor)]

        (t/is (empty? (tu/<-cursor union-cursor)))))))

(t/deftest test-intersection
  (let [a-field (ty/->field "a" ty/bigint-type false)
        b-field (ty/->field "b" ty/bigint-type false)]
    (with-open [left-cursor (tu/->cursor (Schema. [a-field b-field])
                                         [[{:a 12 :b 10}, {:a 0 :b 15}]
                                          [{:a 100 :b 15}]])
                right-cursor (tu/->cursor (Schema. [a-field b-field])
                                          [[{:a 10 :b 1}, {:a 15 :b 2}]
                                           [{:a 0 :b 15}]])
                intersection-cursor (set-op/->intersection-cursor tu/*allocator* #{'a 'b} left-cursor right-cursor)]

      (t/is (= [#{{:a 0, :b 15}}]
               (mapv set (tu/<-cursor intersection-cursor)))))

    (t/testing "empty input and output"
      (with-open [left-cursor (tu/->cursor (Schema. [a-field])
                                           [])
                  right-cursor (tu/->cursor (Schema. [a-field])
                                            [])
                  intersection-cursor (set-op/->intersection-cursor tu/*allocator* #{'a} left-cursor right-cursor)]

        (t/is (empty? (tu/<-cursor intersection-cursor))))

      (with-open [left-cursor (tu/->cursor (Schema. [a-field])
                                           [])
                  right-cursor (tu/->cursor (Schema. [a-field])
                                            [[{:a 10}, {:a 15}]])
                  intersection-cursor (set-op/->intersection-cursor tu/*allocator* #{'a} left-cursor right-cursor)]

        (t/is (empty? (tu/<-cursor intersection-cursor))))

      (with-open [left-cursor (tu/->cursor (Schema. [a-field])
                                           [[{:a 10}, {:a 15}]])
                  right-cursor (tu/->cursor (Schema. [a-field])
                                            [])
                  intersection-cursor (set-op/->intersection-cursor tu/*allocator* #{'a} left-cursor right-cursor)]

        (t/is (empty? (tu/<-cursor intersection-cursor))))

      (with-open [left-cursor (tu/->cursor (Schema. [a-field])
                                           [[{:a 10}]])
                  right-cursor (tu/->cursor (Schema. [a-field])
                                            [[{:a 20}]])
                  intersection-cursor (set-op/->intersection-cursor tu/*allocator* #{'a} left-cursor right-cursor)]

        (t/is (empty? (tu/<-cursor intersection-cursor)))))))

(t/deftest test-difference
  (let [a-field (ty/->field "a" ty/bigint-type false)
        b-field (ty/->field "b" ty/bigint-type false)]
    (with-open [left-cursor (tu/->cursor (Schema. [a-field b-field])
                                         [[{:a 12 :b 10}, {:a 0 :b 15}]
                                          [{:a 100 :b 15}]])
                right-cursor (tu/->cursor (Schema. [a-field b-field])
                                          [[{:a 10 :b 1}, {:a 15 :b 2}]
                                           [{:a 0 :b 15}]])
                difference-cursor (set-op/->difference-cursor tu/*allocator* #{'a 'b} left-cursor right-cursor)]

      (t/is (= [#{{:a 12, :b 10}}
                #{{:a 100 :b 15}}]
               (mapv set (tu/<-cursor difference-cursor)))))

    (t/testing "empty input and output"
      (with-open [left-cursor (tu/->cursor (Schema. [a-field])
                                           [])
                  right-cursor (tu/->cursor (Schema. [a-field])
                                            [])
                  difference-cursor (set-op/->difference-cursor tu/*allocator* #{'a} left-cursor right-cursor)]

        (t/is (empty? (tu/<-cursor difference-cursor))))

      (with-open [left-cursor (tu/->cursor (Schema. [a-field])
                                           [])
                  right-cursor (tu/->cursor (Schema. [a-field])
                                            [[{:a 10}, {:a 15}]])
                  difference-cursor (set-op/->difference-cursor tu/*allocator* #{'a} left-cursor right-cursor)]

        (t/is (empty? (tu/<-cursor difference-cursor))))

      (with-open [left-cursor (tu/->cursor (Schema. [a-field]) [[{:a 10}, {:a 15}]])
                  right-cursor (tu/->cursor (Schema. [a-field]) [])
                  difference-cursor (set-op/->difference-cursor tu/*allocator* #{'a} left-cursor right-cursor)]

        (t/is (= [#{{:a 10} {:a 15}}] (mapv set (tu/<-cursor difference-cursor))))))))

(t/deftest test-distinct
  (let [a-field (ty/->field "a" ty/bigint-type false)
        b-field (ty/->field "b" ty/bigint-type false)]

    (with-open [in-cursor (tu/->cursor (Schema. [a-field b-field])
                                       [[{:a 12 :b 10}, {:a 0 :b 15}]
                                        [{:a 100 :b 15} {:a 0 :b 15}]
                                        [{:a 100 :b 15}]
                                        [{:a 10 :b 15} {:a 10 :b 15}]])
                distinct-cursor (set-op/->distinct-cursor tu/*allocator* #{'a 'b} in-cursor)]

        (t/is (= [#{{:a 12, :b 10} {:a 0, :b 15}}
                  #{{:a 100, :b 15}}
                  #{{:a 10, :b 15}}]
                 (mapv set (tu/<-cursor distinct-cursor)))))


    (t/testing "already distinct"
      (with-open [in-cursor (tu/->cursor (Schema. [a-field b-field])
                                         [[{:a 12 :b 10}, {:a 0 :b 15}]
                                          [{:a 100 :b 15}]])
                  distinct-cursor (set-op/->distinct-cursor tu/*allocator* #{'a 'b} in-cursor)]

        (t/is (= [#{{:a 12, :b 10} {:a 0, :b 15}}
                  #{{:a 100, :b 15}}]
                 (mapv set (tu/<-cursor distinct-cursor))))))

    (t/testing "empty input and output"
      (with-open [in-cursor (tu/->cursor (Schema. [a-field]) [])
                  distinct-cursor (set-op/->distinct-cursor tu/*allocator* #{'a} in-cursor)]

        (t/is (empty? (tu/<-cursor distinct-cursor)))))))

(t/deftest test-fixpoint
  (with-open [in-cursor (tu/->cursor (Schema. [(ty/->field "a" ty/bigint-type false)
                                               (ty/->field "b" ty/bigint-type false)])
                                     [[{:a 0 :b 1}]])
              factorial-cursor (set-op/->fixpoint-cursor
                                tu/*allocator* in-cursor
                                (reify core2.operator.set.IFixpointCursorFactory
                                  (createCursor [_ cursor-factory]
                                    (select/->select-cursor
                                     tu/*allocator*
                                     (project/->project-cursor
                                      tu/*allocator*
                                      (.createCursor cursor-factory)
                                      [(expr/->expression-projection-spec "a" '(+ a 1) '#{a} {})
                                       (expr/->expression-projection-spec "b" '(* (+ a 1) b) '#{a b} {})])

                                     (expr/->expression-relation-selector '(<= a 8) '#{a} {}))))
                                true)]

    (t/is (= [[{:a 0, :b 1}]
              [{:a 1, :b 1}]
              [{:a 2, :b 2}]
              [{:a 3, :b 6}]
              [{:a 4, :b 24}]
              [{:a 5, :b 120}]
              [{:a 6, :b 720}]
              [{:a 7, :b 5040}]
              [{:a 8, :b 40320}]]
             (tu/<-cursor factorial-cursor)))))

(t/deftest first-tuple-in-rhs-is-taken-into-account-test
  (t/is
    (= [{:x1 2}]
       (op/query-ra
         '[:difference
           [:table [{x1 1} {x1 2}]]
           [:table [{x1 1}]]] {}))))


