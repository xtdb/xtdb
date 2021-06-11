(ns core2.operator.set-test
  (:require [clojure.test :as t]
            [core2.operator.project :as project]
            [core2.operator.select :as select]
            [core2.operator.set :as set-op]
            [core2.relation :as rel]
            [core2.test-util :as tu]
            [core2.types :as ty])
  (:import core2.operator.project.ProjectionSpec
           core2.operator.select.IRelationSelector
           org.apache.arrow.vector.types.pojo.Schema
           org.roaringbitmap.RoaringBitmap))

(t/use-fixtures :each tu/with-allocator)

(t/deftest test-union-all
  (let [a-field (ty/->field "a" (ty/->arrow-type :bigint) false)
        b-field (ty/->field "b" (ty/->arrow-type :bigint) false)]
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
  (let [a-field (ty/->field "a" (ty/->arrow-type :bigint) false)
        b-field (ty/->field "b" (ty/->arrow-type :bigint) false)]
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
  (let [a-field (ty/->field "a" (ty/->arrow-type :bigint) false)
        b-field (ty/->field "b" (ty/->arrow-type :bigint) false)]
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

(t/deftest test-distinct
  (let [a-field (ty/->field "a" (ty/->arrow-type :bigint) false)
        b-field (ty/->field "b" (ty/->arrow-type :bigint) false)]

    (with-open [in-cursor (tu/->cursor (Schema. [a-field b-field])
                                       [[{:a 12 :b 10}, {:a 0 :b 15}]
                                        [{:a 100 :b 15} {:a 0 :b 15}]
                                        [{:a 100 :b 15}]
                                        [{:a 10 :b 15} {:a 10 :b 15}]])
                distinct-cursor (set-op/->distinct-cursor tu/*allocator* in-cursor)]

        (t/is (= [#{{:a 12, :b 10} {:a 0, :b 15}}
                  #{{:a 100, :b 15}}
                  #{{:a 10, :b 15}}]
                 (mapv set (tu/<-cursor distinct-cursor)))))


    (t/testing "already distinct"
      (with-open [in-cursor (tu/->cursor (Schema. [a-field b-field])
                                         [[{:a 12 :b 10}, {:a 0 :b 15}]
                                          [{:a 100 :b 15}]])
                  distinct-cursor (set-op/->distinct-cursor tu/*allocator* in-cursor)]

        (t/is (= [#{{:a 12, :b 10} {:a 0, :b 15}}
                  #{{:a 100, :b 15}}]
                 (mapv set (tu/<-cursor distinct-cursor))))))

    (t/testing "empty input and output"
      (with-open [in-cursor (tu/->cursor (Schema. [a-field])
                                          [])
                  distinct-cursor (set-op/->distinct-cursor tu/*allocator* in-cursor)]

        (t/is (empty? (tu/<-cursor distinct-cursor)))))))

(t/deftest test-fixpoint
  (with-open [in-cursor (tu/->cursor (Schema. [(ty/->field "a" (ty/->arrow-type :bigint) false)
                                               (ty/->field "b" (ty/->arrow-type :bigint) false)])
                                     [[{:a 0 :b 1}]])
              factorial-cursor (set-op/->fixpoint-cursor
                                tu/*allocator* in-cursor
                                (reify core2.operator.set.IFixpointCursorFactory
                                  (createCursor [_ cursor-factory]
                                    (select/->select-cursor
                                     (project/->project-cursor
                                      tu/*allocator*
                                      (.createCursor cursor-factory)
                                      [(reify ProjectionSpec
                                         (project [_ allocator in-rel]
                                           (let [a-col (.readColumn in-rel "a")
                                                 out-col (rel/->fresh-append-column allocator "a")]
                                             (dotimes [idx (.rowCount in-rel)]
                                               (.appendLong out-col (inc (.getLong a-col idx))))
                                             (.read out-col))))

                                       (reify ProjectionSpec
                                         (project [_ allocator in-rel]
                                           (let [a-col (.readColumn in-rel "a")
                                                 b-col (.readColumn in-rel "b")
                                                 out-col (rel/->fresh-append-column allocator "b")]
                                             (dotimes [idx (.rowCount in-rel)]
                                               (.appendLong out-col (* (inc (.getLong a-col idx))
                                                                       (.getLong b-col idx))))
                                             (.read out-col))))])

                                     (reify IRelationSelector
                                       (select [_ in-rel]
                                         (let [idx-bitmap (RoaringBitmap.)
                                               a-col (.readColumn in-rel "a")]
                                           (dotimes [idx (.rowCount in-rel)]
                                             (when (<= (.getLong a-col idx) 8)
                                               (.add idx-bitmap idx)))

                                           idx-bitmap))))))
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
