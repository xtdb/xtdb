(ns core2.operator.project-test
  (:require [clojure.test :as t]
            [core2.operator.project :as project]
            [core2.relation :as rel]
            [core2.test-util :as tu]
            [core2.types :as ty])
  (:import core2.operator.project.ProjectionSpec
           org.apache.arrow.vector.BigIntVector
           org.apache.arrow.vector.types.pojo.Schema))

(t/use-fixtures :each tu/with-allocator)

(t/deftest test-project
  (with-open [cursor (tu/->cursor (Schema. [(ty/->field "a" ty/bigint-type false)
                                            (ty/->field "b" ty/bigint-type false)])
                                  [[{:a 12, :b 10}
                                    {:a 0, :b 15}]
                                   [{:a 100, :b 83}]])
              project-cursor (project/->project-cursor tu/*allocator* cursor
                                                       [(project/->identity-projection-spec "a")

                                                        (reify ProjectionSpec
                                                          (project [_ allocator in-rel]
                                                            (let [row-count (.rowCount in-rel)

                                                                  a-col (-> (.columnReader in-rel "a")
                                                                            (rel/reader-for-type ty/bigint-type))
                                                                  ^BigIntVector a-vec (.getVector a-col)

                                                                  b-col (-> (.columnReader in-rel "b")
                                                                            (rel/reader-for-type ty/bigint-type))
                                                                  ^BigIntVector b-vec (.getVector b-col)

                                                                  out (BigIntVector. "c" allocator)]
                                                              (.setValueCount out row-count)
                                                              (dotimes [idx row-count]
                                                                (.set out idx (+ (.get a-vec (.getIndex a-col idx))
                                                                                 (.get b-vec (.getIndex b-col idx)))))
                                                              (rel/vec->reader out))))])]
    (t/is (= [[{:a 12, :c 22}, {:a 0, :c 15}]
              [{:a 100, :c 183}]]
             (tu/<-cursor project-cursor)))))
