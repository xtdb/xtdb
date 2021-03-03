(ns core2.operator.project-test
  (:require [clojure.test :as t]
            [core2.operator.project :as project]
            [core2.test-util :as tu]
            [core2.types :as ty])
  (:import core2.operator.project.ProjectionSpec
           org.apache.arrow.vector.BigIntVector
           org.apache.arrow.vector.types.pojo.Schema
           org.apache.arrow.vector.types.Types$MinorType))

(t/use-fixtures :each tu/with-allocator)

(t/deftest test-project
  (let [a-field (ty/->field "a" (.getType Types$MinorType/BIGINT) false)
        b-field (ty/->field "b" (.getType Types$MinorType/BIGINT) false)]
    (with-open [cursor (tu/->cursor (Schema. [a-field b-field])
                                    [[{:a 12, :b 10}
                                      {:a 0, :b 15}]
                                     [{:a 100, :b 83}]])]
      (t/is (= [[{:a 12, :c 22}, {:a 0, :c 15}]
                [{:a 100, :c 183}]]
               (tu/<-cursor (project/->project-cursor tu/*allocator* cursor
                                                      [(project/->identity-projection-spec "a")
                                                       (reify ProjectionSpec
                                                         (project [_ in-root allocator]
                                                           (let [^BigIntVector a-vec (.getVector in-root a-field)
                                                                 ^BigIntVector b-vec (.getVector in-root b-field)
                                                                 ^BigIntVector c-vec (.createVector (ty/->field "c" (.getType Types$MinorType/BIGINT) false) tu/*allocator*)
                                                                 row-count (.getRowCount in-root)]
                                                             (.setValueCount c-vec row-count)
                                                             (dotimes [idx row-count]
                                                               (.set c-vec idx (+ (.get a-vec idx) (.get b-vec idx))))
                                                             c-vec)))])))))))
