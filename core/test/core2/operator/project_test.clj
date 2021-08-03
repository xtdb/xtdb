(ns core2.operator.project-test
  (:require [clojure.test :as t]
            [core2.operator.project :as project]
            [core2.relation :as rel]
            [core2.test-util :as tu]
            [core2.types :as ty])
  (:import core2.operator.project.ProjectionSpec
           org.apache.arrow.vector.types.pojo.Schema))

(t/use-fixtures :each tu/with-allocator)

(t/deftest test-project
  (with-open [cursor (tu/->cursor (Schema. [(ty/->field "a" (ty/->arrow-type :bigint) false)
                                            (ty/->field "b" (ty/->arrow-type :bigint) false)])
                                  [[{:a 12, :b 10}
                                    {:a 0, :b 15}]
                                   [{:a 100, :b 83}]])
              project-cursor (project/->project-cursor tu/*allocator* cursor
                                                       [(project/->identity-projection-spec "a")

                                                        (reify ProjectionSpec
                                                          (project [_ allocator in-rel]
                                                            (let [row-count (.rowCount in-rel)
                                                                  a-col (.readColumn in-rel "a")
                                                                  b-col (.readColumn in-rel "b")
                                                                  out-col (rel/->fresh-append-column allocator "c")]
                                                              (dotimes [idx row-count]
                                                                (.appendLong out-col (+ (.getLong a-col idx) (.getLong b-col idx))))
                                                              (.read out-col))))])]
    (t/is (= [[{:a 12, :c 22}, {:a 0, :c 15}]
              [{:a 100, :c 183}]]
             (tu/<-cursor project-cursor)))))
