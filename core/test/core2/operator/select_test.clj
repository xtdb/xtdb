(ns core2.operator.select-test
  (:require [clojure.test :as t]
            [core2.operator.select :as select]
            [core2.test-util :as tu]
            [core2.types :as ty])
  (:import core2.operator.select.IRelationSelector
           org.apache.arrow.vector.BigIntVector
           org.apache.arrow.vector.types.pojo.Schema
           org.roaringbitmap.RoaringBitmap))

(t/use-fixtures :each tu/with-allocator)

(t/deftest test-select
  (let [a-field (ty/->field "a" (ty/->arrow-type :bigint) false)
        b-field (ty/->field "b" (ty/->arrow-type :bigint) false)]
    (with-open [cursor (tu/->cursor (Schema. [a-field b-field])
                                    [[{:a 12, :b 10}
                                      {:a 0, :b 15}]
                                     [{:a 100, :b 83}]
                                     [{:a 83, :b 100}]])
                select-cursor (select/->select-cursor tu/*allocator* cursor
                                                      (reify IRelationSelector
                                                        (select [_ in-rel]
                                                          (let [idxs (RoaringBitmap.)
                                                                ^BigIntVector a-vec (.getVector (.readColumn in-rel "a"))
                                                                ^BigIntVector b-vec (.getVector (.readColumn in-rel "b"))]
                                                            (dotimes [idx (.rowCount in-rel)]
                                                              (when (> (.get a-vec idx)
                                                                       (.get b-vec idx))
                                                                (.add idxs idx)))

                                                            idxs))))]
      (t/is (= [[{:a 12, :b 10}]
                [{:a 100, :b 83}]]
               (tu/<-cursor select-cursor))))))
