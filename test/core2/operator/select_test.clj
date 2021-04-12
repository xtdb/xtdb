(ns core2.operator.select-test
  (:require [clojure.test :as t]
            [core2.operator.select :as select]
            [core2.test-util :as tu]
            [core2.types :as ty])
  (:import [core2.select IVectorSchemaRootPredicate IVectorSchemaRootSelector]
           org.apache.arrow.vector.BigIntVector
           org.apache.arrow.vector.types.pojo.Schema
           org.apache.arrow.vector.types.Types$MinorType
           org.roaringbitmap.RoaringBitmap))

(t/use-fixtures :each tu/with-allocator)

(defn pred->selector ^core2.select.IVectorSchemaRootSelector [^IVectorSchemaRootPredicate pred]
  (reify IVectorSchemaRootSelector
    (select [_ in-root]
      (let [idx-bitmap (RoaringBitmap.)]

        (dotimes [idx (.getRowCount in-root)]
          (when (.test pred in-root idx)
            (.add idx-bitmap idx)))

        idx-bitmap))))

(t/deftest test-select
  (let [a-field (ty/->field "a" (.getType Types$MinorType/BIGINT) false)
        b-field (ty/->field "b" (.getType Types$MinorType/BIGINT) false)]
    (with-open [cursor (tu/->cursor (Schema. [a-field b-field])
                                    [[{:a 12, :b 10}
                                      {:a 0, :b 15}]
                                     [{:a 100, :b 83}]
                                     [{:a 83, :b 100}]])
                select-cursor (select/->select-cursor tu/*allocator* cursor
                                                      (pred->selector (reify IVectorSchemaRootPredicate
                                                                        (test [_ root idx]
                                                                          (> (.get ^BigIntVector (.getVector root a-field) idx)
                                                                             (.get ^BigIntVector (.getVector root b-field) idx))))))]
      (t/is (= [[{:a 12, :b 10}]
                [{:a 100, :b 83}]]
               (tu/<-cursor select-cursor))))))
