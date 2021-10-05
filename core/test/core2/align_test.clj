(ns core2.align-test
  (:require [clojure.test :as t]
            [core2.align :as align]
            [core2.test-util :as tu]
            [core2.types :as ty]
            [core2.expression :as expr]
            [core2.relation :as rel])
  (:import java.util.List
           [org.apache.arrow.vector BigIntVector VectorSchemaRoot]
           org.apache.arrow.vector.complex.DenseUnionVector))

(t/use-fixtures :each tu/with-allocator)

(t/deftest test-align
  (with-open [age-vec (DenseUnionVector/empty "age" tu/*allocator*)

              age-row-id-vec (doto (BigIntVector. "_row-id" tu/*allocator*)
                               (.setValueCount 5)
                               (.setSafe 0 2)
                               (.setSafe 1 5)
                               (.setSafe 2 9)
                               (.setSafe 3 12)
                               (.setSafe 4 13))

              age-root (let [^List vecs [age-row-id-vec age-vec]]
                         (VectorSchemaRoot. vecs))

              name-vec (DenseUnionVector/empty "name" tu/*allocator*)

              name-row-id-vec (doto (BigIntVector. "_row-id" tu/*allocator*)
                                (.setValueCount 4)
                                (.setSafe 0 1)
                                (.setSafe 1 2)
                                (.setSafe 2 9)
                                (.setSafe 3 13))

              name-root (let [^List vecs [name-row-id-vec name-vec]]
                          (VectorSchemaRoot. vecs))]

    (let [age-writer (-> (rel/vec->writer age-vec)
                         (.writerForType ty/bigint-type))]
      (doseq [age [12 42 15 83 25]]
        (ty/set-safe! (.getVector age-writer) (.appendIndex age-writer) age)))

    (let [name-writer (-> (rel/vec->writer name-vec)
                         (.writerForType ty/varchar-type))]
      (doseq [name ["Al" "Dave" "Bob" "Steve"]]
        (ty/set-safe! (.getVector name-writer) (.appendIndex name-writer) name)))

    (let [row-ids (doto (align/->row-id-bitmap (.select (expr/->expression-column-selector '(<= age 30) {})
                                                        (rel/vec->reader age-vec))
                                               age-row-id-vec)
                    (.and (align/->row-id-bitmap (.select (expr/->expression-column-selector '(<= name "Frank") {})
                                                          (rel/vec->reader name-vec))
                                                 name-row-id-vec)))
          roots [name-root age-root]]
      (t/is (= [{:name "Dave", :age 12}
                {:name "Bob", :age 15}]
               (rel/rel->rows (align/align-vectors roots row-ids nil)))))))
