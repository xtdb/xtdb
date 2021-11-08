(ns core2.align-test
  (:require [clojure.test :as t]
            [core2.align :as align]
            [core2.expression :as expr]
            [core2.test-util :as tu]
            [core2.vector.indirect :as iv])
  (:import java.util.List
           [org.apache.arrow.vector BigIntVector VarCharVector VectorSchemaRoot]
           org.apache.arrow.vector.util.Text))

(t/use-fixtures :each tu/with-allocator)

(t/deftest test-align
  (with-open [age-vec (doto (BigIntVector. "age" tu/*allocator*)
                        (.setValueCount 5)
                        (.setSafe 0 12)
                        (.setSafe 1 42)
                        (.setSafe 2 15)
                        (.setSafe 3 83)
                        (.setSafe 4 25))

              age-row-id-vec (doto (BigIntVector. "_row-id" tu/*allocator*)
                               (.setValueCount 5)
                               (.setSafe 0 2)
                               (.setSafe 1 5)
                               (.setSafe 2 9)
                               (.setSafe 3 12)
                               (.setSafe 4 13))

              age-root (let [^List vecs [age-row-id-vec age-vec]]
                         (VectorSchemaRoot. vecs))

              name-vec (doto (VarCharVector. "name" tu/*allocator*)
                         (.setValueCount 4)
                         (.setSafe 0 (Text. "Al"))
                         (.setSafe 1 (Text. "Dave"))
                         (.setSafe 2 (Text. "Bob"))
                         (.setSafe 3 (Text. "Steve")))

              name-row-id-vec (doto (BigIntVector. "_row-id" tu/*allocator*)
                                (.setValueCount 4)
                                (.setSafe 0 1)
                                (.setSafe 1 2)
                                (.setSafe 2 9)
                                (.setSafe 3 13))

              name-root (let [^List vecs [name-row-id-vec name-vec]]
                          (VectorSchemaRoot. vecs))]

    (let [row-ids (doto (align/->row-id-bitmap (.select (expr/->expression-column-selector '(<= age 30) {})
                                                        (iv/->direct-vec age-vec))
                                               age-row-id-vec)
                    (.and (align/->row-id-bitmap (.select (expr/->expression-column-selector '(<= name "Frank") {})
                                                          (iv/->direct-vec name-vec))
                                                 name-row-id-vec)))
          roots [name-root age-root]]
      (t/is (= [{:name "Dave", :age 12}
                {:name "Bob", :age 15}]
               (iv/rel->rows (align/align-vectors roots row-ids nil)))))))
