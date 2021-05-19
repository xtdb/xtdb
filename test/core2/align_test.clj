(ns core2.align-test
  (:require [clojure.test :as t]
            [core2.align :as align]
            [core2.test-util :as tu]
            [core2.types :as ty]
            [core2.util :as util]
            [core2.expression :as expr]
            [core2.relation :as rel])
  (:import java.util.List
           [org.apache.arrow.vector BigIntVector VectorSchemaRoot]
           org.apache.arrow.vector.complex.DenseUnionVector))

(t/use-fixtures :each tu/with-allocator)

(t/deftest test-align
  (with-open [age-vec (doto ^DenseUnionVector (.createVector (ty/->primitive-dense-union-field "age" #{:bigint}) tu/*allocator*)
                        (util/set-value-count 5)
                        (ty/set-safe! 0 12)
                        (ty/set-safe! 1 42)
                        (ty/set-safe! 2 15)
                        (ty/set-safe! 3 83)
                        (ty/set-safe! 4 25))

              age-row-id-vec (doto (BigIntVector. "_row-id" tu/*allocator*)
                               (.setValueCount 5)
                               (.setSafe 0 2)
                               (.setSafe 1 5)
                               (.setSafe 2 9)
                               (.setSafe 3 12)
                               (.setSafe 4 13))

              age-root (let [^List vecs [age-row-id-vec age-vec]]
                         (VectorSchemaRoot. vecs))

              name-vec (doto ^DenseUnionVector (.createVector (ty/->primitive-dense-union-field "name" #{:varchar}) tu/*allocator*)
                         (util/set-value-count 4)
                         (ty/set-safe! 0 "Al")
                         (ty/set-safe! 1 "Dave")
                         (ty/set-safe! 2 "Bob")
                         (ty/set-safe! 3 "Steve"))

              name-row-id-vec (doto (BigIntVector. "_row-id" tu/*allocator*)
                                (.setValueCount 4)
                                (.setSafe 0 1)
                                (.setSafe 1 2)
                                (.setSafe 2 9)
                                (.setSafe 3 13))

              name-root (let [^List vecs [name-row-id-vec name-vec]]
                          (VectorSchemaRoot. vecs))]

    (let [row-ids (doto (align/->row-id-bitmap (.select (expr/->expression-column-selector (expr/form->expr '(<= age 30)))
                                                        (rel/<-vector age-vec))
                                               age-row-id-vec)
                    (.and (align/->row-id-bitmap (.select (expr/->expression-column-selector (expr/form->expr '(<= name "Frank")))
                                                          (rel/<-vector name-vec))
                                                 name-row-id-vec)))
          roots [name-root age-root]]
      (t/is (= [{:name "Dave", :age 12}
                {:name "Bob", :age 15}]
               (tu/rel->rows (align/align-vectors roots row-ids nil)))))))
