(ns core2.align-test
  (:require [clojure.test :as t]
            [core2.align :as align]
            [core2.select :as sel]
            [core2.test-util :as tu]
            [core2.types :as ty]
            [core2.util :as util])
  (:import java.util.List
           [org.apache.arrow.vector BigIntVector VectorSchemaRoot]
           org.apache.arrow.vector.complex.DenseUnionVector
           org.apache.arrow.vector.holders.NullableBigIntHolder
           org.apache.arrow.vector.util.Text))

(t/use-fixtures :each tu/with-allocator)

(def ^:private ^long bigint-type-id
  (-> (ty/primitive-type->arrow-type :bigint)
      (ty/arrow-type->type-id)))

(def ^:private ^long varchar-type-id
  (-> (ty/primitive-type->arrow-type :varchar)
      (ty/arrow-type->type-id)))

(t/deftest test-align
  (with-open [age-vec (doto ^DenseUnionVector (.createVector (ty/->primitive-dense-union-field "age" #{:bigint}) tu/*allocator*)
                        (util/set-value-count 5)
                        (util/write-type-id 0 bigint-type-id)
                        (util/write-type-id 1 bigint-type-id)
                        (util/write-type-id 2 bigint-type-id)
                        (util/write-type-id 3 bigint-type-id)
                        (util/write-type-id 4 bigint-type-id)
                        (-> (.getBigIntVector bigint-type-id)
                            (doto (.setSafe 0 12)
                              (.setSafe 1 42)
                              (.setSafe 2 15)
                              (.setSafe 3 83)
                              (.setSafe 4 25))))

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
                         (util/write-type-id 0 varchar-type-id)
                         (util/write-type-id 1 varchar-type-id)
                         (util/write-type-id 2 varchar-type-id)
                         (util/write-type-id 3 varchar-type-id)
                         (-> (.getVarCharVector varchar-type-id)
                             (doto (.setSafe 0 (Text. "Al"))
                               (.setSafe 1 (Text. "Dave"))
                               (.setSafe 2 (Text. "Bob"))
                               (.setSafe 3 (Text. "Steve")))))

              name-row-id-vec (doto (BigIntVector. "_row-id" tu/*allocator*)
                                (.setValueCount 4)
                                (.setSafe 0 1)
                                (.setSafe 1 2)
                                (.setSafe 2 9)
                                (.setSafe 3 13))

              name-root (let [^List vecs [name-row-id-vec name-vec]]
                          (VectorSchemaRoot. vecs))]

    (let [row-ids (doto (align/->row-id-bitmap (sel/select age-vec (sel/->dense-union-pred
                                                                    (sel/->vec-pred sel/pred<= (doto (NullableBigIntHolder.)
                                                                                                 (-> .isSet (set! 1))
                                                                                                 (-> .value (set! 30))))
                                                                    bigint-type-id))
                                               age-row-id-vec)
                    (.and (align/->row-id-bitmap (sel/select name-vec (sel/->dense-union-pred
                                                                       (sel/->str-pred sel/pred<= "Frank")
                                                                       varchar-type-id))
                                                 name-row-id-vec)))
          roots [name-root age-root]]
      (with-open [out-root (VectorSchemaRoot/create (align/align-schemas [(.getSchema name-root) (.getSchema age-root)])
                                                    tu/*allocator*)]
        (align/align-vectors roots row-ids out-root)
        (t/is (= [[(Text. "Dave") 12]
                  [(Text. "Bob") 15]]
                 (tu/root->rows out-root)))))))
