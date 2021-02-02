(ns core2.metadata
  (:require [core2.types :as t])
  (:import java.io.Closeable
           org.apache.arrow.memory.util.ArrowBufPointer
           org.apache.arrow.memory.util.ByteFunctionHelpers
           [org.apache.arrow.vector BigIntVector BitVector DateMilliVector FieldVector Float8Vector VarBinaryVector VarCharVector VectorSchemaRoot]
           org.apache.arrow.vector.complex.DenseUnionVector
           [org.apache.arrow.vector.holders NullableBigIntHolder NullableBitHolder NullableDateMilliHolder NullableFloat8Holder NullableVarBinaryHolder NullableVarCharHolder]
           [org.apache.arrow.vector.types Types Types$MinorType]
           [org.apache.arrow.vector.types.pojo ArrowType ArrowType$ArrowTypeID Field Schema]
           org.apache.arrow.vector.util.Text))

(set! *unchecked-math* :warn-on-boxed)

(definterface IColumnMetadata
  (^void updateMetadata [^org.apache.arrow.vector.FieldVector field-vector])
  (^void writeMetadata [^org.apache.arrow.vector.VectorSchemaRoot metadata-root ^int idx]))

(deftype NullMetadata [^:unsynchronized-mutable ^long cnt]
  IColumnMetadata
  (updateMetadata [this field-vector]
    (set! (.cnt this) (+ cnt (.getValueCount field-vector))))

  (writeMetadata [_ metadata-root idx]
    (.setSafe ^BigIntVector (.getVector metadata-root "count") idx cnt))

  Closeable
  (close [_]))

(deftype BigIntMetadata [^NullableBigIntHolder min-val
                         ^NullableBigIntHolder max-val
                         ^:unsynchronized-mutable ^long cnt]
  IColumnMetadata
  (updateMetadata [this field-vector]
    (let [^BigIntVector field-vector field-vector]
      (set! (.cnt this) (+ cnt (.getValueCount field-vector)))

      (dotimes [idx (.getValueCount field-vector)]
        (let [value (.get field-vector idx)]
          (when (or (zero? (.isSet min-val)) (< value (.value min-val)))
            (.get field-vector idx min-val))

          (when (or (zero? (.isSet max-val)) (> value (.value max-val)))
            (.get field-vector idx max-val))))))

  (writeMetadata [_ metadata-root idx]
    (let [min-vec ^DenseUnionVector (.getVector metadata-root "min")
          max-vec ^DenseUnionVector (.getVector metadata-root "max")
          type-id (.getFlatbufID ArrowType$ArrowTypeID/Int)]
      (.setTypeId min-vec idx type-id)
      (.setSafe min-vec idx min-val)
      (.setTypeId max-vec idx type-id)
      (.setSafe max-vec idx max-val)
      (.setSafe ^BigIntVector (.getVector metadata-root "count") idx cnt)))

  Closeable
  (close [_]))

(deftype Float8Metadata [^NullableFloat8Holder min-val
                         ^NullableFloat8Holder max-val
                         ^:unsynchronized-mutable ^long cnt]
  IColumnMetadata
  (updateMetadata [this field-vector]
    (let [^Float8Vector field-vector field-vector]
      (set! (.cnt this) (+ cnt (.getValueCount field-vector)))

      (dotimes [idx (.getValueCount field-vector)]
        (let [value (.get field-vector idx)]
          (when (or (zero? (.isSet min-val)) (< value (.value min-val)))
            (.get field-vector idx min-val))

          (when (or (zero? (.isSet max-val)) (> value (.value max-val)))
            (.get field-vector idx max-val))))))

  (writeMetadata [_ metadata-root idx]
    (let [min-vec ^DenseUnionVector (.getVector metadata-root "min")
          max-vec ^DenseUnionVector (.getVector metadata-root "max")
          type-id (.getFlatbufID ArrowType$ArrowTypeID/FloatingPoint)]
      (.setTypeId min-vec idx type-id)
      (.setSafe min-vec idx min-val)
      (.setTypeId max-vec idx type-id)
      (.setSafe max-vec idx max-val)
      (.setSafe ^BigIntVector (.getVector metadata-root "count") idx cnt)))

  Closeable
  (close [_]))

(deftype VarBinaryMetadata [^:unsynchronized-mutable ^bytes min-val
                            ^:unsynchronized-mutable ^bytes max-val
                            ^:unsynchronized-mutable ^long cnt
                            ^ArrowBufPointer arrow-buf-pointer]
  IColumnMetadata
  (updateMetadata [this field-vector]
    (let [^VarBinaryVector field-vector field-vector]
      (set! (.cnt this) (+ cnt (.getValueCount field-vector)))

      (dotimes [idx (.getValueCount field-vector)]
        (let [arrow-buf-pointer (.getDataPointer field-vector idx arrow-buf-pointer)]
          (when (or (nil? min-val) (neg? (ByteFunctionHelpers/compare (.getBuf arrow-buf-pointer)
                                                                      (.getOffset arrow-buf-pointer)
                                                                      (+ (.getOffset arrow-buf-pointer) (.getLength arrow-buf-pointer))
                                                                      min-val
                                                                      0
                                                                      (alength min-val))))
            (set! (.min-val this) (.get field-vector idx)))

          (when (or (nil? max-val) (pos? (ByteFunctionHelpers/compare (.getBuf arrow-buf-pointer)
                                                                      (.getOffset arrow-buf-pointer)
                                                                      (+ (.getOffset arrow-buf-pointer) (.getLength arrow-buf-pointer))
                                                                      max-val
                                                                      0
                                                                      (alength max-val))))
            (set! (.max-val this) (.get field-vector idx)))))))


  (writeMetadata [_ metadata-root idx]
    (let [min-vec ^DenseUnionVector (.getVector metadata-root "min")
          max-vec ^DenseUnionVector (.getVector metadata-root "max")
          type-id (.getFlatbufID ArrowType$ArrowTypeID/Binary)
          allocator (.getAllocator min-vec)
          holder (NullableVarBinaryHolder.)]
      (set! (.isSet holder) 1)
      (set! (.start holder) 0)
      (with-open [b (.buffer allocator (max (alength min-val) (alength max-val)))]
        (set! (.buffer holder) b)

        (set! (.end holder) (alength min-val))
        (.setBytes b 0 min-val)
        (.setTypeId min-vec idx type-id)
        (.setSafe min-vec idx holder)

        (set! (.end holder) (alength max-val))
        (.setBytes b 0 max-val)
        (.setTypeId max-vec idx type-id)
        (.setSafe max-vec idx holder))
      (.setSafe ^BigIntVector (.getVector metadata-root "count") idx cnt)))

  Closeable
  (close [_]))

(deftype VarCharMetadata [^:unsynchronized-mutable ^bytes min-val
                          ^:unsynchronized-mutable ^bytes max-val
                          ^:unsynchronized-mutable ^long cnt
                          ^ArrowBufPointer arrow-buf-pointer]
  IColumnMetadata
  (updateMetadata [this field-vector]
    (let [^VarCharVector field-vector field-vector]
      (set! (.cnt this) (+ cnt (.getValueCount field-vector)))

      (dotimes [idx (.getValueCount field-vector)]
        (let [arrow-buf-pointer (.getDataPointer field-vector idx arrow-buf-pointer)]
          (when (or (nil? min-val) (neg? (ByteFunctionHelpers/compare (.getBuf arrow-buf-pointer)
                                                                      (.getOffset arrow-buf-pointer)
                                                                      (+ (.getOffset arrow-buf-pointer) (.getLength arrow-buf-pointer))
                                                                      min-val
                                                                      0
                                                                      (alength min-val))))
            (set! (.min-val this) (.get field-vector idx)))

          (when (or (nil? max-val) (pos? (ByteFunctionHelpers/compare (.getBuf arrow-buf-pointer)
                                                                      (.getOffset arrow-buf-pointer)
                                                                      (+ (.getOffset arrow-buf-pointer) (.getLength arrow-buf-pointer))
                                                                      max-val
                                                                      0
                                                                      (alength max-val))))
            (set! (.max-val this) (.get field-vector idx)))))))

  (writeMetadata [_ metadata-root idx]
    (let [min-vec ^DenseUnionVector (.getVector metadata-root "min")
          max-vec ^DenseUnionVector (.getVector metadata-root "max")
          type-id (.getFlatbufID ArrowType$ArrowTypeID/Utf8)
          allocator (.getAllocator min-vec)
          holder (NullableVarCharHolder.)]
      (set! (.isSet holder) 1)
      (set! (.start holder) 0)
      (with-open [b (.buffer allocator (max (alength min-val) (alength max-val)))]
        (set! (.buffer holder) b)

        (set! (.end holder) (alength min-val))
        (.setBytes b 0 min-val)
        (.setTypeId min-vec idx type-id)
        (.setSafe min-vec idx holder)

        (set! (.end holder) (alength max-val))
        (.setBytes b 0 max-val)
        (.setTypeId max-vec idx type-id)
        (.setSafe max-vec idx holder))
      (.setSafe ^BigIntVector (.getVector metadata-root "count") idx cnt)))

  Closeable
  (close [_]))

(deftype BitMetadata [^NullableBitHolder min-val
                      ^NullableBitHolder max-val
                      ^:unsynchronized-mutable ^long cnt]
  IColumnMetadata
  (updateMetadata [this field-vector]
    (let [^BitVector field-vector field-vector]
      (set! (.cnt this) (+ cnt (.getValueCount field-vector)))

      (dotimes [idx (.getValueCount field-vector)]
        (let [value (.get field-vector idx)]
          (when (or (zero? (.isSet min-val)) (< value (.value min-val)))
            (.get field-vector idx min-val))

          (when (or (zero? (.isSet max-val)) (> value (.value max-val)))
            (.get field-vector idx max-val))))))

  (writeMetadata [_ metadata-root idx]
    (let [min-vec ^DenseUnionVector (.getVector metadata-root "min")
          max-vec ^DenseUnionVector (.getVector metadata-root "max")
          type-id (.getFlatbufID ArrowType$ArrowTypeID/Bool)]
      (.setTypeId min-vec idx type-id)
      (.setSafe min-vec idx min-val)
      (.setTypeId max-vec idx type-id)
      (.setSafe max-vec idx max-val)
      (.setSafe ^BigIntVector (.getVector metadata-root "count") idx cnt)))

  Closeable
  (close [_]))

(deftype DateMilliMetadata [^NullableDateMilliHolder min-val
                            ^NullableDateMilliHolder max-val
                            ^:unsynchronized-mutable ^long cnt]
  IColumnMetadata
  (updateMetadata [this field-vector]
    (let [^DateMilliVector field-vector field-vector]
      (set! (.cnt this) (+ cnt (.getValueCount field-vector)))

      (dotimes [idx (.getValueCount field-vector)]
        (let [value (.get field-vector idx)]
          (when (or (zero? (.isSet min-val)) (< value (.value min-val)))
            (.get field-vector idx min-val))

          (when (or (zero? (.isSet max-val)) (> value (.value max-val)))
            (.get field-vector idx max-val))))))

  (writeMetadata [_ metadata-root idx]
    (let [min-vec ^DenseUnionVector (.getVector metadata-root "min")
          max-vec ^DenseUnionVector (.getVector metadata-root "max")
          type-id (.getFlatbufID ArrowType$ArrowTypeID/Date)]
      (.setTypeId min-vec idx type-id)
      (.setSafe min-vec idx min-val)
      (.setTypeId max-vec idx type-id)
      (.setSafe max-vec idx max-val)
      (.setSafe ^BigIntVector (.getVector metadata-root "count") idx cnt)))

  Closeable
  (close [_]))

(defn ->metadata ^core2.metadata.IColumnMetadata [^ArrowType arrow-type]
  (condp = (Types/getMinorTypeForArrowType arrow-type)
    Types$MinorType/NULL (->NullMetadata 0)
    Types$MinorType/BIGINT (->BigIntMetadata (NullableBigIntHolder.) (NullableBigIntHolder.) 0)
    Types$MinorType/FLOAT8 (->Float8Metadata (NullableFloat8Holder.) (NullableFloat8Holder.) 0)
    Types$MinorType/VARBINARY (->VarBinaryMetadata nil nil 0 (ArrowBufPointer.))
    Types$MinorType/VARCHAR (->VarCharMetadata nil nil 0 (ArrowBufPointer.))
    Types$MinorType/BIT (->BitMetadata (NullableBitHolder.) (NullableBitHolder.) 0)
    Types$MinorType/DECIMAL (throw (UnsupportedOperationException.))
    Types$MinorType/DATEMILLI (->DateMilliMetadata (NullableDateMilliHolder.) (NullableDateMilliHolder.) 0)
    (throw (UnsupportedOperationException.))))

(def ^org.apache.arrow.vector.types.pojo.Schema metadata-schema
  (Schema. [(t/->field "file" (.getType Types$MinorType/VARCHAR) true)
            (t/->field "column" (.getType Types$MinorType/VARCHAR) true)
            (t/->field "field" (.getType Types$MinorType/VARCHAR) true)
            (t/->dense-union-field-with-flatbuf-ids "min" t/dense-union-fields-in-flatbuf-id-order)
            (t/->dense-union-field-with-flatbuf-ids "max" t/dense-union-fields-in-flatbuf-id-order)
            (t/->field "count" (.getType Types$MinorType/BIGINT) true)]))
