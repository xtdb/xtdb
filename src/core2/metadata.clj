(ns core2.metadata
  (:require [core2.types :as t])
  (:import java.io.Closeable
           org.apache.arrow.memory.util.ArrowBufPointer
           org.apache.arrow.memory.util.ByteFunctionHelpers
           [org.apache.arrow.vector BigIntVector BitVector DateMilliVector FieldVector Float8Vector VarBinaryVector VarCharVector]
           [org.apache.arrow.vector.complex StructVector UnionVector]
           [org.apache.arrow.vector.holders NullableBigIntHolder NullableBitHolder NullableDateMilliHolder NullableFloat8Holder]
           [org.apache.arrow.vector.types Types Types$MinorType]
           [org.apache.arrow.vector.types.pojo ArrowType Field Schema]
           org.apache.arrow.vector.util.Text))

(definterface IColumnMetadata
  (^void updateMetadata [^org.apache.arrow.vector.FieldVector field-vector ^int idx])
  (^void writeMetadata [^org.apache.arrow.vector.complex.StructVector metadata-vector ^int idx]))

(deftype BitMetadata [^NullableBitHolder min-val
                      ^NullableBitHolder max-val
                      ^:unsynchronized-mutable ^long cnt]
  IColumnMetadata
  (updateMetadata [this field-vector idx]
    (let [^BitVector field-vector field-vector
          value (.get field-vector idx)]
      (set! (.cnt this) (inc cnt))

      (when (or (zero? (.isSet min-val)) (< value (.value min-val)))
        (.get field-vector idx min-val))

      (when (or (zero? (.isSet max-val)) (> value (.value max-val)))
        (.get field-vector idx max-val))))

  (writeMetadata [_ metadata-vector idx]
    (.setSafe ^BitVector (.getChild metadata-vector "min" BitVector) idx min-val)
    (.setSafe ^BitVector (.getChild metadata-vector "max" BitVector) idx max-val)
    (.setSafe ^BigIntVector (.getChild metadata-vector "count" BigIntVector) idx cnt))

  Closeable
  (close [_]))

(deftype BigIntMetadata [^NullableBigIntHolder min-val
                         ^NullableBigIntHolder max-val
                         ^:unsynchronized-mutable ^long cnt]
  IColumnMetadata
  (updateMetadata [this field-vector idx]
    (let [^BigIntVector field-vector field-vector
          value (.get field-vector idx)]
      (set! (.cnt this) (inc cnt))

      (when (or (zero? (.isSet min-val)) (< value (.value min-val)))
        (.get field-vector idx min-val))

      (when (or (zero? (.isSet max-val)) (> value (.value max-val)))
        (.get field-vector idx max-val))))

  (writeMetadata [_ metadata-vector idx]
    (.setSafe ^BigIntVector (.getChild metadata-vector "min" BigIntVector) idx min-val)
    (.setSafe ^BigIntVector (.getChild metadata-vector "max" BigIntVector) idx max-val)
    (.setSafe ^BigIntVector (.getChild metadata-vector "count" BigIntVector) idx cnt))

  Closeable
  (close [_]))

(deftype DateMilliMetadata [^NullableDateMilliHolder min-val
                            ^NullableDateMilliHolder max-val
                            ^:unsynchronized-mutable ^long cnt]
  IColumnMetadata
  (updateMetadata [this field-vector idx]
    (let [^DateMilliVector field-vector field-vector
          value (.get field-vector idx)]
      (set! (.cnt this) (inc cnt))

      (when (or (zero? (.isSet min-val)) (< value (.value min-val)))
        (.get field-vector idx min-val))

      (when (or (zero? (.isSet max-val)) (> value (.value max-val)))
        (.get field-vector idx max-val))))

  (writeMetadata [_ metadata-vector idx]
    (.setSafe ^DateMilliVector (.getChild metadata-vector "min" DateMilliVector) idx min-val)
    (.setSafe ^DateMilliVector (.getChild metadata-vector "max" DateMilliVector) idx max-val)
    (.setSafe ^BigIntVector (.getChild metadata-vector "count" BigIntVector) idx cnt))

  Closeable
  (close [_]))

(deftype Float8Metadata [^NullableFloat8Holder min-val
                         ^NullableFloat8Holder max-val
                         ^:unsynchronized-mutable ^long cnt]
  IColumnMetadata
  (updateMetadata [this field-vector idx]
    (let [^Float8Vector field-vector field-vector
          value (.get field-vector idx)]
      (set! (.cnt this) (inc cnt))

      (when (or (zero? (.isSet min-val)) (< value (.value min-val)))
        (.get field-vector idx min-val))

      (when (or (zero? (.isSet max-val)) (> value (.value max-val)))
        (.get field-vector idx max-val))))

  (writeMetadata [_ metadata-vector idx]
    (let [^StructVector metadata-vector metadata-vector]
      (.setSafe ^Float8Vector (.getChild metadata-vector "min" Float8Vector) idx min-val)
      (.setSafe ^Float8Vector (.getChild metadata-vector "max" Float8Vector) idx max-val)
      (.setSafe ^BigIntVector (.getChild metadata-vector "count" BigIntVector) idx cnt)))

  Closeable
  (close [_]))

(deftype VarBinaryMetadata [^:unsynchronized-mutable ^bytes min-val
                            ^:unsynchronized-mutable ^bytes max-val
                            ^:unsynchronized-mutable ^long cnt
                            ^ArrowBufPointer arrow-buf-pointer]
  IColumnMetadata
  (updateMetadata [this field-vector idx]
    (let [^VarBinaryVector field-vector field-vector
          arrow-buf-pointer (.getDataPointer field-vector idx arrow-buf-pointer)]
      (set! (.cnt this) (inc cnt))

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
        (set! (.max-val this) (.get field-vector idx)))))

  (writeMetadata [_ metadata-vector idx]
    (let [^StructVector metadata-vector metadata-vector]
      (.setSafe ^VarBinaryVector (.getChild metadata-vector "min" VarBinaryVector) idx min-val)
      (.setSafe ^VarBinaryVector (.getChild metadata-vector "max" VarBinaryVector) idx max-val)
      (.setSafe ^BigIntVector (.getChild metadata-vector "count" BigIntVector) idx cnt)))

  Closeable
  (close [_]))

(deftype VarCharMetadata [^:unsynchronized-mutable ^bytes min-val
                          ^:unsynchronized-mutable ^bytes max-val
                          ^:unsynchronized-mutable ^long cnt
                          ^ArrowBufPointer arrow-buf-pointer]
  IColumnMetadata
  (updateMetadata [this field-vector idx]
    (let [^VarCharVector field-vector field-vector
          arrow-buf-pointer (.getDataPointer field-vector idx arrow-buf-pointer)
          end-offset (+ (.getOffset arrow-buf-pointer) (.getLength arrow-buf-pointer))]
      (set! (.cnt this) (inc cnt))

      (when (or (nil? min-val) (neg? (ByteFunctionHelpers/compare (.getBuf arrow-buf-pointer)
                                                                  (.getOffset arrow-buf-pointer)
                                                                  end-offset
                                                                  min-val
                                                                  0
                                                                  (alength min-val))))
        (set! (.min-val this) (.get field-vector idx)))

      (when (or (nil? max-val) (pos? (ByteFunctionHelpers/compare (.getBuf arrow-buf-pointer)
                                                                  (.getOffset arrow-buf-pointer)
                                                                  end-offset
                                                                  max-val
                                                                  0
                                                                  (alength max-val))))
        (set! (.max-val this) (.get field-vector idx)))))

  (writeMetadata [_ metadata-vector idx]
    (.setSafe ^VarCharVector (.getChild metadata-vector "min" VarCharVector) idx (Text. min-val))
    (.setSafe ^VarCharVector (.getChild metadata-vector "max" VarCharVector) idx (Text. max-val))
    (.setSafe ^BigIntVector (.getChild metadata-vector "count" BigIntVector) idx cnt))

  Closeable
  (close [_]))

(defn ->metadata ^core2.metadata.IColumnMetadata [^ArrowType arrow-type]
  (condp = (Types/getMinorTypeForArrowType arrow-type)
    Types$MinorType/BIT (->BitMetadata (NullableBitHolder.) (NullableBitHolder.) 0)
    Types$MinorType/BIGINT (->BigIntMetadata (NullableBigIntHolder.) (NullableBigIntHolder.) 0)
    Types$MinorType/DATEMILLI (->DateMilliMetadata (NullableDateMilliHolder.) (NullableDateMilliHolder.) 0)
    Types$MinorType/FLOAT8 (->Float8Metadata (NullableFloat8Holder.) (NullableFloat8Holder.) 0)
    Types$MinorType/VARBINARY (->VarBinaryMetadata nil nil 0 (ArrowBufPointer.))
    Types$MinorType/VARCHAR (->VarCharMetadata nil nil 0 (ArrowBufPointer.))
    (throw (UnsupportedOperationException.))))

(defn ->metadata-field ^org.apache.arrow.vector.types.pojo.Field [^Field field]
  (let [field-type (.getType field)]
    (t/->field (.getName field)
               (.getType Types$MinorType/STRUCT)
               false
               (t/->field "min" field-type false)
               (t/->field "max" field-type false)
               (t/->field "count" (.getType Types$MinorType/BIGINT) false))))

;; TODO: flat metadata schema based on sparse unions.

(def ^:private metadata-union-fields
  (vec (for [^Types$MinorType minor-type [Types$MinorType/NULL
                                          Types$MinorType/BIGINT
                                          Types$MinorType/FLOAT8
                                          Types$MinorType/VARBINARY
                                          Types$MinorType/VARCHAR
                                          Types$MinorType/BIT
                                          Types$MinorType/DATEMILLI]]
         (t/->field (.toLowerCase (.name minor-type)) (.getType minor-type) false))))

(def ^org.apache.arrow.flatbuf.Schema metadata-schema
  (Schema. [(t/->field "file" (.getType Types$MinorType/VARCHAR) false)
            (t/->field "column" (.getType Types$MinorType/VARCHAR) false)
            (t/->field "field" (.getType Types$MinorType/VARCHAR) false)
            (apply t/->field "min"
                   (.getType Types$MinorType/UNION)
                   false
                   metadata-union-fields)
            (apply t/->field "max"
                   (.getType Types$MinorType/UNION)
                   false
                   metadata-union-fields)
            (t/->field "count" (.getType Types$MinorType/BIGINT) false)]))
