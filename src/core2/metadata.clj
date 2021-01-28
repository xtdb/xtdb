(ns core2.metadata
  (:require [core2.types :as t])
  (:import java.io.Closeable
           [java.util Arrays]
           [org.apache.arrow.vector BigIntVector BitVector DateMilliVector Float8Vector VarBinaryVector VarCharVector]
           org.apache.arrow.vector.complex.StructVector
           [org.apache.arrow.vector.holders NullableBigIntHolder NullableBitHolder NullableDateMilliHolder NullableFloat8Holder]
           [org.apache.arrow.vector.types Types Types$MinorType]
           [org.apache.arrow.vector.types.pojo ArrowType Field]
           org.apache.arrow.vector.util.Text))

(defprotocol ColumnMetadata
  (update-metadata! [col-metadata field-vector idx])
  (write-metadata! [col-metadata metadata-vector idx]))

(deftype BitMetadata [^NullableBitHolder min-val
                      ^NullableBitHolder max-val
                      ^:unsynchronized-mutable ^long cnt]
  ColumnMetadata
  (update-metadata! [this field-vector idx]
    (let [^BitVector field-vector field-vector
          ^int idx idx]
      (set! (.cnt this) (inc cnt))

      (when (or (zero? (.isSet min-val))
                (neg? (Integer/compare (.get field-vector idx) (.value min-val))))
        (.get field-vector idx min-val))

      (when (or (zero? (.isSet max-val))
                (pos? (Integer/compare (.get field-vector idx) (.value max-val))))
        (.get field-vector idx max-val))))

  (write-metadata! [_ metadata-vector idx]
    (let [^StructVector metadata-vector metadata-vector
          ^int idx idx]
      (.setSafe ^BitVector (.getChild metadata-vector "min" BitVector) idx (.value min-val))
      (.setSafe ^BitVector (.getChild metadata-vector "max" BitVector) idx (.value max-val))
      (.setSafe ^BigIntVector (.getChild metadata-vector "count" BigIntVector) idx cnt)))

  Closeable
  (close [_]))

(deftype BigIntMetadata [^NullableBigIntHolder min-val
                         ^NullableBigIntHolder max-val
                         ^:unsynchronized-mutable ^long cnt]
  ColumnMetadata
  (update-metadata! [this field-vector idx]
    (let [^BigIntVector field-vector field-vector
          ^int idx idx]
      (set! (.cnt this) (inc cnt))

      (when (or (zero? (.isSet min-val))
                (neg? (Long/compare (.get field-vector idx) (.value min-val))))
        (.get field-vector idx min-val))

      (when (or (zero? (.isSet max-val))
                (pos? (Long/compare (.get field-vector idx) (.value max-val))))
        (.get field-vector idx max-val))))

  (write-metadata! [_ metadata-vector idx]
    (let [^StructVector metadata-vector metadata-vector
          ^int idx idx]
      (.setSafe ^BigIntVector (.getChild metadata-vector "min" BigIntVector) idx (.value min-val))
      (.setSafe ^BigIntVector (.getChild metadata-vector "max" BigIntVector) idx (.value max-val))
      (.setSafe ^BigIntVector (.getChild metadata-vector "count" BigIntVector) idx cnt)))

  Closeable
  (close [_]))

(deftype DateMilliMetadata [^NullableDateMilliHolder min-val
                            ^NullableDateMilliHolder max-val
                            ^:unsynchronized-mutable ^long cnt]
  ColumnMetadata
  (update-metadata! [this field-vector idx]
    (let [^DateMilliVector field-vector field-vector
          ^int idx idx]
      (set! (.cnt this) (inc cnt))

      (when (or (zero? (.isSet min-val))
                (neg? (Long/compare (.get field-vector idx) (.value min-val))))
        (.get field-vector idx min-val))

      (when (or (zero? (.isSet max-val))
                (pos? (Long/compare (.get field-vector idx) (.value max-val))))
        (.get field-vector idx max-val))))

  (write-metadata! [_ metadata-vector idx]
    (let [^StructVector metadata-vector metadata-vector
          ^int idx idx]
      (.setSafe ^DateMilliVector (.getChild metadata-vector "min" DateMilliVector) idx (.value min-val))
      (.setSafe ^DateMilliVector (.getChild metadata-vector "max" DateMilliVector) idx (.value max-val))
      (.setSafe ^BigIntVector (.getChild metadata-vector "count" BigIntVector) idx cnt)))

  Closeable
  (close [_]))

(deftype Float8Metadata [^NullableFloat8Holder min-val
                         ^NullableFloat8Holder max-val
                         ^:unsynchronized-mutable ^long cnt]
  ColumnMetadata
  (update-metadata! [this field-vector idx]
    (let [^Float8Vector field-vector field-vector
          ^int idx idx]
      (set! (.cnt this) (inc cnt))

      (when (or (zero? (.isSet min-val))
                (neg? (Double/compare (.get field-vector idx)
                                      (.value min-val))))
        (.get field-vector idx min-val))

      (when (or (zero? (.isSet max-val))
                (pos? (Double/compare (.get field-vector idx)
                                      (.value max-val))))
        (.get field-vector idx max-val))))

  (write-metadata! [_ metadata-vector idx]
    (let [^StructVector metadata-vector metadata-vector
          ^int idx idx]
      (.setSafe ^Float8Vector (.getChild metadata-vector "min" Float8Vector) idx (.value min-val))
      (.setSafe ^Float8Vector (.getChild metadata-vector "max" Float8Vector) idx (.value max-val))
      (.setSafe ^BigIntVector (.getChild metadata-vector "count" BigIntVector) idx cnt)))

  Closeable
  (close [_]))

(deftype VarBinaryMetadata [^:unsynchronized-mutable ^bytes min-val
                            ^:unsynchronized-mutable ^bytes max-val
                            ^:unsynchronized-mutable ^long cnt]
  ColumnMetadata
  (update-metadata! [this field-vector idx]
    (let [^VarBinaryVector field-vector field-vector
          ^int idx idx]
      (set! (.cnt this) (inc cnt))

      (let [value (.getObject field-vector idx)]
        (when (or (nil? min-val) (neg? (Arrays/compareUnsigned value min-val)))
          (set! (.min-val this) value))

        (when (or (nil? max-val) (pos? (Arrays/compareUnsigned value max-val)))
          (set! (.max-val this) value)))))

  (write-metadata! [_ metadata-vector idx]
    (let [^StructVector metadata-vector metadata-vector
          ^int idx idx]
      (.setSafe ^VarBinaryVector (.getChild metadata-vector "min" VarBinaryVector) idx min-val)
      (.setSafe ^VarBinaryVector (.getChild metadata-vector "max" VarBinaryVector) idx max-val)
      (.setSafe ^BigIntVector (.getChild metadata-vector "count" BigIntVector) idx cnt)))

  Closeable
  (close [_]))

(deftype VarCharMetadata [^:unsynchronized-mutable ^String min-val
                          ^:unsynchronized-mutable ^String max-val
                          ^:unsynchronized-mutable ^long cnt]
  ColumnMetadata
  (update-metadata! [this field-vector idx]
    (let [^VarCharVector field-vector field-vector
          ^int idx idx]
      (set! (.cnt this) (inc cnt))

      (let [value (str (.getObject field-vector idx))]
        (when (or (nil? min-val) (neg? (compare value min-val)))
          (set! (.min-val this) value))

        (when (or (nil? max-val) (pos? (compare value max-val)))
          (set! (.max-val this) value)))))

  (write-metadata! [_ metadata-vector idx]
    (let [^StructVector metadata-vector metadata-vector
          ^int idx idx]
      (.setSafe ^VarCharVector (.getChild metadata-vector "min" VarCharVector) idx (Text. min-val))
      (.setSafe ^VarCharVector (.getChild metadata-vector "max" VarCharVector) idx (Text. max-val))
      (.setSafe ^BigIntVector (.getChild metadata-vector "count" BigIntVector) idx cnt)))

  Closeable
  (close [_]))

(defn ->metadata [^ArrowType arrow-type]
  (condp = (Types/getMinorTypeForArrowType arrow-type)
    Types$MinorType/BIT (->BitMetadata (NullableBitHolder.) (NullableBitHolder.) 0)
    Types$MinorType/BIGINT (->BigIntMetadata (NullableBigIntHolder.) (NullableBigIntHolder.) 0)
    Types$MinorType/DATEMILLI (->DateMilliMetadata (NullableDateMilliHolder.) (NullableDateMilliHolder.) 0)
    Types$MinorType/FLOAT8 (->Float8Metadata (NullableFloat8Holder.) (NullableFloat8Holder.) 0)
    Types$MinorType/VARBINARY (->VarBinaryMetadata nil nil 0)
    Types$MinorType/VARCHAR (->VarCharMetadata nil nil 0)
    (throw (UnsupportedOperationException.))))

(defn ->metadata-field [^Field field]
  (let [field-type (.getType field)]
    (t/->field (.getName field)
               (.getType Types$MinorType/STRUCT)
               false
               (t/->field "min" field-type false)
               (t/->field "max" field-type false)
               (t/->field "count" (.getType Types$MinorType/BIGINT) false))))
