(ns core2.metadata
  (:require [core2.types :as t])
  (:import java.io.Closeable
           [java.util Arrays Base64 Date]
           [org.apache.arrow.vector BigIntVector BitVector DateMilliVector Float8Vector VarBinaryVector VarCharVector]
           [org.apache.arrow.vector.holders NullableBigIntHolder NullableBitHolder NullableDateMilliHolder NullableFloat8Holder]
           [org.apache.arrow.vector.types Types Types$MinorType]
           [org.apache.arrow.vector.types.pojo ArrowType Schema]))

(defn- ->metadata-schema [arrow-type]
  (Schema. [(t/->field "min" arrow-type false)
            (t/->field "max" arrow-type false)
            (t/->field "count" (.getType Types$MinorType/BIGINT) false)]))

(defprotocol ColumnMetadata
  (update-metadata! [col-metadata field-vector idx])
  (metadata-schema [col-metadata])
  (metadata->edn [col-metadata]))

(deftype BitMetadata [^NullableBitHolder min-val, ^NullableBitHolder max-val, ^:unsynchronized-mutable ^long cnt]
  ColumnMetadata
  (update-metadata! [this field-vector idx]
    (let [^BitVector field-vector field-vector
          ^int idx idx]
      (set! (.cnt this) (inc cnt))

      (when (or (zero? (.isSet min-val))
                (neg? (Integer/compare (.get field-vector idx)
                                       (.value min-val))))
        (.get field-vector idx min-val))

      (when (or (zero? (.isSet max-val))
                (pos? (Integer/compare (.get field-vector idx)
                                       (.value max-val))))
        (.get field-vector idx max-val))))

  (metadata->edn [_]
    {:min (when (pos? (.isSet min-val)) (.value min-val))
     :max (when (pos? (.isSet max-val)) (.value max-val))
     :count cnt})

  Closeable
  (close [_]))

(deftype BigIntMetadata [^NullableBigIntHolder min-val, ^NullableBigIntHolder max-val, ^:unsynchronized-mutable ^long cnt]
  ColumnMetadata
  (update-metadata! [this field-vector idx]
    (let [^BigIntVector field-vector field-vector
          ^int idx idx]
      (set! (.cnt this) (inc cnt))

      (when (or (zero? (.isSet min-val))
                (neg? (Long/compare (.get field-vector idx)
                                    (.value min-val))))
        (.get field-vector idx min-val))

      (when (or (zero? (.isSet max-val))
                (pos? (Long/compare (.get field-vector idx)
                                    (.value max-val))))
        (.get field-vector idx max-val))))

  (metadata->edn [_]
    {:min (when (pos? (.isSet min-val)) (.value min-val))
     :max (when (pos? (.isSet max-val)) (.value max-val))
     :count cnt})

  Closeable
  (close [_]))

(deftype DateMilliMetadata [^NullableDateMilliHolder min-val, ^NullableDateMilliHolder max-val, ^:unsynchronized-mutable ^long cnt]
  ColumnMetadata
  (update-metadata! [this field-vector idx]
    (let [^DateMilliVector field-vector field-vector
          ^int idx idx]
      (set! (.cnt this) (inc cnt))

      (when (or (zero? (.isSet min-val))
                (neg? (Long/compare (.get field-vector idx)
                                    (.value min-val))))
        (.get field-vector idx min-val))

      (when (or (zero? (.isSet max-val))
                (pos? (Long/compare (.get field-vector idx)
                                    (.value max-val))))
        (.get field-vector idx max-val))))

  (metadata->edn [_]
    {:min (when (pos? (.isSet min-val)) (Date. (.value min-val)))
     :max (when (pos? (.isSet max-val)) (Date. (.value max-val)))
     :count cnt})

  Closeable
  (close [_]))

(deftype Float8Metadata [^NullableFloat8Holder min-val, ^NullableFloat8Holder max-val, ^:unsynchronized-mutable ^long cnt]
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

  (metadata->edn [_]
    {:min (when (pos? (.isSet min-val)) (.value min-val))
     :max (when (pos? (.isSet max-val)) (.value max-val))
     :count cnt})

  Closeable
  (close [_]))

(deftype VarBinaryMetadata [^:unsynchronized-mutable ^bytes min-val,
                            ^:unsynchronized-mutable ^bytes max-val,
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

  (metadata->edn [_]
    {:min (when min-val (.encodeToString (Base64/getEncoder) min-val))
     :max (when max-val (.encodeToString (Base64/getEncoder) max-val))
     :count cnt})

  Closeable
  (close [_]))

(deftype VarCharMetadata [^:unsynchronized-mutable min-val,
                          ^:unsynchronized-mutable max-val,
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

  (metadata->edn [_]
    {:min min-val
     :max max-val
     :count cnt})

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
