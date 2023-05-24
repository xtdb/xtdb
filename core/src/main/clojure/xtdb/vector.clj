(ns xtdb.vector
  (:require [xtdb.error :as err]
            [xtdb.types :as types]
            [xtdb.util :as util])
  (:import (clojure.lang Keyword MapEntry)
           java.net.URI
           (java.nio ByteBuffer CharBuffer)
           java.nio.charset.StandardCharsets
           (java.time Duration Instant LocalDate LocalDateTime LocalTime OffsetDateTime Period ZoneOffset ZonedDateTime)
           (java.util ArrayList Date HashMap List Map Set UUID)
           java.util.function.Function
           (org.apache.arrow.vector BaseVariableWidthVector BigIntVector BitVector DateDayVector DateMilliVector DurationVector ExtensionTypeVector FixedSizeBinaryVector Float4Vector Float8Vector IntVector IntervalDayVector IntervalMonthDayNanoVector IntervalYearVector NullVector PeriodDuration SmallIntVector TimeMicroVector TimeMilliVector TimeNanoVector TimeSecVector TimeStampVector TinyIntVector ValueVector VarBinaryVector VarCharVector)
           (org.apache.arrow.vector.complex DenseUnionVector ListVector StructVector)
           (org.apache.arrow.vector.types.pojo ArrowType$List ArrowType$Struct ArrowType$Union Field FieldType)
           xtdb.api.protocols.ClojureForm
           (xtdb.types IntervalDayTime IntervalMonthDayNano IntervalYearMonth)
           (xtdb.vector IIndirectVector IMonoVectorReader IPolyVectorReader IRowCopier IStructValueReader IVectorWriter IWriterPosition)
           (xtdb.vector.extensions SetType)))

(set! *unchecked-math* :warn-on-boxed)

;; NOTE: if we were properly engaging in the memory management, we'd make all of these `Closeable`,
;; retain the buffers on the way in, and release them on `.close`.

;; However, this complicates the generated code in the expression engine, so we instead assume
;; that none of these readers will outlive their respective vectors.

(defprotocol MonoFactory
  (->mono-reader ^xtdb.vector.IMonoVectorReader [arrow-vec col-type]))

(defprotocol PolyFactory
  (->poly-reader ^xtdb.vector.IPolyVectorReader [arrow-vec col-type]))

(extend-protocol MonoFactory
  ValueVector
  (->mono-reader [arrow-vec _]
    (reify IMonoVectorReader
      (readObject [_ idx] (.getObject arrow-vec idx))))

  NullVector
  (->mono-reader [_ _]
    (reify IMonoVectorReader))

  DenseUnionVector
  (->mono-reader [arrow-vec col-type]
    ;; we delegate to the only child vector, and assume the offsets are just 0..n (i.e. no indirection required)
    (if-let [child-vec (first (seq arrow-vec))]
      (->mono-reader child-vec col-type)
      (->mono-reader (NullVector.) col-type))))

(extend-protocol MonoFactory
  FixedSizeBinaryVector
  (->mono-reader [arrow-vec _col-type]
    (let [byte-width (.getByteWidth arrow-vec)]
      (reify IMonoVectorReader
        (valueCount [_] (.getValueCount arrow-vec))
        (readBytes [_ idx]
          (.nioBuffer (.getDataBuffer arrow-vec) (* byte-width idx) byte-width))
        (readObject [this idx]
          (.readBytes this idx))))))

(defmacro def-fixed-width-mono-factory [clazz read-method]
  `(extend-protocol MonoFactory
     ~clazz
     (~'->mono-reader [arrow-vec# _col-type#]
      (reify IMonoVectorReader
        (~'valueCount [_] (.getValueCount arrow-vec#))
        (~read-method [_# idx#] (.get arrow-vec# idx#))))))

#_{:clj-kondo/ignore [:unresolved-symbol]}
(do
  (def-fixed-width-mono-factory TinyIntVector readByte)
  (def-fixed-width-mono-factory SmallIntVector readShort)
  (def-fixed-width-mono-factory IntVector readInt)
  (def-fixed-width-mono-factory BigIntVector readLong)
  (def-fixed-width-mono-factory Float4Vector readFloat)
  (def-fixed-width-mono-factory Float8Vector readDouble)

  (def-fixed-width-mono-factory DateDayVector readLong)
  (def-fixed-width-mono-factory DateMilliVector readLong)
  (def-fixed-width-mono-factory TimeStampVector readLong)
  (def-fixed-width-mono-factory TimeSecVector readLong)
  (def-fixed-width-mono-factory TimeMilliVector readLong)
  (def-fixed-width-mono-factory TimeMicroVector readLong)
  (def-fixed-width-mono-factory TimeNanoVector readLong))

(extend-protocol MonoFactory
  BitVector
  (->mono-reader [arrow-vec _col-type]
    (reify IMonoVectorReader
      (valueCount [_] (.getValueCount arrow-vec))
      (readBoolean [_ idx] (== 1 (.get arrow-vec idx)))))

  DurationVector
  (->mono-reader [arrow-vec _col-type]
    (reify IMonoVectorReader
      (valueCount [_] (.getValueCount arrow-vec))
      ;; `.get` here returns an ArrowBuf, naturally.
      (readLong [_ idx] (DurationVector/get (.getDataBuffer arrow-vec) idx)))))

(extend-protocol MonoFactory
  BaseVariableWidthVector
  (->mono-reader [arrow-vec _col-type]
    (reify IMonoVectorReader
      (valueCount [_] (.getValueCount arrow-vec))

      (readBytes [_ idx]
        (.nioBuffer (.getDataBuffer arrow-vec) (.getStartOffset arrow-vec idx) (.getValueLength arrow-vec idx)))

      (readObject [_ idx]
        (.nioBuffer (.getDataBuffer arrow-vec) (.getStartOffset arrow-vec idx) (.getValueLength arrow-vec idx))))))

(extend-protocol MonoFactory
  ExtensionTypeVector
  (->mono-reader [arrow-vec col-type] (->mono-reader (.getUnderlyingVector arrow-vec) col-type)))

;; (@wot) read as an epoch int, do not think it is worth branching for both cases in all date functions.
(extend-protocol MonoFactory
  DateMilliVector
  (->mono-reader [arrow-vec _col-type]
    (reify IMonoVectorReader
      (valueCount [_] (.getValueCount arrow-vec))
      (readInt [_ idx] (-> (.get arrow-vec idx) (quot 86400000) (int))))))

(extend-protocol MonoFactory
  TimeSecVector
  (->mono-reader [arrow-vec _col-type]
    (reify IMonoVectorReader
      (valueCount [_] (.getValueCount arrow-vec))
      (readLong [_ idx] (.get arrow-vec idx))))

  TimeMilliVector
  (->mono-reader [arrow-vec _col-type]
    (reify IMonoVectorReader
      (valueCount [_] (.getValueCount arrow-vec))
      (readLong [_ idx] (.get arrow-vec idx))))

  TimeMicroVector
  (->mono-reader [arrow-vec _col-type]
    (reify IMonoVectorReader
      (valueCount [_] (.getValueCount arrow-vec))
      (readLong [_ idx] (.get arrow-vec idx))))

  TimeNanoVector
  (->mono-reader [arrow-vec _col-type]
    (reify IMonoVectorReader
      (valueCount [_] (.getValueCount arrow-vec))
      (readLong [_ idx] (.get arrow-vec idx)))))

;; the EE uses PDs internally for all types of intervals for now.
;; we could migrate it to use `xtdb.types.Interval*`
(extend-protocol MonoFactory
  IntervalYearVector
  (->mono-reader [arrow-vec _col-type]
    (reify IMonoVectorReader
      (valueCount [_] (.getValueCount arrow-vec))
      (readObject [_ idx]
        (PeriodDuration. (Period/ofMonths (.get arrow-vec idx)) Duration/ZERO))))

  IntervalDayVector
  (->mono-reader [arrow-vec _col-type]
    (reify IMonoVectorReader
      (valueCount [_] (.getValueCount arrow-vec))
      (readObject [_ idx]
        (let [^IntervalDayTime idt (types/get-object arrow-vec idx)]
          (PeriodDuration. (.-period idt) (.-duration idt))))))

  IntervalMonthDayNanoVector
  (->mono-reader [arrow-vec _col-type]
    (reify IMonoVectorReader
      (valueCount [_] (.getValueCount arrow-vec))
      (readObject [_ idx]
        (let [^IntervalMonthDayNano imdn (types/get-object arrow-vec idx)]
          (PeriodDuration. (.-period imdn) (.-duration imdn)))))))

(extend-protocol MonoFactory
  ListVector
  (->mono-reader [arrow-vec [_ el-type]]
    (if (types/union? el-type)
      (let [inner-rdr (->poly-reader (.getDataVector arrow-vec) el-type)]
        (reify IMonoVectorReader
          (valueCount [_] (.getValueCount arrow-vec))
          (readObject [_ idx]
            (let [start-idx (.getElementStartIndex arrow-vec idx)
                  value-count (- (.getElementEndIndex arrow-vec idx)
                                 start-idx)]
              (reify IPolyVectorReader
                (valueCount [_] value-count)
                (read [_ idx] (.read inner-rdr (+ start-idx idx)))
                (readBoolean [_] (.readBoolean inner-rdr))
                (readByte [_] (.readByte inner-rdr))
                (readShort [_] (.readShort inner-rdr))
                (readInt [_] (.readInt inner-rdr))
                (readLong [_] (.readLong inner-rdr))
                (readFloat [_] (.readFloat inner-rdr))
                (readDouble [_] (.readDouble inner-rdr))
                (readBytes [_] (.readBytes inner-rdr))
                (readObject [_] (.readObject inner-rdr)))))))

      (let [inner-rdr (->mono-reader (.getDataVector arrow-vec) el-type)]
        (reify IMonoVectorReader
          (valueCount [_] (.getValueCount arrow-vec))
          (readObject [_ idx]
            (let [start-idx (.getElementStartIndex arrow-vec idx)
                  value-count (- (.getElementEndIndex arrow-vec idx)
                                 start-idx)]
              (reify IMonoVectorReader
                (valueCount [_] value-count)
                (readBoolean [_ idx] (.readBoolean inner-rdr (+ start-idx idx)))
                (readByte [_ idx] (.readByte inner-rdr (+ start-idx idx)))
                (readShort [_ idx] (.readShort inner-rdr (+ start-idx idx)))
                (readInt [_ idx] (.readInt inner-rdr (+ start-idx idx)))
                (readLong [_ idx] (.readLong inner-rdr (+ start-idx idx)))
                (readFloat [_ idx] (.readFloat inner-rdr (+ start-idx idx)))
                (readDouble [_ idx] (.readDouble inner-rdr (+ start-idx idx)))
                (readBytes [_ idx] (.readBytes inner-rdr (+ start-idx idx)))
                (readObject [_ idx] (.readObject inner-rdr (+ start-idx idx)))))))))))

(extend-protocol MonoFactory
  StructVector
  (->mono-reader [arrow-vec [_ val-types]]
    (let [inner-readers (->> (for [[field val-type] val-types
                                   :let [field-name (str field)
                                         child-vec (.getChild arrow-vec field-name)]]
                               (MapEntry/create field-name
                                                (if (types/union? val-type)
                                                  (->poly-reader child-vec val-type)
                                                  (->mono-reader child-vec val-type))))
                             (into {}))]
      (reify IMonoVectorReader
        (valueCount [_] (.getValueCount arrow-vec))
        (readObject [_ idx]
          (reify IStructValueReader
            (readBoolean [_ field-name] (.readBoolean ^IMonoVectorReader (get inner-readers field-name) idx))
            (readByte [_ field-name] (.readByte ^IMonoVectorReader (get inner-readers field-name) idx))
            (readShort [_ field-name] (.readShort ^IMonoVectorReader (get inner-readers field-name) idx))
            (readInt [_ field-name] (.readInt ^IMonoVectorReader (get inner-readers field-name) idx))
            (readLong [_ field-name] (.readLong ^IMonoVectorReader (get inner-readers field-name) idx))
            (readFloat [_ field-name] (.readFloat ^IMonoVectorReader (get inner-readers field-name) idx))
            (readDouble [_ field-name] (.readDouble ^IMonoVectorReader (get inner-readers field-name) idx))
            (readBytes [_ field-name] (.readBytes ^IMonoVectorReader (get inner-readers field-name) idx))
            (readObject [_ field-name] (.readObject ^IMonoVectorReader (get inner-readers field-name) idx))

            (readField [_ field-name]
              (let [^IPolyVectorReader inner-rdr (get inner-readers field-name)]
                (.read inner-rdr idx)
                inner-rdr))))))))

(deftype NullableVectorReader [^ValueVector arrow-vec
                               ^IMonoVectorReader inner,
                               ^byte null-type-id, ^byte nn-type-id
                               ^:unsynchronized-mutable ^byte type-id
                               ^:unsynchronized-mutable ^int idx]
  IPolyVectorReader
  (valueCount [_] (.getValueCount arrow-vec))

  (read [this idx]
    (let [type-id (if (.isNull arrow-vec idx) null-type-id nn-type-id)]
      (set! (.type-id this) type-id)
      (set! (.idx this) idx)
      type-id))

  (read [_] type-id)

  (readBoolean [_] (.readBoolean inner idx))
  (readByte [_] (.readByte inner idx))
  (readShort [_] (.readShort inner idx))
  (readInt [_] (.readInt inner idx))
  (readLong [_] (.readLong inner idx))
  (readFloat [_] (.readFloat inner idx))
  (readDouble [_] (.readDouble inner idx))
  (readBytes [_] (.readBytes inner idx))
  (readObject [_] (.readObject inner idx)))

(deftype MonoToPolyReader [^IMonoVectorReader inner
                           ^:byte type-id
                           ^:unsynchronized-mutable ^int idx]
  IPolyVectorReader
  (read [this idx]
    (set! (.idx this) idx)
    type-id)

  (read [_] type-id)

  (valueCount [_] (.valueCount inner))

  (readBoolean [_] (.readBoolean inner idx))
  (readByte [_] (.readByte inner idx))
  (readShort [_] (.readShort inner idx))
  (readInt [_] (.readInt inner idx))
  (readLong [_] (.readLong inner idx))
  (readFloat [_] (.readFloat inner idx))
  (readDouble [_] (.readDouble inner idx))
  (readBytes [_] (.readBytes inner idx))
  (readObject [_] (.readObject inner idx)))

(deftype RemappedTypeIdReader [^IPolyVectorReader inner-rdr, ^bytes type-id-mapping]
  IPolyVectorReader
  (read [_ idx] (aget type-id-mapping (.read inner-rdr idx)))
  (read [_] (aget type-id-mapping (.read inner-rdr)))

  (valueCount [_] (.valueCount inner-rdr))

  (readBoolean [_] (.readBoolean inner-rdr))
  (readByte [_] (.readByte inner-rdr))
  (readShort [_] (.readShort inner-rdr))
  (readInt [_] (.readInt inner-rdr))
  (readLong [_] (.readLong inner-rdr))
  (readFloat [_] (.readFloat inner-rdr))
  (readDouble [_] (.readDouble inner-rdr))
  (readBytes [_] (.readBytes inner-rdr))
  (readObject [_] (.readObject inner-rdr)))

(deftype DuvReader [^DenseUnionVector duv
                    ^"[Lxtdb.vector.IMonoVectorReader;" inner-readers
                    ^:unsynchronized-mutable ^byte type-id
                    ^:unsynchronized-mutable ^IMonoVectorReader inner-rdr
                    ^:unsynchronized-mutable ^int inner-offset]
  IPolyVectorReader
  (read [this idx]
    (let [type-id (.getTypeId duv idx)]
      (set! (.type-id this) type-id)
      (set! (.inner-offset this) (.getOffset duv idx))
      (set! (.inner-rdr this) (aget inner-readers type-id))
      type-id))

  (read [_] type-id)

  (valueCount [_] (.getValueCount duv))

  (readBoolean [_] (.readBoolean inner-rdr inner-offset))
  (readByte [_] (.readByte inner-rdr inner-offset))
  (readShort [_] (.readShort inner-rdr inner-offset))
  (readInt [_] (.readInt inner-rdr inner-offset))
  (readLong [_] (.readLong inner-rdr inner-offset))
  (readFloat [_] (.readFloat inner-rdr inner-offset))
  (readDouble [_] (.readDouble inner-rdr inner-offset))
  (readBytes [_] (.readBytes inner-rdr inner-offset))
  (readObject [_] (.readObject inner-rdr inner-offset)))

(extend-protocol PolyFactory
  NullVector
  (->poly-reader [_vec [_ inner-types]]
    (let [null-type-id (byte (.indexOf ^List (vec inner-types) :null))]
      (reify IPolyVectorReader
        (read [_ _idx] null-type-id)
        (read [_] null-type-id)))))

(defn- duv-reader-type-id-mapping
  "returns a mapping from DUV type-id -> reader type-id"
  ^bytes [^DenseUnionVector duv ordered-col-types]

  (let [child-count (count (seq duv))
        type-id-mapping (byte-array child-count)]
    (dotimes [type-id child-count]
      (aset type-id-mapping
            type-id
            (byte (.indexOf ^List ordered-col-types
                            (-> (.getVectorByType duv type-id) .getField
                                types/field->col-type)))))
    type-id-mapping))

(extend-protocol PolyFactory
  DenseUnionVector
  (->poly-reader [duv [_ inner-types]]
    (let [leg-count (count inner-types)]
      (if (= 1 leg-count)
        (->MonoToPolyReader (->mono-reader duv (first inner-types)) 0 0)

        (let [ordered-col-types (vec inner-types)
              type-id-mapping (duv-reader-type-id-mapping duv ordered-col-types)
              readers (object-array leg-count)]
          (dotimes [duv-type-id leg-count]
            (let [col-type (nth ordered-col-types (aget type-id-mapping duv-type-id))]
              (aset readers duv-type-id (->mono-reader (.getVectorByType duv duv-type-id) col-type))))

          (-> (->DuvReader duv readers 0 nil 0)
              (->RemappedTypeIdReader type-id-mapping))))))

  ExtensionTypeVector
  (->poly-reader [arrow-vec col-type]
    ;; HACK: this'll likely be [:union #{:null [:ext ...]}]
    ;; suspect we have to unwrap the extension-type but ensure the same order in the union?
    ;; tough, given we're relying on set ordering...
    (->poly-reader (.getUnderlyingVector arrow-vec) col-type))

  ValueVector
  (->poly-reader [arrow-vec [_ inner-types]]
    (let [ordered-col-types (vec inner-types)
          field (.getField arrow-vec)
          null-type-id (.indexOf ^List ordered-col-types :null)
          nn-type-id (case null-type-id 0 1, 1 0)
          mono-reader (->mono-reader arrow-vec (nth ordered-col-types nn-type-id))]
      (if (.isNullable field)
        (->NullableVectorReader arrow-vec mono-reader
                                null-type-id nn-type-id
                                0 0)
        (->MonoToPolyReader mono-reader
                            (.indexOf ^List ordered-col-types (types/field->col-type field))
                            0)))))

(deftype IndirectVectorMonoReader [^IMonoVectorReader inner, ^IIndirectVector col]
  IMonoVectorReader
  (valueCount [_] (.getValueCount col))
  (readBoolean [_ idx] (.readBoolean inner (.getIndex col idx)))
  (readByte [_ idx] (.readByte inner (.getIndex col idx)))
  (readShort [_ idx] (.readShort inner (.getIndex col idx)))
  (readInt [_ idx] (.readInt inner (.getIndex col idx)))
  (readLong [_ idx] (.readLong inner (.getIndex col idx)))
  (readFloat [_ idx] (.readFloat inner (.getIndex col idx)))
  (readDouble [_ idx] (.readDouble inner (.getIndex col idx)))
  (readBytes [_ idx] (.readBytes inner (.getIndex col idx)))
  (readObject [_ idx] (.readObject inner (.getIndex col idx))))

(deftype IndirectVectorPolyReader [^IPolyVectorReader inner, ^IIndirectVector col]
  IPolyVectorReader
  (valueCount [_] (.getValueCount col))
  (read [_ idx] (.read inner (.getIndex col idx)))
  (read [_] (.read inner))

  (readBoolean [_] (.readBoolean inner))
  (readByte [_] (.readByte inner))
  (readShort [_] (.readShort inner))
  (readInt [_] (.readInt inner))
  (readLong [_] (.readLong inner))
  (readFloat [_] (.readFloat inner))
  (readDouble [_] (.readDouble inner))
  (readBytes [_] (.readBytes inner))
  (readObject [_] (.readObject inner)))
