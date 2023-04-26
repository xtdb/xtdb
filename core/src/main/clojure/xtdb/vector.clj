(ns xtdb.vector
  (:require [xtdb.error :as err]
            [xtdb.types :as types]
            [xtdb.util :as util])
  (:import (clojure.lang Keyword MapEntry)
           java.net.URI
           (java.nio ByteBuffer CharBuffer)
           java.nio.charset.StandardCharsets
           (java.time Duration Instant LocalDate LocalDateTime LocalTime OffsetDateTime Period ZoneOffset ZonedDateTime)
           (java.util ArrayList Date HashMap List Set Map UUID)
           java.util.function.Function
           (org.apache.arrow.vector BaseVariableWidthVector BigIntVector BitVector DateDayVector DateMilliVector DurationVector ExtensionTypeVector FixedSizeBinaryVector Float4Vector Float8Vector IntVector IntervalDayVector IntervalMonthDayNanoVector IntervalYearVector NullVector PeriodDuration SmallIntVector TimeMicroVector TimeMilliVector TimeNanoVector TimeSecVector TimeStampVector TinyIntVector ValueVector VarBinaryVector VarCharVector)
           (org.apache.arrow.vector.complex DenseUnionVector ListVector StructVector)
           (org.apache.arrow.vector.types.pojo ArrowType$List ArrowType$Struct Field FieldType)
           xtdb.api.ClojureForm
           (xtdb.types IntervalDayTime IntervalMonthDayNano IntervalYearMonth)
           (xtdb.vector IIndirectVector IMonoVectorReader IMonoVectorWriter IPolyVectorReader IPolyVectorWriter IStructValueReader IVectorWriter2 IWriterPosition)
           (xtdb.vector.extensions SetType)))

(set! *unchecked-math* :warn-on-boxed)

;; NOTE: if we were properly engaging in the memory management, we'd make all of these `Closeable`,
;; retain the buffers on the way in, and release them on `.close`.

;; However, this complicates the generated code in the expression engine, so we instead assume
;; that none of these readers will outlive their respective vectors.

(defprotocol MonoFactory
  (->mono-reader ^xtdb.vector.IMonoVectorReader [arrow-vec col-type])
  (->mono-writer ^xtdb.vector.IMonoVectorWriter [arrow-vec col-type]))

(defprotocol PolyFactory
  (->poly-reader ^xtdb.vector.IPolyVectorReader [arrow-vec col-type])
  (->poly-writer ^xtdb.vector.IPolyVectorWriter [arrow-vec col-type]))

(extend-protocol MonoFactory
  ValueVector
  (->mono-reader [arrow-vec _]
    (reify IMonoVectorReader
      (readObject [_ idx] (.getObject arrow-vec idx))))

  NullVector
  (->mono-reader [_ _]
    (reify IMonoVectorReader))

  (->mono-writer [arrow-vec _]
    (let [wp (IWriterPosition/build (.getValueCount arrow-vec))]
      (reify IMonoVectorWriter
        (writerPosition [_] wp)
        (writeNull [_ _] (.getPositionAndIncrement wp)))))

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
        (readObject [_ idx]
          (.nioBuffer (.getDataBuffer arrow-vec) (* byte-width idx) byte-width)))))

  (->mono-writer [arrow-vec _col-type]
    (let [byte-width (.getByteWidth arrow-vec)
          wp (IWriterPosition/build (.getValueCount arrow-vec))]
      (reify IMonoVectorWriter
        (writerPosition [_] wp)
        (writeNull [_ _] (.setNull arrow-vec (.getPositionAndIncrement wp)))
        (writeObject [_ obj]
          (let [^ByteBuffer buf obj
                pos (.position buf)
                idx (.getPositionAndIncrement wp)]
            (.setIndexDefined arrow-vec idx)
            (.setBytes (.getDataBuffer arrow-vec) (* byte-width idx) buf)
            (.position buf pos)))))))

(defmacro def-fixed-width-mono-factory [clazz read-method write-method]
  `(extend-protocol MonoFactory
     ~clazz
     (~'->mono-reader [arrow-vec# _col-type#]
      (reify IMonoVectorReader
        (~'valueCount [_] (.getValueCount arrow-vec#))
        (~read-method [_# idx#] (.get arrow-vec# idx#))))

     (~'->mono-writer [arrow-vec# _col-type#]
      (let [wp# (IWriterPosition/build (.getValueCount arrow-vec#))]
        (reify IMonoVectorWriter
          (~'writerPosition [_#] wp#)
          (~'writeNull [_# _#] (.setNull arrow-vec# (.getPositionAndIncrement wp#)))

          (~write-method [_# v#]
           (.setSafe arrow-vec# (.getPositionAndIncrement wp#) v#)))))))

#_{:clj-kondo/ignore [:unresolved-symbol]}
(do
  (def-fixed-width-mono-factory TinyIntVector readByte writeByte)
  (def-fixed-width-mono-factory SmallIntVector readShort writeShort)
  (def-fixed-width-mono-factory IntVector readInt writeInt)
  (def-fixed-width-mono-factory BigIntVector readLong writeLong)
  (def-fixed-width-mono-factory Float4Vector readFloat writeFloat)
  (def-fixed-width-mono-factory Float8Vector readDouble writeDouble)

  (def-fixed-width-mono-factory DateDayVector readInt writeInt)
  (def-fixed-width-mono-factory DateMilliVector readLong writeLong)
  (def-fixed-width-mono-factory TimeStampVector readLong writeLong)
  (def-fixed-width-mono-factory TimeSecVector readLong writeLong)
  (def-fixed-width-mono-factory TimeMilliVector readLong writeLong)
  (def-fixed-width-mono-factory TimeMicroVector readLong writeLong)
  (def-fixed-width-mono-factory TimeNanoVector readLong writeLong))

(extend-protocol MonoFactory
  BitVector
  (->mono-reader [arrow-vec _col-type]
    (reify IMonoVectorReader
      (valueCount [_] (.getValueCount arrow-vec))
      (readBoolean [_ idx] (== 1 (.get arrow-vec idx)))))

  (->mono-writer [arrow-vec _col-type]
    (let [wp (IWriterPosition/build (.getValueCount arrow-vec))]
      (reify
        IMonoVectorWriter
        (writerPosition [_] wp)
        (writeNull [_ _] (.setNull arrow-vec (.getPositionAndIncrement wp)))
        (writeBoolean [_ v] (.setSafe arrow-vec (.getPositionAndIncrement wp) (if v 1 0))))))

  DurationVector
  (->mono-reader [arrow-vec _col-type]
    (reify IMonoVectorReader
      (valueCount [_] (.getValueCount arrow-vec))
      ;; `.get` here returns an ArrowBuf, naturally.
      (readLong [_ idx] (DurationVector/get (.getDataBuffer arrow-vec) idx))))

  (->mono-writer [arrow-vec _col-type]
    (let [wp (IWriterPosition/build (.getValueCount arrow-vec))]
      (reify
        IMonoVectorWriter
        (writerPosition [_] wp)
        (writeNull [_ _] (.setNull arrow-vec (.getPositionAndIncrement wp)))
        (writeLong [_ v] (.setSafe arrow-vec (.getPositionAndIncrement wp) v))))))

(extend-protocol MonoFactory
  BaseVariableWidthVector
  (->mono-reader [arrow-vec _col-type]
    (reify IMonoVectorReader
      (valueCount [_] (.getValueCount arrow-vec))
      (readObject [_ idx]
        (.nioBuffer (.getDataBuffer arrow-vec) (.getStartOffset arrow-vec idx) (.getValueLength arrow-vec idx)))))

  (->mono-writer [arrow-vec _col-type]
    (let [wp (IWriterPosition/build (.getValueCount arrow-vec))]
      (reify IMonoVectorWriter
        (writerPosition [_] wp)

        (writeNull [_ _] (.setNull arrow-vec (.getPositionAndIncrement wp)))

        (writeObject [_ obj]
          (let [^ByteBuffer buf obj
                pos (.position buf)]
            (.setSafe arrow-vec (.getPositionAndIncrement wp) buf pos (- (.limit buf) pos))
            (.position buf pos)))))))

(extend-protocol MonoFactory
  ExtensionTypeVector
  (->mono-reader [arrow-vec col-type] (->mono-reader (.getUnderlyingVector arrow-vec) col-type))
  (->mono-writer [arrow-vec col-type] (->mono-writer (.getUnderlyingVector arrow-vec) col-type)))

;; (@wot) read as an epoch int, do not think it is worth branching for both cases in all date functions.
(extend-protocol MonoFactory
  DateMilliVector
  (->mono-reader [arrow-vec _col-type]
    (reify IMonoVectorReader
      (valueCount [_] (.getValueCount arrow-vec))
      (readInt [_ idx] (-> (.get arrow-vec idx) (quot 86400000) (int)))))

  (->mono-writer [arrow-vec _col-type]
    (let [wp (IWriterPosition/build (.getValueCount arrow-vec))]
      (reify IMonoVectorWriter
        (writerPosition [_] wp)
        (writeNull [_ _] (.setNull arrow-vec (.getPositionAndIncrement wp)))
        (writeInt [_ v] (.set arrow-vec (.getPositionAndIncrement wp) (* v 86400000)))))))

(extend-protocol MonoFactory
  TimeSecVector
  (->mono-reader [arrow-vec _col-type]
    (reify IMonoVectorReader
      (valueCount [_] (.getValueCount arrow-vec))
      (readLong [_ idx] (.get arrow-vec idx))))

  (->mono-writer [arrow-vec _col-type]
    (let [wp (IWriterPosition/build (.getValueCount arrow-vec))]
      (reify IMonoVectorWriter
        (writerPosition [_] wp)
        (writeNull [_ _] (.setNull arrow-vec (.getPositionAndIncrement wp)))
        (writeLong [_ v] (.set arrow-vec (.getPositionAndIncrement wp) v)))))

  TimeMilliVector
  (->mono-reader [arrow-vec _col-type]
    (reify IMonoVectorReader
      (valueCount [_] (.getValueCount arrow-vec))
      (readLong [_ idx] (.get arrow-vec idx))))

  (->mono-writer [arrow-vec _col-type]
    (let [wp (IWriterPosition/build (.getValueCount arrow-vec))]
      (reify IMonoVectorWriter
        (writerPosition [_] wp)
        (writeNull [_ _] (.setNull arrow-vec (.getPositionAndIncrement wp)))
        (writeLong [_ v] (.set arrow-vec (.getPositionAndIncrement wp) v)))))

  TimeMicroVector
  (->mono-reader [arrow-vec _col-type]
    (reify IMonoVectorReader
      (valueCount [_] (.getValueCount arrow-vec))
      (readLong [_ idx] (.get arrow-vec idx))))

  (->mono-writer [arrow-vec _col-type]
    (let [wp (IWriterPosition/build (.getValueCount arrow-vec))]
      (reify IMonoVectorWriter
        (writerPosition [_] wp)
        (writeNull [_ _] (.setNull arrow-vec (.getPositionAndIncrement wp)))
        (writeLong [_ v] (.set arrow-vec (.getPositionAndIncrement wp) v)))))

  TimeNanoVector
  (->mono-reader [arrow-vec _col-type]
    (reify IMonoVectorReader
      (valueCount [_] (.getValueCount arrow-vec))
      (readLong [_ idx] (.get arrow-vec idx))))

  (->mono-writer [arrow-vec _col-type]
    (let [wp (IWriterPosition/build (.getValueCount arrow-vec))]
      (reify IMonoVectorWriter
        (writerPosition [_] wp)
        (writeNull [_ _] (.setNull arrow-vec (.getPositionAndIncrement wp)))
        (writeLong [_ v] (.setSafe arrow-vec (.getPositionAndIncrement wp) v))))))

;; the EE uses PDs internally for all types of intervals for now.
;; we could migrate it to use `xtdb.types.Interval*`
(extend-protocol MonoFactory
  IntervalYearVector
  (->mono-reader [arrow-vec _col-type]
    (reify IMonoVectorReader
      (valueCount [_] (.getValueCount arrow-vec))
      (readObject [_ idx]
        (PeriodDuration. (Period/ofMonths (.get arrow-vec idx)) Duration/ZERO))))

  (->mono-writer [arrow-vec _col-type]
    (let [wp (IWriterPosition/build (.getValueCount arrow-vec))]
      (reify IMonoVectorWriter
        (writerPosition [_] wp)
        (writeNull [_ _] (.setNull arrow-vec (.getPositionAndIncrement wp)))
        (writeObject [_ v]
          (let [^PeriodDuration period-duration v]
            (.set arrow-vec (.getPositionAndIncrement wp)
                  (.toTotalMonths (.getPeriod period-duration))))))))

  IntervalDayVector
  (->mono-reader [arrow-vec _col-type]
    (reify IMonoVectorReader
      (valueCount [_] (.getValueCount arrow-vec))
      (readObject [_ idx]
        (let [^IntervalDayTime idt (types/get-object arrow-vec idx)]
          (PeriodDuration. (.-period idt) (.-duration idt))))))

  (->mono-writer [arrow-vec _col-type]
    (let [wp (IWriterPosition/build (.getValueCount arrow-vec))]
      (reify IMonoVectorWriter
        (writerPosition [_] wp)
        (writeNull [_ _] (.setNull arrow-vec (.getPositionAndIncrement wp)))
        (writeObject [_ v]
          (let [^PeriodDuration period-duration v
                duration (.getDuration period-duration)
                dsecs (.getSeconds duration)
                dmillis (.toMillisPart duration)
                period (.getPeriod period-duration)]
            (when (or (not (zero? (.getYears period))) (not (zero? (.getMonths period))))
              (throw (err/illegal-arg "Period of PeriodDuration can not contain years or months!")))
            (.set arrow-vec (.getPositionAndIncrement wp)
                  (.getDays (.getPeriod period-duration))
                  (Math/addExact (Math/multiplyExact (long dsecs) (long 1000)) (long dmillis))))))))

  IntervalMonthDayNanoVector
  (->mono-reader [arrow-vec _col-type]
    (reify IMonoVectorReader
      (valueCount [_] (.getValueCount arrow-vec))
      (readObject [_ idx]
        (let [^IntervalMonthDayNano imdn (types/get-object arrow-vec idx)]
          (PeriodDuration. (.-period imdn) (.-duration imdn))))))

  (->mono-writer [arrow-vec _col-type]
    (let [wp (IWriterPosition/build (.getValueCount arrow-vec))]
      (reify IMonoVectorWriter
        (writerPosition [_] wp)
        (writeNull [_ _] (.setNull arrow-vec (.getPositionAndIncrement wp)))
        (writeObject [_ v]
          (let [^PeriodDuration period-duration v
                period (.getPeriod period-duration)
                duration (.getDuration period-duration)
                dsecs (.getSeconds duration)
                dnanos (.toNanosPart duration)]
            (.set arrow-vec (.getPositionAndIncrement wp)
                  (.toTotalMonths period)
                  (.getDays period)
                  (Math/addExact (Math/multiplyExact dsecs (long 1000000000)) (long dnanos)))))))))

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
                (readObject [_ idx] (.readObject inner-rdr (+ start-idx idx))))))))))

  (->mono-writer [arrow-vec [_ el-type]]
    (let [wp (IWriterPosition/build (.getValueCount arrow-vec))
          data-vec (.getDataVector arrow-vec)]
      (if (types/union? el-type)
        (let [inner-writer (->poly-writer data-vec el-type)]
          (reify IMonoVectorWriter
            (writerPosition [_] wp)
            (writeNull [_ _] (.setNull arrow-vec (.getPositionAndIncrement wp)))
            (writePolyListElements [_ el-count]
              (let [pos (.getPositionAndIncrement wp)
                    inner-pos (.startNewValue arrow-vec pos)]
                (.endValue arrow-vec pos el-count)
                (.setPosition (.writerPosition inner-writer) inner-pos))
              inner-writer)))

        (let [inner-writer (->mono-writer data-vec el-type)]
          (reify IMonoVectorWriter
            (writerPosition [_] wp)
            (writeNull [_ _] (.setNull arrow-vec (.getPositionAndIncrement wp)))
            (writeMonoListElements [_ el-count]
              (let [pos (.getPositionAndIncrement wp)
                    inner-pos (.startNewValue arrow-vec pos)]
                (.endValue arrow-vec pos el-count)
                (.setPosition (.writerPosition inner-writer) inner-pos))
              inner-writer)))))))

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
            (readObject [_ field-name] (.readObject ^IMonoVectorReader (get inner-readers field-name) idx))

            (readField [_ field-name]
              (let [^IPolyVectorReader inner-rdr (get inner-readers field-name)]
                (.read inner-rdr idx)
                inner-rdr)))))))

  (->mono-writer [arrow-vec [_ val-types]]
    (let [wp (IWriterPosition/build (.getValueCount arrow-vec))
          inner-writers (->> (for [[field val-type] val-types
                                   :let [field-name (str field)
                                         child-vec (.getChild arrow-vec field-name)]]
                               (MapEntry/create field-name
                                                (if (types/union? val-type)
                                                  (->poly-writer child-vec val-type)
                                                  (->mono-writer child-vec val-type))))
                             (into {}))]
      (reify IMonoVectorWriter
        (writerPosition [_] wp)
        (writeNull [_ _] (.setNull arrow-vec (.getPositionAndIncrement wp)))
        (monoStructFieldWriter [_ field] (get inner-writers field))
        (polyStructFieldWriter [_ field] (get inner-writers field))
        (writeStructEntries [_]
          (let [pos (.getPositionAndIncrement wp)]
            (.setIndexDefined arrow-vec pos)
            (doseq [inner-writer (vals inner-writers)
                    :let [^IWriterPosition wp
                          (cond
                            (instance? IMonoVectorWriter inner-writer)
                            (.writerPosition ^IMonoVectorWriter inner-writer)
                            (instance? IPolyVectorWriter inner-writer)
                            (.writerPosition ^IPolyVectorWriter inner-writer))]]
              ;; HACK this should be a super-interface
              (.setPosition wp pos))))))))

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
  (readObject [_] (.readObject inner idx)))

(deftype NullableVectorWriter [^IMonoVectorWriter inner]
  IPolyVectorWriter
  (writerPosition [_] (.writerPosition inner))

  (writeNull [_ _type-id v] (.writeNull inner v))
  (writeBoolean [_ _type-id v] (.writeBoolean inner v))
  (writeByte [_ _type-id v] (.writeByte inner v))
  (writeShort [_ _type-id v] (.writeShort inner v))
  (writeInt [_ _type-id v] (.writeInt inner v))
  (writeLong [_ _type-id v] (.writeLong inner v))
  (writeFloat [_ _type-id v] (.writeFloat inner v))
  (writeDouble [_ _type-id v] (.writeDouble inner v))
  (writeObject [_ _type-id v] (.writeObject inner v))
  (writeMonoListElements [_ _type-id el-count] (.writeMonoListElements inner el-count))
  (writePolyListElements [_ _type-id el-count] (.writePolyListElements inner el-count))
  (writeStructEntries [_ _type-id] (.writeStructEntries inner))
  (monoStructFieldWriter [_ _type-id field-name] (.monoStructFieldWriter inner field-name))
  (polyStructFieldWriter [_ _type-id field-name] (.polyStructFieldWriter inner field-name)))

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
  (readObject [_] (.readObject inner-rdr inner-offset)))

(extend-protocol PolyFactory
  NullVector
  (->poly-reader [_vec [_ inner-types]]
    (let [null-type-id (byte (.indexOf ^List (vec inner-types) :null))]
      (reify IPolyVectorReader
        (read [_ _idx] null-type-id)
        (read [_] null-type-id))))

  (->poly-writer [arrow-vec _col-type]
    (let [wp (IWriterPosition/build (.getValueCount arrow-vec))]
      (reify IPolyVectorWriter
        (writeNull [_ _type-id _] (.getPositionAndIncrement wp))
        (writerPosition [_] wp)))))

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

(defn- duv-writer-type-id-mapping
  "returns a mapping from writer type-id -> DUV type-id"
  ^bytes [^DenseUnionVector duv ordered-col-types]

  (let [type-count (count ordered-col-types)
        type-id-mapping (byte-array type-count)
        ^List duv-leg-keys (->> (.getChildren (.getField duv))
                                (mapv (comp types/col-type->duv-leg-key types/field->col-type)))]
    (dotimes [idx type-count]
      (aset type-id-mapping idx
            (byte (.indexOf duv-leg-keys (types/col-type->duv-leg-key (nth ordered-col-types idx))))))

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

  (->poly-writer [duv [_ inner-types]]
    (let [wp (IWriterPosition/build (.getValueCount duv))
          type-count (count inner-types)
          ordered-col-types (vec inner-types)
          type-id-mapping (duv-writer-type-id-mapping duv ordered-col-types)
          writers (object-array type-count)]

      (dotimes [type-id type-count]
        (aset writers type-id (->mono-writer (.getVectorByType duv (aget type-id-mapping type-id))
                                             (nth ordered-col-types type-id))))

      (letfn [(duv-child-writer [type-id]
                (let [duv-idx (.getPositionAndIncrement wp)
                      ^IMonoVectorWriter w (aget writers type-id)]
                  (.setTypeId duv duv-idx (aget type-id-mapping type-id))
                  (.setOffset duv duv-idx (.getPosition (.writerPosition w)))
                  w))]
        (reify IPolyVectorWriter
          (writerPosition [_] wp)

          (writeNull [_ type-id v] (.writeNull ^IMonoVectorWriter (duv-child-writer type-id) v))
          (writeBoolean [_ type-id v] (.writeBoolean ^IMonoVectorWriter (duv-child-writer type-id) v))
          (writeByte [_ type-id v] (.writeByte ^IMonoVectorWriter (duv-child-writer type-id) v))
          (writeShort [_ type-id v] (.writeShort ^IMonoVectorWriter (duv-child-writer type-id) v))
          (writeInt [_ type-id v] (.writeInt ^IMonoVectorWriter (duv-child-writer type-id) v))
          (writeLong [_ type-id v] (.writeLong ^IMonoVectorWriter (duv-child-writer type-id) v))
          (writeFloat [_ type-id v] (.writeFloat ^IMonoVectorWriter (duv-child-writer type-id) v))
          (writeDouble [_ type-id v] (.writeDouble ^IMonoVectorWriter (duv-child-writer type-id) v))
          (writeObject [_ type-id v] (.writeObject ^IMonoVectorWriter (duv-child-writer type-id) v))

          (writeStructEntries [_ type-id] (.writeStructEntries ^IMonoVectorWriter (duv-child-writer type-id)))
          (monoStructFieldWriter [_ type-id field-name] (.monoStructFieldWriter ^IMonoVectorWriter (aget writers type-id) field-name))
          (polyStructFieldWriter [_ type-id field-name] (.polyStructFieldWriter ^IMonoVectorWriter (aget writers type-id) field-name))

          (writeMonoListElements [_ type-id el-count] (.writeMonoListElements ^IMonoVectorWriter (duv-child-writer type-id) el-count))
          (writePolyListElements [_ type-id el-count] (.writePolyListElements ^IMonoVectorWriter (duv-child-writer type-id) el-count))))))

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
                            0))))

  (->poly-writer [arrow-vec [_ inner-types]]
    (let [ordered-col-types (vec inner-types)
          nn-type-id (case (.indexOf ^List ordered-col-types :null) 0 1, 1 0)]
      (->NullableVectorWriter (->mono-writer arrow-vec (nth ordered-col-types nn-type-id))))))

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
  (readObject [_] (.readObject inner)))

(defprotocol WriterFactory
  (^xtdb.vector.IVectorWriter2 ->writer [arrow-vec]))

(defprotocol ArrowWriteable
  (value->col-type [v])
  (write-value! [v ^xtdb.vector.IVectorWriter2 writer]))

(extend-protocol WriterFactory
  NullVector
  (->writer [arrow-vec]
    (let [wp (IWriterPosition/build (.getValueCount arrow-vec))]
      (reify IVectorWriter2
        (writerPosition [_] wp)
        (writeNull [_ _] (.setNull arrow-vec (.getPositionAndIncrement wp)))
        (writerForType [this _col-type] this))))

  BitVector
  (->writer [arrow-vec]
    (let [wp (IWriterPosition/build (.getValueCount arrow-vec))]
      (reify IVectorWriter2
        (writerPosition [_] wp)
        (writeNull [_ _] (.setNull arrow-vec (.getPositionAndIncrement wp)))
        (writeBoolean [_ v] (.setSafe arrow-vec (.getPositionAndIncrement wp) (if v 1 0)))
        (writerForType [this _col-type] this)))))

(defmacro def-writer-factory [clazz write-method]
  `(extend-protocol WriterFactory
     ~clazz
     (~'->writer [arrow-vec#]
      (let [wp# (IWriterPosition/build (.getValueCount arrow-vec#))]
        (reify IVectorWriter2
          (~'writerPosition [_#] wp#)

          (~'writeNull [_# _#] (.setNull arrow-vec# (.getPositionAndIncrement wp#)))

          (~write-method [_# v#]
           (.setSafe arrow-vec# (.getPositionAndIncrement wp#) v#))

          (~'writerForType [this# _col-type#] this#))))))

#_{:clj-kondo/ignore [:unresolved-symbol]}
(do
  (def-writer-factory TinyIntVector writeByte)
  (def-writer-factory SmallIntVector writeShort)
  (def-writer-factory IntVector writeInt)
  (def-writer-factory BigIntVector writeLong)
  (def-writer-factory Float4Vector writeFloat)
  (def-writer-factory Float8Vector writeDouble)

  (def-writer-factory DateDayVector writeInt)
  (def-writer-factory DateMilliVector writeLong)
  (def-writer-factory TimeStampVector writeLong)
  (def-writer-factory TimeSecVector writeLong)
  (def-writer-factory TimeMilliVector writeLong)
  (def-writer-factory TimeMicroVector writeLong)
  (def-writer-factory TimeNanoVector writeLong))

(extend-protocol ArrowWriteable
  nil
  (value->col-type [_] :null)
  (write-value! [_v ^IVectorWriter2 w] (.writeNull w nil))

  Boolean
  (value->col-type [_] :bool)
  (write-value! [v ^IVectorWriter2 w] (.writeBoolean w v))

  Byte
  (value->col-type [_] :i8)
  (write-value! [v ^IVectorWriter2 w] (.writeByte w v))

  Short
  (value->col-type [_] :i16)
  (write-value! [v ^IVectorWriter2 w] (.writeShort w v))

  Integer
  (value->col-type [_] :i32)
  (write-value! [v ^IVectorWriter2 w] (.writeInt w v))

  Long
  (value->col-type [_] :i64)
  (write-value! [v ^IVectorWriter2 w] (.writeLong w v))

  Float
  (value->col-type [_] :f32)
  (write-value! [v ^IVectorWriter2 w] (.writeFloat w v))

  Double
  (value->col-type [_] :f64)
  (write-value! [v ^IVectorWriter2 w] (.writeDouble w v)))

(extend-protocol ArrowWriteable
  Date
  (value->col-type [_] [:timestamp-tz :micro "UTC"])
  (write-value! [v ^IVectorWriter2 w] (.writeLong w (Math/multiplyExact (.getTime v) 1000)))

  Instant
  (value->col-type [_] [:timestamp-tz :micro "UTC"])
  (write-value! [v ^IVectorWriter2 w] (.writeLong w (util/instant->micros v)))

  ZonedDateTime
  (value->col-type [v] [:timestamp-tz :micro (.getId (.getZone v))])
  (write-value! [v ^IVectorWriter2 w] (write-value! (.toInstant v) w))

  OffsetDateTime
  (value->col-type [v] [:timestamp-tz :micro (.getId (.getOffset v))])
  (write-value! [v ^IVectorWriter2 w] (write-value! (.toInstant v) w))

  LocalDateTime
  (value->col-type [_] [:timestamp-local :micro])
  (write-value! [v ^IVectorWriter2 w] (write-value! (.toInstant v ZoneOffset/UTC) w))

  Duration
  (value->col-type [_] [:duration :micro])
  (write-value! [v ^IVectorWriter2 w] (.writeLong w (quot (.toNanos v) 1000)))

  LocalDate
  (value->col-type [_] [:date :day])
  (write-value! [v ^IVectorWriter2 w] (.writeInt w (.toEpochDay v)))

  LocalTime
  (value->col-type [_] [:time-local :nano])
  (write-value! [v ^IVectorWriter2 w] (.writeLong w (.toNanoOfDay v)))

  IntervalYearMonth
  (value->col-type [_] [:interval :year-month])
  (write-value! [v ^IVectorWriter2 w] (.writeInt w (.toTotalMonths (.-period v))))

  IntervalDayTime
  (value->col-type [_] [:interval :day-time])
  (write-value! [v ^IVectorWriter2 w] (.writeObject w v))

  IntervalMonthDayNano
  (value->col-type [_] [:interval :month-day-nano])
  (write-value! [v ^IVectorWriter2 w] (.writeObject w v))

  ;; allow the use of PeriodDuration for more precision
  PeriodDuration
  (value->col-type [_] [:interval :month-day-nano])
  (write-value! [v ^IVectorWriter2 w] (write-value! (IntervalMonthDayNano. (.getPeriod v) (.getDuration v)) w)))

(extend-protocol WriterFactory
  VarCharVector
  (->writer [arrow-vec]
    (let [wp (IWriterPosition/build (.getValueCount arrow-vec))]
      (reify IVectorWriter2
        (writerPosition [_] wp)
        (writeNull [_ _] (.setNull arrow-vec (.getPositionAndIncrement wp)))

        (writeBytes [_ v]
          (.setSafe arrow-vec (.getPositionAndIncrement wp)
                    v (.position v) (.remaining v)))

        (writeObject [this v]
          (.writeBytes this (.encode (.newEncoder StandardCharsets/UTF_8) (CharBuffer/wrap ^CharSequence v))))

        (writerForType [this _] this))))

  VarBinaryVector
  (->writer [arrow-vec]
    (let [wp (IWriterPosition/build (.getValueCount arrow-vec))]
      (reify IVectorWriter2
        (writerPosition [_] wp)
        (writeNull [_ _] (.setNull arrow-vec (.getPositionAndIncrement wp)))

        (writeBytes [_ v]
          (let [pos (.position v)]
            (.setSafe arrow-vec (.getPositionAndIncrement wp)
                      v (.position v) (.remaining v))
            (.position v pos)))

        (writeObject [this v] (.writeBytes this (ByteBuffer/wrap ^bytes v)))

        (writerForType [this _] this))))

  FixedSizeBinaryVector
  (->writer [arrow-vec]
    (let [byte-width (.getByteWidth arrow-vec)
          wp (IWriterPosition/build (.getValueCount arrow-vec))]
      (reify IVectorWriter2
        (writerPosition [_] wp)
        (writeNull [_ _] (.setNull arrow-vec (.getPositionAndIncrement wp)))

        (writeBytes [_ buf]
          (let [pos (.position buf)
                idx (.getPositionAndIncrement wp)]
            (.setIndexDefined arrow-vec idx)
            (.setBytes (.getDataBuffer arrow-vec) (* byte-width idx) buf)
            (.position buf pos)))

        (writeObject [this bytes] (.writeBytes this (ByteBuffer/wrap bytes)))))))

(extend-protocol ArrowWriteable
  (Class/forName "[B")
  (value->col-type [_] :varbinary)
  (write-value! [v ^IVectorWriter2 w] (.writeObject w v))

  ByteBuffer
  (value->col-type [_] :varbinary)
  (write-value! [v ^IVectorWriter2 w] (.writeBytes w v))

  CharSequence
  (value->col-type [_] :utf8)
  (write-value! [v ^IVectorWriter2 w] (.writeObject w v)))

(defn- populate-with-absents [^IVectorWriter2 w, ^long pos]
  (let [absents (- pos (.getPosition (.writerPosition w)))]
    (when (pos? absents)
      (let [absent-writer (.writerForType w :absent)]
        (dotimes [_ absents]
          (.writeNull absent-writer nil))))))

(extend-protocol WriterFactory
  ListVector
  (->writer [arrow-vec]
    (let [wp (IWriterPosition/build (.getValueCount arrow-vec))
          data-vec (.getDataVector arrow-vec)]
      (let [el-writer (->writer data-vec)
            el-wp (.writerPosition el-writer)]
        (reify IVectorWriter2
          (writerPosition [_] wp)
          (writeNull [_ _] (.setNull arrow-vec (.getPositionAndIncrement wp)))

          (listElementWriter [_] el-writer)
          (listWritten [_]
            (let [pos (.getPositionAndIncrement wp)
                  start-pos (.startNewValue arrow-vec pos)
                  end-pos (.getPosition el-wp)]
              (.endValue arrow-vec pos (- end-pos start-pos))))

          (writerForType [this _col-type] this)))))

  StructVector
  (->writer [arrow-vec]
    (let [wp (IWriterPosition/build (.getValueCount arrow-vec))
          writers (HashMap.)]
      (reify IVectorWriter2
        (writerPosition [_] wp)
        (writeNull [_ _] (.setNull arrow-vec (.getPositionAndIncrement wp)))

        (structKeyWriter [_ col-name]
          (.computeIfAbsent writers col-name
                      (reify Function
                        (apply [_ col-name]
                          (or (some-> (.getChild arrow-vec col-name)
                                      (->writer))

                              (doto (->writer (.addOrGet arrow-vec col-name
                                                         (FieldType/notNullable types/dense-union-type)
                                                         DenseUnionVector))
                                (populate-with-absents (.getPosition wp))))))))

        (structWritten [_]
          (.setIndexDefined arrow-vec (.getPositionAndIncrement wp))
          (doseq [[col-name ^IVectorWriter2 w] writers]
            (populate-with-absents w (.getPosition wp))))

        (writerForType [this _col-type] this)))))

(extend-protocol ArrowWriteable
  List
  (value->col-type [v] [:list (apply types/merge-col-types (into #{} (map value->col-type) v))])
  (write-value! [v ^IVectorWriter2 writer]
    (let [el-writer (.listElementWriter writer)]
      (doseq [el v]
        (write-value! el (.writerForType el-writer (value->col-type el))))
      (.listWritten writer)))

  Set
  (value->col-type [v] [:set (apply types/merge-col-types (into #{} (map value->col-type) v))])
  (write-value! [v ^IVectorWriter2 writer] (write-value! (vec v) writer))

  Map
  (value->col-type [v]
    (if (every? keyword? (keys v))
      [:struct (->> v
                    (into {} (map (juxt (comp symbol key)
                                        (comp value->col-type val)))))]
      [:map
       (apply types/merge-col-types (into #{} (map (comp value->col-type key)) v))
       (apply types/merge-col-types (into #{} (map (comp value->col-type val)) v))]))

  (write-value! [m ^IVectorWriter2 writer]
    (if (every? keyword? (keys m))
      (do
        (doseq [[k v] m
                :let [v-writer (-> (.structKeyWriter writer (str (symbol k)))
                                   (.writerForType (value->col-type v)))]]
          (write-value! v v-writer))

        (.structWritten writer))

      (throw (UnsupportedOperationException.)))))

(defn- duv-child-writer [^IVectorWriter2 w, write-value!]
  (reify IVectorWriter2
    (writerPosition [_] (.writerPosition w))

    (writeNull [_ v] (write-value!) (.writeNull w v))
    (writeBoolean [_ v] (write-value!) (.writeBoolean w v))
    (writeByte [_ v] (write-value!) (.writeByte w v))
    (writeShort [_ v] (write-value!) (.writeShort w v))
    (writeInt [_ v] (write-value!) (.writeInt w v))
    (writeLong [_ v] (write-value!) (.writeLong w v))
    (writeFloat [_ v] (write-value!) (.writeFloat w v))
    (writeDouble [_ v] (write-value!) (.writeDouble w v))
    (writeBytes [_ v] (write-value!) (.writeBytes w v))
    (writeObject [_ v] (write-value!) (.writeObject w v))

    (structKeyWriter [_ k] (.structKeyWriter w k))
    (structWritten [_] (write-value!) (.structWritten w))

    (listElementWriter [_] (.listElementWriter w))
    (listWritten [_] (write-value!) (.listWritten w))

    (registerNewType [_ field] (.registerNewType w field))
    (writerForType [_ col-type] (.writerForType w col-type))
    (writerForTypeId [_ type-id] (.writerForTypeId w type-id))))

(extend-protocol WriterFactory
  DenseUnionVector
  (->writer [duv]
    (let [wp (IWriterPosition/build (.getValueCount duv))
          type-count (count (.getChildren (.getField duv)))
          writers-by-type-id (ArrayList.)
          writers-by-type (HashMap.)]

      (letfn [(->child-writer [^long type-id]
                (let [v (.getVectorByType duv type-id)
                      child-wtr (->writer v)
                      child-wp (.writerPosition child-wtr)

                      child-wtr (-> child-wtr
                                    (duv-child-writer (fn write-value! []
                                                        (let [pos (.getPositionAndIncrement wp)]
                                                          (.setTypeId duv pos type-id)
                                                          (.setOffset duv pos (.getPosition child-wp))))))]
                  (.add writers-by-type-id type-id child-wtr)
                  child-wtr))]

        (dotimes [type-id type-count]
          (let [child-wtr (->child-writer type-id)
                col-type (-> (.getVectorByType duv type-id)
                             (.getField)
                             (types/field->col-type)
                             (types/col-type->duv-leg-key))]
            (.put writers-by-type col-type child-wtr)
            child-wtr))

        (reify IVectorWriter2
          (writerPosition [_] wp)

          (registerNewType [_ field]
            (let [type-id (.registerNewTypeId duv field)
                  new-vec (.createVector field (.getAllocator duv))]
              (.addVector duv type-id new-vec)
              (->child-writer type-id)
              type-id))

          (writerForType [this col-type]
            (.computeIfAbsent writers-by-type (types/col-type->duv-leg-key col-type)
                      (reify Function
                        (apply [_ _]
                          (let [field-name (types/col-type->field-name col-type)

                                ^Field field (case (types/col-type-head col-type)
                                               :list
                                               (types/->field field-name ArrowType$List/INSTANCE false (types/->field "$data$" types/dense-union-type false))


                                               :set
                                               (types/->field field-name SetType/INSTANCE false (types/->field "$data$" types/dense-union-type false))

                                               :struct
                                               (types/->field field-name ArrowType$Struct/INSTANCE false)

                                               (types/col-type->field field-name col-type))

                                type-id (.registerNewType this field)]

                            (.writerForTypeId this type-id))))))

          (writerForTypeId [_ type-id]
            (.get writers-by-type-id type-id)))))))

(extend-protocol WriterFactory
  ExtensionTypeVector
  (->writer [arrow-vec] (->writer (.getUnderlyingVector arrow-vec))))

(extend-protocol ArrowWriteable
  Keyword
  (value->col-type [_] :keyword)
  (write-value! [kw ^IVectorWriter2 w]
    (write-value! (str (symbol kw)) w))

  UUID
  (value->col-type [_] :uuid)
  (write-value! [^UUID uuid ^IVectorWriter2 w]
    (write-value! (util/uuid->bytes uuid) w))

  URI
  (value->col-type [_] :uri)
  (write-value! [^URI uri ^IVectorWriter2 w]
    (write-value! (str uri) w))

  ClojureForm
  (value->col-type [_] :clj-form)
  (write-value! [{:keys [form]} ^IVectorWriter2 w]
    (write-value! (pr-str form) w)))
