(ns core2.vector
  (:require [core2.types :as types])
  (:import (clojure.lang MapEntry)
           (core2.types IntervalDayTime IntervalMonthDayNano)
           (core2.vector IMonoVectorReader IMonoVectorWriter IPolyVectorReader IPolyVectorWriter IStructValueReader IWriterPosition)
           java.nio.ByteBuffer
           (java.time Duration Period)
           java.util.List
           (org.apache.arrow.vector BaseVariableWidthVector BigIntVector BitVector DateDayVector DateMilliVector DurationVector ExtensionTypeVector
                                    FixedSizeBinaryVector Float4Vector Float8Vector IntVector IntervalDayVector IntervalMonthDayNanoVector
                                    IntervalYearVector NullVector PeriodDuration SmallIntVector TimeMicroVector TimeMilliVector TimeNanoVector TimeSecVector
                                    TimeStampVector TinyIntVector ValueVector)
           (org.apache.arrow.vector.complex DenseUnionVector ListVector StructVector)))

(set! *unchecked-math* :warn-on-boxed)

;; NOTE: if we were properly engaging in the memory management, we'd make all of these `Closeable`,
;; retain the buffers on the way in, and release them on `.close`.

;; However, this complicates the generated code in the expression engine, so we instead assume
;; that none of these readers will outlive their respective vectors.

(defprotocol MonoFactory
  (->mono-reader ^core2.vector.IMonoVectorReader [arrow-vec col-type])
  (->mono-writer ^core2.vector.IMonoVectorWriter [arrow-vec col-type]))

(defprotocol PolyFactory
  (->poly-reader ^core2.vector.IPolyVectorReader [arrow-vec col-type])
  (->poly-writer ^core2.vector.IPolyVectorWriter [arrow-vec col-type]))

(extend-protocol MonoFactory
  ValueVector
  (->mono-reader [arrow-vec _]
    (reify IMonoVectorReader
      (readObject [_ idx] (.getObject arrow-vec idx))))

  NullVector
  (->mono-reader [_ _]
    (reify IMonoVectorReader))

  (->mono-writer [_ _]
    (let [wp (IWriterPosition/build)]
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
        (readObject [_ idx]
          (.nioBuffer (.getDataBuffer arrow-vec) (* byte-width idx) byte-width)))))

  (->mono-writer [arrow-vec _col-type]
    (let [byte-width (.getByteWidth arrow-vec)
          wp (IWriterPosition/build)]
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
        (~read-method [_# idx#] (.get arrow-vec# idx#))))

     (~'->mono-writer [arrow-vec# _col-type#]
      (let [wp# (IWriterPosition/build)]
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
      (readBoolean [_ idx] (== 1 (.get arrow-vec idx)))))

  (->mono-writer [arrow-vec _col-type]
    (let [wp (IWriterPosition/build)]
      (reify
        IMonoVectorWriter
        (writerPosition [_] wp)
        (writeNull [_ _] (.setNull arrow-vec (.getPositionAndIncrement wp)))
        (writeBoolean [_ v] (.setSafe arrow-vec (.getPositionAndIncrement wp) (if v 1 0))))))

  DurationVector
  (->mono-reader [arrow-vec _col-type]
    (reify IMonoVectorReader
      ;; `.get` here returns an ArrowBuf, naturally.
      (readLong [_ idx] (DurationVector/get (.getDataBuffer arrow-vec) idx))))

  (->mono-writer [arrow-vec _col-type]
    (let [wp (IWriterPosition/build)]
      (reify
        IMonoVectorWriter
        (writerPosition [_] wp)
        (writeNull [_ _] (.setNull arrow-vec (.getPositionAndIncrement wp)))
        (writeLong [_ v] (.setSafe arrow-vec (.getPositionAndIncrement wp) v))))))

(extend-protocol MonoFactory
  BaseVariableWidthVector
  (->mono-reader [arrow-vec _col-type]
    (reify IMonoVectorReader
      (readObject [_ idx]
        (.nioBuffer (.getDataBuffer arrow-vec) (.getStartOffset arrow-vec idx) (.getValueLength arrow-vec idx)))))

  (->mono-writer [arrow-vec _col-type]
    (let [wp (IWriterPosition/build)]
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
  (->mono-reader [arrow-vec [_ _ underlying-col-type _]] (->mono-reader (.getUnderlyingVector arrow-vec) underlying-col-type))
  (->mono-writer [arrow-vec [_ _ underlying-col-type _]] (->mono-writer (.getUnderlyingVector arrow-vec) underlying-col-type)))

;; (@wot) read as an epoch int, do not think it is worth branching for both cases in all date functions.
(extend-protocol MonoFactory
  DateMilliVector
  (->mono-reader [arrow-vec _col-type]
    (reify IMonoVectorReader
      (readInt [_ idx] (-> (.get arrow-vec idx) (quot 86400000) (int)))))

  (->mono-writer [arrow-vec _col-type]
    (let [wp (IWriterPosition/build)]
      (reify IMonoVectorWriter
        (writerPosition [_] wp)
        (writeNull [_ _] (.setNull arrow-vec (.getPositionAndIncrement wp)))
        (writeInt [_ v] (.set arrow-vec (.getPositionAndIncrement wp) (* v 86400000)))))))

(extend-protocol MonoFactory
  TimeSecVector
  (->mono-reader [arrow-vec _col-type]
    (reify IMonoVectorReader
      (readLong [_ idx] (.get arrow-vec idx))))

  (->mono-writer [arrow-vec _col-type]
    (let [wp (IWriterPosition/build)]
      (reify IMonoVectorWriter
        (writerPosition [_] wp)
        (writeNull [_ _] (.setNull arrow-vec (.getPositionAndIncrement wp)))
        (writeLong [_ v] (.set arrow-vec (.getPositionAndIncrement wp) v)))))

  TimeMilliVector
  (->mono-reader [arrow-vec _col-type]
    (reify IMonoVectorReader
      (readLong [_ idx] (.get arrow-vec idx))))

  (->mono-writer [arrow-vec _col-type]
    (let [wp (IWriterPosition/build)]
      (reify IMonoVectorWriter
        (writerPosition [_] wp)
        (writeNull [_ _] (.setNull arrow-vec (.getPositionAndIncrement wp)))
        (writeLong [_ v] (.set arrow-vec (.getPositionAndIncrement wp) v)))))

  TimeMicroVector
  (->mono-reader [arrow-vec _col-type]
    (reify IMonoVectorReader
      (readLong [_ idx] (.get arrow-vec idx))))

  (->mono-writer [arrow-vec _col-type]
    (let [wp (IWriterPosition/build)]
      (reify IMonoVectorWriter
        (writerPosition [_] wp)
        (writeNull [_ _] (.setNull arrow-vec (.getPositionAndIncrement wp)))
        (writeLong [_ v] (.set arrow-vec (.getPositionAndIncrement wp) v)))))

  TimeNanoVector
  (->mono-reader [arrow-vec _col-type]
    (reify IMonoVectorReader
      (readLong [_ idx] (.get arrow-vec idx))))

  (->mono-writer [arrow-vec _col-type]
    (let [wp (IWriterPosition/build)]
      (reify IMonoVectorWriter
        (writerPosition [_] wp)
        (writeNull [_ _] (.setNull arrow-vec (.getPositionAndIncrement wp)))
        (writeLong [_ v] (.set arrow-vec (.getPositionAndIncrement wp) v))))))

;; the EE uses PDs internally for all types of intervals for now.
;; we could migrate it to use `core2.types.Interval*`
(extend-protocol MonoFactory
  IntervalYearVector
  (->mono-reader [arrow-vec _col-type]
    (reify IMonoVectorReader
      (readObject [_ idx]
        (PeriodDuration. (Period/ofMonths (.get arrow-vec idx)) Duration/ZERO))))

  (->mono-writer [arrow-vec _col-type]
    (let [wp (IWriterPosition/build)]
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
      (readObject [_ idx]
        (let [^IntervalDayTime idt (types/get-object arrow-vec idx)]
          (PeriodDuration. (.-period idt) (.-duration idt))))))

  (->mono-writer [arrow-vec _col-type]
    (let [wp (IWriterPosition/build)]
      (reify IMonoVectorWriter
        (writerPosition [_] wp)
        (writeNull [_ _] (.setNull arrow-vec (.getPositionAndIncrement wp)))
        (writeObject [_ v]
          (let [^PeriodDuration period-duration v
                duration (.getDuration period-duration)
                ddays (.toDaysPart duration)
                dsecs (Math/subtractExact (.getSeconds duration) (Math/multiplyExact ddays (long 86400)))
                dmillis (.toMillisPart duration)]
            (.set arrow-vec (.getPositionAndIncrement wp)
                  (Math/addExact (.getDays (.getPeriod period-duration)) (int ddays))
                  (Math/addExact (Math/multiplyExact (int dsecs) (int 1000)) dmillis)))))))

  IntervalMonthDayNanoVector
  (->mono-reader [arrow-vec _col-type]
    (reify IMonoVectorReader
      (readObject [_ idx]
        (let [^IntervalMonthDayNano imdn (types/get-object arrow-vec idx)]
          (PeriodDuration. (.-period imdn) (.-duration imdn))))))

  (->mono-writer [arrow-vec _col-type]
    (let [wp (IWriterPosition/build)]
      (reify IMonoVectorWriter
        (writerPosition [_] wp)
        (writeNull [_ _] (.setNull arrow-vec (.getPositionAndIncrement wp)))
        (writeObject [_ v]
          (let [^PeriodDuration period-duration v
                period (.getPeriod period-duration)
                duration (.getDuration period-duration)
                ddays (.toDaysPart duration)
                dsecs (Math/subtractExact (.getSeconds duration) (Math/multiplyExact ddays (long 86400)))
                dnanos (.toNanosPart duration)]
            (.set arrow-vec (.getPositionAndIncrement wp)
                  (.toTotalMonths period)
                  (Math/addExact (.getDays period) (int ddays))
                  (Math/addExact (Math/multiplyExact dsecs (long 1000000000)) (long dnanos)))))))))

(extend-protocol MonoFactory
  ListVector
  (->mono-reader [arrow-vec [_ el-type]]
    (if (types/union? el-type)
      (let [inner-rdr (->poly-reader (.getDataVector arrow-vec) el-type)]
        (reify IMonoVectorReader
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
    (let [wp (IWriterPosition/build)
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
                                   :let [field-name (name field)
                                         child-vec (.getChild arrow-vec field-name)]]
                               (MapEntry/create field-name
                                                (if (types/union? val-type)
                                                  (->poly-reader child-vec val-type)
                                                  (->mono-reader child-vec val-type))))
                             (into {}))]
      (reify IMonoVectorReader
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
    (let [wp (IWriterPosition/build)
          inner-writers (->> (for [[field val-type] val-types
                                   :let [field-name (name field)
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

  (readBoolean [_] (.readBoolean inner idx))
  (readByte [_] (.readByte inner idx))
  (readShort [_] (.readShort inner idx))
  (readInt [_] (.readInt inner idx))
  (readLong [_] (.readLong inner idx))
  (readFloat [_] (.readFloat inner idx))
  (readDouble [_] (.readDouble inner idx))
  (readObject [_] (.readObject inner idx)))

(deftype DuvReader [^DenseUnionVector duv, ^bytes type-id-mapping,
                    ^"[Lcore2.vector.IMonoVectorReader;" inner-readers
                    ^:unsynchronized-mutable ^byte mapped-type-id
                    ^:unsynchronized-mutable ^IMonoVectorReader inner-rdr
                    ^:unsynchronized-mutable ^int inner-offset]
  IPolyVectorReader
  (read [this idx]
    (let [type-id (.getTypeId duv idx)
          mapped-type-id (aget type-id-mapping type-id)]
      (set! (.mapped-type-id this) mapped-type-id)
      (set! (.inner-offset this) (.getOffset duv idx))
      (set! (.inner-rdr this) (aget inner-readers type-id))
      mapped-type-id))

  (read [_] mapped-type-id)

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

  (->poly-writer [_vec _col-type]
    (let [wp (IWriterPosition/build)]
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

          (->DuvReader duv type-id-mapping readers 0 nil 0)))))

  (->poly-writer [duv [_ inner-types]]
    (let [wp (IWriterPosition/build)
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

(deftype IndirectVectorMonoReader [^IMonoVectorReader inner, ^ints idxs]
  IMonoVectorReader
  (readBoolean [_ idx] (.readBoolean inner (aget idxs idx)))
  (readByte [_ idx] (.readByte inner (aget idxs idx)))
  (readShort [_ idx] (.readShort inner (aget idxs idx)))
  (readInt [_ idx] (.readInt inner (aget idxs idx)))
  (readLong [_ idx] (.readLong inner (aget idxs idx)))
  (readFloat [_ idx] (.readFloat inner (aget idxs idx)))
  (readDouble [_ idx] (.readDouble inner (aget idxs idx)))
  (readObject [_ idx] (.readObject inner (aget idxs idx))))

(deftype IndirectVectorPolyReader [^IPolyVectorReader inner, ^ints idxs]
  IPolyVectorReader
  (read [_ idx] (.read inner (aget idxs idx)))
  (read [_] (.read inner))

  (readBoolean [_] (.readBoolean inner))
  (readByte [_] (.readByte inner))
  (readShort [_] (.readShort inner))
  (readInt [_] (.readInt inner))
  (readLong [_] (.readLong inner))
  (readFloat [_] (.readFloat inner))
  (readDouble [_] (.readDouble inner))
  (readObject [_] (.readObject inner)))
