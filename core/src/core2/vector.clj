(ns core2.vector
  (:require [core2.types :as types])
  (:import (core2.types IntervalDayTime IntervalMonthDayNano)
           (core2.vector IMonoVectorWriter IMonoVectorReader IPolyVectorWriter IPolyVectorReader IWriterPosition)
           (java.time Duration Period)
           java.util.List
           (org.apache.arrow.vector BaseFixedWidthVector BaseVariableWidthVector BitVectorHelper DateMilliVector ExtensionTypeVector FixedSizeBinaryVector IntervalDayVector IntervalMonthDayNanoVector IntervalYearVector NullVector PeriodDuration TimeMicroVector TimeMilliVector TimeNanoVector TimeSecVector ValueVector)
           (org.apache.arrow.vector.complex DenseUnionVector)))

(set! *unchecked-math* :warn-on-boxed)

;; NOTE: if we were properly engaging in the memory management, we'd make all of these `Closeable`,
;; retain the buffers on the way in, and release them on `.close`.

;; However, this complicates the generated code in the expression engine, so we instead assume
;; that none of these readers will outlive their respective vectors.

(defprotocol MonoFactory
  (->mono-reader ^core2.vector.IMonoVectorReader [arrow-vec])
  (->mono-writer ^core2.vector.IMonoVectorWriter [arrow-vec]))

(defprotocol PolyFactory
  (->poly-reader ^core2.vector.IPolyVectorReader [arrow-vec, ^List ordered-col-types])
  (->poly-writer ^core2.vector.IPolyVectorWriter [arrow-vec, ^List ordered-col-types]))

(extend-protocol MonoFactory
  ValueVector
  (->mono-reader [arrow-vec]
    (reify IMonoVectorReader
      (readObject [_ idx] (.getObject arrow-vec idx))))

  NullVector
  (->mono-reader [_]
    (reify IMonoVectorReader))

  (->mono-writer [_]
    (let [wp (IWriterPosition/build)]
      (reify IMonoVectorWriter
        (writerPosition [_] wp)
        (writeNull [_ _] (.getPositionAndIncrement wp)))))

  DenseUnionVector
  (->mono-reader [arrow-vec]
    ;; we delegate to the only child vector, and assume the offsets are just 0..n (i.e. no indirection required)
    (if-let [child-vec (first (seq arrow-vec))]
      (->mono-reader child-vec)
      (->mono-reader (NullVector.)))))

(extend-protocol MonoFactory
  FixedSizeBinaryVector
  (->mono-reader [arrow-vec]
    (let [byte-width (.getByteWidth arrow-vec)]
      (reify IMonoVectorReader
        (readBuffer [_ idx]
          (.nioBuffer (.getDataBuffer arrow-vec) (* byte-width idx) byte-width)))))

  (->mono-writer [arrow-vec]
    (let [byte-width (.getByteWidth arrow-vec)
          wp (IWriterPosition/build)]
      (reify IMonoVectorWriter
        (writerPosition [_] wp)
        (writeNull [_ _] (.setNull arrow-vec (.getPositionAndIncrement wp)))
        (writeBuffer [_ buf]
          (let [pos (.position buf)
                idx (.getPositionAndIncrement wp)]
            (.setIndexDefined arrow-vec idx)
            (.setBytes (.getDataBuffer arrow-vec) (* byte-width idx) buf)
            (.position buf pos)))))))

(defn- fwv-write-idx ^long [^BaseFixedWidthVector arrow-vec, ^IWriterPosition wp]
  (let [idx (.getPositionAndIncrement wp)]
    (.setIndexDefined arrow-vec idx)
    idx))

(extend-protocol MonoFactory
  BaseFixedWidthVector
  (->mono-reader [arrow-vec]
    (reify IMonoVectorReader
      (readBoolean [_ idx] (== 1 (BitVectorHelper/get (.getDataBuffer arrow-vec) idx)))
      (readByte [_ idx] (.getByte (.getDataBuffer arrow-vec) idx))
      (readShort [_ idx] (.getShort (.getDataBuffer arrow-vec) (* idx Short/BYTES)))
      (readInt [_ idx] (.getInt (.getDataBuffer arrow-vec) (* idx Integer/BYTES)))
      (readLong [_ idx] (.getLong (.getDataBuffer arrow-vec) (* idx Long/BYTES)))
      (readFloat [_ idx] (.getFloat (.getDataBuffer arrow-vec) (* idx Float/BYTES)))
      (readDouble [_ idx] (.getDouble (.getDataBuffer arrow-vec) (* idx Double/BYTES)))))

  (->mono-writer [arrow-vec]
    (let [wp (IWriterPosition/build)]
      (reify
        IMonoVectorWriter
        (writerPosition [_] wp)

        (writeNull [_ _] (.setNull arrow-vec (.getPositionAndIncrement wp)))

        (writeBoolean [_ v]
          (let [buf (.getDataBuffer arrow-vec)
                idx (fwv-write-idx arrow-vec wp)]
            (if v
              (BitVectorHelper/setBit buf idx)
              (BitVectorHelper/unsetBit buf idx))))

        (writeByte [_ v] (.setByte (.getDataBuffer arrow-vec) (fwv-write-idx arrow-vec wp) v))
        (writeShort [_ v] (.setShort (.getDataBuffer arrow-vec) (* (fwv-write-idx arrow-vec wp) Short/BYTES) v))
        (writeInt [_ v] (.setInt (.getDataBuffer arrow-vec) (* (fwv-write-idx arrow-vec wp) Integer/BYTES) v))
        (writeLong [_ v] (.setLong (.getDataBuffer arrow-vec) (* (fwv-write-idx arrow-vec wp) Long/BYTES) v))
        (writeFloat [_ v] (.setFloat (.getDataBuffer arrow-vec) (* (fwv-write-idx arrow-vec wp) Float/BYTES) v))
        (writeDouble [_ v] (.setDouble (.getDataBuffer arrow-vec) (* (fwv-write-idx arrow-vec wp) Double/BYTES) v))))))

(extend-protocol MonoFactory
  BaseVariableWidthVector
  (->mono-reader [arrow-vec]
    (reify IMonoVectorReader
      (readBuffer [_ idx]
        (.nioBuffer (.getDataBuffer arrow-vec) (.getStartOffset arrow-vec idx) (.getValueLength arrow-vec idx)))))

  (->mono-writer [arrow-vec]
    (let [wp (IWriterPosition/build)]
      (reify IMonoVectorWriter
        (writerPosition [_] wp)

        (writeNull [_ _] (.setNull arrow-vec (.getPositionAndIncrement wp)))

        (writeBuffer [_ buf]
          (let [pos (.position buf)]
            (.setSafe arrow-vec (.getPositionAndIncrement wp) buf pos (- (.limit buf) pos))
            (.position buf pos)))))))

(extend-protocol MonoFactory
  ExtensionTypeVector
  (->mono-reader [arrow-vec] (->mono-reader (.getUnderlyingVector arrow-vec)))
  (->mono-writer [arrow-vec] (->mono-writer (.getUnderlyingVector arrow-vec))))

;; (@wot) read as an epoch int, do not think it is worth branching for both cases in all date functions.
(extend-protocol MonoFactory
  DateMilliVector
  (->mono-reader [arrow-vec]
    (reify IMonoVectorReader
      (readInt [_ idx] (-> (.get arrow-vec idx) (quot 86400000) (int)))))

  (->mono-writer [arrow-vec]
    (let [wp (IWriterPosition/build)]
      (reify IMonoVectorWriter
        (writerPosition [_] wp)
        (writeNull [_ _] (.setNull arrow-vec (.getPositionAndIncrement wp)))
        (writeInt [_ v] (.set arrow-vec (.getPositionAndIncrement wp) (* v 86400000)))))))

(extend-protocol MonoFactory
  TimeSecVector
  (->mono-reader [arrow-vec]
    (reify IMonoVectorReader
      (readLong [_ idx] (.get arrow-vec idx))))
  (->mono-writer [arrow-vec]
    (let [wp (IWriterPosition/build)]
      (reify IMonoVectorWriter
        (writerPosition [_] wp)
        (writeNull [_ _] (.setNull arrow-vec (.getPositionAndIncrement wp)))
        (writeLong [_ v] (.set arrow-vec (.getPositionAndIncrement wp) v)))))

  TimeMilliVector
  (->mono-reader [arrow-vec]
    (reify IMonoVectorReader
      (readLong [_ idx] (.get arrow-vec idx))))
  (->mono-writer [arrow-vec]
    (let [wp (IWriterPosition/build)]
      (reify IMonoVectorWriter
        (writerPosition [_] wp)
        (writeNull [_ _] (.setNull arrow-vec (.getPositionAndIncrement wp)))
        (writeLong [_ v] (.set arrow-vec (.getPositionAndIncrement wp) v)))))

  TimeMicroVector
  (->mono-reader [arrow-vec]
    (reify IMonoVectorReader
      (readLong [_ idx] (.get arrow-vec idx))))
  (->mono-writer [arrow-vec]
    (let [wp (IWriterPosition/build)]
      (reify IMonoVectorWriter
        (writerPosition [_] wp)
        (writeNull [_ _] (.setNull arrow-vec (.getPositionAndIncrement wp)))
        (writeLong [_ v] (.set arrow-vec (.getPositionAndIncrement wp) v)))))

  TimeNanoVector
  (->mono-reader [arrow-vec]
    (reify IMonoVectorReader
      (readLong [_ idx] (.get arrow-vec idx))))
  (->mono-writer [arrow-vec]
    (let [wp (IWriterPosition/build)]
      (reify IMonoVectorWriter
        (writerPosition [_] wp)
        (writeNull [_ _] (.setNull arrow-vec (.getPositionAndIncrement wp)))
        (writeLong [_ v] (.set arrow-vec (.getPositionAndIncrement wp) v))))))

;; the EE uses PDs internally for all types of intervals for now.
;; we could migrate it to use `core2.types.Interval*`
(extend-protocol MonoFactory
  IntervalYearVector
  (->mono-reader [arrow-vec]
    (reify IMonoVectorReader
      (readObject [_ idx]
        (PeriodDuration. (Period/ofMonths (.get arrow-vec idx)) Duration/ZERO))))

  (->mono-writer [arrow-vec]
    (let [wp (IWriterPosition/build)]
      (reify IMonoVectorWriter
        (writerPosition [_] wp)
        (writeNull [_ _] (.setNull arrow-vec (.getPositionAndIncrement wp)))
        (writeObject [_ v]
          (let [^PeriodDuration period-duration v]
            (.set arrow-vec (.getPositionAndIncrement wp)
                  (.toTotalMonths (.getPeriod period-duration))))))))

  IntervalDayVector
  (->mono-reader [arrow-vec]
    (reify IMonoVectorReader
      (readObject [_ idx]
        (let [^IntervalDayTime idt (types/get-object arrow-vec idx)]
          (PeriodDuration. (.-period idt) (.-duration idt))))))

  (->mono-writer [arrow-vec]
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
  (->mono-reader [arrow-vec]
    (reify IMonoVectorReader
      (readObject [_ idx]
        (let [^IntervalMonthDayNano imdn (types/get-object arrow-vec idx)]
          (PeriodDuration. (.-period imdn) (.-duration imdn))))))

  (->mono-writer [arrow-vec]
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

  (getTypeId [_] type-id)

  (readBoolean [_] (.readBoolean inner idx))
  (readByte [_] (.readByte inner idx))
  (readShort [_] (.readShort inner idx))
  (readInt [_] (.readInt inner idx))
  (readLong [_] (.readLong inner idx))
  (readFloat [_] (.readFloat inner idx))
  (readDouble [_] (.readDouble inner idx))
  (readBuffer [_] (.readBuffer inner idx))
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
  (writeBuffer [_ _type-id v] (.writeBuffer inner v))
  (writeObject [_ _type-id v] (.writeObject inner v)))

(deftype MonoToPolyReader [^IMonoVectorReader inner
                           ^:byte type-id
                           ^:unsynchronized-mutable ^int idx]
  IPolyVectorReader
  (read [this idx]
    (set! (.idx this) idx)
    type-id)

  (getTypeId [_] type-id)

  (readBoolean [_] (.readBoolean inner idx))
  (readByte [_] (.readByte inner idx))
  (readShort [_] (.readShort inner idx))
  (readInt [_] (.readInt inner idx))
  (readLong [_] (.readLong inner idx))
  (readFloat [_] (.readFloat inner idx))
  (readDouble [_] (.readDouble inner idx))
  (readBuffer [_] (.readBuffer inner idx))
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

  (getTypeId [_] mapped-type-id)

  (readBoolean [_] (.readBoolean inner-rdr inner-offset))
  (readByte [_] (.readByte inner-rdr inner-offset))
  (readShort [_] (.readShort inner-rdr inner-offset))
  (readInt [_] (.readInt inner-rdr inner-offset))
  (readLong [_] (.readLong inner-rdr inner-offset))
  (readFloat [_] (.readFloat inner-rdr inner-offset))
  (readDouble [_] (.readDouble inner-rdr inner-offset))
  (readBuffer [_] (.readBuffer inner-rdr inner-offset))
  (readObject [_] (.readObject inner-rdr inner-offset)))

(extend-protocol PolyFactory
  NullVector
  (->poly-reader [_vec ordered-col-types]
    (let [null-type-id (byte (.indexOf ^List ordered-col-types :null))]
      (reify IPolyVectorReader
        (read [_ _idx] null-type-id)
        (getTypeId [_] null-type-id))))

  (->poly-writer [_vec _ordered-col-types]
    (let [wp (IWriterPosition/build)]
      (reify IPolyVectorWriter
        (writeNull [_ _type-id _] (.getPositionAndIncrement wp))
        (writerPosition [_] wp)))))

(defn- duv-reader-type-id-mapping ^bytes [^DenseUnionVector duv ordered-col-types]
  (let [child-count (count (seq duv))
        type-id-mapping (byte-array child-count)]
    (dotimes [type-id child-count]
      (aset type-id-mapping
            type-id
            (byte (.indexOf ^List ordered-col-types
                            (-> (.getVectorByType duv type-id) .getField
                                types/field->col-type)))))
    type-id-mapping))

(defn- duv-writer-type-id-mapping ^bytes [^DenseUnionVector duv ordered-col-types]
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
  (->poly-reader [duv ordered-col-types]
    (if (= 1 (count ordered-col-types))
      (->MonoToPolyReader (->mono-reader duv) 0 0)

      (->DuvReader duv
                   (duv-reader-type-id-mapping duv ordered-col-types)
                   (object-array (mapv ->mono-reader duv))
                   0 nil 0)))

  (->poly-writer [duv ordered-col-types]
    (let [wp (IWriterPosition/build)
          type-count (count ordered-col-types)
          type-id-mapping (duv-writer-type-id-mapping duv ordered-col-types)
          writers (object-array type-count)]

      (dotimes [type-id type-count]
        (aset writers type-id (->mono-writer (.getVectorByType duv (aget type-id-mapping type-id)))))

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
          (writeBuffer [_ type-id v] (.writeBuffer ^IMonoVectorWriter (duv-child-writer type-id) v))
          (writeObject [_ type-id v] (.writeObject ^IMonoVectorWriter (duv-child-writer type-id) v))))))

  ExtensionTypeVector
  (->poly-reader [arrow-vec ordered-col-types]
    (->poly-reader (.getUnderlyingVector arrow-vec) ordered-col-types))

  ValueVector
  (->poly-reader [arrow-vec ordered-col-types]
    (let [field (.getField arrow-vec)
          mono-reader (->mono-reader arrow-vec)]
      (if (.isNullable field)
        (let [null-type-id (.indexOf ^List ordered-col-types :null)
              nn-type-id (case null-type-id 0 1, 1 0)]
          (->NullableVectorReader arrow-vec mono-reader
                                  null-type-id nn-type-id
                                  0 0))
        (->MonoToPolyReader mono-reader
                            (.indexOf ^List ordered-col-types (types/field->col-type field))
                            0))))

  (->poly-writer [arrow-vec _ordered-col-types]
    (->NullableVectorWriter (->mono-writer arrow-vec))))

(deftype IndirectVectorMonoReader [^IMonoVectorReader inner, ^ints idxs]
  IMonoVectorReader
  (readBoolean [_ idx] (.readBoolean inner (aget idxs idx)))
  (readByte [_ idx] (.readByte inner (aget idxs idx)))
  (readShort [_ idx] (.readShort inner (aget idxs idx)))
  (readInt [_ idx] (.readInt inner (aget idxs idx)))
  (readLong [_ idx] (.readLong inner (aget idxs idx)))
  (readFloat [_ idx] (.readFloat inner (aget idxs idx)))
  (readDouble [_ idx] (.readDouble inner (aget idxs idx)))
  (readBuffer [_ idx] (.readBuffer inner (aget idxs idx)))
  (readObject [_ idx] (.readObject inner (aget idxs idx))))

(deftype IndirectVectorPolyReader [^IPolyVectorReader inner, ^ints idxs]
  IPolyVectorReader
  (read [_ idx] (.read inner (aget idxs idx)))
  (getTypeId [_] (.getTypeId inner))

  (readBoolean [_] (.readBoolean inner))
  (readByte [_] (.readByte inner))
  (readShort [_] (.readShort inner))
  (readInt [_] (.readInt inner))
  (readLong [_] (.readLong inner))
  (readFloat [_] (.readFloat inner))
  (readDouble [_] (.readDouble inner))
  (readBuffer [_] (.readBuffer inner))
  (readObject [_] (.readObject inner)))
