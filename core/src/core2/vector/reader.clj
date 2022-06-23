(ns core2.vector.reader
  (:require [core2.types :as types])
  (:import (core2.vector.reader BitUtil IMonoVectorReader IPolyVectorReader)
           java.util.List
           (org.apache.arrow.vector BaseFixedWidthVector BaseVariableWidthVector DateMilliVector ExtensionTypeVector FixedSizeBinaryVector IntervalDayVector IntervalMonthDayNanoVector IntervalYearVector NullVector TimeMicroVector TimeMilliVector TimeNanoVector TimeSecVector ValueVector)
           (org.apache.arrow.vector.complex DenseUnionVector)))

(set! *unchecked-math* :warn-on-boxed)

;; NOTE: if we were properly engaging in the memory management, we'd make all of these `Closeable`,
;; retain the buffers on the way in, and release them on `.close`.

;; However, this complicates the generated code in the expression engine, so we instead assume
;; that none of these readers will outlive their respective vectors.

(defprotocol MonoReaderFactory
  (->mono-reader ^core2.vector.reader.IMonoVectorReader [arrow-vec]))

(defprotocol PolyReaderFactory
  (->poly-reader ^core2.vector.reader.IPolyVectorReader [arrow-vec, ^List ordered-col-types]))

(deftype NullReader [^byte null-type-id]
   IMonoVectorReader
   IPolyVectorReader
   (read [_ _idx] null-type-id)
   (getTypeId [_] null-type-id))

(deftype FixedWidthReader [^BaseFixedWidthVector arrow-vec]
  IMonoVectorReader
  (readBoolean [_ idx] (BitUtil/readBoolean (.getDataBuffer arrow-vec) idx))
  (readByte [_ idx] (.getByte (.getDataBuffer arrow-vec) idx))
  (readShort [_ idx] (.getShort (.getDataBuffer arrow-vec) (* idx Short/BYTES)))
  (readInt [_ idx] (.getInt (.getDataBuffer arrow-vec) (* idx Integer/BYTES)))
  (readLong [_ idx] (.getLong (.getDataBuffer arrow-vec) (* idx Long/BYTES)))
  (readFloat [_ idx] (.getFloat (.getDataBuffer arrow-vec) (* idx Float/BYTES)))
  (readDouble [_ idx] (.getDouble (.getDataBuffer arrow-vec) (* idx Double/BYTES))))

(deftype FixedSizeBinaryReader [^FixedSizeBinaryVector arrow-vec, ^int byte-width]
  IMonoVectorReader
  (readBuffer [_ idx]
    (.nioBuffer (.getDataBuffer arrow-vec) (* byte-width idx) byte-width)))

(deftype VariableWidthReader [^BaseVariableWidthVector arrow-vec]
  IMonoVectorReader
  (readBuffer [_ idx]
    (.nioBuffer (.getDataBuffer arrow-vec) (.getStartOffset arrow-vec idx) (.getValueLength arrow-vec idx))))

(extend-protocol MonoReaderFactory
  ValueVector
  (->mono-reader [arrow-vec]
    (reify IMonoVectorReader
      (readObject [_ idx] (.getObject arrow-vec idx))))

  NullVector
  (->mono-reader [_] (->NullReader 0))

  DenseUnionVector
  (->mono-reader [arrow-vec]
    ;; we delegate to the only child vector, and assume the offsets are just 0..n (i.e. no indirection required)
    (if-let [child-vec (first (seq arrow-vec))]
      (->mono-reader child-vec)
      (->NullReader 0)))

  FixedSizeBinaryVector
  (->mono-reader [arrow-vec]
    (->FixedSizeBinaryReader arrow-vec (.getByteWidth arrow-vec)))

  BaseFixedWidthVector
  (->mono-reader [arrow-vec]
    (->FixedWidthReader arrow-vec))

  BaseVariableWidthVector
  (->mono-reader [arrow-vec]
    (->VariableWidthReader arrow-vec))

  ExtensionTypeVector
  (->mono-reader [arrow-vec] (->mono-reader (.getUnderlyingVector arrow-vec))))

;; (@wot) read as an epoch int, do not think it is worth branching for both cases in all date functions.
(extend-protocol MonoReaderFactory
  DateMilliVector
  (->mono-reader [arrow-vec]
    (reify IMonoVectorReader
      (readInt [_ idx] (-> (.get arrow-vec idx) (quot 86400000) (int))))))

(extend-protocol MonoReaderFactory
  TimeSecVector
  (->mono-reader [arrow-vec]
    (reify IMonoVectorReader
      (readLong [_ idx] (.get arrow-vec idx))))

  TimeMilliVector
  (->mono-reader [arrow-vec]
    (reify IMonoVectorReader
      (readLong [_ idx] (.get arrow-vec idx))))

  TimeMicroVector
  (->mono-reader [arrow-vec]
    (reify IMonoVectorReader
      (readLong [_ idx] (.get arrow-vec idx))))

  TimeNanoVector
  (->mono-reader [arrow-vec]
    (reify IMonoVectorReader
      (readLong [_ idx] (.get arrow-vec idx)))))

;; the Interval vectors just return PeriodDuration objects for now.
;; eventually (see discussion on #112) we may want these to have their own box objects.
(extend-protocol MonoReaderFactory
  IntervalYearVector
  (->mono-reader [arrow-vec]
    (reify IMonoVectorReader
      (readObject [_ idx] (.getObject arrow-vec idx))))

  IntervalDayVector
  (->mono-reader [arrow-vec]
    (reify IMonoVectorReader
      (readObject [_ idx] (.getObject arrow-vec idx))))

  IntervalMonthDayNanoVector
  (->mono-reader [arrow-vec]
    (reify IMonoVectorReader
      (readObject [_ idx] (.getObject arrow-vec idx)))))

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
                    ^"[Lcore2.vector.reader.IMonoVectorReader;" inner-readers
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

(extend-protocol PolyReaderFactory
  NullVector
  (->poly-reader [_vec ordered-col-types]
    (->NullReader (.indexOf ^List ordered-col-types :null)))

  DenseUnionVector
  (->poly-reader [duv ordered-col-types]
    (if (= 1 (count ordered-col-types))
      (->MonoToPolyReader (->mono-reader duv) 0 0)

      (let [child-count (count (seq duv))
            type-id-mapping (byte-array child-count)]
        (dotimes [type-id child-count]
          (aset type-id-mapping
                type-id
                (byte (.indexOf ^List ordered-col-types
                                (-> (.getVectorByType duv type-id) .getField
                                    types/field->col-type)))))
        (->DuvReader duv
                     type-id-mapping
                     (object-array (mapv ->mono-reader duv))
                     0 nil 0))))

  ExtensionTypeVector
  (->poly-reader [arrow-vec ordered-col-types]
    (->poly-reader (.getUnderlyingVector arrow-vec) ordered-col-types))

  ValueVector
  (->poly-reader [arrow-vec ordered-col-types]
    (let [mono-reader (->mono-reader arrow-vec)]
      (if (.isNullable (.getField arrow-vec))
        (let [null-type-id (.indexOf ^List ordered-col-types :null)
              nn-type-id (case null-type-id 0 1, 1 0)]
          (->NullableVectorReader arrow-vec mono-reader
                                  null-type-id nn-type-id
                                  0 0))
        (->MonoToPolyReader mono-reader 0 0)))))

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
