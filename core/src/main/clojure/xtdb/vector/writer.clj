(ns xtdb.vector.writer
  (:require [xtdb.error :as err]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector :as vec]
            [xtdb.vector.reader :as vr])
  (:import (clojure.lang Keyword)
           (java.lang AutoCloseable)
           (java.math BigDecimal)
           java.net.URI
           (java.nio ByteBuffer CharBuffer)
           java.nio.charset.StandardCharsets
           (java.time Duration Instant LocalDate LocalDateTime LocalTime OffsetDateTime ZoneOffset ZonedDateTime)
           (java.util Date HashMap LinkedHashMap List Map Set UUID)
           (java.util.function Function)
           (org.apache.arrow.memory BufferAllocator)
           (org.apache.arrow.vector BigIntVector BitVector DateDayVector DateMilliVector DecimalVector DurationVector ExtensionTypeVector FixedSizeBinaryVector Float4Vector Float8Vector IntVector IntervalDayVector IntervalMonthDayNanoVector IntervalYearVector NullVector PeriodDuration SmallIntVector TimeMicroVector TimeMilliVector TimeNanoVector TimeSecVector TimeStampVector TinyIntVector ValueVector VarBinaryVector VarCharVector VectorSchemaRoot)
           (org.apache.arrow.vector.complex DenseUnionVector ListVector StructVector)
           (org.apache.arrow.vector.types.pojo ArrowType ArrowType$Map ArrowType$Union Field FieldType)
           (xtdb.types IntervalDayTime IntervalMonthDayNano IntervalYearMonth)
           xtdb.types.ClojureForm
           (xtdb.vector IRelationWriter IRowCopier IVectorPosition IVectorReader IVectorWriter RelationReader)))

(set! *unchecked-math* :warn-on-boxed)

(defprotocol WriterFactory
  (^xtdb.vector.IVectorWriter ->writer* [arrow-vec notify!]
   "->writer* will call `notify!` whenever its col-type changes"))

(defn ->writer ^xtdb.vector.IVectorWriter [arrow-vec]
  (->writer* arrow-vec (fn [_])))

(defprotocol ArrowWriteable
  (value->col-type [v])

  (^org.apache.arrow.vector.types.pojo.ArrowType value->arrow-type [v])
  (write-value! [v ^xtdb.vector.IVectorWriter writer]))

(defn- null->vec-copier [^IVectorWriter dest-wtr]
  (let [wp (.writerPosition dest-wtr)]
    (reify IRowCopier
      (copyRow [_ _src-idx]
        (let [pos (.getPosition wp)]
          (.writeNull dest-wtr nil)
          pos)))))

(defn- duv->vec-copier [^IVectorWriter dest-wtr, ^DenseUnionVector src-vec]
  (let [copiers (object-array (for [child-vec src-vec]
                                (.rowCopier dest-wtr child-vec)))]
    (reify IRowCopier
      (copyRow [_ src-idx]
        (.copyRow ^IRowCopier (aget copiers (.getTypeId src-vec src-idx))
                  (.getOffset src-vec src-idx))))))

(defn- scalar-copier [^IVectorWriter dest-wtr, ^ValueVector src-vec]
  (let [wp (.writerPosition dest-wtr)
        dest-vec (.getVector dest-wtr)]
    (cond
      (instance? NullVector src-vec) (null->vec-copier dest-wtr)
      (instance? DenseUnionVector src-vec) (duv->vec-copier dest-wtr src-vec)

      :else
      (reify IRowCopier
        (copyRow [_ src-idx]
          (let [pos (.getPositionAndIncrement wp)]
            (.copyFromSafe dest-vec src-idx pos src-vec)
            pos))))))

(defn- scalar-leg-writer [^IVectorWriter w, ^ArrowType arrow-type]
  (let [field (.getField w)]
    (if (or (and (= arrow-type #xt.arrow/type :null) (.isNullable field))
            (= arrow-type (.getType field)))
      w
      (throw (IllegalArgumentException. (format "arrow-type mismatch: got <%s>, requested <%s>" (.getType field) arrow-type))))))

(extend-protocol WriterFactory
  NullVector
  (->writer* [arrow-vec _notify!]
    (let [wp (IVectorPosition/build (.getValueCount arrow-vec))]
      (reify IVectorWriter
        (getVector [_] arrow-vec)
        (getField [_] (.getField arrow-vec))

        (clear [_] (.clear arrow-vec) (.setPosition wp 0))

        (rowCopier [this-wtr _src-vec]
          ;; NullVector throws UOE on copyValueSafe 🙄
          (reify IRowCopier
            (copyRow [_ _src-idx]
              (let [pos (.getPosition wp)]
                (.writeNull this-wtr nil)
                pos))))

        (writerPosition [_] wp)
        (writeNull [_ _] (.setNull arrow-vec (.getPositionAndIncrement wp)))

        (^IVectorWriter legWriter [this ^ArrowType arrow-type] (scalar-leg-writer this arrow-type)))))

  BitVector
  (->writer* [arrow-vec _notify!]
    (let [wp (IVectorPosition/build (.getValueCount arrow-vec))]
      (reify IVectorWriter
        (getVector [_] arrow-vec)
        (getField [_] (.getField arrow-vec))
        (clear [_] (.clear arrow-vec) (.setPosition wp 0))
        (rowCopier [this src-vec] (scalar-copier this src-vec))
        (writerPosition [_] wp)
        (writeNull [_ _] (.setNull arrow-vec (.getPositionAndIncrement wp)))
        (writeBoolean [_ v] (.setSafe arrow-vec (.getPositionAndIncrement wp) (if v 1 0)))

        (^IVectorWriter legWriter [this ^ArrowType arrow-type] (scalar-leg-writer this arrow-type))))))

(defmacro def-writer-factory [clazz write-method]
  `(extend-protocol WriterFactory
     ~clazz
     (~'->writer* [arrow-vec# _notify!#]
      (let [wp# (IVectorPosition/build (.getValueCount arrow-vec#))
            field# (.getField arrow-vec#)]
        (reify IVectorWriter
          (~'getVector [_] arrow-vec#)
          (~'getField [_] field#)
          (~'clear [_] (.clear arrow-vec#) (.setPosition wp# 0))
          (~'rowCopier [this# src-vec#] (scalar-copier this# src-vec#))
          (~'writerPosition [_#] wp#)

          (~'writeNull [_# _#] (.setNull arrow-vec# (.getPositionAndIncrement wp#)))
          (~write-method [_# v#] (.setSafe arrow-vec# (.getPositionAndIncrement wp#) v#))

          (~(vary-meta 'legWriter assoc :tag 'IVectorWriter) [this# ^ArrowType arrow-type#] (scalar-leg-writer this# arrow-type#)))))))

#_{:clj-kondo/ignore [:unresolved-symbol]}
(do
  (def-writer-factory TinyIntVector writeByte)
  (def-writer-factory SmallIntVector writeShort)
  (def-writer-factory IntVector writeInt)
  (def-writer-factory BigIntVector writeLong)
  (def-writer-factory Float4Vector writeFloat)
  (def-writer-factory Float8Vector writeDouble)

  (def-writer-factory DateDayVector writeLong)
  (def-writer-factory TimeStampVector writeLong)
  (def-writer-factory TimeSecVector writeLong)
  (def-writer-factory TimeMilliVector writeLong)
  (def-writer-factory TimeMicroVector writeLong)
  (def-writer-factory TimeNanoVector writeLong))

(extend-protocol WriterFactory
  DateMilliVector
  (->writer* [arrow-vec _notify!]
    (let [wp (IVectorPosition/build (.getValueCount arrow-vec))]
      (reify IVectorWriter
        (getVector [_] arrow-vec)
        (getField [_] (.getField arrow-vec))
        (clear [_] (.clear arrow-vec) (.setPosition wp 0))
        (rowCopier [this src-vec] (scalar-copier this src-vec))
        (writerPosition [_] wp)
        (writeNull [_ _] (.setNull arrow-vec (.getPositionAndIncrement wp)))
        (writeLong [_ days] (.setSafe arrow-vec (.getPositionAndIncrement wp) (* days 86400000)))

        (^IVectorWriter legWriter [this ^ArrowType arrow-type] (scalar-leg-writer this arrow-type))))))

(extend-protocol WriterFactory
  DecimalVector
  (->writer* [arrow-vec _notify!]
    (let [wp (IVectorPosition/build (.getValueCount arrow-vec))]
      (reify IVectorWriter
        (getVector [_] arrow-vec)
        (getField [_] (.getField arrow-vec))
        (clear [_] (.clear arrow-vec) (.setPosition wp 0))
        (rowCopier [this src-vec] (scalar-copier this src-vec))
        (writerPosition [_] wp)
        (writeNull [_ _] (.setNull arrow-vec (.getPositionAndIncrement wp)))
        (writeObject [_  decimal]
          (let [new-decimal (.setScale ^BigDecimal decimal (.getScale arrow-vec))]
            (.setSafe arrow-vec (.getPositionAndIncrement wp) new-decimal)))

        (^IVectorWriter legWriter [this ^ArrowType arrow-type] (scalar-leg-writer this arrow-type))))))


(extend-protocol ArrowWriteable
  nil
  (value->arrow-type [_] #xt.arrow/type :null)
  (value->col-type [_] :null)
  (write-value! [_v ^IVectorWriter w] (.writeNull w nil))

  Boolean
  (value->arrow-type [_] #xt.arrow/type :bool)
  (value->col-type [_] :bool)
  (write-value! [v ^IVectorWriter w] (.writeBoolean w v))

  Byte
  (value->arrow-type [_] #xt.arrow/type :i8)
  (value->col-type [_] :i8)
  (write-value! [v ^IVectorWriter w] (.writeByte w v))

  Short
  (value->arrow-type [_] #xt.arrow/type :i16)
  (value->col-type [_] :i16)
  (write-value! [v ^IVectorWriter w] (.writeShort w v))

  Integer
  (value->arrow-type [_] #xt.arrow/type :i32)
  (value->col-type [_] :i32)
  (write-value! [v ^IVectorWriter w] (.writeInt w v))

  Long
  (value->arrow-type [_] #xt.arrow/type :i64)
  (value->col-type [_] :i64)
  (write-value! [v ^IVectorWriter w] (.writeLong w v))

  Float
  (value->arrow-type [_] #xt.arrow/type :f32)
  (value->col-type [_] :f32)
  (write-value! [v ^IVectorWriter w] (.writeFloat w v))

  Double
  (value->arrow-type [_] #xt.arrow/type :f64)
  (value->col-type [_] :f64)
  (write-value! [v ^IVectorWriter w] (.writeDouble w v))

  BigDecimal
  (value->arrow-type [_] #xt.arrow/type :decimal)
  (value->col-type [_] :decimal)
  (write-value! [v ^IVectorWriter w] (.writeObject w v)))

(extend-protocol WriterFactory
  DurationVector
  (->writer* [arrow-vec _notify!]
    (let [wp (IVectorPosition/build (.getValueCount arrow-vec))
          field (.getField arrow-vec)]
      (reify IVectorWriter
        (getVector [_] arrow-vec)
        (getField [_] field)
        (clear [_] (.clear arrow-vec) (.setPosition wp 0))
        (rowCopier [this src-vec] (scalar-copier this src-vec))
        (writerPosition [_] wp)
        (writeNull [_ _] (.setNull arrow-vec (.getPositionAndIncrement wp)))
        (writeLong [_ v] (.setSafe arrow-vec (.getPositionAndIncrement wp) v))

        (^IVectorWriter legWriter [this ^ArrowType arrow-type] (scalar-leg-writer this arrow-type)))))

  IntervalYearVector
  (->writer* [arrow-vec _notify!]
    (let [wp (IVectorPosition/build (.getValueCount arrow-vec))]
      (reify IVectorWriter
        (getVector [_] arrow-vec)
        (getField [_] (.getField arrow-vec))
        (clear [_] (.clear arrow-vec) (.setPosition wp 0))
        (rowCopier [this src-vec] (scalar-copier this src-vec))
        (writerPosition [_] wp)
        (writeNull [_ _] (.setNull arrow-vec (.getPositionAndIncrement wp)))
        (writeInt [_ total-months] (.setSafe arrow-vec (.getPositionAndIncrement wp) total-months))
        (writeObject [this v]
          (let [^PeriodDuration period-duration v]
            (.writeInt this (.toTotalMonths (.getPeriod period-duration)))))

        (^IVectorWriter legWriter [this ^ArrowType arrow-type] (scalar-leg-writer this arrow-type)))))

  IntervalDayVector
  (->writer* [arrow-vec _notify!]
    (let [wp (IVectorPosition/build (.getValueCount arrow-vec))]
      (reify IVectorWriter
        (getVector [_] arrow-vec)
        (getField [_] (.getField arrow-vec))
        (clear [_] (.clear arrow-vec) (.setPosition wp 0))
        (rowCopier [this src-vec] (scalar-copier this src-vec))
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
            (.setSafe arrow-vec (.getPositionAndIncrement wp)
                      (.getDays (.getPeriod period-duration))
                      (Math/addExact (Math/multiplyExact (long dsecs) (long 1000)) (long dmillis)))))

        (^IVectorWriter legWriter [this ^ArrowType arrow-type] (scalar-leg-writer this arrow-type)))))

  IntervalMonthDayNanoVector
  (->writer* [arrow-vec _notify!]
    (let [wp (IVectorPosition/build (.getValueCount arrow-vec))]
      (reify IVectorWriter
        (getVector [_] arrow-vec)
        (getField [_] (.getField arrow-vec))
        (clear [_] (.clear arrow-vec) (.setPosition wp 0))
        (rowCopier [this src-vec] (scalar-copier this src-vec))
        (writerPosition [_] wp)
        (writeNull [_ _] (.setNull arrow-vec (.getPositionAndIncrement wp)))
        (writeObject [_ v]
          (let [^PeriodDuration period-duration v
                period (.getPeriod period-duration)
                duration (.getDuration period-duration)
                dsecs (.getSeconds duration)
                dnanos (.toNanosPart duration)]
            (.setSafe arrow-vec (.getPositionAndIncrement wp)
                      (.toTotalMonths period)
                      (.getDays period)
                      (Math/addExact (Math/multiplyExact dsecs (long 1000000000)) (long dnanos)))))

        (^IVectorWriter legWriter [this ^ArrowType arrow-type] (scalar-leg-writer this arrow-type))))))

(extend-protocol ArrowWriteable
  Date
  (value->arrow-type [_] #xt.arrow/type [:timestamp-tz :micro "UTC"])
  (value->col-type [_] [:timestamp-tz :micro "UTC"])
  (write-value! [v ^IVectorWriter w] (.writeLong w (Math/multiplyExact (.getTime v) 1000)))

  Instant
  (value->arrow-type [_] #xt.arrow/type [:timestamp-tz :micro "UTC"])
  (value->col-type [_] [:timestamp-tz :micro "UTC"])
  (write-value! [v ^IVectorWriter w] (.writeLong w (util/instant->micros v)))

  ZonedDateTime
  (value->arrow-type [v] (types/->arrow-type [:timestamp-tz :micro (.getId (.getZone v))]))
  (value->col-type [v] [:timestamp-tz :micro (.getId (.getZone v))])
  (write-value! [v ^IVectorWriter w] (write-value! (.toInstant v) w))

  OffsetDateTime
  (value->arrow-type [v] (types/->arrow-type [:timestamp-tz :micro (.getId (.getOffset v))]))
  (value->col-type [v] [:timestamp-tz :micro (.getId (.getOffset v))])
  (write-value! [v ^IVectorWriter w] (write-value! (.toInstant v) w))

  LocalDateTime
  (value->arrow-type [_] #xt.arrow/type [:timestamp-local :micro])
  (value->col-type [_] [:timestamp-local :micro])
  (write-value! [v ^IVectorWriter w] (write-value! (.toInstant v ZoneOffset/UTC) w))

  Duration
  (value->arrow-type [_] #xt.arrow/type [:duration :micro])
  (value->col-type [_] [:duration :micro])
  (write-value! [v ^IVectorWriter w] (.writeLong w (quot (.toNanos v) 1000)))

  LocalDate
  (value->arrow-type [_] #xt.arrow/type [:date :day])
  (value->col-type [_] [:date :day])
  (write-value! [v ^IVectorWriter w] (.writeLong w (.toEpochDay v)))

  LocalTime
  (value->arrow-type [_] #xt.arrow/type [:time-local :nano])
  (value->col-type [_] [:time-local :nano])
  (write-value! [v ^IVectorWriter w] (.writeLong w (.toNanoOfDay v)))

  IntervalYearMonth
  (value->arrow-type [_] #xt.arrow/type [:interval :year-month])
  (value->col-type [_] [:interval :year-month])
  (write-value! [v ^IVectorWriter w] (.writeInt w (.toTotalMonths (.period v))))

  IntervalDayTime
  (value->arrow-type [_] #xt.arrow/type [:interval :day-time])
  (value->col-type [_] [:interval :day-time])
  (write-value! [v ^IVectorWriter w] (write-value! (PeriodDuration. (.period v) (.duration v)) w))

  IntervalMonthDayNano
  (value->arrow-type [_] #xt.arrow/type [:interval :month-day-nano])
  (value->col-type [_] [:interval :month-day-nano])
  (write-value! [v ^IVectorWriter w] (write-value! (PeriodDuration. (.period v) (.duration v)) w))

  ;; allow the use of PeriodDuration for more precision
  PeriodDuration
  (value->arrow-type [_] #xt.arrow/type [:interval :month-day-nano])
  (value->col-type [_] [:interval :month-day-nano])
  (write-value! [v ^IVectorWriter w] (.writeObject w v)))

(extend-protocol WriterFactory
  VarCharVector
  (->writer* [arrow-vec _notify!]
    (let [wp (IVectorPosition/build (.getValueCount arrow-vec))]
      (reify IVectorWriter
        (getVector [_] arrow-vec)
        (getField [_] (.getField arrow-vec))
        (clear [_] (.clear arrow-vec) (.setPosition wp 0))
        (rowCopier [this src-vec] (scalar-copier this src-vec))
        (writerPosition [_] wp)
        (writeNull [_ _] (.setNull arrow-vec (.getPositionAndIncrement wp)))

        (writeBytes [_ v]
          (let [pos (.position v)]
            (.setSafe arrow-vec (.getPositionAndIncrement wp)
                      v (.position v) (.remaining v))
            (.position v pos)))

        (writeObject [this v]
          (.writeBytes this (.encode (.newEncoder StandardCharsets/UTF_8) (CharBuffer/wrap ^CharSequence v))))

        (^IVectorWriter legWriter [this ^ArrowType arrow-type] (scalar-leg-writer this arrow-type)))))

  VarBinaryVector
  (->writer* [arrow-vec _notify!]
    (let [wp (IVectorPosition/build (.getValueCount arrow-vec))]
      (reify IVectorWriter
        (getVector [_] arrow-vec)
        (getField [_] (.getField arrow-vec))
        (clear [_] (.clear arrow-vec) (.setPosition wp 0))
        (rowCopier [this src-vec] (scalar-copier this src-vec))
        (writerPosition [_] wp)
        (writeNull [_ _] (.setNull arrow-vec (.getPositionAndIncrement wp)))

        (writeBytes [_ v]
          (let [pos (.position v)]
            (.setSafe arrow-vec (.getPositionAndIncrement wp)
                      v (.position v) (.remaining v))
            (.position v pos)))

        (writeObject [this v] (.writeBytes this (ByteBuffer/wrap ^bytes v)))

        (^IVectorWriter legWriter [this ^ArrowType arrow-type] (scalar-leg-writer this arrow-type)))))

  FixedSizeBinaryVector
  (->writer* [arrow-vec _notify!]
    (let [byte-width (.getByteWidth arrow-vec)
          wp (IVectorPosition/build (.getValueCount arrow-vec))]
      (reify IVectorWriter
        (getVector [_] arrow-vec)
        (getField [_] (.getField arrow-vec))
        (clear [_] (.clear arrow-vec) (.setPosition wp 0))
        (rowCopier [this src-vec] (scalar-copier this src-vec))
        (writerPosition [_] wp)
        (writeNull [_ _] (.setNull arrow-vec (.getPositionAndIncrement wp)))

        (writeBytes [_ buf]
          (let [pos (.position buf)
                idx (.getPositionAndIncrement wp)]
            (.setIndexDefined arrow-vec idx)
            (.setBytes (.getDataBuffer arrow-vec) (* byte-width idx) buf)
            (.position buf pos)))

        (writeObject [this bytes] (.writeBytes this (ByteBuffer/wrap bytes)))

        (^IVectorWriter legWriter [this ^ArrowType arrow-type] (scalar-leg-writer this arrow-type))))))

(extend-protocol ArrowWriteable
  (Class/forName "[B")
  (value->arrow-type [_] #xt.arrow/type :varbinary)
  (value->col-type [_] :varbinary)
  (write-value! [v ^IVectorWriter w] (.writeObject w v))

  ByteBuffer
  (value->arrow-type [_] #xt.arrow/type :varbinary)
  (value->col-type [_] :varbinary)
  (write-value! [v ^IVectorWriter w] (.writeBytes w v))

  CharSequence
  (value->arrow-type [_] #xt.arrow/type :utf8)
  (value->col-type [_] :utf8)
  (write-value! [v ^IVectorWriter w] (.writeObject w v)))

(defn populate-with-absents [^IVectorWriter w, ^long pos]
  (let [absents (- pos (.getPosition (.writerPosition w)))]
    (when (pos? absents)
      (if-let [absent-writer (let [field (.getField w)]
                               (cond (= #xt.arrow/type :union (.getType field))
                                     (.legWriter w #xt.arrow/type :absent)
                                     (.isNullable field)
                                     w
                                     :else nil))]
        (dotimes [_ absents]
          (.writeNull absent-writer nil))
        (throw (UnsupportedOperationException. "populate-with-absents needs a nullable or union underneath!"))))))

(extend-protocol WriterFactory
  ListVector
  (->writer* [arrow-vec notify!]
    (let [col-name (.getName arrow-vec)
          field (.getField arrow-vec)
          nullable? (.isNullable field)
          wp (IVectorPosition/build (.getValueCount arrow-vec))
          !field (atom field)]

      (letfn [(set-field! [el-field]
                (reset! !field (types/->field col-name #xt.arrow/type :list nullable? el-field)))

              (->el-writer [data-vec]
                (->writer* data-vec
                           (fn notify-list-writer! [el-field]
                             (notify! (set-field! el-field)))))]

        (let [!el-writer (atom (->el-writer (.getDataVector arrow-vec)))]

          (reify IVectorWriter
            (getVector [_] arrow-vec)
            (getField [_] @!field)
            (clear [_] (.clear arrow-vec) (.setPosition wp 0) (.clear ^IVectorWriter @!el-writer))

            (rowCopier [this-wtr src-vec]
              (cond
                (instance? NullVector src-vec) (null->vec-copier this-wtr)
                (instance? DenseUnionVector src-vec) (duv->vec-copier this-wtr src-vec)
                :else (let [^ListVector src-vec src-vec
                            data-vec (.getDataVector src-vec)
                            inner-copier (.rowCopier (.listElementWriter this-wtr) data-vec)]
                        (reify IRowCopier
                          (copyRow [_ src-idx]
                            (let [pos (.getPosition wp)]
                              (if (.isNull src-vec src-idx)
                                (.writeNull this-wtr nil)
                                (do
                                  (.startList this-wtr)
                                  (let [start-idx (.getElementStartIndex src-vec src-idx)
                                        end-idx (.getElementEndIndex src-vec src-idx)]
                                    (dotimes [n (- end-idx start-idx)]
                                      (.copyRow inner-copier (+ start-idx n))))
                                  (.endList this-wtr)))
                              pos))))))

            (writerPosition [_] wp)
            (writeNull [_ _] (.setNull arrow-vec (.getPositionAndIncrement wp)))

            (listElementWriter [this]
              (if (instance? NullVector (.getDataVector arrow-vec))
                (.listElementWriter this (FieldType/notNullable #xt.arrow/type :union))
                @!el-writer))

            (listElementWriter [_ field-type]
              (let [create-vec-res (.addOrGetVector arrow-vec field-type)]
                (if (.isCreated create-vec-res)
                  (let [new-data-vec (.getVector create-vec-res)]
                    (notify! (set-field! (.getField new-data-vec)))
                    (reset! !el-writer (->el-writer new-data-vec)))
                  @!el-writer)))

            (startList [_]
              (.startNewValue arrow-vec (.getPosition wp)))

            (endList [_]
              (let [pos (.getPositionAndIncrement wp)
                    end-pos (.getPosition (.writerPosition ^IVectorWriter @!el-writer))]
                (.endValue arrow-vec pos (- end-pos (.getElementStartIndex arrow-vec pos)))))

            (^IVectorWriter legWriter [this ^ArrowType arrow-type] (scalar-leg-writer this arrow-type)))))))

  StructVector
  (->writer* [arrow-vec notify!]
    (let [col-name (.getName arrow-vec)
          nullable? (.isNullable (.getField arrow-vec))
          wp (IVectorPosition/build (.getValueCount arrow-vec))
          writers (HashMap.)
          !fields (atom {})]

      (letfn [(update-child-field! [^Field field]
                (apply types/->field col-name #xt.arrow/type :struct nullable?
                       (vals (swap! !fields assoc (.getName field) field))))

              (->key-writer [^ValueVector child-vec]
                (let [w (->writer* child-vec (comp notify! update-child-field!))]
                  (update-child-field! (.getField w))
                  w))]

        (doseq [^ValueVector child-vec arrow-vec]
          (.put writers (.getName child-vec) (->key-writer child-vec)))

        (reify IVectorWriter
          (getVector [_] arrow-vec)

          (getField [_]
            (apply types/->field col-name #xt.arrow/type :struct nullable? (vals @!fields)))

          (clear [_] (.clear arrow-vec) (.setPosition wp 0) (run! #(.clear ^IVectorWriter %) (.values writers)))

          (rowCopier [this-wtr src-vec]
            (cond
              (instance? NullVector src-vec) (null->vec-copier this-wtr)
              (instance? DenseUnionVector src-vec) (duv->vec-copier this-wtr src-vec)
              :else (let [^StructVector src-vec src-vec
                          inner-copiers (mapv (fn [^ValueVector inner]
                                                (-> (.structKeyWriter this-wtr (.getName inner))
                                                    (.rowCopier inner)))
                                              src-vec)]
                      (reify IRowCopier
                        (copyRow [_ src-idx]
                          (let [pos (.getPosition wp)]
                            (if (.isNull src-vec src-idx)
                              (.writeNull this-wtr nil)
                              (do
                                (.startStruct this-wtr)
                                (doseq [^IRowCopier inner inner-copiers]
                                  (.copyRow inner src-idx))
                                (.endStruct this-wtr)))
                            pos))))))

          (writerPosition [_] wp)

          (writeNull [_ _]
            (.setNull arrow-vec (.getPositionAndIncrement wp))

            (doseq [^IVectorWriter w (.values writers)]
              (.writeNull w nil)))

          (structKeyWriter [this-wtr col-name]
            (or (.get writers col-name)
                (.structKeyWriter this-wtr col-name (FieldType/notNullable #xt.arrow/type :union))))

          (^IVectorWriter structKeyWriter [this-wtr ^String col-name ^FieldType field-type]
           (when-let [^IVectorWriter wrt (.get writers col-name)]
             (when-not (= (.getFieldType (.getField wrt)) field-type)
               (throw (IllegalStateException. "Field type mismatch"))))

           (.computeIfAbsent writers col-name
                             (reify Function
                               (apply [_ col-name]
                                 (let [w (doto (->key-writer (.addOrGet arrow-vec col-name field-type ValueVector))
                                           (populate-with-absents (.getPosition wp)))]
                                   (notify! (.getField this-wtr))
                                   w)))))

          (startStruct [_]
            (.setIndexDefined arrow-vec (.getPosition wp)))

          (endStruct [_]
            (let [pos (.getPositionAndIncrement wp)]
              (doseq [^IVectorWriter w (.values writers)]
                (populate-with-absents w (inc pos)))))

          (^IVectorWriter legWriter [this ^ArrowType arrow-type] (scalar-leg-writer this arrow-type))

          Iterable
          (iterator [_] (.iterator (.entrySet writers))))))))

(extend-protocol ArrowWriteable
  List
  (value->arrow-type [_] #xt.arrow/type :list)
  (value->col-type [v] [:list (apply types/merge-col-types (into #{} (map value->col-type) v))])
  (write-value! [v ^IVectorWriter writer]
    (let [el-writer (.listElementWriter writer)]
      (.startList writer)
      (doseq [el v]
        (write-value! el (.legWriter el-writer (value->arrow-type el))))
      (.endList writer)))

  Set
  (value->arrow-type [_] #xt.arrow/type :set)
  (value->col-type [v] [:set (apply types/merge-col-types (into #{} (map value->col-type) v))])
  (write-value! [v ^IVectorWriter writer] (write-value! (vec v) writer))

  Map
  (value->arrow-type [v]
    (if (every? keyword? (keys v))
      #xt.arrow/type :struct
      (ArrowType$Map. false)))

  (value->col-type [v]
    (if (every? keyword? (keys v))
      [:struct (->> v
                    (into {} (map (juxt (comp symbol key)
                                        (comp value->col-type val)))))]
      [:map
       (apply types/merge-col-types (into #{} (map (comp value->col-type key)) v))
       (apply types/merge-col-types (into #{} (map (comp value->col-type val)) v))]))

  (write-value! [m ^IVectorWriter writer]
    (if (every? keyword? (keys m))
      (do
        (.startStruct writer)

        (doseq [[k v] m
                :let [v-writer (-> (.structKeyWriter writer (util/str->normal-form-str (str (symbol k))))
                                   (.legWriter (value->arrow-type v)))]]
          (write-value! v v-writer))

        (.endStruct writer))

      (throw (UnsupportedOperationException.)))))

(defn- duv-child-writer [^IVectorWriter w, write-value!]
  (reify IVectorWriter
    (getVector [_] (.getVector w))
    (getField [_] (.getField w))
    (clear [_] (.clear w))

    (rowCopier [_ src-vec]
      (let [inner-copier (.rowCopier w src-vec)]
        (reify IRowCopier
          (copyRow [_ src-idx]
            (write-value!)
            (.copyRow inner-copier src-idx)))))

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

    ( structKeyWriter [_ k] (.structKeyWriter w k))
    (startStruct [_] (.startStruct w))
    (endStruct [_] (write-value!) (.endStruct w))

    (listElementWriter [_] (.listElementWriter w))
    (startList [_] (.startList w))
    (endList [_] (write-value!) (.endList w))

    (^IVectorWriter legWriter [_ ^Keyword leg] (.legWriter w leg))
    (^IVectorWriter legWriter [_ ^ArrowType arrow-type] (.legWriter w arrow-type))
    (writerForTypeId [_ type-id] (.writerForTypeId w type-id))))

(defn- duv->duv-copier ^xtdb.vector.IRowCopier [^IVectorWriter dest-col, ^DenseUnionVector src-vec]
  (let [src-field (.getField src-vec)
        src-type (.getType src-field)
        type-ids (.getTypeIds ^ArrowType$Union src-type)
        child-fields (.getChildren src-field)
        child-count (count child-fields)
        copier-mapping (object-array child-count)]

    (dotimes [n child-count]
      (let [src-type-id (or (when type-ids (aget type-ids n))
                            n)
            child-vec (.getVectorByType src-vec src-type-id)
            child-field (.getField child-vec)]
        (aset copier-mapping src-type-id
              (.rowCopier (.legWriter dest-col (keyword (.getName child-field)) (.getFieldType child-field))
                          child-vec))))

    (reify IRowCopier
      (copyRow [_ src-idx]
        (try
          (let [type-id (.getTypeId src-vec src-idx)]
            (assert (not (neg? type-id)))
            (-> ^IRowCopier (aget copier-mapping type-id)
                (.copyRow (.getOffset src-vec src-idx))))
          (catch Throwable t
            (throw t)))))))

(defn- vec->duv-copier ^xtdb.vector.IRowCopier [^IVectorWriter dest-col, ^ValueVector src-vec]
  (let [field (.getField src-vec)
        nn-type (.getType field)
        nullable? (.isNullable field)
        ^IRowCopier non-null-copier (when-not (= #xt.arrow/type :null nn-type)
                                      (-> (.legWriter dest-col nn-type)
                                          (.rowCopier src-vec)))
        !null-copier (delay
                       (-> (.legWriter dest-col #xt.arrow/type :null)
                           (.rowCopier src-vec)))]
      (reify IRowCopier
        (copyRow [_ src-idx]
          (if (and nullable? (.isNull src-vec src-idx))
            (.copyRow ^IRowCopier @!null-copier src-idx)
            (.copyRow non-null-copier src-idx))))))

(extend-protocol WriterFactory
  DenseUnionVector
  (->writer* [duv notify!]
    (let [col-name (.getName duv)
          wp (IVectorPosition/build (.getValueCount duv))
          writers-by-leg (HashMap.)
          !field (atom nil)]

      (letfn [(->field []
                (apply types/->field col-name #xt.arrow/type :union false
                       (map (fn [^ValueVector child-vec]
                              (let [^IVectorWriter w (or (.get writers-by-leg (keyword (.getName child-vec)))
                                                         (throw (NullPointerException. (pr-str {:legs (keys writers-by-leg)
                                                                                                :leg (keyword (.getName child-vec))}))))]
                                (.getField w)))
                            duv)))

              (->child-writer [^long type-id]
                (let [v (.getVectorByType duv type-id)
                      child-wtr (->writer* v (fn [_]
                                               (notify! (reset! !field (->field)))))
                      child-wp (.writerPosition child-wtr)

                      child-wtr (-> child-wtr
                                    (duv-child-writer (fn write-value! []
                                                        (let [pos (.getPositionAndIncrement wp)]
                                                          (.setTypeId duv pos type-id)
                                                          (.setOffset duv pos (.getPosition child-wp))))))]
                  child-wtr))

              (->new-child-writer [leg ^FieldType field-type]
                (let [field-name (name leg)
                      field (Field. field-name field-type [])
                      type-id (.registerNewTypeId duv field)
                      new-vec (.createNewSingleVector field-type field-name (.getAllocator duv) nil)]
                  (.addVector duv type-id new-vec)
                  (->child-writer type-id)))]

        ;; HACK this makes assumption about initial type-id layout which might not hold for arbitrary arrow data
        (doseq [[type-id ^Field field] (map-indexed vector (.getChildren (.getField duv)))]
          (.put writers-by-leg (keyword (.getName field)) (->child-writer type-id)))

        (reset! !field (->field))

        (reify IVectorWriter
          (getVector [_] duv)
          (getField [_] @!field)

          (writeNull [_ _]
            ;; DUVs can't technically contain null, but when they're stored within a nullable struct/list vector,
            ;; we don't have anything else to write here :/
            (.getPositionAndIncrement wp))

          (clear [_] (.clear duv) (.setPosition wp 0) (run! #(.clear ^IVectorWriter %) (vals writers-by-leg)))

          (rowCopier [this-writer src-vec]
            (let [inner-copier (if (instance? DenseUnionVector src-vec)
                                 (duv->duv-copier this-writer src-vec)
                                 (vec->duv-copier this-writer src-vec))]
              (reify IRowCopier
                (copyRow [_ src-idx] (.copyRow inner-copier src-idx)))))

          (writerPosition [_] wp)

          (^IVectorWriter legWriter [_this ^Keyword leg]
           (or (.get writers-by-leg leg)
               (throw (NullPointerException. (pr-str {:legs (set (keys writers-by-leg))
                                                      :leg leg})))))
          (legWriter [_ leg field-type]
            (let [new-field? (not (.containsKey writers-by-leg leg))
                  ^IVectorWriter w (.computeIfAbsent writers-by-leg leg
                                                     (reify Function
                                                       (apply [_ leg]
                                                         (->new-child-writer leg field-type))))]

              (when new-field?
                (notify! (reset! !field (->field))))

              w))


          (^IVectorWriter legWriter [this ^ArrowType leg-type]
           (.legWriter this (types/arrow-type->leg leg-type) (FieldType/notNullable leg-type))))))))

(extend-protocol WriterFactory
  ExtensionTypeVector
  (->writer* [arrow-vec notify!]
    (let [!field (atom nil)
          inner (->writer* (.getUnderlyingVector arrow-vec)
                           (fn [_]
                             (notify! (reset! !field (.getField arrow-vec)))))]

      (reset! !field (.getField arrow-vec))

      (reify IVectorWriter
        (getVector [_] arrow-vec)
        (getField [_] @!field)
        (clear [_] (.clear inner))
        (rowCopier [this-wtr src-vec]
          (cond
            (instance? NullVector src-vec) (null->vec-copier this-wtr)
            (instance? DenseUnionVector src-vec) (duv->vec-copier this-wtr src-vec)
            :else (do
                    (assert (instance? ExtensionTypeVector src-vec)
                            (pr-str (class src-vec)))
                    (.rowCopier inner (.getUnderlyingVector ^ExtensionTypeVector src-vec)))))

        (writerPosition [_] (.writerPosition inner))

        (writeNull [_ v] (.writeNull inner v))
        (writeBoolean [_ v] (.writeBoolean inner v))
        (writeByte [_ v] (.writeByte inner v))
        (writeShort [_ v] (.writeShort inner v))
        (writeInt [_ v] (.writeInt inner v))
        (writeLong [_ v] (.writeLong inner v))
        (writeFloat [_ v] (.writeFloat inner v))
        (writeDouble [_ v] (.writeDouble inner v))
        (writeBytes [_ v] (.writeBytes inner v))
        (writeObject [_ v] (.writeObject inner v))

        (structKeyWriter [_ k] (.structKeyWriter inner k))
        (startStruct [_] (.startStruct inner))
        (endStruct [_] (.endStruct inner))

        (listElementWriter [_] (.listElementWriter inner))
        (startList [_] (.startList inner))
        (endList [_] (.endList inner))

        (^IVectorWriter legWriter [this ^ArrowType arrow-type] (scalar-leg-writer this arrow-type))
        (writerForTypeId [_ type-id] (.writerForTypeId inner type-id))))))

(extend-protocol ArrowWriteable
  Keyword
  (value->arrow-type [_] #xt.arrow/type :keyword)
  (value->col-type [_] :keyword)
  (write-value! [kw ^IVectorWriter w]
    (write-value! (str (symbol kw)) w))

  UUID
  (value->arrow-type [_] #xt.arrow/type :uuid)
  (value->col-type [_] :uuid)
  (write-value! [^UUID uuid ^IVectorWriter w]
    (write-value! (util/uuid->bytes uuid) w))

  URI
  (value->arrow-type [_] #xt.arrow/type :uri)
  (value->col-type [_] :uri)
  (write-value! [^URI uri ^IVectorWriter w]
    (write-value! (str uri) w))

  ClojureForm
  (value->arrow-type [_] #xt.arrow/type :clj-form)
  (value->col-type [_] :clj-form)
  (write-value! [clj-form ^IVectorWriter w]
    (write-value! (pr-str (.form clj-form)) w)))

(defn write-vec! [^ValueVector v, vs]
  (.clear v)

  (let [writer (->writer v)]
    (doseq [v vs]
      (write-value! v (.legWriter writer (value->arrow-type v))))

    (.syncValueCount writer)

    v))

(defn rows->col-types [rows]
  (->> (for [col-name (into #{} (mapcat keys) rows)]
         [(symbol col-name) (->> rows
                                 (into #{} (map (fn [row]
                                                  (value->col-type (get row col-name)))))
                                 (apply types/merge-col-types))])
       (into {})))

(defn ->vec-writer ^xtdb.vector.IVectorWriter [^BufferAllocator allocator, ^String col-name, ^FieldType field-type]
   (->writer (.createNewSingleVector field-type col-name allocator nil)))

(defn ->rel-copier ^xtdb.vector.IRowCopier [^IRelationWriter rel-wtr, ^RelationReader in-rel]
  (let [wp (.writerPosition rel-wtr)
        copiers (vec (for [^IVectorReader in-vec in-rel]
                       (.rowCopier in-vec (.colWriter rel-wtr (.getName in-vec)))))]
    (reify IRowCopier
      (copyRow [_ src-idx]
        (.startRow rel-wtr)
        (let [pos (.getPosition wp)]
          (doseq [^IRowCopier copier copiers]
            (.copyRow copier src-idx))
          (.endRow rel-wtr)
          pos)))))

(defn ->rel-writer ^xtdb.vector.IRelationWriter [^BufferAllocator allocator]
  (let [writer-array (volatile! nil)
        writers (LinkedHashMap.)
        wp (IVectorPosition/build)]
    (reify IRelationWriter
      (writerPosition [_] wp)

      (startRow [_])

      (endRow [_]
        (when (nil? @writer-array)
          (vreset! writer-array (object-array (.values writers))))

        (let [pos (.getPositionAndIncrement wp)
              ^objects arr @writer-array]
          (dotimes [i (alength arr)]
            (populate-with-absents (aget arr i) (inc pos)))))

      ;; TODO get rid of or
      (^IVectorWriter colWriter [this ^String col-name]
       (or (.get writers col-name)
           (.colWriter this col-name (FieldType/notNullable #xt.arrow/type :union))))

      (colWriter [_  col-name field-type]
        (when-let [^IVectorWriter wrt (.get writers col-name)]
          (when-not (= (.getFieldType (.getField wrt)) field-type)
            (throw (IllegalStateException. "Field type mismatch"))))
        (.computeIfAbsent writers col-name
                          (reify Function
                            (apply [_ _col-name]
                              (doto (->vec-writer allocator col-name field-type)
                                (populate-with-absents (.getPosition wp)))))))

      (rowCopier [this in-rel] (->rel-copier this in-rel))

      (iterator [_] (.iterator (.entrySet writers)))

      AutoCloseable
      (close [this]
        (run! util/try-close (vals this))))))


(defn root->writer ^xtdb.vector.IRelationWriter [^VectorSchemaRoot root]
  (let [writer-array (volatile! nil)
        writers (LinkedHashMap.)
        wp (IVectorPosition/build)]
    (doseq [^ValueVector vec (.getFieldVectors root)]
      (.put writers (.getName vec) (->writer vec)))

    (reify IRelationWriter
      (writerPosition [_] wp)

      (startRow [_])
      (endRow [_]
        (when (nil? @writer-array)
          (vreset! writer-array (object-array (.values writers))))
        (let [pos (.getPositionAndIncrement wp)
              ^objects arr @writer-array]
          (dotimes [i (alength arr)]
            (populate-with-absents (aget arr i) (inc pos)))))

      (^IVectorWriter colWriter [_ ^String col-name]
       (or (.get writers col-name)
           ;; TODO could we add things here dynamically ?
           (throw (NullPointerException. (pr-str {:cols (keys writers), :col col-name})))))

      (colWriter [_ _col-name _field-type]
        (throw (UnsupportedOperationException. "Dynamic column creation unsupported for this RelationWriter!")))

      (rowCopier [this in-rel] (->rel-copier this in-rel))

      (iterator [_] (.iterator (.entrySet writers)))

      (syncRowCount [_]
        (.syncSchema root)
        (.setRowCount root (.getPosition wp))

        (doseq [^IVectorWriter w (vals writers)]
          (.syncValueCount w)))

      AutoCloseable
      (close [this]
        (run! util/try-close (vals this))))))

(defn struct-writer->rel-copier ^xtdb.vector.IRowCopier [^IVectorWriter vec-wtr, ^RelationReader in-rel]
  (let [wp (.writerPosition vec-wtr)
        copiers (for [^IVectorReader src-vec in-rel]
                  (.rowCopier src-vec (.structKeyWriter vec-wtr (.getName src-vec))))]
    (reify IRowCopier
      (copyRow [_ src-idx]
        (let [pos (.getPosition wp)]
          (.startStruct vec-wtr)
          (doseq [^IRowCopier copier copiers]
            (.copyRow copier src-idx))
          (.endStruct vec-wtr)
          pos)))))

(defmulti open-vec (fn [_allocator col-name-or-field _vs]
                     (if (instance? Field col-name-or-field)
                       :field
                       :col-name)))

(alter-meta! #'open-vec assoc :tag ValueVector)

(defmethod open-vec :col-name [^BufferAllocator allocator, ^String col-name, vs]
  (util/with-close-on-catch [res (-> (FieldType/notNullable #xt.arrow/type :union)
                                     (.createNewSingleVector (str col-name) allocator nil))]
    (doto res (write-vec! vs))))

(defmethod open-vec :field [allocator ^Field field vs]
  (util/with-close-on-catch [res (.createVector field allocator)]
    (doto res (write-vec! vs))))

(defn open-rel ^xtdb.vector.RelationReader [vecs]
  (vr/rel-reader (map vr/vec->reader vecs)))

(defn open-params ^xtdb.vector.RelationReader [allocator params-map]
  (open-rel (for [[k v] params-map]
              (open-vec allocator k [v]))))

(def empty-params (vr/rel-reader [] 1))

(defn vec-wtr->rdr ^xtdb.vector.IVectorReader [^xtdb.vector.IVectorWriter w]
  (vr/vec->reader (.getVector (doto w (.syncValueCount)))))

(defn rel-wtr->rdr ^xtdb.vector.RelationReader [^xtdb.vector.IRelationWriter w]
  (vr/rel-reader (map vec-wtr->rdr (vals w))
                 (.getPosition (.writerPosition w))))

(defn append-vec [^IVectorWriter vec-writer, ^IVectorReader in-col]
  (let [row-copier (.rowCopier in-col vec-writer)]
    (dotimes [src-idx (.valueCount in-col)]
      (.copyRow row-copier src-idx))))

(defn append-rel [^IRelationWriter dest-rel, ^RelationReader src-rel]
  (doseq [^IVectorReader src-col src-rel]
    (append-vec (.colWriter dest-rel (.getName src-col)) src-col))

  (let [wp (.writerPosition dest-rel)]
    (.setPosition wp (+ (.getPosition wp) (.rowCount src-rel)))))
