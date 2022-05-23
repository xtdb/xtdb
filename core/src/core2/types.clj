(ns core2.types
  (:require [clojure.string :as str]
            [core2.util :as util]
            [core2.types :as types])
  (:import clojure.lang.Keyword
           (core2.types LegType LegType$StructLegType)
           (core2.vector IDenseUnionWriter IVectorWriter)
           (core2.vector.extensions KeywordType KeywordVector UuidType UuidVector UriType)
           java.io.Writer
           java.net.URI
           (java.nio ByteBuffer CharBuffer)
           java.nio.charset.StandardCharsets
           (java.time Duration Instant OffsetDateTime ZonedDateTime ZoneId LocalDate LocalTime Period)
           (java.util Date List Map UUID)
           java.util.concurrent.ConcurrentHashMap
           java.util.function.Function
           (org.apache.arrow.vector BigIntVector BitVector DurationVector FixedSizeBinaryVector Float4Vector Float8Vector IntVector NullVector SmallIntVector TimeStampMicroTZVector TimeStampMilliTZVector TimeStampNanoTZVector TimeStampSecTZVector TinyIntVector ValueVector VarBinaryVector VarCharVector DateDayVector DateMilliVector TimeNanoVector TimeMilliVector TimeMicroVector TimeSecVector TimeStampSecVector TimeStampMilliVector TimeStampMicroVector TimeStampNanoVector IntervalMonthDayNanoVector IntervalDayVector IntervalYearVector PeriodDuration)
           (org.apache.arrow.vector.complex DenseUnionVector ListVector StructVector)
           (org.apache.arrow.vector.holders NullableIntervalDayHolder)
           (org.apache.arrow.vector.types TimeUnit Types Types$MinorType UnionMode DateUnit IntervalUnit)
           (org.apache.arrow.vector.types.pojo ArrowType ArrowType$Binary ArrowType$Bool ArrowType$Duration ArrowType$ExtensionType ArrowType$FloatingPoint ArrowType$Int ArrowType$Null ArrowType$Struct ArrowType$Time ArrowType$Timestamp ArrowType$Union ArrowType$Utf8 Field FieldType ArrowType$Date ArrowType$Time ArrowType$Interval)
           org.apache.arrow.vector.util.Text))

(set! *unchecked-math* :warn-on-boxed)

(def null-type (.getType Types$MinorType/NULL))
(def bool-type (.getType Types$MinorType/BIT))
(def smallint-type (.getType Types$MinorType/SMALLINT))
(def int-type (.getType Types$MinorType/INT))
(def bigint-type (.getType Types$MinorType/BIGINT))
(def float4-type (.getType Types$MinorType/FLOAT4))
(def float8-type (.getType Types$MinorType/FLOAT8))
(def varchar-type (.getType Types$MinorType/VARCHAR))
(def varbinary-type (.getType Types$MinorType/VARBINARY))
(def timestamp-micro-tz-type (ArrowType$Timestamp. TimeUnit/MICROSECOND "UTC"))
(def duration-micro-type (ArrowType$Duration. TimeUnit/MICROSECOND))
(def struct-type (.getType Types$MinorType/STRUCT))
(def dense-union-type (ArrowType$Union. UnionMode/Dense (int-array 0)))
(def list-type (.getType Types$MinorType/LIST))
(def keyword-type KeywordType/INSTANCE)
(def date-day-type (ArrowType$Date. DateUnit/DAY))
(def time-micros-type (ArrowType$Time. TimeUnit/MICROSECOND 64))
(def interval-day-time-type (ArrowType$Interval. IntervalUnit/DAY_TIME))
(def interval-year-month-type (ArrowType$Interval. IntervalUnit/YEAR_MONTH))
(def interval-month-day-nano-type (ArrowType$Interval. IntervalUnit/MONTH_DAY_NANO))

(defprotocol ArrowWriteable
  (^core2.types.LegType value->leg-type [v])
  (write-value! [v ^core2.vector.IVectorWriter writer]))

(extend-protocol ArrowWriteable
  nil
  (value->leg-type [_] LegType/NULL)
  (write-value! [v ^IVectorWriter writer])

  Boolean
  (value->leg-type [_] LegType/BOOL)
  (write-value! [v ^IVectorWriter writer]
    (.setSafe ^BitVector (.getVector writer) (.getPosition writer) (if v 1 0)))

  Byte
  (value->leg-type [_] LegType/TINYINT)
  (write-value! [v ^IVectorWriter writer]
    (.setSafe ^TinyIntVector (.getVector writer) (.getPosition writer) v))

  Short
  (value->leg-type [_] LegType/SMALLINT)
  (write-value! [v ^IVectorWriter writer]
    (.setSafe ^SmallIntVector (.getVector writer) (.getPosition writer) v))

  Integer
  (value->leg-type [_] LegType/INT)
  (write-value! [v ^IVectorWriter writer]
    (.setSafe ^IntVector (.getVector writer) (.getPosition writer) v))

  Long
  (value->leg-type [_] LegType/BIGINT)
  (write-value! [v ^IVectorWriter writer]
    (.setSafe ^BigIntVector (.getVector writer) (.getPosition writer) v))

  Float
  (value->leg-type [_] LegType/FLOAT4)
  (write-value! [v ^IVectorWriter writer]
    (.setSafe ^Float4Vector (.getVector writer) (.getPosition writer) v))

  Double
  (value->leg-type [_] LegType/FLOAT8)
  (write-value! [v ^IVectorWriter writer]
    (.setSafe ^Float8Vector (.getVector writer) (.getPosition writer) v)))

(extend-protocol ArrowWriteable
  Date
  (value->leg-type [_] LegType/TIMESTAMPMICROTZ)
  (write-value! [v ^IVectorWriter writer]
    (.setSafe ^TimeStampMicroTZVector (.getVector writer) (.getPosition writer)
              (Math/multiplyExact (.getTime v) 1000)))

  Instant
  (value->leg-type [_] LegType/TIMESTAMPMICROTZ)
  (write-value! [v ^IVectorWriter writer]
    (.setSafe ^TimeStampMicroTZVector (.getVector writer) (.getPosition writer)
              (util/instant->micros v)))

  ZonedDateTime
  (value->leg-type [v] (LegType. (ArrowType$Timestamp. TimeUnit/MICROSECOND (.getId (.getZone v)))))
  (write-value! [v ^IVectorWriter writer]
    (write-value! (.toInstant v) writer))

  OffsetDateTime
  (value->leg-type [v] (LegType. (ArrowType$Timestamp. TimeUnit/MICROSECOND (.getId (.getOffset v)))))
  (write-value! [v ^IVectorWriter writer]
    (write-value! (.toInstant v) writer))

  Duration
  (value->leg-type [_] LegType/DURATIONMICRO)
  (write-value! [v ^IVectorWriter writer]
    (.setSafe ^DurationVector (.getVector writer) (.getPosition writer)
              (quot (.toNanos v) 1000)))

  LocalDate
  (value->leg-type [_] LegType/DATEDAY)
  (write-value! [v ^IVectorWriter writer]
    (.setSafe ^DateDayVector (.getVector writer) (.getPosition writer) (.toEpochDay v)))

  LocalTime
  (value->leg-type [_] LegType/TIMENANO)
  (write-value! [v ^IVectorWriter writer]
    (.setSafe ^TimeNanoVector (.getVector writer) (.getPosition writer) (.toNanoOfDay v)))

  ;; allow the use of PeriodDuration for more precision
  PeriodDuration
  (value->leg-type [_] LegType/INTERVALMONTHDAYNANO)
  (write-value! [v ^IVectorWriter writer]
    (let [period (.getPeriod v)
          duration (.getDuration v)]
      (.setSafe ^IntervalMonthDayNanoVector (.getVector writer) (.getPosition writer) (.toTotalMonths period) (.getDays period) (.toNanos duration)))))

(extend-protocol ArrowWriteable
  (Class/forName "[B")
  (value->leg-type [_] LegType/BINARY)
  (write-value! [v ^IVectorWriter writer]
    (.setSafe ^VarBinaryVector (.getVector writer) (.getPosition writer) ^bytes v))

  ByteBuffer
  (value->leg-type [_] LegType/BINARY)
  (write-value! [buf ^IVectorWriter writer]
    (.setSafe ^VarBinaryVector (.getVector writer) (.getPosition writer)
              buf (.position buf) (.remaining buf)))

  CharSequence
  (value->leg-type [_] LegType/UTF8)
  (write-value! [v ^IVectorWriter writer]
    (let [buf (.encode (.newEncoder StandardCharsets/UTF_8) (CharBuffer/wrap v))]
      (.setSafe ^VarCharVector (.getVector writer) (.getPosition writer)
                buf (.position buf) (.remaining buf))))

  Text
  (value->leg-type [_] LegType/UTF8)
  (write-value! [v ^IVectorWriter writer]
    (.setSafe ^VarCharVector (.getVector writer) (.getPosition writer) v)))

(extend-protocol ArrowWriteable
  List
  (value->leg-type [_] LegType/LIST)
  (write-value! [v ^IVectorWriter writer]
    (let [writer (.asList writer)
          data-writer (.getDataWriter writer)
          data-duv-writer (.asDenseUnion data-writer)]
      (doseq [el v]
        (.startValue data-writer)
        (doto (.writerForType data-duv-writer (value->leg-type el))
          (.startValue)
          (->> (write-value! el))
          (.endValue))
        (.endValue data-writer))))

  Map
  (value->leg-type [v]
    (let [ks (keys v)]
      (if (every? keyword? ks)
        (LegType$StructLegType. (into #{} (map name) (keys v)))
        LegType/MAP)))

  (write-value! [m ^IVectorWriter writer]
    (let [dest-vec (.getVector writer)]
      (cond
        (instance? StructVector dest-vec)
        (let [writer (.asStruct writer)]
          (doseq [[k v] m
                  :let [v-writer (.writerForName writer (name k))]]
            (if (instance? IDenseUnionWriter v-writer)
              (doto (-> v-writer
                        (.asDenseUnion)
                        (.writerForType (value->leg-type v)))
                (.startValue)
                (->> (write-value! v))
                (.endValue))

              (write-value! v v-writer))))

        ;; TODO
        :else (throw (UnsupportedOperationException.))))))

(def ^:private ^core2.types.LegType keyword-leg-type (LegType. KeywordType/INSTANCE))
(def ^:private ^core2.types.LegType uuid-leg-type (LegType. UuidType/INSTANCE))
(def ^:private ^core2.types.LegType uri-leg-type (LegType. UriType/INSTANCE))

(extend-protocol ArrowWriteable
  Keyword
  (value->leg-type [_] keyword-leg-type)
  (write-value! [kw ^IVectorWriter writer]
    (write-value! (str (symbol kw)) (.getUnderlyingWriter (.asExtension writer))))

  UUID
  (value->leg-type [_] uuid-leg-type)
  (write-value! [^UUID uuid ^IVectorWriter writer]
    (let [underlying-writer (.getUnderlyingWriter (.asExtension writer))
          bb (doto (ByteBuffer/allocate 16)
               (.putLong (.getMostSignificantBits uuid))
               (.putLong (.getLeastSignificantBits uuid)))]
      (.setSafe ^FixedSizeBinaryVector (.getVector underlying-writer)
                (.getPosition underlying-writer)
                (.array bb))))

  URI
  (value->leg-type [_] uri-leg-type)
  (write-value! [^URI uri ^IVectorWriter writer]
    (write-value! (str uri) (.getUnderlyingWriter (.asExtension writer)))))

(defprotocol VectorType
  (^java.lang.Class arrow-type->vector-type [^ArrowType arrow-type]))

(extend-protocol VectorType
  ArrowType$Null (arrow-type->vector-type [_] NullVector)
  ArrowType$Bool (arrow-type->vector-type [_] BitVector)

  ArrowType$Int
  (arrow-type->vector-type [arrow-type]
    (let [^ArrowType$Int arrow-type arrow-type]
      (if (.getIsSigned arrow-type)
        (case (.getBitWidth arrow-type)
          8 TinyIntVector
          16 SmallIntVector
          32 IntVector
          64 BigIntVector)
        (throw (UnsupportedOperationException.)))))

  ArrowType$FloatingPoint
  (arrow-type->vector-type [arrow-type]
    (case (.name (.getPrecision ^ArrowType$FloatingPoint arrow-type))
      "SINGLE" Float4Vector
      "DOUBLE" Float8Vector))

  ArrowType$Binary (arrow-type->vector-type [_] VarBinaryVector)
  ArrowType$Utf8 (arrow-type->vector-type [_] VarCharVector)

  ArrowType$Timestamp
  (arrow-type->vector-type [arrow-type]
    (let [^ArrowType$Timestamp arrow-type arrow-type]
      (if (.getTimezone arrow-type)
        (case (.name (.getUnit arrow-type))
          "SECOND" TimeStampSecTZVector
          "MILLISECOND" TimeStampMilliTZVector
          "MICROSECOND" TimeStampMicroTZVector
          "NANOSECOND" TimeStampNanoTZVector)
        (case (.name (.getUnit arrow-type))
          "SECOND" TimeStampSecVector
          "MILLISECOND" TimeStampMilliVector
          "MICROSECOND" TimeStampMicroVector
          "NANOSECOND" TimeStampNanoVector))))

  ArrowType$Duration (arrow-type->vector-type [_] DurationVector)

  ArrowType$Date
  (arrow-type->vector-type [arrow-type]
    (let [^ArrowType$Date arrow-type arrow-type]
      (util/case-enum (.getUnit arrow-type)
        DateUnit/DAY DateDayVector
        DateUnit/MILLISECOND DateMilliVector
        (throw (UnsupportedOperationException.)))))

  ArrowType$Time
  (arrow-type->vector-type [arrow-type]
    (let [^ArrowType$Time arrow-type arrow-type]
      (util/case-enum (.getUnit arrow-type)
        TimeUnit/NANOSECOND TimeNanoVector
        TimeUnit/MICROSECOND TimeMicroVector
        TimeUnit/MILLISECOND TimeMilliVector
        TimeUnit/SECOND TimeSecVector)))

  ArrowType$Interval
  (arrow-type->vector-type [arrow-type]
    (let [^ArrowType$Interval arrow-type arrow-type]
      (util/case-enum (.getUnit arrow-type)
        IntervalUnit/DAY_TIME IntervalDayVector
        IntervalUnit/YEAR_MONTH IntervalYearVector
        IntervalUnit/MONTH_DAY_NANO IntervalMonthDayNanoVector))))

(extend-protocol VectorType
  KeywordType (arrow-type->vector-type [_] KeywordVector)
  UuidType (arrow-type->vector-type [_] UuidVector))

(defprotocol ArrowReadable
  (get-object [value-vector idx]))

(extend-protocol ArrowReadable
  ;; NOTE: Vectors not explicitly listed here have useful getObject methods and are handled by `ValueVector`.
  ValueVector (get-object [this idx] (.getObject this ^int idx))

  BitVector
  (get-object [this idx]
    ;; `BitVector/getObject` returns `new Boolean(...)` rather than `Boolean/valueOf`
    (when-not (.isNull this idx)
      (= (.get this ^int idx) 1)))

  VarBinaryVector
  (get-object [this idx]
    (when-not (.isNull this idx)
      (ByteBuffer/wrap (.getObject this ^int idx))))

  VarCharVector
  (get-object [this idx]
    (when-not (.isNull this idx)
      (String. (.get this ^int idx) StandardCharsets/UTF_8))))

(let [zones (ConcurrentHashMap.)]
  (defn- zone-id ^java.time.ZoneId [^ValueVector v]
    (.computeIfAbsent zones (.getTimezone ^ArrowType$Timestamp (.getType (.getField v)))
                      (reify Function
                        (apply [_ zone-str]
                          (ZoneId/of zone-str))))))

(extend-protocol ArrowReadable
  TimeStampSecTZVector
  (get-object [this idx]
    (-> (Instant/ofEpochSecond (.get this idx))
        (.atZone (zone-id this))))

  TimeStampMilliTZVector
  (get-object [this idx]
    (-> (Instant/ofEpochMilli (.get this idx))
        (.atZone (zone-id this))))

  TimeStampMicroTZVector
  (get-object [this idx]
    (-> ^Instant (util/micros->instant (.get this ^int idx))
        (.atZone (zone-id this))))

  TimeStampNanoTZVector
  (get-object [this idx]
    (-> (Instant/ofEpochSecond 0 (.get this idx))
        (.atZone (zone-id this)))))

(extend-protocol ArrowReadable
  DateDayVector
  (get-object [this idx]
    (LocalDate/ofEpochDay (.get this idx)))
  DateMilliVector
  (get-object [this idx]
    (LocalDate/ofEpochDay (quot (.get this idx) 86400000))))

(extend-protocol ArrowReadable
  TimeNanoVector
  (get-object [this idx]
    (LocalTime/ofNanoOfDay (.get this idx)))
  TimeMicroVector
  (get-object [this idx]
    (LocalTime/ofNanoOfDay (* (.get this idx) 1e3)))
  TimeMilliVector
  (get-object [this idx]
    (LocalTime/ofNanoOfDay (* (long (.get this idx)) 1e6)))
  TimeSecVector
  (get-object [this idx]
    (LocalTime/ofSecondOfDay (.get this idx))))

(extend-protocol ArrowReadable
  ;; we are going to override the get-object function
  ;; to unify the representation on read for non nanovectors
  IntervalDayVector
  (get-object [this idx]
    (let [holder (NullableIntervalDayHolder.)
          _ (.get this idx holder)
          period (Period/ofDays (.-days holder))
          duration (Duration/ofMillis (.-milliseconds holder))]
      (PeriodDuration. period duration)))
  IntervalYearVector
  (get-object [this idx]
    (PeriodDuration. (Period/ofMonths (.get this idx)) Duration/ZERO)))

(extend-protocol ArrowReadable
  ListVector
  (get-object [this idx]
    (let [data-vec (.getDataVector this)
          x (loop [element-idx (.getElementStartIndex this idx)
                   acc (transient [])]
              (if (= (.getElementEndIndex this idx) element-idx)
                acc
                (recur (inc element-idx)
                       (conj! acc (get-object data-vec element-idx)))))]
      (persistent! x)))

  StructVector
  (get-object [this idx]
    (-> (reduce (fn [acc k]
                  (let [duv (.getChild this k ValueVector)]
                    (cond-> acc
                      (not (.isNull duv idx))
                      (assoc! (keyword k) (get-object duv idx)))))
                (transient {})
                (.getChildFieldNames this))
        (persistent!)))

  DenseUnionVector
  (get-object [this idx]
    (get-object (.getVectorByType this (.getTypeId this idx))
                (.getOffset this idx))))

(def arrow-type-hierarchy
  (-> (make-hierarchy)
      (derive ArrowType$FloatingPoint ::Number)
      (derive ArrowType$Int ::Number)
      (derive ArrowType ::Object)
      (derive ::Number ::Object)))

(defmulti least-upper-bound2
  (fn [x-type y-type] [(class x-type) (class y-type)])
  :hierarchy #'arrow-type-hierarchy)

(defmethod least-upper-bound2 [ArrowType$Int ArrowType$Int]
  [^ArrowType$Int x-type, ^ArrowType$Int y-type]
  (assert (and (.getIsSigned x-type) (.getIsSigned y-type)))

  (ArrowType$Int. (max (.getBitWidth x-type) (.getBitWidth y-type)) true))

(defmethod least-upper-bound2 [ArrowType$Int ArrowType$FloatingPoint]
  [^ArrowType$Int _x-type, ^ArrowType$FloatingPoint y-type]
  y-type)

(defmethod least-upper-bound2 [ArrowType$FloatingPoint ArrowType$Int]
  [^ArrowType$FloatingPoint x-type, ^ArrowType$Int _y-type]
  x-type)

(defmethod least-upper-bound2 [ArrowType$FloatingPoint ArrowType$FloatingPoint]
  [^ArrowType$FloatingPoint x-type, ^ArrowType$FloatingPoint y-type]
  (let [x-precision (.getPrecision x-type)
        y-precision (.getPrecision y-type)]
    (ArrowType$FloatingPoint. (if (pos? (compare x-precision y-precision))
                                x-precision
                                y-precision))))

(defmethod least-upper-bound2 :default [_ _] ::Object)

(alter-meta! #'least-upper-bound2 assoc :private true)

(defn least-upper-bound [arrow-types]
  (reduce (fn [lub arrow-type]
            (if (= lub arrow-type)
              lub
              (least-upper-bound2 lub arrow-type)))
          arrow-types))

(defn ->field ^org.apache.arrow.vector.types.pojo.Field [^String field-name ^ArrowType arrow-type nullable & children]
  (Field. field-name (FieldType. nullable arrow-type nil nil) children))

(def ^org.apache.arrow.vector.types.pojo.Field row-id-field
  (->field "_row-id" bigint-type false))

(defn type->field-name ^String [^ArrowType arrow-type]
  (let [minor-type-name (.toLowerCase (.name (Types/getMinorTypeForArrowType arrow-type)))]
    (case minor-type-name
      "duration" (format "%s-%s" minor-type-name (.toLowerCase (.name (.getUnit ^ArrowType$Duration arrow-type))))
      "timestampmicrotz" (format "%s-%s" minor-type-name
                                 (-> (.toLowerCase (.getTimezone ^ArrowType$Timestamp arrow-type))
                                     (str/replace #"[/:]" "_")))
      "extensiontype" (.extensionName ^ArrowType$ExtensionType arrow-type)
      "date" (format "%s-%s" minor-type-name (.toLowerCase (.name (.getUnit ^ArrowType$Date arrow-type))))
      "time" (format "%s-%s" minor-type-name (.toLowerCase (.name (.getUnit ^ArrowType$Time arrow-type))))
      "interval" (format "%s-%s" minor-type-name (.toLowerCase (.name (.getUnit ^ArrowType$Interval arrow-type))))
      minor-type-name)))

(defn field-with-name ^org.apache.arrow.vector.types.pojo.Field [^Field field, ^String col-name]
  (Field. col-name (.getFieldType field) (.getChildren field)))

(defn merge-fields [& fields]
  (let [new-fields (->> (for [^Field field fields
                              child-field (if (= dense-union-type (.getType field))
                                            (.getChildren field)
                                            [field])]
                          child-field)
                        (group-by (fn [^Field field] (.getType field)))
                        (mapcat (fn [[arrow-type fields]]
                                  (let [field-name (.getName ^Field (first fields))]
                                    (condp = arrow-type
                                      list-type
                                      [(->field field-name list-type false
                                                (->> fields
                                                     (map #(first (.getChildren ^Field %)))
                                                     (apply merge-fields)))]

                                      struct-type
                                      (->> fields
                                           (group-by (fn [^Field struct-field]
                                                       (->> (.getChildren struct-field)
                                                            (into #{} (map #(.getName ^Field %))))))
                                           (map (comp (fn [fields]
                                                        (apply ->field field-name struct-type false
                                                               (->> (mapcat #(.getChildren ^Field %) fields)
                                                                    (group-by #(.getName ^Field %))
                                                                    (map #(apply merge-fields (val %))))))
                                                      val)))

                                      [(first fields)])))))]
    (case (count new-fields)
      0 (->field "$data" types/null-type true)
      1 (first new-fields)

      (apply ->field (.getName ^Field (first fields)) dense-union-type false
             (->> new-fields
                  (map-indexed (fn [idx ^Field field]
                                 (let [arrow-type (.getType field)]
                                   (field-with-name field
                                                    (if (= struct-type arrow-type)
                                                      (str "struct" idx)
                                                      (type->field-name arrow-type)))))))))))

(defn field->leg-type ^core2.types.LegType [^Field field]
  (let [arrow-type (.getType field)]
    (if (instance? ArrowType$Struct arrow-type)
      (LegType$StructLegType. (->> (.getChildren field) (into #{} (map #(.getName ^Field %)))))
      (LegType. arrow-type))))

(defmethod print-method LegType [^LegType leg-type, ^Writer w]
  (.write w (format "(LegType %s)" (.arrowType leg-type))))

(defmethod print-method LegType$StructLegType [^LegType$StructLegType leg-type, ^Writer w]
  (.write w (format "(StructLegType %s)" (pr-str (.keys leg-type)))))
