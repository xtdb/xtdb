(ns core2.types
  (:require [clojure.string :as str]
            [core2.util :as util])
  (:import core2.vector.IVectorWriter
           [java.nio ByteBuffer CharBuffer]
           java.nio.charset.StandardCharsets
           [java.time Duration Instant OffsetDateTime ZonedDateTime ZoneId]
           [java.util Date List Map]
           java.util.concurrent.ConcurrentHashMap
           java.util.function.Function
           [org.apache.arrow.vector BigIntVector BitVector DurationVector Float4Vector Float8Vector IntVector NullVector SmallIntVector TimeStampMicroTZVector TimeStampMilliTZVector TimeStampNanoTZVector TimeStampSecTZVector TinyIntVector ValueVector VarBinaryVector VarCharVector]
           [org.apache.arrow.vector.complex DenseUnionVector ListVector StructVector]
           [org.apache.arrow.vector.types TimeUnit Types Types$MinorType]
           [org.apache.arrow.vector.types.pojo ArrowType ArrowType$Binary ArrowType$Bool ArrowType$Duration ArrowType$FloatingPoint ArrowType$Int ArrowType$Map ArrowType$Null ArrowType$Timestamp ArrowType$Utf8 Field FieldType]
           org.apache.arrow.vector.util.Text))

(set! *unchecked-math* :warn-on-boxed)

(def bigint-type (.getType Types$MinorType/BIGINT))
(def float8-type (.getType Types$MinorType/FLOAT8))
(def varchar-type (.getType Types$MinorType/VARCHAR))
(def varbinary-type (.getType Types$MinorType/VARBINARY))
(def timestamp-micro-tz-type (ArrowType$Timestamp. TimeUnit/MICROSECOND "UTC"))
(def duration-micro-type (ArrowType$Duration. TimeUnit/MICROSECOND))
(def struct-type (.getType Types$MinorType/STRUCT))
(def dense-union-type (.getType Types$MinorType/DENSEUNION))
(def list-type (.getType Types$MinorType/LIST))
(def map-type (ArrowType$Map. false))

(defprotocol ArrowWriteable
  (value->arrow-type [v])
  (write-value! [v ^core2.vector.IVectorWriter writer]))

(extend-protocol ArrowWriteable
  nil
  (value->arrow-type [_] ArrowType$Null/INSTANCE)
  (write-value! [v ^IVectorWriter writer])

  Boolean
  (value->arrow-type [_] ArrowType$Bool/INSTANCE)
  (write-value! [v ^IVectorWriter writer]
    (.setSafe ^BitVector (.getVector writer) (.getPosition writer) (if v 1 0)))

  Byte
  (value->arrow-type [_] (.getType Types$MinorType/TINYINT))
  (write-value! [v ^IVectorWriter writer]
    (.setSafe ^TinyIntVector (.getVector writer) (.getPosition writer) v))

  Short
  (value->arrow-type [_] (.getType Types$MinorType/SMALLINT))
  (write-value! [v ^IVectorWriter writer]
    (.setSafe ^SmallIntVector (.getVector writer) (.getPosition writer) v))

  Integer
  (value->arrow-type [_] (.getType Types$MinorType/INT))
  (write-value! [v ^IVectorWriter writer]
    (.setSafe ^IntVector (.getVector writer) (.getPosition writer) v))

  Long
  (value->arrow-type [_] bigint-type)
  (write-value! [v ^IVectorWriter writer]
    (.setSafe ^BigIntVector (.getVector writer) (.getPosition writer) v))

  Float
  (value->arrow-type [_] (.getType Types$MinorType/FLOAT4))
  (write-value! [v ^IVectorWriter writer]
    (.setSafe ^Float4Vector (.getVector writer) (.getPosition writer) v))

  Double
  (value->arrow-type [_] float8-type)
  (write-value! [v ^IVectorWriter writer]
    (.setSafe ^Float8Vector (.getVector writer) (.getPosition writer) v)))

(extend-protocol ArrowWriteable
  Date
  (value->arrow-type [_] timestamp-micro-tz-type)
  (write-value! [v ^IVectorWriter writer]
    (.setSafe ^TimeStampMicroTZVector (.getVector writer) (.getPosition writer)
              (Math/multiplyExact (.getTime v) 1000)))

  Instant
  (value->arrow-type [_] timestamp-micro-tz-type)
  (write-value! [v ^IVectorWriter writer]
    (.setSafe ^TimeStampMicroTZVector (.getVector writer) (.getPosition writer)
              (util/instant->micros v)))

  ZonedDateTime
  (value->arrow-type [v] (ArrowType$Timestamp. TimeUnit/MICROSECOND (.getId (.getZone v))))
  (write-value! [v ^IVectorWriter writer]
    (write-value! (.toInstant v) writer))

  OffsetDateTime
  (value->arrow-type [v] (ArrowType$Timestamp. TimeUnit/MICROSECOND (.getId (.getOffset v))))
  (write-value! [v ^IVectorWriter writer]
    (write-value! (.toInstant v) writer))

  Duration ; HACK assumes micros for now
  (value->arrow-type [_] duration-micro-type)
  (write-value! [v ^IVectorWriter writer]
    (.setSafe ^DurationVector (.getVector writer) (.getPosition writer)
              (quot (.toNanos v) 1000))))

(extend-protocol ArrowWriteable
  (Class/forName "[B")
  (value->arrow-type [_] ArrowType$Binary/INSTANCE)
  (write-value! [v ^IVectorWriter writer]
    (.setSafe ^VarBinaryVector (.getVector writer) (.getPosition writer) ^bytes v))

  ByteBuffer
  (value->arrow-type [_] ArrowType$Binary/INSTANCE)
  (write-value! [buf ^IVectorWriter writer]
    (.setSafe ^VarBinaryVector (.getVector writer) (.getPosition writer)
              buf (.position buf) (.remaining buf)))

  CharSequence
  (value->arrow-type [_] ArrowType$Utf8/INSTANCE)
  (write-value! [v ^IVectorWriter writer]
    (let [buf (.encode (.newEncoder StandardCharsets/UTF_8) (CharBuffer/wrap v))]
      (.setSafe ^VarCharVector (.getVector writer) (.getPosition writer)
                buf (.position buf) (.remaining buf))))

  Text
  (value->arrow-type [_] ArrowType$Utf8/INSTANCE)
  (write-value! [v ^IVectorWriter writer]
    (.setSafe ^VarCharVector (.getVector writer) (.getPosition writer) v)))

(extend-protocol ArrowWriteable
  List
  (value->arrow-type [_] list-type)
  (write-value! [v ^IVectorWriter writer]
    (let [writer (.asList writer)
          data-writer (.getDataWriter writer)
          data-duv-writer (.asDenseUnion data-writer)]
      (doseq [el v]
        (.startValue data-writer)
        (write-value! el (doto (.writerForType data-duv-writer (value->arrow-type el))
                           (.startValue)))
        (.endValue data-writer))))

  Map
  (value->arrow-type [v]
    (if (every? keyword? (keys v))
      struct-type
      map-type))

  (write-value! [m ^IVectorWriter writer]
    (let [dest-vec (.getVector writer)]
      (cond
        (instance? StructVector dest-vec)
        (let [writer (.asStruct writer)]
          (doseq [[k v] m]
            (write-value! v (doto (-> (.writerForName writer (name k))
                                      (.asDenseUnion)
                                      (.writerForType (value->arrow-type v)))
                              (.startValue)))))

        ;; TODO
        :else (throw (UnsupportedOperationException.))))))

(def arrow-type->vector-type
  {ArrowType$Null/INSTANCE NullVector
   ArrowType$Bool/INSTANCE BitVector
   (.getType Types$MinorType/TINYINT) TinyIntVector
   (.getType Types$MinorType/SMALLINT) SmallIntVector
   (.getType Types$MinorType/INT) IntVector
   bigint-type BigIntVector
   (.getType Types$MinorType/FLOAT4) Float4Vector
   float8-type Float8Vector

   ArrowType$Binary/INSTANCE VarBinaryVector
   ArrowType$Utf8/INSTANCE VarCharVector

   timestamp-micro-tz-type TimeStampMicroTZVector
   duration-micro-type DurationVector})

(defprotocol ArrowReadable
  (get-object [value-vector idx]))

(extend-protocol ArrowReadable
  ;; NOTE: Vectors not explicitly listed here have useful getObject methods and are handled by `ValueVector`.
  ValueVector (get-object [this idx] (.getObject this ^int idx))

  VarBinaryVector (get-object [this idx] (ByteBuffer/wrap (.getObject this ^int idx)))

  VarCharVector
  (get-object [this idx] (String. (.get this ^int idx) StandardCharsets/UTF_8)))

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
      (derive ArrowType ::Object)))

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

(defmethod least-upper-bound2 :default [x-type y-type]
  (throw (UnsupportedOperationException. (format "Can't LUB: %s ⊔ %s" x-type y-type))))

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

(defn type->field-name [^ArrowType arrow-type]
  (let [minor-type-name (.toLowerCase (.name (Types/getMinorTypeForArrowType arrow-type)))]
    (case minor-type-name
      "duration" (format "%s-%s" minor-type-name (.toLowerCase (.name (.getUnit ^ArrowType$Duration arrow-type))))
      "timestampmicrotz" (format "%s-%s" minor-type-name
                                 (-> (.toLowerCase (.getTimezone ^ArrowType$Timestamp arrow-type))
                                     (str/replace #"[/:]" "_")))
      minor-type-name)))

(defn arrow-type->field [^ArrowType arrow-type]
  (let [field-name (type->field-name arrow-type)
        minor-type-name (.name (Types/getMinorTypeForArrowType arrow-type))]
    (case minor-type-name
      "LIST" (->field field-name arrow-type false (->field "$data$" dense-union-type false))
      (->field field-name arrow-type false))))
