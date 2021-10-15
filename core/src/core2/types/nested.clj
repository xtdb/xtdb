(ns core2.types.nested
  (:import [java.nio.charset StandardCharsets]
           [java.nio ByteBuffer CharBuffer]
           [java.io ByteArrayInputStream ByteArrayOutputStream ObjectInputStream ObjectOutputStream Serializable]
           [java.util Date List Map Set UUID]
           [java.time Duration Instant LocalDate LocalDateTime LocalTime OffsetDateTime Period ZoneId ZonedDateTime ZoneOffset]
           [java.time.temporal ChronoField]
           [java.util.concurrent TimeUnit]
           [org.apache.arrow.vector.types Types$MinorType]
           [org.apache.arrow.vector.types.pojo ArrowType ArrowType$Decimal ArrowType$Duration ArrowType$ExtensionType ArrowType$FixedSizeBinary ArrowType$Map ArrowType$Timestamp ArrowType$Union Field FieldType]
           [org.apache.arrow.vector.complex DenseUnionVector FixedSizeListVector LargeListVector ListVector MapVector StructVector]
           [org.apache.arrow.vector DateDayVector DateMilliVector DecimalVector DurationVector FixedSizeBinaryVector IntervalYearVector
            LargeVarBinaryVector LargeVarCharVector
            TimeMicroVector TimeMilliVector TimeNanoVector TimeSecVector
            TimeStampMicroVector TimeStampMilliVector TimeStampNanoVector TimeStampSecVector
            TimeStampMicroTZVector TimeStampMilliTZVector TimeStampNanoTZVector TimeStampSecTZVector
            VarBinaryVector VarCharVector ValueVector
            UInt1Vector UInt2Vector UInt4Vector UInt8Vector]
           [core2 DenseUnionUtil]
           [clojure.lang IPersistentList IPersistentSet Keyword Symbol]))

;; Type mapping aims to stay close to the Arrow JDBC adapter:
;; https://github.com/apache/arrow/blob/master/java/adapter/jdbc/src/main/java/org/apache/arrow/adapter/jdbc/JdbcToArrow.java

;; For example, unlike Arrow Java itself, TimeMicroVector is mapped to
;; LocalTime and DateMicroVector to LocalDate like they are above via
;; the java.sql definitions.

;; NOTE, potential future improvements, mainly efficiency/performance:
;; - registering type ids requires scan of the union's children.
;; - generates needless DenseUnions, could attempt to promote during
;;   append or remove them before read.
;; - get-value is a protocol, which implies the index argument and
;;   return value is always boxed, may need some access object
;;   instead?
;; - append-value is also a protocol, which means that while dispatch
;;   is fast, the receiver is boxed, something that could be avoided
;;   if types are known during expression compilation.

;; C2 Integration:

;; Will require changes to tx-produce, ingest, metadata, late
;; materialisation (relation) and expression system. The latter two
;; (query side) should preferably also support -any- valid Arrow, but
;; we may only ingest and have our expressions generate the types
;; supported by append-value. Apart from the initial support, there
;; will be further things that are not supported today, new predicates
;; and functions, ability to navigate the nested data, and some form
;; of unnest operator in the logical plan that can flatten arrays.

;; As an aside, tx-produce and ingest should in theory also preferably
;; be able to take arbitrary Arrow at the lowest level (by passing the
;; inference implemented here) eventually, which would affect
;; metadata. This is a slightly future problem though, we just want to
;; keep an eye on it.

(set! *unchecked-math* :warn-on-boxed)

(defn- extension-type [^ValueVector v]
  (get (.getMetadata (.getField v)) ArrowType$ExtensionType/EXTENSION_METADATA_KEY_NAME))

(defn- zone-id ^java.time.ZoneId [^ValueVector v]
  (ZoneId/of (.getTimezone ^ArrowType$Timestamp (.getType (.getField v)))))

(defprotocol ArrowAppendable
  (append-value [_ v]))

(defprotocol ArrowReadable
  (get-value [_ idx]))

;; NOTE: Vectors not explicitly listed here have useful getObject
;; methods and are handled by ValueVector.

(extend-protocol ArrowReadable
  nil
  (get-value [_ _])

  ValueVector
  (get-value [v idx]
    (.getObject v idx))

  UInt1Vector
  (get-value [v idx]
    (.getObjectNoOverflow v idx))

  UInt2Vector
  (get-value [v idx]
    (let [x (.getObject v ^long idx)]
      (case (extension-type v)
        "char"
        x

        (int x))))

  UInt4Vector
  (get-value [v idx]
    (.getObjectNoOverflow v idx))

  UInt8Vector
  (get-value [v idx]
    (bigdec (.getObjectNoOverflow v idx)))

  DenseUnionVector
  (get-value [v idx]
    (get-value (.getVectorByType v (.getTypeId v idx)) (.getOffset v idx)))

  FixedSizeBinaryVector
  (get-value [v idx]
    (let [x (ByteBuffer/wrap (.getObject v ^long idx))]
      (case (extension-type v)
        "uuid"
        (UUID. (.getLong x) (.getLong x))

        x)))

  VarBinaryVector
  (get-value [v idx]
    (let [x (.getObject v ^long idx)]
      (case (extension-type v)
        "java.io.Serializable"
        (with-open [in (ObjectInputStream. (ByteArrayInputStream. x))]
          (.readObject in))

        (ByteBuffer/wrap x))))

  LargeVarBinaryVector
  (get-value [v idx]
    (ByteBuffer/wrap (.getObject v ^long idx)))

  VarCharVector
  (get-value [v idx]
    (let [x (str (.getObject v ^long idx))]
      (case (extension-type v)
        "clojure.lang.Keyword"
        (keyword x)

        "clojure.lang.Symbol"
        (symbol x)

        x)))

  LargeVarCharVector
  (get-value [v idx]
    (str (.getObject v ^long idx)))

  DateDayVector
  (get-value [v idx]
    (LocalDate/ofEpochDay (.get v idx)))

  DateMilliVector
  (get-value [v idx]
    (LocalDate/ofEpochDay (.toDays TimeUnit/MILLISECONDS (.get v idx))))

  TimeMicroVector
  (get-value [v idx]
    (LocalTime/ofNanoOfDay (.toNanos TimeUnit/MICROSECONDS (.get v idx))))

  TimeMilliVector
  (get-value [v idx]
    (LocalTime/ofNanoOfDay (.toNanos TimeUnit/MILLISECONDS (.get v idx))))

  TimeNanoVector
  (get-value [v idx]
    (LocalTime/ofNanoOfDay (.get v idx)))

  TimeSecVector
  (get-value [v idx]
    (LocalTime/ofSecondOfDay (.get v idx)))

  TimeStampMicroTZVector
  (get-value [v idx]
    (.atZone (Instant/ofEpochSecond 0 (.toNanos TimeUnit/MICROSECONDS (.get v idx))) (zone-id v)))

  TimeStampMilliTZVector
  (get-value [v idx]
    (.atZone (Instant/ofEpochSecond 0 (.toNanos TimeUnit/MILLISECONDS (.get v idx))) (zone-id v)))

  TimeStampNanoTZVector
  (get-value [v idx]
    (.atZone (Instant/ofEpochSecond 0 (.get v idx)) (zone-id v)))

  TimeStampSecTZVector
  (get-value [v idx]
    (.atZone (Instant/ofEpochSecond (.get v idx) 0) (zone-id v)))

  IntervalYearVector
  (get-value [v idx]
    (.normalized (.getObject ^IntervalYearVector v ^long idx)))

  ListVector
  (get-value [v idx]
    (let [x (loop [element-idx (.getElementStartIndex v idx)
                   acc []]
              (if (= (.getElementEndIndex v idx) element-idx)
                acc
                (recur (inc element-idx)
                       (conj acc (get-value (.getDataVector v) element-idx)))))]
      (case (extension-type v)
        "clojure.lang.IPersistentSet"
        (set x)

        "clojure.lang.IPersistentList"
        (apply list x)

        x)))

  FixedSizeListVector
  (get-value [v idx]
    (loop [element-idx (.getElementStartIndex v idx)
           acc []]
      (if (= (.getElementEndIndex v idx) element-idx)
        acc
        (recur (inc element-idx)
               (conj acc (get-value (.getDataVector v) element-idx))))))

  LargeListVector
  (get-value [v idx]
    (loop [element-idx (.getElementStartIndex v idx)
           acc []]
      (if (= (.getElementEndIndex v idx) element-idx)
        acc
        (recur (inc element-idx)
               (conj acc (get-value (.getDataVector v) element-idx))))))

  StructVector
  (get-value [v idx]
    (reduce
     (fn [acc k]
       (assoc acc (keyword k) (get-value (.getChild v k ValueVector) idx)))
     {}
     (.getChildFieldNames v)))

  MapVector
  (get-value [v idx]
    (let [^StructVector element-vec (.getDataVector v)
          key-vec (.getChild element-vec MapVector/KEY_NAME)
          value-vec (.getChild element-vec MapVector/VALUE_NAME)]
      (loop [element-idx (.getElementStartIndex v idx)
             acc {}]
        (if (= (.getElementEndIndex v idx) element-idx)
          acc
          (recur (inc element-idx)
                 (assoc acc (get-value key-vec element-idx) (get-value value-vec element-idx))))))))

(def ^:private dense-union-field-type (FieldType. false (.getType Types$MinorType/DENSEUNION) nil))

(defn- kw-name ^String [x]
  (if (keyword? x)
    (subs (str x) 1)
    (str x)))

;; TODO: this scans the children all the time.
(defn- get-or-create-type-id
  (^long [^DenseUnionVector v ^ArrowType arrow-type]
   (get-or-create-type-id v arrow-type (fn [^Field f]
                                         (empty? (.getMetadata f)))))
  (^long [^DenseUnionVector v ^ArrowType arrow-type field-pred]
   (let [children (.getChildren (.getField v))]
     (loop [idx 0]
       (cond
         (= idx (.size children))
         (.registerNewTypeId v (Field/nullable "" arrow-type))

         (and (= arrow-type (.getType ^Field (.get children idx)))
              (field-pred ^Field (.get children idx)))
         (if-let [type-ids (.getTypeIds ^ArrowType$Union (.getType (.getField v)))]
           (aget type-ids idx)
           idx)

         :else
         (recur (inc idx)))))))

(defn- get-or-add-vector
  ([^DenseUnionVector v ^ArrowType arrow-type ^String prefix ^Long type-id]
   (get-or-add-vector v arrow-type prefix type-id nil))
  ([^DenseUnionVector v ^ArrowType arrow-type ^String prefix ^Long type-id extension-type]
   (or (.getVectorByType v type-id)
       (.addVector v type-id (.createNewSingleVector
                              (FieldType. false
                                          arrow-type
                                          nil
                                          (when extension-type
                                            {ArrowType$ExtensionType/EXTENSION_METADATA_KEY_NAME extension-type}))
                              (str prefix type-id)
                              (.getAllocator v)
                              nil)))))

(defn- ->extension-type-pred [extension-type]
  (fn [^Field f]
    (= extension-type (get (.getMetadata f) ArrowType$ExtensionType/EXTENSION_METADATA_KEY_NAME))))

(defn- append-struct [x ^DenseUnionVector v]
  (let [key-set (set (map kw-name (keys x)))
        type-id (get-or-create-type-id v
                                       (.getType Types$MinorType/STRUCT)
                                       (fn [^Field f]
                                         (= key-set (set (for [^Field f (.getChildren f)]
                                                           (.getName f))))))
        inner-vec (.getStruct v type-id)]
    (doseq [k key-set
            :let [data-vec (.addOrGet inner-vec k dense-union-field-type DenseUnionVector)]]
      (append-value (get x (keyword k)) data-vec))

    (let [offset (DenseUnionUtil/writeTypeId v (.getValueCount v) type-id)]
      (.setIndexDefined inner-vec offset))
    v))

;; JSON

(extend-protocol ArrowAppendable
  nil
  (append-value [_ ^DenseUnionVector v]
    (let [arrow-type (.getType Types$MinorType/NULL)
          type-id (get-or-create-type-id v arrow-type)
          ^NullVector inner-vec (get-or-add-vector v arrow-type "null" type-id)
          offset (DenseUnionUtil/writeTypeId v (.getValueCount v) type-id)]
      v))

  Boolean
  (append-value [x ^DenseUnionVector v]
    (let [type-id (get-or-create-type-id v (.getType Types$MinorType/BIT))
          inner-vec (.getBitVector v type-id)
          offset (DenseUnionUtil/writeTypeId v (.getValueCount v) type-id)]
      (.setSafe inner-vec offset (if x 1 0))
      v))

  Byte
  (append-value [x ^DenseUnionVector v]
    (let [type-id (get-or-create-type-id v (.getType Types$MinorType/TINYINT))
          inner-vec (.getTinyIntVector v type-id)
          offset (DenseUnionUtil/writeTypeId v (.getValueCount v) type-id)]
      (.setSafe inner-vec offset x)
      v))

  Short
  (append-value [x ^DenseUnionVector v]
    (let [type-id (get-or-create-type-id v (.getType Types$MinorType/SMALLINT))
          inner-vec (.getSmallIntVector v type-id)
          offset (DenseUnionUtil/writeTypeId v (.getValueCount v) type-id)]
      (.setSafe inner-vec offset x)
      v))

  Integer
  (append-value [x ^DenseUnionVector v]
    (let [type-id (get-or-create-type-id v (.getType Types$MinorType/INT))
          inner-vec (.getIntVector v type-id)
          offset (DenseUnionUtil/writeTypeId v (.getValueCount v) type-id)]
      (.setSafe inner-vec offset x)
      v))

  Long
  (append-value [x ^DenseUnionVector v]
    (let [type-id (get-or-create-type-id v (.getType Types$MinorType/BIGINT))
          inner-vec (.getBigIntVector v type-id)
          offset (DenseUnionUtil/writeTypeId v (.getValueCount v) type-id)]
      (.setSafe inner-vec offset x)
      v))

  Float
  (append-value [x ^DenseUnionVector v]
    (let [type-id (get-or-create-type-id v (.getType Types$MinorType/FLOAT4))
          inner-vec (.getFloat4Vector v type-id)
          offset (DenseUnionUtil/writeTypeId v (.getValueCount v) type-id)]
      (.setSafe inner-vec offset x)
      v))

  Double
  (append-value [x ^DenseUnionVector v]
    (let [type-id (get-or-create-type-id v (.getType Types$MinorType/FLOAT8))
          inner-vec (.getFloat8Vector v type-id)
          offset (DenseUnionUtil/writeTypeId v (.getValueCount v) type-id)]
      (.setSafe inner-vec offset x)
      v))

  CharSequence
  (append-value [x ^DenseUnionVector v]
    (let [type-id (get-or-create-type-id v (.getType Types$MinorType/VARCHAR))
          inner-vec (.getVarCharVector v type-id)
          offset (DenseUnionUtil/writeTypeId v (.getValueCount v) type-id)
          x (.encode (.newEncoder StandardCharsets/UTF_8) (CharBuffer/wrap x))]
      (.setSafe inner-vec offset x (.position x) (.remaining x))
      v))

  List
  (append-value [x ^DenseUnionVector v]
    (let [type-id (get-or-create-type-id v (.getType Types$MinorType/LIST))
          inner-vec (.getList v type-id)
          offset (DenseUnionUtil/writeTypeId v (.getValueCount v) type-id)
          data-vec (.getVector (.addOrGetVector inner-vec dense-union-field-type))]
      (.startNewValue inner-vec offset)
      (doseq [v x]
        (append-value v data-vec))
      (.endValue inner-vec offset (count x))
      v))

  Map
  (append-value [x ^DenseUnionVector v]
    (append-struct x v)))

;; SQL / Arrow

(extend-protocol ArrowAppendable
  (class (byte-array 0))
  (append-value [x ^DenseUnionVector v]
    (append-value (ByteBuffer/wrap x) v))

  ByteBuffer
  (append-value [x ^DenseUnionVector v]
    (let [type-id (get-or-create-type-id v (.getType Types$MinorType/VARBINARY))
          inner-vec (.getVarBinaryVector v type-id)
          offset (DenseUnionUtil/writeTypeId v (.getValueCount v) type-id)]
      (.setSafe inner-vec offset (.duplicate x) (.position x) (.remaining x))
      v))

  BigInteger
  (append-value [x ^DenseUnionVector v]
    (append-value (bigdec x) v))

  BigDecimal
  (append-value [x ^DenseUnionVector v]
    (let [arrow-type (ArrowType$Decimal. (.precision x) (.scale x) 128)
          type-id (get-or-create-type-id v arrow-type)
          ^DecimalVector inner-vec (get-or-add-vector v arrow-type "decimal" type-id)
          offset (DenseUnionUtil/writeTypeId v (.getValueCount v) type-id)]
      (.setSafe inner-vec offset x)
      v))

  LocalDate
  (append-value [x ^DenseUnionVector v]
    (let [type-id (get-or-create-type-id v (.getType Types$MinorType/DATEMILLI))
          inner-vec (.getDateMilliVector v type-id)
          offset (DenseUnionUtil/writeTypeId v (.getValueCount v) type-id)]
      (.setSafe inner-vec offset (* (.getLong x ChronoField/EPOCH_DAY) 86400000))
      v))

  LocalDateTime
  (append-value [x ^DenseUnionVector v]
    (let [type-id (get-or-create-type-id v (.getType Types$MinorType/TIMESTAMPMICRO))
          inner-vec (.getTimeStampMicroVector v type-id)
          offset (DenseUnionUtil/writeTypeId v (.getValueCount v) type-id)
          x (.toInstant x ZoneOffset/UTC)]
      (.setSafe inner-vec offset (+ (.toMicros TimeUnit/SECONDS (.getEpochSecond x))
                                    (.toMicros TimeUnit/NANOSECONDS (.getNano x))))
      v))

  LocalTime
  (append-value [x ^DenseUnionVector v]
    (let [type-id (get-or-create-type-id v (.getType Types$MinorType/TIMEMICRO))
          inner-vec (.getTimeMicroVector v type-id)
          offset (DenseUnionUtil/writeTypeId v (.getValueCount v) type-id)]
      (.setSafe inner-vec offset (quot (.getLong x ChronoField/NANO_OF_DAY) 1000))
      v))

  Date
  (append-value [x ^DenseUnionVector v]
    (append-value (.toInstant x) v))

  Instant
  (append-value [x ^DenseUnionVector v]
    (append-value (ZonedDateTime/ofInstant x (ZoneId/of "UTC")) v))

  ZonedDateTime
  (append-value [x ^DenseUnionVector v]
    (let [arrow-type (ArrowType$Timestamp. org.apache.arrow.vector.types.TimeUnit/MICROSECOND (str (.getZone x)))
          type-id (get-or-create-type-id v arrow-type)
          ^TimeStampMicroTZVector inner-vec (get-or-add-vector v arrow-type "timestamp" type-id)
          offset (DenseUnionUtil/writeTypeId v (.getValueCount v) type-id)
          x (.toInstant x)]
      (.setSafe inner-vec offset (+ (.toMicros TimeUnit/SECONDS (.getEpochSecond x))
                                    (.toMicros TimeUnit/NANOSECONDS (.getNano x))))
      v))

  OffsetDateTime
  (append-value [x ^DenseUnionVector v]
    (append-value (.toZonedDateTime x) v))

  Duration
  (append-value [x ^DenseUnionVector v]
    (let [arrow-type (ArrowType$Duration. org.apache.arrow.vector.types.TimeUnit/MICROSECOND)
          type-id (get-or-create-type-id v arrow-type)
          ^DurationVector inner-vec (get-or-add-vector v arrow-type "duration" type-id)
          offset (DenseUnionUtil/writeTypeId v (.getValueCount v) type-id)]
      (.setSafe inner-vec offset (quot (.toNanos x) 1000))
      v))

  Period
  (append-value [x ^DenseUnionVector v]
    (let [type-id (get-or-create-type-id v (.getType Types$MinorType/INTERVALYEAR))
          inner-vec (.getIntervalYearVector v type-id)
          offset (DenseUnionUtil/writeTypeId v (.getValueCount v) type-id)]
      (.setSafe inner-vec offset (.toTotalMonths x))
      v)))

;; EDN

(extend-protocol ArrowAppendable
  Character
  (append-value [x ^DenseUnionVector v]
    (let [arrow-type (.getType Types$MinorType/UINT2)
          extension-type "char"
          type-id (get-or-create-type-id v arrow-type (->extension-type-pred extension-type))
          ^UInt2Vector inner-vec (get-or-add-vector v arrow-type "extensiontype" type-id extension-type)
          offset (DenseUnionUtil/writeTypeId v (.getValueCount v) type-id)]
      (.setSafe inner-vec offset x)
      v))

  Keyword
  (append-value [x ^DenseUnionVector v]
    (let [arrow-type (.getType Types$MinorType/VARCHAR)
          extension-type "clojure.lang.Keyword"
          type-id (get-or-create-type-id v arrow-type (->extension-type-pred extension-type))
          ^VarCharVector inner-vec (get-or-add-vector v arrow-type "extensiontype" type-id extension-type)
          offset (DenseUnionUtil/writeTypeId v (.getValueCount v) type-id)
          x (kw-name x)
          bb (.encode (.newEncoder StandardCharsets/UTF_8) (CharBuffer/wrap x))]
      (.setSafe inner-vec offset bb (.position bb) (.remaining bb))
      v))

  Symbol
  (append-value [x ^DenseUnionVector v]
    (let [arrow-type (.getType Types$MinorType/VARCHAR)
          extension-type "clojure.lang.Symbol"
          type-id (get-or-create-type-id v arrow-type (->extension-type-pred extension-type))
          ^VarCharVector inner-vec (get-or-add-vector v arrow-type "extensiontype" type-id extension-type)
          offset (DenseUnionUtil/writeTypeId v (.getValueCount v) type-id)
          x (str x)
          bb (.encode (.newEncoder StandardCharsets/UTF_8) (CharBuffer/wrap x))]
      (.setSafe inner-vec offset bb (.position bb) (.remaining bb))
      v))

  UUID
  (append-value [x ^DenseUnionVector v]
    (let [arrow-type (ArrowType$FixedSizeBinary. 16)
          extension-type "uuid"
          type-id (get-or-create-type-id v arrow-type (->extension-type-pred extension-type))
          ^FixedSizeBinaryVector inner-vec (get-or-add-vector v arrow-type "extensiontype" type-id extension-type)
          offset (DenseUnionUtil/writeTypeId v (.getValueCount v) type-id)]
      (.setSafe inner-vec offset (-> (ByteBuffer/allocate 16)
                                     (.putLong (.getMostSignificantBits x))
                                     (.putLong (.getLeastSignificantBits x))
                                     (.array)))
      v))

  Set
  (append-value [x ^DenseUnionVector v]
    (let [arrow-type (.getType Types$MinorType/LIST)
          extension-type "clojure.lang.IPersistentSet"
          type-id (get-or-create-type-id v arrow-type (->extension-type-pred extension-type))
          ^ListVector inner-vec (get-or-add-vector v arrow-type "extensiontype" type-id extension-type)
          offset (DenseUnionUtil/writeTypeId v (.getValueCount v) type-id)
          data-vec (.getVector (.addOrGetVector inner-vec dense-union-field-type))]
      (.startNewValue inner-vec offset)
      (doseq [v x]
        (append-value v data-vec))
      (.endValue inner-vec offset (count x))
      v))

  IPersistentList
  (append-value [x ^DenseUnionVector v]
    (let [arrow-type (.getType Types$MinorType/LIST)
          extension-type "clojure.lang.IPersistentList"
          type-id (get-or-create-type-id v arrow-type (->extension-type-pred extension-type))
          ^ListVector inner-vec (get-or-add-vector v arrow-type "extensiontype" type-id extension-type)
          offset (DenseUnionUtil/writeTypeId v (.getValueCount v) type-id)
          data-vec (.getVector (.addOrGetVector inner-vec dense-union-field-type))]
      (.startNewValue inner-vec offset)
      (doseq [v x]
        (append-value v data-vec))
      (.endValue inner-vec offset (count x))
      v))

  Map
  (append-value [x ^DenseUnionVector v]
    (if (every? keyword? (keys x))
      (append-struct x v)
      (let [arrow-type (ArrowType$Map. false)
            type-id (get-or-create-type-id v arrow-type)
            ^MapVector inner-vec (get-or-add-vector v arrow-type "map" type-id)
            offset (DenseUnionUtil/writeTypeId v (.getValueCount v) type-id)
            ^StructVector element-vec (.getVector (.addOrGetVector inner-vec (FieldType. false (.getType Types$MinorType/STRUCT) nil)))
            key-vec (.addOrGet element-vec MapVector/KEY_NAME dense-union-field-type DenseUnionVector)
            value-vec (.addOrGet element-vec MapVector/VALUE_NAME dense-union-field-type DenseUnionVector)]
        (.startNewValue inner-vec offset)
        (doseq [[k v] x]
          (append-value k key-vec)
          (append-value v value-vec))
        (.endValue inner-vec offset (count x))
        v))))

;; Java

(extend-protocol ArrowAppendable
  Object
  (append-value [x ^DenseUnionVector v]
    (let [arrow-type (.getType Types$MinorType/VARBINARY)
          extension-type "java.io.Serializable"
          type-id (get-or-create-type-id v arrow-type (->extension-type-pred extension-type))
          ^VarBinaryVector inner-vec (get-or-add-vector v arrow-type "extensiontype" type-id extension-type)
          offset (DenseUnionUtil/writeTypeId v (.getValueCount v) type-id)
          ^bytes ba (with-open [baos (ByteArrayOutputStream.)
                                out (ObjectOutputStream. baos)]
                      (.writeObject out x)
                      (.toByteArray baos))]
      (.setSafe inner-vec offset ba)
      v)))
