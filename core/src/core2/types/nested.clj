(ns core2.types.nested
  (:require [clojure.data.json :as json]
            [clojure.walk :as w])
  (:import [java.nio.charset StandardCharsets]
           [java.nio ByteBuffer CharBuffer]
           [java.util Date Collection List Map Set UUID]
           [java.time Duration Instant LocalDate LocalTime Period ZoneId ZonedDateTime]
           [java.time.temporal ChronoField ChronoUnit]
           [org.apache.arrow.vector.types Types$MinorType TimeUnit]
           [org.apache.arrow.vector.types.pojo ArrowType ArrowType$Decimal ArrowType$Duration ArrowType$ExtensionType ArrowType$FixedSizeBinary ArrowType$Timestamp ArrowType$Union Field FieldType]
           [org.apache.arrow.vector.complex DenseUnionVector ListVector StructVector]
           [org.apache.arrow.vector DateMilliVector DecimalVector DurationVector FixedSizeBinaryVector IntervalYearVector
            TimeMicroVector TimeStampMicroVector TimeStampMicroTZVector VarBinaryVector VarCharVector ValueVector UInt2Vector]
           [org.apache.arrow.vector.util Text VectorBatchAppender]
           [core2 DenseUnionUtil]
           [clojure.lang IPersistentList IPersistentSet Keyword Symbol]))

;; Type mapping aims to stay close to the Arrow JDBC adapter:
;; https://github.com/apache/arrow/blob/master/java/adapter/jdbc/src/main/java/org/apache/arrow/adapter/jdbc/JdbcToArrow.java

;; For example, unlike Arrow Java itself, TimeMilliVector is mapped to
;; LocalTime and DateMilliVector to LocalDate like they are above via
;; the java.sql definitions.

;; Java maps are always assumed to have named keys and are mapped to
;; structs. Arrow maps with arbitrary key types isn't (currently)
;; supported.

;; TODO:
;; - append-value support for maps (for non keyword maps).
;; - experimental append-value support for Serializable?
;; - support java.sql types for append-value?
;; - consistent get-value implementations for vectors we don't generate but may read.

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

(def ^:private extension-metadata-key-name ArrowType$ExtensionType/EXTENSION_METADATA_KEY_NAME)

(defprotocol ArrowAppendable
  (append-value [_ v]))

(defprotocol ArrowReadable
  (get-value [_ idx]))

(extend-protocol ArrowReadable
  nil
  (get-value [_ _])

  ValueVector
  (get-value [v idx]
    (.getObject v idx))

  DenseUnionVector
  (get-value [v idx]
    (get-value (.getVectorByType v (.getTypeId v idx)) (.getOffset v idx)))

  FixedSizeBinaryVector
  (get-value [v idx]
    (let [x (ByteBuffer/wrap (.getObject v ^long idx))]
      (case (get (.getMetadata (.getField v)) extension-metadata-key-name)
        "uuid"
        (UUID. (.getLong x) (.getLong x))

        x)))

  VarBinaryVector
  (get-value [v idx]
    (ByteBuffer/wrap (.getObject v ^long idx)))

  VarCharVector
  (get-value [v idx]
    (let [x (str (.getObject v ^long idx))]
      (case (get (.getMetadata (.getField v)) extension-metadata-key-name)
        "clojure.lang.Keyword"
        (keyword x)

        "clojure.lang.Symbol"
        (symbol x)

        x)))

  DateMilliVector
  (get-value [v idx]
    (LocalDate/ofEpochDay (quot (.get v idx) 86400000)))

  TimeMicroVector
  (get-value [v idx]
    (LocalTime/ofNanoOfDay (* 1000 (.get v idx))))

  TimeStampMicroVector
  (get-value [v idx]
    (Instant/ofEpochSecond 0 (* 1000 (.get v idx))))

  TimeStampMicroTZVector
  (get-value [v idx]
    (.atZone (Instant/ofEpochSecond 0 (* 1000 (.get v idx)))
             (ZoneId/of (.getTimezone ^ArrowType$Timestamp (.getType (.getField v))))))

  IntervalYearVector
  (get-value [v idx]
    (.normalized (.getObject ^IntervalYearVector v ^long idx)))

  ListVector
  (get-value [v idx]
    (loop [element-idx (.getElementStartIndex v idx)
           acc []]
      (if (= (.getElementEndIndex v idx) element-idx)
        (case (get (.getMetadata (.getField v)) extension-metadata-key-name)
          "clojure.lang.IPersistentSet"
          (set acc)

          "clojure.lang.IPersistentList"
          (apply list acc)

          acc)
        (recur (inc element-idx)
               (conj acc (get-value (.getDataVector v) element-idx))))))

  StructVector
  (get-value [v idx]
    (reduce
     (fn [acc n]
       (assoc acc (keyword n) (get-value (.getChild v n ValueVector) idx)))
     {}
     (.getChildFieldNames v))))

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
  ([^DenseUnionVector v ^ArrowType arrow-type ^String prefix ^Long type-id metadata]
   (or (.getVectorByType v type-id)
       (.addVector v type-id (.createNewSingleVector (FieldType. true arrow-type nil metadata) (str prefix type-id) (.getAllocator v) nil)))))

(extend-protocol ArrowAppendable
  (class (byte-array 0))
  (append-value [x ^DenseUnionVector v]
    (append-value (ByteBuffer/wrap x) v))

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
    (let [type-id (get-or-create-type-id v (.getType Types$MinorType/TIMESTAMPMICRO))
          inner-vec (.getTimeStampMicroVector v type-id)
          offset (DenseUnionUtil/writeTypeId v (.getValueCount v) type-id)]
      (.setSafe inner-vec offset (+ (* (.getEpochSecond x) 1000000)
                                    (quot (.getNano x) 1000)))
      v))

  ZonedDateTime
  (append-value [x ^DenseUnionVector v]
    (let [arrow-type (ArrowType$Timestamp. TimeUnit/MICROSECOND (str (.getZone x)))
          type-id (get-or-create-type-id v arrow-type)
          ^TimeStampMicroTZVector inner-vec (get-or-add-vector v arrow-type "timestamp" type-id)
          offset (DenseUnionUtil/writeTypeId v (.getValueCount v) type-id)
          x (.toInstant x)]
      (.setSafe inner-vec offset (+ (* (.getEpochSecond x) 1000000)
                                    (quot (.getNano x) 1000)))
      v))

  Duration
  (append-value [x ^DenseUnionVector v]
    (let [arrow-type (ArrowType$Duration. TimeUnit/MICROSECOND)
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
      v))


  Character
  (append-value [x ^DenseUnionVector v]
    (let [arrow-type (.getType Types$MinorType/UINT2)
          extension-type "char"
          type-id (get-or-create-type-id v arrow-type (fn [^Field f]
                                                        (= extension-type (get (.getMetadata f) extension-metadata-key-name))))
          ^UInt2Vector inner-vec (get-or-add-vector v arrow-type "extensiontype" type-id {extension-metadata-key-name extension-type})
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

  Keyword
  (append-value [x ^DenseUnionVector v]
    (let [arrow-type (.getType Types$MinorType/VARCHAR)
          extension-type "clojure.lang.Keyword"
          type-id (get-or-create-type-id v arrow-type (fn [^Field f]
                                                        (= extension-type (get (.getMetadata f) extension-metadata-key-name))))
          ^VarCharVector inner-vec (get-or-add-vector v arrow-type "extensiontype" type-id {extension-metadata-key-name extension-type})
          offset (DenseUnionUtil/writeTypeId v (.getValueCount v) type-id)
          x (kw-name x)
          x (.encode (.newEncoder StandardCharsets/UTF_8) (CharBuffer/wrap x))]
      (.setSafe inner-vec offset x (.position x) (.remaining x))
      v))

  Symbol
  (append-value [x ^DenseUnionVector v]
    (let [arrow-type (.getType Types$MinorType/VARCHAR)
          extension-type "clojure.lang.Symbol"
          type-id (get-or-create-type-id v arrow-type (fn [^Field f]
                                                        (= extension-type (get (.getMetadata f) extension-metadata-key-name))))
          ^VarCharVector inner-vec (get-or-add-vector v arrow-type "extensiontype" type-id {extension-metadata-key-name extension-type})
          offset (DenseUnionUtil/writeTypeId v (.getValueCount v) type-id)
          x (str x)
          bb (.encode (.newEncoder StandardCharsets/UTF_8) (CharBuffer/wrap x))]
      (.setSafe inner-vec offset bb (.position bb) (.remaining bb))
      v))

  UUID
  (append-value [x ^DenseUnionVector v]
    (let [arrow-type (ArrowType$FixedSizeBinary. 16)
          extension-type "uuid"
          type-id (get-or-create-type-id v arrow-type (fn [^Field f]
                                                        (= extension-type (get (.getMetadata f) extension-metadata-key-name))))
          ^FixedSizeBinaryVector inner-vec (get-or-add-vector v arrow-type "extensiontype" type-id {extension-metadata-key-name extension-type})
          offset (DenseUnionUtil/writeTypeId v (.getValueCount v) type-id)]
      (.setSafe inner-vec offset (-> (ByteBuffer/allocate 16)
                                     (.putLong (.getMostSignificantBits x))
                                     (.putLong (.getLeastSignificantBits x))
                                     (.array)))
      v))

  ByteBuffer
  (append-value [x ^DenseUnionVector v]
    (let [type-id (get-or-create-type-id v (.getType Types$MinorType/VARBINARY))
          inner-vec (.getVarBinaryVector v type-id)
          offset (DenseUnionUtil/writeTypeId v (.getValueCount v) type-id)]
      (.setSafe inner-vec offset (.duplicate x) (.position x) (.remaining x))
      v))

  List
  (append-value [x ^DenseUnionVector v]
    (let [type-id (get-or-create-type-id v (.getType Types$MinorType/LIST))
          inner-vec (.getList v type-id)
          offset (DenseUnionUtil/writeTypeId v (.getValueCount v) type-id)
          data-vec (.getVector (.addOrGetVector inner-vec (FieldType/nullable (.getType Types$MinorType/DENSEUNION))))]
      (.startNewValue inner-vec offset)
      (doseq [v x]
        (append-value v data-vec))
      (.endValue inner-vec offset (count x))
      v))

  Set
  (append-value [x ^DenseUnionVector v]
    (let [arrow-type (.getType Types$MinorType/LIST)
          extension-type "clojure.lang.IPersistentSet"
          type-id (get-or-create-type-id v arrow-type (fn [^Field f]
                                                        (= extension-type (get (.getMetadata f) extension-metadata-key-name))))
          ^ListVector inner-vec (get-or-add-vector v arrow-type "extensiontype" type-id {extension-metadata-key-name extension-type})
          offset (DenseUnionUtil/writeTypeId v (.getValueCount v) type-id)
          data-vec (.getVector (.addOrGetVector inner-vec (FieldType/nullable (.getType Types$MinorType/DENSEUNION))))]
      (.startNewValue inner-vec offset)
      (doseq [v x]
        (append-value v data-vec))
      (.endValue inner-vec offset (count x))
      v))

  IPersistentList
  (append-value [x ^DenseUnionVector v]
    (let [arrow-type (.getType Types$MinorType/LIST)
          extension-type "clojure.lang.IPersistentList"
          type-id (get-or-create-type-id v arrow-type (fn [^Field f]
                                                        (= extension-type (get (.getMetadata f) extension-metadata-key-name))))
          ^ListVector inner-vec (get-or-add-vector v arrow-type "extensiontype" type-id {extension-metadata-key-name extension-type})
          offset (DenseUnionUtil/writeTypeId v (.getValueCount v) type-id)
          data-vec (.getVector (.addOrGetVector inner-vec (FieldType/nullable (.getType Types$MinorType/DENSEUNION))))]
      (.startNewValue inner-vec offset)
      (doseq [v x]
        (append-value v data-vec))
      (.endValue inner-vec offset (count x))
      v))

  Map
  (append-value [x ^DenseUnionVector v]
    (let [key-set (set (map kw-name (keys x)))
          type-id (get-or-create-type-id v
                                         (.getType Types$MinorType/STRUCT)
                                         (fn [^Field f]
                                           (= key-set (set (for [^Field f (.getChildren f)]
                                                             (.getName f))))))
          inner-vec (.getStruct v type-id)
          offset (DenseUnionUtil/writeTypeId v (.getValueCount v) type-id)]
      (.setIndexDefined inner-vec offset)
      (doseq [k key-set
              :let [data-vec (.addOrGet inner-vec k (FieldType/nullable (.getType Types$MinorType/DENSEUNION)) DenseUnionVector)]]
        (append-value (get x (keyword k)) data-vec))
      v)))
