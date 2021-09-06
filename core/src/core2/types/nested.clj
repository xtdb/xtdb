(ns core2.types.nested
  (:require [clojure.data.json :as json]
            [clojure.walk :as w])
  (:import [java.nio.charset StandardCharsets]
           [java.nio ByteBuffer CharBuffer]
           [java.util Date List Map]
           [java.time Duration Instant LocalDate LocalTime]
           [java.time.temporal ChronoField]
           [org.apache.arrow.vector.types Types$MinorType]
           [org.apache.arrow.vector.types.pojo ArrowType$Union Field FieldType]
           [org.apache.arrow.vector.complex DenseUnionVector]
           [org.apache.arrow.vector ValueVector]
           [org.apache.arrow.vector.util Text VectorBatchAppender]
           [core2 DenseUnionUtil]))

;; Type mapping aims to stay close to the Arrow JDBC adapter:
;; https://github.com/apache/arrow/blob/master/java/adapter/jdbc/src/main/java/org/apache/arrow/adapter/jdbc/JdbcToArrow.java

;; For example, unlike Arrow Java itself, TimeMilliVector is mapped to
;; LocalTime and DateMilliVector to LocalDate like they are above via
;; the java.sql definitions.

;; Timezones aren't supported as UnionVector doesn't support it.
;; Decimals aren't (currently) supported as UnionVector requires a
;; single scale/precision.

;; Java maps are always assumed to have named keys and are mapped to
;; structs. Arrow maps with arbitrary key types isn't (currently)
;; supported.

(set! *unchecked-math* :warn-on-boxed)

(defprotocol ArrowAppendable
  (append-value [_ v]))

(defn- kw-name ^String [x]
  (if (keyword? x)
    (subs (str x) 1)
    (str x)))

;; TODO: this scans the children all the time.
(defn- get-or-create-type-id ^long [^DenseUnionVector v ^Types$MinorType type]
  (let [arrow-type (.getType type)
        children (.getChildren (.getField v))]
    (loop [idx 0]
      (cond
        (= idx (.size children))
        (.registerNewTypeId v (Field/nullable (.name type) arrow-type))

        (= arrow-type (.getType ^Field (.get children idx)))
        (if-let [type-ids (.getTypeIds ^ArrowType$Union (.getType (.getMinorType v)))]
          (aget type-ids idx)
          idx)

        :else
        (recur (inc idx))))))

(extend-protocol ArrowAppendable
  (class (byte-array 0))
  (append-value [x ^DenseUnionVector v]
    (append-value (ByteBuffer/wrap x) v))

  nil
  (append-value [_ ^DenseUnionVector v]
    (.setValueCount v (inc (.getValueCount v)))
    v)

  Boolean
  (append-value [x ^DenseUnionVector v]
    (let [type-id (get-or-create-type-id v Types$MinorType/BIT)
          inner-vec (.getBitVector v type-id)
          offset (DenseUnionUtil/writeTypeId v (.getValueCount v) type-id)]
      (.setSafe inner-vec offset (if x 1 0))
      v))

  Byte
  (append-value [x ^DenseUnionVector v]
    (let [type-id (get-or-create-type-id v Types$MinorType/TINYINT)
          inner-vec (.getTinyIntVector v type-id)
          offset (DenseUnionUtil/writeTypeId v (.getValueCount v) type-id)]
      (.setSafe inner-vec offset x)
      v))

  Short
  (append-value [x ^DenseUnionVector v]
    (let [type-id (get-or-create-type-id v Types$MinorType/SMALLINT)
          inner-vec (.getSmallIntVector v type-id)
          offset (DenseUnionUtil/writeTypeId v (.getValueCount v) type-id)]
      (.setSafe inner-vec offset x)
      v))

  Integer
  (append-value [x ^DenseUnionVector v]
    (let [type-id (get-or-create-type-id v Types$MinorType/INT)
          inner-vec (.getIntVector v type-id)
          offset (DenseUnionUtil/writeTypeId v (.getValueCount v) type-id)]
      (.setSafe inner-vec offset x)
      v))

  Long
  (append-value [x ^DenseUnionVector v]
    (let [type-id (get-or-create-type-id v Types$MinorType/BIGINT)
          inner-vec (.getBigIntVector v type-id)
          offset (DenseUnionUtil/writeTypeId v (.getValueCount v) type-id)]
      (.setSafe inner-vec offset x)
      v))

  Float
  (append-value [x ^DenseUnionVector v]
    (let [type-id (get-or-create-type-id v Types$MinorType/FLOAT4)
          inner-vec (.getFloat4Vector v type-id)
          offset (DenseUnionUtil/writeTypeId v (.getValueCount v) type-id)]
      (.setSafe inner-vec offset x)
      v))

  Double
  (append-value [x ^DenseUnionVector v]
    (let [type-id (get-or-create-type-id v Types$MinorType/FLOAT8)
          inner-vec (.getFloat8Vector v type-id)
          offset (DenseUnionUtil/writeTypeId v (.getValueCount v) type-id)]
      (.setSafe inner-vec offset x)
      v))

  LocalDate
  (append-value [x ^DenseUnionVector v]
    (let [type-id (get-or-create-type-id v Types$MinorType/DATEMILLI)
          inner-vec (.getDateMilliVector v type-id)
          offset (DenseUnionUtil/writeTypeId v (.getValueCount v) type-id)]
      (assign-type-id v type-id (.getField inner-vec))
      (.setSafe inner-vec offset (* (.getLong x ChronoField/EPOCH_DAY) 86400000))
      v))

  LocalTime
  (append-value [x ^DenseUnionVector v]
    (let [type-id (get-or-create-type-id v Types$MinorType/TIMEMICRO)
          inner-vec (.getTimeMicroVector v type-id)
          offset (DenseUnionUtil/writeTypeId v (.getValueCount v) type-id)]
      (.setSafe inner-vec offset (quot (.getLong x ChronoField/NANO_OF_DAY) 1000))
      v))

  Date
  (append-value [x ^DenseUnionVector v]
    (append-value (.toInstant x) v))

  Instant
  (append-value [x ^DenseUnionVector v]
    (let [type-id (get-or-create-type-id v Types$MinorType/TIMESTAMPMICRO)
          inner-vec (.getTimeStampMicroVector v type-id)
          offset (DenseUnionUtil/writeTypeId v (.getValueCount v) type-id)]
      (.setSafe inner-vec offset (+ (* (.getEpochSecond x) 1000000)
                                    (quot (.getNano x) 1000)))
      v))

  Duration
  (append-value [x ^DenseUnionVector v]
    (let [type-id (get-or-create-type-id v Types$MinorType/INTERVALDAY)
          inner-vec (.getIntervalDayVector v type-id)
          offset (DenseUnionUtil/writeTypeId v (.getValueCount v) type-id)]
      (.setSafe inner-vec offset (.toDays x) (rem (.toMillis x) 86400000))
      v))

  Character
  (append-value [x ^DenseUnionVector v]
    (append-value (str x) v))

  CharSequence
  (append-value [x ^DenseUnionVector v]
    (let [type-id (get-or-create-type-id v Types$MinorType/VARCHAR)
          inner-vec (.getVarCharVector v type-id)
          offset (DenseUnionUtil/writeTypeId v (.getValueCount v) type-id)
          x (.encode (.newEncoder StandardCharsets/UTF_8) (CharBuffer/wrap x))]
      (.setSafe inner-vec offset x (.position x) (.remaining x))
      v))

  ByteBuffer
  (append-value [x ^DenseUnionVector v]
    (let [type-id (get-or-create-type-id v Types$MinorType/VARBINARY)
          inner-vec (.getVarBinaryVector v type-id)
          offset (DenseUnionUtil/writeTypeId v (.getValueCount v) type-id)]
      (.setSafe inner-vec offset (.duplicate x) (.position x) (.remaining x))
      v))

  List
  (append-value [x ^DenseUnionVector v]
    (let [type-id (get-or-create-type-id v Types$MinorType/LIST)
          inner-vec (.getList v type-id)
          offset (DenseUnionUtil/writeTypeId v (.getValueCount v) type-id)
          data-vec (.getVector (.addOrGetVector inner-vec (FieldType/nullable (.getType Types$MinorType/DENSEUNION))))]
       (.startNewValue inner-vec offset)
      (doseq [v x]
        (append-value v data-vec))
      (.endValue inner-vec offset (count x))
      v))

  Map
  (append-value [x ^DenseUnionVector v]
    (let [type-id (get-or-create-type-id v Types$MinorType/STRUCT)
          inner-vec (.getStruct v type-id)
          offset (DenseUnionUtil/writeTypeId v (.getValueCount v) type-id)]
      (.setIndexDefined inner-vec offset)
      (doseq [k (distinct (concat (.getChildFieldNames inner-vec)
                                  (map kw-name (keys x))))
              :let [data-vec (or (.getChild inner-vec k DenseUnionVector)
                                 (.addOrGet inner-vec k (FieldType/nullable (.getType Types$MinorType/DENSEUNION)) DenseUnionVector))]]
        (.setValueCount data-vec (dec (.getValueCount inner-vec)))
        (append-value (get x (keyword k)) data-vec))
      v)))
