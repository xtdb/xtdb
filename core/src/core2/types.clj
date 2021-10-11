(ns core2.types
  (:require [core2.util :as util])
  (:import [core2.relation IColumnWriter]
           [java.nio ByteBuffer CharBuffer]
           java.nio.charset.StandardCharsets
           [java.time Duration LocalDateTime]
           java.util.Date
           [org.apache.arrow.vector BigIntVector BitVector DurationVector Float8Vector NullVector TimeStampMilliVector VarBinaryVector VarCharVector]
           org.apache.arrow.vector.complex.DenseUnionVector
           [org.apache.arrow.vector.types TimeUnit Types Types$MinorType]
           [org.apache.arrow.vector.types.pojo ArrowType ArrowType$Binary ArrowType$Bool ArrowType$Duration ArrowType$FloatingPoint ArrowType$Int ArrowType$Null ArrowType$Utf8 Field FieldType]
           org.apache.arrow.vector.util.Text))

(set! *unchecked-math* :warn-on-boxed)

(def byte-array-class (Class/forName "[B"))

(def bigint-type (.getType Types$MinorType/BIGINT))
(def float8-type (.getType Types$MinorType/FLOAT8))
(def varchar-type (.getType Types$MinorType/VARCHAR))
(def varbinary-type (.getType Types$MinorType/VARBINARY))
(def timestamp-milli-type (.getType Types$MinorType/TIMESTAMPMILLI))
(def duration-milli-type (ArrowType$Duration. TimeUnit/MILLISECOND))
(def struct-type (.getType Types$MinorType/STRUCT))
(def dense-union-type (.getType Types$MinorType/DENSEUNION))
(def list-type (.getType Types$MinorType/LIST))

(defn type->field-name [^ArrowType arrow-type]
  (let [minor-type-name (.name (Types/getMinorTypeForArrowType arrow-type))]
    (case minor-type-name
      "DURATION" (format "%s-%s" (.toLowerCase minor-type-name) (.toLowerCase (.name (.getUnit ^ArrowType$Duration arrow-type))))
      (.toLowerCase minor-type-name))))

(def class->arrow-type
  {nil ArrowType$Null/INSTANCE
   Long bigint-type
   Double float8-type
   byte-array-class ArrowType$Binary/INSTANCE
   ByteBuffer ArrowType$Binary/INSTANCE
   String ArrowType$Utf8/INSTANCE
   Text ArrowType$Utf8/INSTANCE
   Boolean ArrowType$Bool/INSTANCE
   Date timestamp-milli-type
   Duration duration-milli-type
   LocalDateTime timestamp-milli-type})

(def arrow-type->vector-type
  {ArrowType$Null/INSTANCE NullVector
   bigint-type BigIntVector
   float8-type Float8Vector
   ArrowType$Binary/INSTANCE VarBinaryVector
   ArrowType$Utf8/INSTANCE VarCharVector
   timestamp-milli-type TimeStampMilliVector
   duration-milli-type DurationVector
   ArrowType$Bool/INSTANCE BitVector})

(def arrow-type-hierarchy
  (-> (make-hierarchy)
      (derive ArrowType$FloatingPoint ::Number)
      (derive ArrowType$Int ::Number)
      (derive ArrowType ::Object)))

(defmulti least-upper-bound2
  (fn [x-type y-type] [(class x-type) (class y-type)])
  :hierarchy #'arrow-type-hierarchy)

(defmethod least-upper-bound2 [::Number ::Number] [x-type y-type]
  ;; TODO this is naive of the different types of Ints/Floats
  (if (and (instance? ArrowType$Int x-type) (instance? ArrowType$Int y-type))
    bigint-type
    float8-type))

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

(defprotocol ArrowWriteable
  (write-value! [v ^core2.relation.IColumnWriter writer]))

(extend-protocol ArrowWriteable
  nil
  (write-value! [v ^IColumnWriter writer])

  Boolean
  (write-value! [v ^IColumnWriter writer]
    (.setSafe ^BitVector (.getVector writer) (.getPosition writer) (if v 1 0)))

  CharSequence
  (write-value! [v ^IColumnWriter writer]
    (let [buf (.encode (.newEncoder StandardCharsets/UTF_8) (CharBuffer/wrap v))]
      (.setSafe ^VarCharVector (.getVector writer) (.getPosition writer)
                buf (.position buf) (.remaining buf))))

  Double
  (write-value! [v ^IColumnWriter writer]
    (.setSafe ^Float8Vector (.getVector writer) (.getPosition writer) v))

  Long
  (write-value! [v ^IColumnWriter writer]
    (.setSafe ^BigIntVector (.getVector writer) (.getPosition writer) v))

  Date
  (write-value! [v ^IColumnWriter writer]
    (.setSafe ^TimeStampMilliVector (.getVector writer) (.getPosition writer) (.getTime v)))

  Duration
  (write-value! [v ^IColumnWriter writer]
    ;; HACK assumes millis for now
    (.setSafe ^DurationVector (.getVector writer) (.getPosition writer) (.toMillis v))))

(defprotocol PValueVector
  (set-safe! [value-vector idx v])
  (set-null! [value-vector idx])
  (get-object [value-vector idx]))

(extend-protocol PValueVector
  BigIntVector
  (set-safe! [this idx v] (.setSafe this ^int idx ^long v))
  (set-null! [this idx] (.setNull this ^int idx))
  (get-object [this idx] (.get this ^int idx))

  BitVector
  (set-safe! [this idx v] (.setSafe this ^int idx ^int (if v 1 0)))
  (set-null! [this idx] (.setNull this ^int idx))
  (get-object [this idx] (.getObject this ^int idx))

  TimeStampMilliVector
  (set-safe! [this idx v]
    (.setSafe this ^int idx (if (int? v)
                              ^long v
                              (.getTime (if (instance? LocalDateTime v)
                                          (util/local-date-time->date v)
                                          ^Date v)))))

  (set-null! [this idx] (.setNull this ^int idx))
  (get-object [this idx] (Date. (.get this ^int idx)))

  DurationVector
  (set-safe! [this idx v]
    (.setSafe this ^int idx (if (int? v)
                              ^long v
                              (.toMillis ^Duration v))))

  (set-null! [this idx] (.setNull this ^int idx))
  (get-object [this idx] (.getObject this ^int idx))

  Float8Vector
  (set-safe! [this idx v] (.setSafe this ^int idx ^double v))
  (set-null! [this idx] (.setNull this ^int idx))
  (get-object [this idx] (.get this ^int idx))

  NullVector
  (set-safe! [this idx v])
  (set-null! [this idx])
  (get-object [this idx] (.getObject this ^int idx))

  VarBinaryVector
  (set-safe! [this idx v]
    (cond
      (instance? ByteBuffer v)
      (.setSafe this ^int idx ^ByteBuffer v (.position ^ByteBuffer v) (.remaining ^ByteBuffer v))

      (bytes? v)
      (.setSafe this ^int idx ^bytes v)

      :else
      (throw (IllegalArgumentException.))))

  (set-null! [this idx] (.setNull this ^int idx))
  (get-object [this idx] (.get this ^int idx))

  VarCharVector
  (set-safe! [this idx v]
    (cond
      (instance? ByteBuffer v)
      (.setSafe this ^int idx ^ByteBuffer v (.position ^ByteBuffer v) (.remaining ^ByteBuffer v))

      (bytes? v)
      (.setSafe this ^int idx ^bytes v)

      (string? v)
      (.setSafe this ^int idx (.getBytes ^String v StandardCharsets/UTF_8))

      (instance? Text v)
      (.setSafe this ^int idx ^Text v)

      :else
      (throw (IllegalArgumentException.))))

  (set-null! [this idx] (.setNull this ^int idx))
  (get-object [this idx] (String. (.get this ^int idx) StandardCharsets/UTF_8))

  DenseUnionVector
  (set-null! [this idx]
    (set-safe! this idx nil))

  (get-object [this idx]
    (get-object (.getVectorByType this (.getTypeId this idx))
                (.getOffset this idx))))
