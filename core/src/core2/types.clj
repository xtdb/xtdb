(ns core2.types
  (:import core2.relation.IColumnWriter
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

(defprotocol ValueToArrowType
  (value->arrow-type [v]))

(extend-protocol ValueToArrowType
  nil (value->arrow-type [_] ArrowType$Null/INSTANCE)
  Long (value->arrow-type [_] bigint-type)
  Double (value->arrow-type [_] float8-type)
  Boolean (value->arrow-type [_] ArrowType$Bool/INSTANCE)
  Date (value->arrow-type [_] timestamp-milli-type)
  Duration (value->arrow-type [_] duration-milli-type)
  LocalDateTime (value->arrow-type [_] timestamp-milli-type))

(extend-protocol ValueToArrowType
  (Class/forName "[B") (value->arrow-type [_] ArrowType$Binary/INSTANCE)
  ByteBuffer (value->arrow-type [_] ArrowType$Binary/INSTANCE)
  String (value->arrow-type [_] ArrowType$Utf8/INSTANCE)
  Text (value->arrow-type [_] ArrowType$Utf8/INSTANCE))

(def arrow-type->vector-type
  {ArrowType$Null/INSTANCE NullVector
   bigint-type BigIntVector
   float8-type Float8Vector
   ArrowType$Binary/INSTANCE VarBinaryVector
   ArrowType$Utf8/INSTANCE VarCharVector
   timestamp-milli-type TimeStampMilliVector
   duration-milli-type DurationVector
   ArrowType$Bool/INSTANCE BitVector})

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
  (get-object [value-vector idx]))

(extend-protocol PValueVector
  BigIntVector
  (get-object [this idx] (.get this ^int idx))

  BitVector
  (get-object [this idx] (.getObject this ^int idx))

  TimeStampMilliVector
  (get-object [this idx] (Date. (.get this ^int idx)))

  DurationVector
  (get-object [this idx] (.getObject this ^int idx))

  Float8Vector
  (get-object [this idx] (.get this ^int idx))

  NullVector
  (get-object [this idx] (.getObject this ^int idx))

  VarBinaryVector
  (get-object [this idx] (.get this ^int idx))

  VarCharVector
  (get-object [this idx] (String. (.get this ^int idx) StandardCharsets/UTF_8))

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
