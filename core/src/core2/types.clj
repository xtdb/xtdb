(ns core2.types
  (:require [clojure.set :as set]
            [core2.util :as util])
  (:import java.nio.ByteBuffer
           java.nio.charset.StandardCharsets
           [java.time Duration LocalDateTime]
           java.util.Date
           [org.apache.arrow.vector BigIntVector BitVector DurationVector Float8Vector NullVector TimeStampMilliVector VarBinaryVector VarCharVector]
           org.apache.arrow.vector.complex.DenseUnionVector
           [org.apache.arrow.vector.types TimeUnit Types$MinorType UnionMode]
           [org.apache.arrow.vector.types.pojo ArrowType ArrowType$FloatingPoint ArrowType$Int ArrowType$Duration ArrowType$Union Field FieldType]
           org.apache.arrow.vector.util.Text))

(set! *unchecked-math* :warn-on-boxed)

(def byte-array-class (Class/forName "[B"))

(def duration-milli-arrow-type (ArrowType$Duration. TimeUnit/MILLISECOND))

(def ->arrow-type
  {:null (.getType Types$MinorType/NULL)
   :bigint (.getType Types$MinorType/BIGINT)
   :float8 (.getType Types$MinorType/FLOAT8)
   :varbinary (.getType Types$MinorType/VARBINARY)
   :varchar (.getType Types$MinorType/VARCHAR)
   :bit (.getType Types$MinorType/BIT)
   :timestamp-milli (.getType Types$MinorType/TIMESTAMPMILLI)
   :duration-milli duration-milli-arrow-type})

(def <-arrow-type (set/map-invert ->arrow-type))

(defn type->field-name [^ArrowType arrow-type]
  (name (<-arrow-type arrow-type)))

(def struct-type (.getType Types$MinorType/STRUCT))
(def dense-union-type (.getType Types$MinorType/DENSEUNION))
(def list-type (.getType Types$MinorType/LIST))

(def class->arrow-type
  {nil (->arrow-type :null)
   Long (->arrow-type :bigint)
   Double (->arrow-type :float8)
   byte-array-class (->arrow-type :varbinary)
   ByteBuffer (->arrow-type :varbinary)
   String (->arrow-type :varchar)
   Text (->arrow-type :varchar)
   Boolean (->arrow-type :bit)
   Date (->arrow-type :timestamp-milli)
   Duration (->arrow-type :duration-milli)
   LocalDateTime (->arrow-type :timestamp-milli)})

(def arrow-type->vector-type
  {(->arrow-type :null) NullVector
   (->arrow-type :bigint) BigIntVector
   (->arrow-type :float8) Float8Vector
   (->arrow-type :varbinary) VarBinaryVector
   (->arrow-type :varchar) VarCharVector
   (->arrow-type :timestamp-milli) TimeStampMilliVector
   (->arrow-type :duration-milli) DurationVector
   (->arrow-type :bit) BitVector})

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
    (->arrow-type :bigint)
    (->arrow-type :float8)))

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
  (->field "_row-id" (->arrow-type :bigint) false))

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
