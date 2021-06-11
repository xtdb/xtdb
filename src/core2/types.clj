(ns core2.types
  (:require [clojure.string :as str]
            [core2.util :as util])
  (:import core2.DenseUnionUtil
           java.nio.ByteBuffer
           java.nio.charset.StandardCharsets
           [java.time Duration LocalDateTime]
           java.util.Date
           [org.apache.arrow.vector BigIntVector BitVector DurationVector Float8Vector NullVector TimeStampMilliVector VarBinaryVector VarCharVector]
           org.apache.arrow.vector.complex.DenseUnionVector
           [org.apache.arrow.vector.types Types$MinorType TimeUnit UnionMode]
           [org.apache.arrow.vector.types.pojo ArrowType ArrowType$Duration ArrowType$Union Field FieldType]
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

(def class->arrow-type
  {nil (.getType Types$MinorType/NULL)
   Long (.getType Types$MinorType/BIGINT)
   Double (.getType Types$MinorType/FLOAT8)
   byte-array-class (.getType Types$MinorType/VARBINARY)
   ByteBuffer (.getType Types$MinorType/VARBINARY)
   String (.getType Types$MinorType/VARCHAR)
   Text (.getType Types$MinorType/VARCHAR)
   Boolean (.getType Types$MinorType/BIT)
   Date (.getType Types$MinorType/TIMESTAMPMILLI)
   Duration duration-milli-arrow-type
   LocalDateTime (.getType Types$MinorType/TIMESTAMPMILLI)})

(def arrow-type->vector-type
  {(.getType Types$MinorType/NULL) NullVector
   (.getType Types$MinorType/BIGINT) BigIntVector
   (.getType Types$MinorType/FLOAT8) Float8Vector
   (.getType Types$MinorType/VARBINARY) VarBinaryVector
   (.getType Types$MinorType/VARCHAR) VarCharVector
   (.getType Types$MinorType/TIMESTAMPMILLI) TimeStampMilliVector
   duration-milli-arrow-type DurationVector
   (.getType Types$MinorType/BIT) BitVector})

(def minor-type->java-type
  {Types$MinorType/NULL nil
   Types$MinorType/BIGINT Long
   Types$MinorType/FLOAT8 Double
   Types$MinorType/VARBINARY byte-array-class
   Types$MinorType/VARCHAR String
   Types$MinorType/TIMESTAMPMILLI Date
   Types$MinorType/DURATION Duration
   Types$MinorType/BIT Boolean})

(def arrow-type->java-type
  (->> (for [[^Types$MinorType k v] minor-type->java-type]
         [(if (= Types$MinorType/DURATION k)
            duration-milli-arrow-type
            (.getType k))
          v])
       (into {})))

(defn minor-type->arrow-type ^org.apache.arrow.vector.types.pojo.ArrowType [^Types$MinorType minor-type]
  (if (= Types$MinorType/DURATION minor-type)
    duration-milli-arrow-type
    (.getType minor-type)))

(defn arrow-type->type-id ^long [^ArrowType arrow-type]
  (long (.getFlatbufID (.getTypeID arrow-type))))

(defn ->field ^org.apache.arrow.vector.types.pojo.Field [^String field-name ^ArrowType arrow-type nullable & children]
  (Field. field-name (FieldType. nullable arrow-type nil nil) children))

(def ^org.apache.arrow.vector.types.pojo.Field row-id-field
  (->field "_row-id" (class->arrow-type Long) false))

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
  (set-safe! [this idx v] (.setSafe this ^int idx (if (int? v)
                                                    ^long v
                                                    (.getTime (if (instance? LocalDateTime v)
                                                                (util/local-date-time->date v)
                                                                ^Date v)))))
  (set-null! [this idx] (.setNull this ^int idx))
  (get-object [this idx] (Date. (.get this ^int idx)))

  DurationVector
  (set-safe! [this idx v] (.setSafe this ^int idx (if (int? v)
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
  (set-safe! [this idx v] (cond
                            (instance? ByteBuffer v)
                            (.setSafe this ^int idx ^ByteBuffer v (.position ^ByteBuffer v) (.remaining ^ByteBuffer v))

                            (bytes? v)
                            (.setSafe this ^int idx ^bytes v)

                            :else
                            (throw (IllegalArgumentException.))))
  (set-null! [this idx] (.setNull this ^int idx))
  (get-object [this idx] (.get this ^int idx))

  VarCharVector
  (set-safe! [this idx v] (cond
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
  (set-safe! [this idx v] (let [type-id (arrow-type->type-id (class->arrow-type (class v)))
                                offset (DenseUnionUtil/writeTypeId this idx type-id)]
                            (set-safe! (.getVectorByType this (.getTypeId this idx)) offset v)))
  (set-null! [this idx] (set-safe! this idx nil))
  (get-object [this idx] (get-object (.getVectorByType this (.getTypeId this idx))
                                     (.getOffset this idx))))

(def ^:private ->minor-type
  (->> (for [^Types$MinorType t (Types$MinorType/values)]
         [(keyword (str/lower-case (.name t))) t])
       (into {})))

(def primitive-types
  #{:null :bigint :float8 :varbinary :varchar :bit :timestampmilli :durationmilli})

(defn primitive-type->arrow-type ^org.apache.arrow.vector.types.pojo.ArrowType [type-k]
  (if (= :durationmilli type-k)
    duration-milli-arrow-type
    (.getType ^Types$MinorType (->minor-type type-k))))

(defn ->primitive-dense-union-field
  (^org.apache.arrow.vector.types.pojo.Field [field-name]
   (->primitive-dense-union-field field-name primitive-types))
  (^org.apache.arrow.vector.types.pojo.Field [^String field-name type-ks]
   (let [type-ks (sort-by (comp arrow-type->type-id primitive-type->arrow-type) type-ks)
         type-ids (int-array (for [type-k type-ks]
                               (-> type-k primitive-type->arrow-type arrow-type->type-id)))]
     (apply ->field field-name
            (ArrowType$Union. UnionMode/Dense type-ids)
            false
            (for [type-k type-ks]
              (if (= type-k :null)
                (->field "$data$"
                         (primitive-type->arrow-type type-k)
                         true)
                (->field (name type-k)
                         (primitive-type->arrow-type type-k)
                         false)))))))
