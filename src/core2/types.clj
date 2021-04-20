(ns core2.types
  (:require [clojure.string :as str]
            [core2.util :as util])
  (:import java.time.LocalDateTime
           java.util.Date
           [org.apache.arrow.vector BigIntVector BitVector Float8Vector NullVector TimeStampMilliVector TinyIntVector VarBinaryVector VarCharVector]
           [org.apache.arrow.vector.types Types$MinorType UnionMode]
           [org.apache.arrow.vector.types.pojo ArrowType ArrowType$Union Field FieldType]
           org.apache.arrow.vector.util.Text))

(set! *unchecked-math* :warn-on-boxed)

(def ->arrow-type
  {nil (.getType Types$MinorType/NULL)
   Long (.getType Types$MinorType/BIGINT)
   Double (.getType Types$MinorType/FLOAT8)
   (Class/forName "[B") (.getType Types$MinorType/VARBINARY)
   String (.getType Types$MinorType/VARCHAR)
   Text (.getType Types$MinorType/VARCHAR)
   Boolean (.getType Types$MinorType/BIT)
   Date (.getType Types$MinorType/TIMESTAMPMILLI)
   LocalDateTime (.getType Types$MinorType/TIMESTAMPMILLI)})

(def ^:private byte-array-class (Class/forName "[B"))

(def arrow-type->vector-type
  {(.getType Types$MinorType/NULL) NullVector
   (.getType Types$MinorType/BIGINT) BigIntVector
   (.getType Types$MinorType/FLOAT8) Float8Vector
   (.getType Types$MinorType/VARBINARY) VarBinaryVector
   (.getType Types$MinorType/VARCHAR) VarCharVector
   (.getType Types$MinorType/TIMESTAMPMILLI) TimeStampMilliVector
   (.getType Types$MinorType/TINYINT) TinyIntVector
   (.getType Types$MinorType/BIT) BitVector})

(def arrow-type->java-type
  {(.getType Types$MinorType/NULL) nil
   (.getType Types$MinorType/BIGINT) Long
   (.getType Types$MinorType/FLOAT8) Double
   (.getType Types$MinorType/VARBINARY) byte-array-class
   (.getType Types$MinorType/VARCHAR) String
   (.getType Types$MinorType/TIMESTAMPMILLI) Date
   (.getType Types$MinorType/TINYINT) Byte
   (.getType Types$MinorType/BIT) Boolean})

(defn ->field ^org.apache.arrow.vector.types.pojo.Field [^String field-name ^ArrowType arrow-type nullable & children]
  (Field. field-name (FieldType. nullable arrow-type nil nil) children))

(def ^org.apache.arrow.vector.types.pojo.Field row-id-field
  (->field "_row-id" (->arrow-type Long) false))

(defprotocol PValueVector
  (set-safe! [value-vector idx v])
  (set-null! [value-vector idx]))

(extend-protocol PValueVector
  BigIntVector
  (set-safe! [this idx v] (.setSafe this ^int idx ^long v))
  (set-null! [this idx] (.setNull this ^int idx))

  BitVector
  (set-safe! [this idx v] (.setSafe this ^int idx ^int (if v 1 0)))
  (set-null! [this idx] (.setNull this ^int idx))

  TimeStampMilliVector
  (set-safe! [this idx v] (.setSafe this ^int idx (if (int? v)
                                                    ^long v
                                                    (.getTime (if (instance? LocalDateTime v)
                                                                (util/local-date-time->date v)
                                                                ^Date v)))))
  (set-null! [this idx] (.setNull this ^int idx))

  Float8Vector
  (set-safe! [this idx v] (.setSafe this ^int idx ^double v))
  (set-null! [this idx] (.setNull this ^int idx))

  NullVector
  (set-safe! [this idx v])
  (set-null! [this idx])

  VarBinaryVector
  (set-safe! [this idx v] (.setSafe this ^int idx ^bytes v))
  (set-null! [this idx] (.setNull this ^int idx))

  VarCharVector
  (set-safe! [this idx v] (.setSafe this ^int idx (if (instance? Text v)
                                                    ^Text v
                                                    (Text. (str v)))))
  (set-null! [this idx] (.setNull this ^int idx)))

(def ->minor-type
  (->> (for [^Types$MinorType t (Types$MinorType/values)]
         [(keyword (str/lower-case (.name t))) t])
       (into {})))

(def primitive-types
  #{:null :bigint :float8 :varbinary :varchar :bit :timestampmilli})

(defn arrow-type->type-id ^long [^ArrowType arrow-type]
  (long (.getFlatbufID (.getTypeID arrow-type))))

(defn primitive-type->arrow-type ^org.apache.arrow.vector.types.pojo.ArrowType [type-k]
  (.getType ^Types$MinorType (->minor-type type-k)))

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
