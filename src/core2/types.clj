(ns core2.types
  (:import java.util.Date
           [org.apache.arrow.vector BigIntVector BitVector DateMilliVector Float8Vector NullVector VarBinaryVector VarCharVector]
           [org.apache.arrow.vector.types Types Types$MinorType]
           [org.apache.arrow.vector.types.pojo ArrowType ArrowType$Decimal Field FieldType]
           org.apache.arrow.vector.util.Text))

(set! *unchecked-math* :warn-on-boxed)

(def ->arrow-type
  {Boolean (.getType Types$MinorType/BIT)
   (Class/forName "[B") (.getType Types$MinorType/VARBINARY)
   Double (.getType Types$MinorType/FLOAT8)
   Long (.getType Types$MinorType/BIGINT)
   String (.getType Types$MinorType/VARCHAR)
   Date (.getType Types$MinorType/DATEMILLI)
   nil (.getType Types$MinorType/NULL)})

(defn ->field-type ^org.apache.arrow.vector.types.pojo.FieldType [^ArrowType arrow-type nullable]
  (FieldType. nullable arrow-type nil nil))

(defn ->field ^org.apache.arrow.vector.types.pojo.Field [^String field-name ^ArrowType arrow-type nullable & children]
  (Field. field-name (->field-type arrow-type nullable) children))

(def ^org.apache.arrow.vector.types.pojo.Field row-id-field
  (->field "_row-id" (.getType Types$MinorType/BIGINT) false))

(def ^org.apache.arrow.vector.types.pojo.Field tx-time-field
  (->field "_tx-time" (.getType Types$MinorType/DATEMILLI) false))

(def ^org.apache.arrow.vector.types.pojo.Field tx-id-field
  (->field "_tx-id" (.getType Types$MinorType/BIGINT) false))

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

  DateMilliVector
  (set-safe! [this idx v] (.setSafe this ^int idx (.getTime ^Date v)))
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
  (set-safe! [this idx v] (.setSafe this ^int idx (Text. (str v))))
  (set-null! [this idx] (.setNull this ^int idx)))

(def dense-union-fields-in-flatbuf-id-order
  (vec (for [^ArrowType arrow-type [(.getType Types$MinorType/NULL)
                                    (.getType Types$MinorType/BIGINT)
                                    (.getType Types$MinorType/FLOAT8)
                                    (.getType Types$MinorType/VARBINARY)
                                    (.getType Types$MinorType/VARCHAR)
                                    (.getType Types$MinorType/BIT)
                                    (ArrowType$Decimal/createDecimal 16 16 (Integer/valueOf 128))
                                    (.getType Types$MinorType/DATEMILLI)]]
         (->field (.toLowerCase (.name (Types/getMinorTypeForArrowType arrow-type))) arrow-type true))))


(defn ->dense-union-field ^org.apache.arrow.vector.types.pojo.Field [^String field-name union-fields]
  (apply ->field field-name
         (.getType Types$MinorType/DENSEUNION)
         false
         union-fields))
