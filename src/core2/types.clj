(ns core2.types
  (:import java.util.Date
           [org.apache.arrow.vector BigIntVector BitVector Float8Vector NullVector VarBinaryVector VarCharVector TimeStampMilliVector]
           [org.apache.arrow.vector.types Types Types$MinorType UnionMode]
           [org.apache.arrow.vector.types.pojo ArrowType ArrowType$Timestamp ArrowType$Union Field FieldType]
           org.apache.arrow.vector.util.Text))

(set! *unchecked-math* :warn-on-boxed)

(def ->arrow-type
  {nil (.getType Types$MinorType/NULL)
   Long (.getType Types$MinorType/BIGINT)
   Double (.getType Types$MinorType/FLOAT8)
   (Class/forName "[B") (.getType Types$MinorType/VARBINARY)
   String (.getType Types$MinorType/VARCHAR)
   Boolean (.getType Types$MinorType/BIT)
   Date (.getType Types$MinorType/TIMESTAMPMILLI)})

(defn ->field-type ^org.apache.arrow.vector.types.pojo.FieldType [^ArrowType arrow-type nullable]
  (FieldType. nullable arrow-type nil nil))

(defn ->field ^org.apache.arrow.vector.types.pojo.Field [^String field-name ^ArrowType arrow-type nullable & children]
  (Field. field-name (->field-type arrow-type nullable) children))

(def ^org.apache.arrow.vector.types.pojo.Field row-id-field
  (->field "_row-id" (->arrow-type Long) false))

(def ^org.apache.arrow.vector.types.pojo.Field tx-time-field
  (->field "_tx-time" (->arrow-type Date) false))

(def ^org.apache.arrow.vector.types.pojo.Field tx-id-field
  (->field "_tx-id" (->arrow-type Long) false))

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
                                    (.getType Types$MinorType/TIMESTAMPMILLI)]]
         (->field (.toLowerCase (if (= arrow-type (.getType Types$MinorType/NULL))
                                  "$data$"
                                  (.name (Types/getMinorTypeForArrowType arrow-type))))
                  arrow-type
                  true))))


(defn ->dense-union-field-with-flatbuf-ids ^org.apache.arrow.vector.types.pojo.Field [^String field-name union-fields]
  (let [type-ids (int-array (for [^Field field union-fields]
                              (.getFlatbufID (.getTypeID (.getType field)))))]
    (apply ->field field-name
           (ArrowType$Union. UnionMode/Dense type-ids)
           false
           union-fields)))
