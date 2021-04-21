(ns core2.types
  (:require [clojure.string :as str]
            [core2.util :as util])
  (:import [java.util Comparator Date]
           java.time.LocalDateTime
           org.apache.arrow.memory.util.ByteFunctionHelpers
           [org.apache.arrow.vector BigIntVector BitVector Float8Vector NullVector TimeStampMilliVector TinyIntVector VarBinaryVector VarCharVector]
           org.apache.arrow.vector.complex.DenseUnionVector
           [org.apache.arrow.vector.holders NullableBigIntHolder NullableBitHolder NullableFloat8Holder NullableTimeStampMilliHolder NullableTinyIntHolder NullableVarBinaryHolder NullableVarCharHolder ValueHolder]
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

(defn holder-minor-type ^org.apache.arrow.vector.types.Types$MinorType [holder]
  (condp = (type holder)
    nil Types$MinorType/NULL
    NullableTinyIntHolder Types$MinorType/TINYINT
    NullableBigIntHolder Types$MinorType/BIGINT
    NullableBitHolder Types$MinorType/BIT
    NullableFloat8Holder Types$MinorType/FLOAT8
    NullableVarBinaryHolder Types$MinorType/VARBINARY
    NullableVarCharHolder Types$MinorType/VARCHAR
    NullableTimeStampMilliHolder Types$MinorType/TIMESTAMPMILLI))

;; generics ftw

(definterface ReadWrite
  (^Object newHolder [])
  (^boolean isSet [holder])
  (^void read [^org.apache.arrow.vector.FieldVector in-vec, ^int idx, holder])
  (^void write [^org.apache.arrow.vector.FieldVector out-vec, ^int idx, holder]))

(defmacro ->rw [vec-class holder-class]
  (let [vec-sym (gensym 'vec)
        holder-sym (gensym 'holder)]
    `(reify ReadWrite
       (~'newHolder [this#] (new ~holder-class))

       (~'isSet [this# ~holder-sym]
        (let [~(with-meta holder-sym {:tag holder-class}) ~holder-sym]
          (pos? (.isSet ~holder-sym))))

       (~'read [this# ~vec-sym idx# ~holder-sym]
        (let [~(with-meta vec-sym {:tag vec-class}) ~vec-sym
              ~(with-meta holder-sym {:tag holder-class}) ~holder-sym]
          (.get ~vec-sym idx# ~holder-sym)))

       (~'write [this# ~vec-sym idx# ~holder-sym]
        (let [~(with-meta vec-sym {:tag vec-class}) ~vec-sym
              ~(with-meta holder-sym {:tag holder-class}) ~holder-sym]
          (.setSafe ~vec-sym idx# ~holder-sym))))))

(def type->rw
  {Types$MinorType/NULL (reify ReadWrite
                          (newHolder [_] nil)
                          (isSet [_ _holder] false)
                          (read [_ _fv _idx _holder])
                          (write [_ _holder _fv _idx]))
   Types$MinorType/TINYINT (->rw TinyIntVector NullableTinyIntHolder)
   Types$MinorType/BIGINT (->rw BigIntVector NullableBigIntHolder)
   Types$MinorType/BIT (->rw BitVector NullableBitHolder)
   Types$MinorType/TIMESTAMPMILLI (->rw TimeStampMilliVector NullableTimeStampMilliHolder)
   Types$MinorType/FLOAT8 (->rw Float8Vector NullableFloat8Holder)
   Types$MinorType/VARBINARY (->rw VarBinaryVector NullableVarBinaryHolder)
   Types$MinorType/VARCHAR (->rw VarCharVector NullableVarCharHolder)})

(defmacro ->comp {:style/indent [2 :form :form]} [holder-class [left right] & body]
  `(reify Comparator
     (~'compare [this# ~left ~right]
      (let [~(with-meta left {:tag holder-class}) ~left
            ~(with-meta right {:tag holder-class}) ~right]
        ~@body))))

(def type->comp
  {Types$MinorType/NULL (Comparator/nullsFirst (Comparator/naturalOrder))

   Types$MinorType/TINYINT (->comp NullableTinyIntHolder [left right]
                             (Byte/compare (.value left) (.value right)))

   Types$MinorType/BIGINT (->comp NullableBigIntHolder [left right]
                            (Long/compare (.value left) (.value right)))

   Types$MinorType/BIT (->comp NullableBitHolder [left right]
                         (Integer/compare (.value left) (.value right)))

   Types$MinorType/TIMESTAMPMILLI (->comp NullableTimeStampMilliHolder [left right]
                                    (Long/compare (.value left) (.value right)))

   Types$MinorType/FLOAT8 (->comp NullableFloat8Holder [left right]
                            (Double/compare (.value left) (.value right)))

   Types$MinorType/VARBINARY (->comp NullableVarBinaryHolder [left right]
                               (ByteFunctionHelpers/compare (.buffer left) (.start left) (.end left)
                                                            (.buffer right) (.start right) (.end right)))

   Types$MinorType/VARCHAR (->comp NullableVarCharHolder [left right]
                             (ByteFunctionHelpers/compare (.buffer left) (.start left) (.end left)
                                                          (.buffer right) (.start right) (.end right)))})

(defn read-duv-value [^DenseUnionVector duv, ^long idx, ^ValueHolder out-holder]
  (let [minor-type (holder-minor-type out-holder)
        type-id (arrow-type->type-id (.getType minor-type))
        ^ReadWrite rw (type->rw minor-type)]
    (.read rw (.getVectorByType duv type-id) (.getOffset duv idx) out-holder)))
