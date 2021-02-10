(ns core2.types
  (:require [clojure.string :as str])
  (:import [java.util Comparator Date]
           org.apache.arrow.memory.util.ByteFunctionHelpers
           [org.apache.arrow.vector BigIntVector BitVector Float8Vector NullVector TimeStampMilliVector VarBinaryVector VarCharVector]
           [org.apache.arrow.vector.holders NullableBigIntHolder NullableBitHolder NullableFloat8Holder NullableTimeStampMilliHolder NullableVarBinaryHolder NullableVarCharHolder]
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
   Boolean (.getType Types$MinorType/BIT)
   Date (.getType Types$MinorType/TIMESTAMPMILLI)})

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

(def ->minor-type
  (->> (for [^Types$MinorType t (Types$MinorType/values)]
         [(keyword (str/lower-case (.name t))) t])
       (into {})))

(def primitive-types
  #{:null :bigint :float8 :varbinary :varchar :bit :timestampmilli})

(defn arrow-type->type-id [^ArrowType arrow-type]
  (.getFlatbufID (.getTypeID arrow-type)))

(defn primitive-type->arrow-type [type-k]
  (.getType ^Types$MinorType (->minor-type type-k)))

(defn ->primitive-dense-union-field ^org.apache.arrow.vector.types.pojo.Field
  ([field-name]
   (->primitive-dense-union-field field-name primitive-types))
  ([^String field-name type-ks]
   (let [type-ks (sort-by (comp arrow-type->type-id primitive-type->arrow-type) type-ks)
         type-ids (int-array (for [type-k type-ks]
                               (-> type-k primitive-type->arrow-type arrow-type->type-id)))]
     (apply ->field field-name
            (ArrowType$Union. UnionMode/Dense type-ids)
            false
            (for [type-k type-ks]
              (->field (name type-k)
                       (primitive-type->arrow-type type-k)
                       true))))))

;; generics ftw
(definterface ReadWrite
  (^Object newHolder [])
  (^boolean isSet [holder])
  (^void read [^org.apache.arrow.vector.FieldVector in-vec, ^int idx, holder])
  (^void write [^org.apache.arrow.vector.FieldVector out-vec, ^int idx, holder]))

(defmacro def-rw [sym vec-class holder-class]
  (let [vec-sym (gensym 'vec)
        holder-sym (gensym 'holder)]
    `(def ~sym
       (reify ReadWrite
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
            (.setSafe ~vec-sym idx# ~holder-sym)))))))

(def-rw bigint-rw BigIntVector NullableBigIntHolder)
(def-rw bit-rw BitVector NullableBitHolder)
(def-rw timestamp-milli-rw TimeStampMilliVector NullableTimeStampMilliHolder)
(def-rw float8-rw Float8Vector NullableFloat8Holder)

(def null-rw
  (reify ReadWrite
    (newHolder [_] nil)
    (isSet [_ _holder] false)
    (read [_ _fv _idx _holder])
    (write [_ _holder _fv _idx])))

(def-rw varbinary-rw VarBinaryVector NullableVarBinaryHolder)
(def-rw varchar-rw VarCharVector NullableVarCharHolder)

(defmacro def-comp {:style/indent [3 :form :form :form]} [sym holder-class [left right] & body]
  `(def ~sym
     (reify Comparator
       (~'compare [this# ~left ~right]
        (let [~(with-meta left {:tag holder-class}) ~left
              ~(with-meta right {:tag holder-class}) ~right]
          ~@body)))))

(def-comp bigint-comp NullableBigIntHolder [left right]
  (Long/compare (.value left) (.value right)))

(def-comp bit-comp NullableBitHolder [left right]
  (Integer/compare (.value left) (.value right)))

(def-comp timestamp-milli-comp NullableTimeStampMilliHolder [left right]
  (Long/compare (.value left) (.value right)))

(def-comp float8-comp NullableFloat8Holder [left right]
  (Double/compare (.value left) (.value right)))

(def-comp varbinary-comp NullableVarBinaryHolder [left right]
  (ByteFunctionHelpers/compare (.buffer left) (.start left) (.end left)
                               (.buffer right) (.start right) (.end right)))

(def-comp varchar-comp NullableVarCharHolder [left right]
  (ByteFunctionHelpers/compare (.buffer left) (.start left) (.end left)
                               (.buffer right) (.start right) (.end right)))
