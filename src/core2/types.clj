(ns core2.types
  (:require [clojure.string :as str])
  (:import java.util.Date
           [org.apache.arrow.memory.util ArrowBufPointer ByteFunctionHelpers]
           [org.apache.arrow.vector BigIntVector BitVector FieldVector Float8Vector NullVector TimeStampMilliVector VarBinaryVector VarCharVector]
           [org.apache.arrow.vector.holders NullableBigIntHolder NullableBitHolder NullableFloat8Holder NullableTimeStampMilliHolder ValueHolder]
           [org.apache.arrow.vector.types Types Types$MinorType UnionMode]
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
(definterface PrimitiveComparator
  (^int compare [^org.apache.arrow.vector.holders.ValueHolder left
                 ^org.apache.arrow.vector.holders.ValueHolder right]))

(defmacro def-prim-comp {:style/indent [2 :form :form [1]]} [sym holder-class & specs]
  `(def ~sym
     (reify PrimitiveComparator
       ~(let [[_compare [left right] & body] (->> specs
                                                  (filter (comp #{'compare} first))
                                                  first)]
          `(~'compare [this# ~left ~right]
            (let [~(with-meta left {:tag holder-class}) ~left
                  ~(with-meta right {:tag holder-class}) ~right]
              ~@body))))))

(def-prim-comp bigint-comp NullableBigIntHolder
  (compare [left right] (Long/compare (.value left) (.value right))))

(def-prim-comp bit-comp NullableBitHolder
  (compare [left right] (Integer/compare (.value left) (.value right))))

(def-prim-comp timestamp-milli-comp NullableTimeStampMilliHolder
  (compare [left right] (Long/compare (.value left) (.value right))))

(def-prim-comp float8-comp NullableFloat8Holder
  (compare [left right] (Double/compare (.value left) (.value right))))

(definterface PrimitiveReadWrite
  (^org.apache.arrow.vector.holders.ValueHolder newHolder [])

  (^boolean isSet [^org.apache.arrow.vector.holders.ValueHolder holder])

  (^void read [^org.apache.arrow.vector.FieldVector field-vec
               ^int idx
               ^org.apache.arrow.vector.holders.ValueHolder holder])

  (^void write [^org.apache.arrow.vector.holders.ValueHolder holder
                ^org.apache.arrow.vector.FieldVector out-vec
                ^int idx]))

(defmacro def-prim-rw [sym vec-class holder-class]
  (let [vec-sym (gensym 'vec)
        holder-sym (gensym 'holder)]
    `(def ~sym
       (reify PrimitiveReadWrite
         (~'newHolder [this#] (new ~holder-class))

         (~'isSet [this# ~holder-sym]
          (let [~(with-meta holder-sym {:tag holder-class}) ~holder-sym]
            (pos? (.isSet ~holder-sym))))

         (~'read [this# ~vec-sym idx# ~holder-sym]
          (let [~(with-meta vec-sym {:tag vec-class}) ~vec-sym
                ~(with-meta holder-sym {:tag holder-class}) ~holder-sym]
            (.get ~vec-sym idx# ~holder-sym)))

         (~'write [this# ~holder-sym ~vec-sym idx#]
          (let [~(with-meta vec-sym {:tag vec-class}) ~vec-sym
                ~(with-meta holder-sym {:tag holder-class}) ~holder-sym]
            (.setSafe ~vec-sym idx# ~holder-sym)))))))

(def-prim-rw bigint-rw BigIntVector NullableBigIntHolder)
(def-prim-rw bit-rw BitVector NullableBitHolder)
(def-prim-rw timestamp-milli-rw TimeStampMilliVector NullableTimeStampMilliHolder)
(def-prim-rw float8-rw Float8Vector NullableFloat8Holder)

(definterface ObjectComparator
  (^int compare [left right]))

(defmacro def-obj-comp {:style/indent [2 :form :form [1]]} [sym val-tag & specs]
  `(def ~sym
     (reify ObjectComparator
       ~(let [[_compare [left right] & body] (->> specs
                                                  (filter (comp #{'compare} first))
                                                  first)]
          `(~'compare [this# ~left ~right]
            (let [~(with-meta left {:tag val-tag}) ~left
                  ~(with-meta right {:tag val-tag}) ~right]
              ~@body))))))

(def-obj-comp null-comp Object
  (compare [left right] 0))

(def-obj-comp varbinary-comp ArrowBufPointer
  (compare [left right]
    (ByteFunctionHelpers/compare (.getBuf left)
                                 (.getOffset left)
                                 (+ (.getOffset left) (.getLength left))
                                 (.getBuf right)
                                 (.getOffset right)
                                 (+ (.getOffset right) (.getLength right)))))

(def-obj-comp varchar-comp String
  (compare [left right] (.compareTo left right)))

(definterface ObjectReadWrite
  (read [^org.apache.arrow.vector.FieldVector field-vec, ^int idx])
  (^void write [value ^org.apache.arrow.vector.FieldVector out-vec ^int idx]))

(defmacro def-obj-rw {:style/indent [3 :form :form :form [1]]} [sym vec-class val-tag & specs]
  `(def ~sym
     (reify ObjectReadWrite
       ~(let [[_read [vec-sym idx-sym] & body] (->> specs
                                                    (filter (comp #{'read} first))
                                                    first)]
          `(~'read [this# ~vec-sym ~idx-sym]
            (let [~(with-meta vec-sym {:tag vec-class}) ~vec-sym]
              ~@body)))

       ~(let [[_write [value-sym vec-sym idx-sym] & body] (->> specs
                                                               (filter (comp #{'write} first))
                                                               first)]
          `(~'write [this# ~value-sym ~vec-sym ~idx-sym]
            (let [~(with-meta value-sym {:tag val-tag}) ~value-sym
                  ~(with-meta vec-sym {:tag vec-class}) ~vec-sym]
              ~@body))))))

(def-obj-rw null-rw NullVector Object
  (read [_fv _idx])
  (write [_value fv idx]))

(def-obj-rw varbinary-rw VarBinaryVector ArrowBufPointer
  (read [fv idx]
    (.getDataPointer fv idx))

  (write [v fv idx]
    (.setSafe fv idx (.getOffset v) (.getLength v) (.getBuf v))))

(def-obj-rw varchar-rw VarCharVector String
  (read [fv idx]
    (str (.getObject fv idx)))
  (write [v fv idx]
    (.setSafe fv idx (Text. v))))
