(ns xtdb.types
  (:require [clojure.set :as set]
            [clojure.string :as str]
            [xtdb.api]
            [xtdb.rewrite :refer [zmatch]]
            [xtdb.serde.types :as st]
            [xtdb.util :as util])
  (:import (clojure.lang MapEntry)
           (org.apache.arrow.vector.types FloatingPointPrecision)
           (org.apache.arrow.vector.types.pojo ArrowType ArrowType$Binary ArrowType$Bool ArrowType$Date ArrowType$Decimal ArrowType$Duration ArrowType$FixedSizeBinary ArrowType$FixedSizeList ArrowType$FloatingPoint ArrowType$Int ArrowType$Interval ArrowType$List ArrowType$Map ArrowType$Null ArrowType$Struct ArrowType$Time ArrowType$Time ArrowType$Timestamp ArrowType$Union ArrowType$Utf8 Field FieldType)
           (xtdb.arrow ArrowTypes MergeTypes ValueTypes VectorType)
           xtdb.types.LeastUpperBound
           (xtdb.vector.extensions IntervalMDMType KeywordType OidType RegClassType RegProcType SetType TransitType TsTzRangeType UriType UuidType)))

(set! *unchecked-math* :warn-on-boxed)

(defn col-type-head [col-type]
  (if (vector? col-type)
    (first col-type)
    col-type))

;;;; fields

(defn arrow-type->leg ^String [^ArrowType arrow-type]
  (ArrowTypes/toLeg arrow-type))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]} ; xt.arrow/field-type reader macro
(defn ->field-type ^org.apache.arrow.vector.types.pojo.FieldType [[arrow-type nullable? dictionary metadata]]
  (FieldType. nullable? arrow-type dictionary metadata))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]} ; xt.arrow/field reader macro
(defn ->field* ^org.apache.arrow.vector.types.pojo.Field [[field-name field-type & children]]
  (Field. field-name field-type children))

(def temporal-col-type [:timestamp-tz :micro "UTC"])
(def nullable-temporal-col-type [:union #{:null temporal-col-type}])
(def temporal-arrow-type (st/->arrow-type :instant))

(defn ->type ^xtdb.arrow.VectorType [type-spec]
  (st/->type type-spec))

(defn ->nullable-type ^xtdb.arrow.VectorType [type-spec]
  (VectorType/maybe (->type type-spec)))

(defn ->field
  (^org.apache.arrow.vector.types.pojo.Field [type-spec] (st/->field type-spec))

  (^org.apache.arrow.vector.types.pojo.Field [field-name type-spec] (st/->field* field-name type-spec)))

(defn field-with-name ^org.apache.arrow.vector.types.pojo.Field [^Field field, name]
  (Field. name (.getFieldType field) (.getChildren field)))

(def temporal-vec-types
  {"_iid" (st/->type :iid)
   "_system_from" (st/->type :instant), "_system_to" (st/->type [:? :instant])
   "_valid_from" (st/->type :instant), "_valid_to" (st/->type [:? :instant])})

(defn temporal-col-name? [col-name]
  (contains? temporal-vec-types (str col-name)))

;;;; col-types

(defn union? [col-type]
  (= :union (col-type-head col-type)))

(def col-type-hierarchy
  (-> (make-hierarchy)
      (derive :null :any)
      (derive :bool :any)

      (derive :f32 :float) (derive :f64 :float)
      (derive :i8 :int) (derive :i16 :int) (derive :i32 :int) (derive :i64 :int)
      (derive :u8 :uint) (derive :u16 :uint) (derive :u32 :uint) (derive :u64 :uint)

      (derive :uint :num) (derive :int :num) (derive :float :num) (derive :decimal :num)
      (derive :num :any)

      (derive :date-time :any)

      (derive :instant :timestamp-tz)
      (derive :timestamp-tz :date-time) (derive :timestamp-local :date-time) (derive :date :date-time)
      (derive :time-local :any) (derive :interval :any) (derive :duration :any)
      (derive :fixed-size-binary :varbinary) (derive :varbinary :any) (derive :utf8 :any)

      (derive :tstz-range :any)

      (derive :keyword :any) (derive :uri :any) (derive :uuid :any) (derive :transit :any)

      (derive :oid :any) (derive :regclass :oid) (derive :regproc :oid)

      (derive :list :any) (derive :struct :any) (derive :set :any) (derive :map :any)))

(defn flatten-union-field [^Field field]
  (if (instance? ArrowType$Union (.getType field))
    (.getChildren field)
    [field]))

(defn merge-col-types [& col-types]
  (letfn [(merge-col-type* [acc col-type]
            (zmatch col-type
              [:union inner-types] (reduce merge-col-type* acc inner-types)
              [:list inner-type] (update acc :list merge-col-type* inner-type)
              [:set inner-type] (update acc :set merge-col-type* inner-type)
              [:fixed-size-list el-count inner-type] (update acc [:fixed-size-list el-count] merge-col-type* inner-type)
              [:struct struct-col-types] (update acc :struct
                                                 (fn [acc]
                                                   (let [default-col-type (if acc {:null nil} nil)]
                                                     (as-> acc acc
                                                           (reduce-kv (fn [acc col-name col-type]
                                                                        (update acc col-name (fnil merge-col-type* default-col-type) col-type))
                                                                      acc
                                                                      struct-col-types)
                                                           (reduce (fn [acc null-k]
                                                                     (update acc null-k merge-col-type* :null))
                                                                   acc
                                                                   (set/difference (set (keys acc))
                                                                                   (set (keys struct-col-types))))))))
              (assoc acc col-type nil)))

          (kv->col-type [[head opts]]
            (case (if (vector? head) (first head) head)
              :list [:list (map->col-type opts)]
              :set [:set (map->col-type opts)]
              :fixed-size-list [:fixed-size-list (second head) (map->col-type opts)]
              :struct [:struct (->> (for [[col-name col-type-map] opts]
                                      (MapEntry/create col-name (map->col-type col-type-map)))
                                    (into {}))]
              head))

          (map->col-type [col-type-map]
            (case (count col-type-map)
              0 :null
              1 (kv->col-type (first col-type-map))
              [:union (into #{} (map kv->col-type) col-type-map)]))]

    (-> (transduce (comp (remove nil?) (distinct)) (completing merge-col-type*) {} col-types)
        (map->col-type))))

(defn merge-types ^xtdb.arrow.VectorType [& types]
  (MergeTypes/mergeTypes (vec types)))

(defn value->vec-type ^xtdb.arrow.VectorType [v]
  (ValueTypes/fromValue v))

(defn vec-type->field
  (^org.apache.arrow.vector.types.pojo.Field [^VectorType vec-type field-name] (->field (str field-name) vec-type))
  (^org.apache.arrow.vector.types.pojo.Field [^VectorType vec-type] (->field vec-type)))

;;; time units

(defn ts-units-per-second ^long [time-unit]
  (case time-unit
    :second 1
    :milli #=(long 1e3)
    :micro #=(long 1e6)
    :nano #=(long 1e9)))

(defn smallest-ts-unit
  ([x-unit y-unit]
   (if (> (ts-units-per-second x-unit) (ts-units-per-second y-unit))
     x-unit
     y-unit))

  ([x-unit y-unit & more]
   (reduce smallest-ts-unit (smallest-ts-unit x-unit y-unit) more)))

;;; multis

#_{:clj-kondo/ignore [:unused-binding]}
(defmulti arrow-type->col-type
  (fn [arrow-type & child-fields] 
    (class arrow-type)))

(defmethod arrow-type->col-type ArrowType$Null [_] :null)
(defmethod arrow-type->col-type ArrowType$Bool [_] :bool)
(defmethod arrow-type->col-type ArrowType$Utf8 [_] :utf8)
(defmethod arrow-type->col-type ArrowType$Binary [_] :varbinary)
(defmethod arrow-type->col-type ArrowType$Decimal  [^ArrowType$Decimal arrow-type]
  [:decimal (.getPrecision arrow-type) (.getScale arrow-type) (.getBitWidth arrow-type)])

(defn col-type->nullable-col-type [col-type]
  (zmatch col-type
    [:union inner-types] [:union (conj inner-types :null)]
    :null :null
    [:union #{:null col-type}]))

(defn field->col-type [^Field field]
  (let [inner-type (apply arrow-type->col-type (.getType field) (or (.getChildren field) []))]
    (cond-> inner-type
      (.isNullable field) col-type->nullable-col-type)))

(defn vec-type->col-type [^VectorType vec-type]
  (field->col-type (VectorType/field vec-type)))

(defn nullable-vec-type? [^VectorType vec-type]
  (boolean (some VectorType/.getNullable (.getLegs vec-type))))

;;; fixed size binary

(defmethod arrow-type->col-type ArrowType$FixedSizeBinary [^ArrowType$FixedSizeBinary fsb-type]
  [:fixed-size-binary (.getByteWidth fsb-type)])

;;; list

(defmethod arrow-type->col-type ArrowType$List [_ & [data-field]]
  [:list (or (some-> data-field field->col-type) :null)])

(defmethod arrow-type->col-type ArrowType$FixedSizeList [^ArrowType$FixedSizeList list-type & [data-field]]
  [:fixed-size-list (.getListSize list-type) (or (some-> data-field field->col-type) :null)])

(defmethod arrow-type->col-type SetType [_ & [data-field]]
  [:set (or (some-> data-field field->col-type) :null)])

(defmethod arrow-type->col-type ArrowType$Map [^ArrowType$Map arrow-field & [data-field]]
  [:map {:sorted? (.getKeysSorted arrow-field)} (field->col-type data-field)])

(defn unnest-type ^VectorType [^VectorType vec-type]
  (condp instance? (.getArrowType vec-type)
    ArrowType$List (.getFirstChildOrNull (.asMono vec-type))
    SetType (.getFirstChildOrNull (.asMono vec-type))
    ArrowType$FixedSizeList (.getFirstChildOrNull (.asMono vec-type))
    #xt/type :null))

;;; struct

(defmethod arrow-type->col-type ArrowType$Struct [_ & child-fields]
  [:struct (->> (for [^Field child-field child-fields]
                  [(symbol (.getName child-field)) (field->col-type child-field)])
                (into {}))])

;;; union

(defmethod arrow-type->col-type ArrowType$Union [_ & child-fields]
  (->> child-fields
       (into #{} (map field->col-type))
       (apply merge-col-types)))

;;; number

(defmethod arrow-type->col-type ArrowType$Int [^ArrowType$Int arrow-type]
  (if (.getIsSigned arrow-type)
    (case (.getBitWidth arrow-type)
      8 :i8, 16 :i16, 32 :i32, 64 :i64)
    (case (.getBitWidth arrow-type)
      8 :u8, 16 :u16, 32 :u32, 64 :u64)))

(defmethod arrow-type->col-type ArrowType$FloatingPoint [^ArrowType$FloatingPoint arrow-type]
  (util/case-enum (.getPrecision arrow-type)
    FloatingPointPrecision/HALF :f16
    FloatingPointPrecision/SINGLE :f32
    FloatingPointPrecision/DOUBLE :f64))

;;; timestamp

(defmethod arrow-type->col-type ArrowType$Timestamp [^ArrowType$Timestamp arrow-type]
  (let [time-unit (st/time-unit->kw (.getUnit arrow-type))]
    (if-let [tz (.getTimezone arrow-type)]
      [:timestamp-tz time-unit tz]
      [:timestamp-local time-unit])))

;;; date

(defmethod arrow-type->col-type ArrowType$Date [^ArrowType$Date arrow-type]
  [:date (st/date-unit->kw (.getUnit arrow-type))])

;;; time

(defmethod arrow-type->col-type ArrowType$Time [^ArrowType$Time arrow-type]
  [:time-local (st/time-unit->kw (.getUnit arrow-type))])

;;; duration

(defmethod arrow-type->col-type ArrowType$Duration [^ArrowType$Duration arrow-type]
  [:duration (st/time-unit->kw (.getUnit arrow-type))])

;;; interval

(defmethod arrow-type->col-type ArrowType$Interval [^ArrowType$Interval arrow-type]
  [:interval (st/interval-unit->kw (.getUnit arrow-type))])

;;; extension types

(defmethod arrow-type->col-type TsTzRangeType [_ _] :tstz-range)
(defmethod arrow-type->col-type KeywordType [_] :keyword)
(defmethod arrow-type->col-type OidType [_] :oid)
(defmethod arrow-type->col-type RegClassType [_] :regclass)
(defmethod arrow-type->col-type RegProcType [_] :regproc)
(defmethod arrow-type->col-type UriType [_] :uri)
(defmethod arrow-type->col-type UuidType [_] :uuid)
(defmethod arrow-type->col-type TransitType [_] :transit)
(defmethod arrow-type->col-type IntervalMDMType [_] [:interval :month-day-micro])

;; HACK
;; the decimal widening to other types currently only works without
;; casting elsewhere because we are using Clojure's polymorphic functions
;; or the Math/* functions cast things to the correct type.
(def widening-hierarchy
  (-> col-type-hierarchy
      (derive :i8 :i16) (derive :i16 :i32) (derive :i32 :i64)
      (derive :decimal :float) (derive :decimal :f64) (derive :decimal :f32)
      (derive :f32 :f64)
      (derive :int :f64) (derive :int :f32)
      (derive :int :decimal) (derive :uint :decimal)))

(defn least-upper-bound [vec-types]
  (LeastUpperBound/of vec-types))

(defn with-nullable-types [vec-types]
  (update-vals vec-types VectorType/maybe))
