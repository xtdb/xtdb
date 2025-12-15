(ns xtdb.types
  (:require [clojure.set :as set]
            [clojure.string :as str]
            [xtdb.api]
            [xtdb.rewrite :refer [zmatch]]
            [xtdb.serde.types :as st]
            [xtdb.util :as util])
  (:import (clojure.lang MapEntry)
           (org.apache.arrow.vector.types DateUnit FloatingPointPrecision IntervalUnit TimeUnit Types$MinorType)
           (org.apache.arrow.vector.types.pojo ArrowType ArrowType$Binary ArrowType$Bool ArrowType$Date ArrowType$Decimal ArrowType$Duration ArrowType$FixedSizeBinary ArrowType$FixedSizeList ArrowType$FloatingPoint ArrowType$Int ArrowType$Interval ArrowType$List ArrowType$Map ArrowType$Null ArrowType$Struct ArrowType$Time ArrowType$Time ArrowType$Timestamp ArrowType$Union ArrowType$Utf8 Field FieldType)
           (xtdb.arrow ArrowTypes MergeTypes VectorType)
           (xtdb.vector.extensions IntervalMDMType KeywordType RegClassType RegProcType SetType TransitType TsTzRangeType UriType UuidType)))

(set! *unchecked-math* :warn-on-boxed)

(defn col-type-head [col-type]
  (if (vector? col-type)
    (first col-type)
    col-type))

;;;; fields

(defn arrow-type->leg ^String [^ArrowType arrow-type]
  (ArrowTypes/toLeg arrow-type))

(defn- date-unit->kw [unit]
  (util/case-enum unit
    DateUnit/DAY :day
    DateUnit/MILLISECOND :milli))

(defn- time-unit->kw [unit]
  (util/case-enum unit
    TimeUnit/SECOND :second
    TimeUnit/MILLISECOND :milli
    TimeUnit/MICROSECOND :micro
    TimeUnit/NANOSECOND :nano))

(defn- interval-unit->kw [unit]
  (util/case-enum unit
    IntervalUnit/DAY_TIME :day-time
    IntervalUnit/MONTH_DAY_NANO :month-day-nano
    IntervalUnit/YEAR_MONTH :year-month))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]} ; xt.arrow/field-type reader macro
(defn ->field-type ^org.apache.arrow.vector.types.pojo.FieldType [[arrow-type nullable? dictionary metadata]]
  (FieldType. nullable? arrow-type dictionary metadata))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]} ; xt.arrow/field reader macro
(defn ->field* ^org.apache.arrow.vector.types.pojo.Field [[field-name field-type & children]]
  (Field. field-name field-type children))

(def temporal-col-type [:timestamp-tz :micro "UTC"])
(def temporal-type (st/->type :instant))
(def nullable-temporal-col-type [:union #{:null temporal-col-type}])
(def nullable-temporal-type (st/->type [:? :instant]))
(def temporal-arrow-type (st/->arrow-type :instant))

(defn ->type ^xtdb.arrow.VectorType [type-spec]
  (st/->type type-spec))

(defn ->field
  (^org.apache.arrow.vector.types.pojo.Field [type-spec]
   (VectorType/field (->type type-spec)))

  (^org.apache.arrow.vector.types.pojo.Field [type-spec, field-name]
   (VectorType/field (str field-name) (->type type-spec))))

(defn field-with-name ^org.apache.arrow.vector.types.pojo.Field [^Field field, name]
  (Field. name (.getFieldType field) (.getChildren field)))

(defn ->nullable-field ^org.apache.arrow.vector.types.pojo.Field [^Field field]
  (if (.isNullable field)
    field
    (Field. (.getName field) (FieldType. true (.getType field) nil nil) (.getChildren field))))

(def temporal-fields
  {"_iid" (->field :iid "_iid"),
   "_system_from" (->field :instant "_system_from"), "_system_to" (->field [:? :instant] "_system_to")
   "_valid_from" (->field :instant "_valid_from"), "_valid_to" (->field [:? :instant] "_valid_to")})

(defn temporal-col-name? [col-name]
  (contains? temporal-fields (str col-name)))

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

      (derive :timestamp-tz :date-time) (derive :timestamp-local :date-time) (derive :date :date-time)
      (derive :time-local :any) (derive :interval :any) (derive :duration :any)
      (derive :fixed-size-binary :varbinary) (derive :varbinary :any) (derive :utf8 :any)

      (derive :tstz-range :any)

      (derive :keyword :any) (derive :uri :any) (derive :uuid :any) (derive :transit :any)

      (derive :regclass :regoid) (derive :regproc :regoid) (derive :regoid :any)

      (derive :list :any) (derive :struct :any) (derive :set :any) (derive :map :any)))

(defn flatten-union-types [col-type]
  (if (= :union (col-type-head col-type))
    (second col-type)
    #{col-type}))

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

(def ^Field null-field (st/->field {"null" :null}))

(defn merge-types ^xtdb.arrow.VectorType [& types]
  (MergeTypes/mergeTypes (vec types)))

(defn merge-fields [& fields]
  (let [merged-type (MergeTypes/mergeFields (vec fields))]
    (VectorType/field (or (some (fn [^Field f] (some-> f .getName)) fields)
                          (ArrowTypes/toLeg (.getArrowType merged-type)))
                      merged-type)))

(defn value->vec-type ^xtdb.arrow.VectorType [v]
  (VectorType/fromValue v))

(defn vec-type->field
  (^org.apache.arrow.vector.types.pojo.Field [^VectorType vec-type field-name] (->field vec-type (str field-name)))
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

(defmulti ^String col-type->field-name col-type-head, :default ::default, :hierarchy #'col-type-hierarchy)
(defmethod col-type->field-name ::default [col-type] (name (col-type-head col-type)))
(defmethod col-type->field-name :varbinary [_col-type] "binary")

#_{:clj-kondo/ignore [:unused-binding]}
(defmulti col-type->vec-type
  (fn [nullable? col-type]
    (col-type-head col-type))
  :default ::default, :hierarchy #'col-type-hierarchy)

(alter-meta! #'col-type->vec-type assoc :tag Field)

(defmethod col-type->vec-type ::default [nullable? col-type]
  (VectorType. (st/->arrow-type col-type) (or nullable? (= col-type :null)) []))

(defmethod col-type->field-name :decimal [[type-head precision scale bit-width]]
  (str (name type-head) "-" precision "-" scale "-" bit-width))

(defmethod col-type->vec-type :tstz-range [nullable? _col-type]
  (VectorType. TsTzRangeType/INSTANCE nullable?
               [(->field temporal-type "$data$")]))

(defn col-type->field
  (^org.apache.arrow.vector.types.pojo.Field [col-type]
   (col-type->field (col-type->field-name col-type) col-type))

  (^org.apache.arrow.vector.types.pojo.Field [col-name col-type]
   (VectorType/field (str col-name) (col-type->vec-type false col-type))))

(defn col-type->leg [col-type]
  (let [head (col-type-head col-type)]
    (case head
      (:struct :list :set :map) (str (symbol head))
      :union (let [without-null (-> (flatten-union-types col-type)
                                    (disj :null))]
               (if (= 1 (count without-null))
                 (recur (first without-null))
                 (name :union)))
      (col-type->field-name col-type))))

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

;;; fixed size binary

(defmethod arrow-type->col-type ArrowType$FixedSizeBinary [^ArrowType$FixedSizeBinary fsb-type]
  [:fixed-size-binary (.getByteWidth fsb-type)])

;;; list

(defmethod col-type->vec-type :list [nullable? [_ inner-col-type]]
  (VectorType. ArrowType$List/INSTANCE nullable?
               [(col-type->field "$data$" inner-col-type)]))

(defmethod arrow-type->col-type ArrowType$List [_ & [data-field]]
  [:list (or (some-> data-field field->col-type) :null)])

(defmethod col-type->vec-type :fixed-size-list [nullable? [_ list-size inner-col-type]]
  (VectorType. (ArrowType$FixedSizeList. list-size) nullable?
               [(col-type->field inner-col-type)]))

(defmethod arrow-type->col-type ArrowType$FixedSizeList [^ArrowType$FixedSizeList list-type & [data-field]]
  [:fixed-size-list (.getListSize list-type) (or (some-> data-field field->col-type) :null)])

(defmethod col-type->vec-type :set [nullable? [_ inner-col-type]]
  (VectorType. SetType/INSTANCE nullable?
               [(col-type->field "$data$" inner-col-type)]))

(defmethod arrow-type->col-type SetType [_ & [data-field]]
  [:set (or (some-> data-field field->col-type) :null)])

(defmethod col-type->vec-type :map [nullable? [_ {:keys [sorted?]} inner-col-type]]
  (VectorType. (ArrowType$Map. (boolean sorted?)) nullable?
               [(col-type->field inner-col-type)]))

(defmethod arrow-type->col-type ArrowType$Map [^ArrowType$Map arrow-field & [data-field]]
  [:map {:sorted? (.getKeysSorted arrow-field)} (field->col-type data-field)])

(defn unnest-field [^Field field]
  (condp = (class (.getType field))
    ArrowType$List (first (.getChildren field))
    SetType (first (.getChildren field))
    ArrowType$FixedSizeList (first (.getChildren field))
    (col-type->field :null)))

;;; struct

(defmethod col-type->vec-type :struct [nullable? [_ inner-col-types]]
  (VectorType. ArrowType$Struct/INSTANCE nullable?
               (for [[col-name col-type] inner-col-types]
                 (col-type->field col-name col-type))))

(defmethod arrow-type->col-type ArrowType$Struct [_ & child-fields]
  [:struct (->> (for [^Field child-field child-fields]
                  [(symbol (.getName child-field)) (field->col-type child-field)])
                (into {}))])

;;; union

(defmethod col-type->vec-type :union [nullable? col-type]
  (let [col-types (cond-> (flatten-union-types col-type)
                    nullable? (conj :null))
        nullable? (contains? col-types :null)
        without-null (disj col-types :null)]
    (case (count without-null)
      0 (col-type->vec-type true :null)
      1 (col-type->vec-type nullable? (first without-null))

      (VectorType. (.getType Types$MinorType/DENSEUNION) false
                   (map col-type->field col-types)))))

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

(defmethod col-type->field-name :timestamp-tz [[type-head time-unit tz]]
  (str (name type-head) "-" (name time-unit) "-" (-> (str/lower-case tz) (str/replace #"[/:]" "_"))))

(defmethod col-type->field-name :timestamp-local [[type-head time-unit]]
  (str (name type-head) "-" (name time-unit)))

(defmethod arrow-type->col-type ArrowType$Timestamp [^ArrowType$Timestamp arrow-type]
  (let [time-unit (time-unit->kw (.getUnit arrow-type))]
    (if-let [tz (.getTimezone arrow-type)]
      [:timestamp-tz time-unit tz]
      [:timestamp-local time-unit])))

;;; date

(defmethod col-type->field-name :date [[type-head date-unit]]
  (str (name type-head) "-" (name date-unit)))

(defmethod arrow-type->col-type ArrowType$Date [^ArrowType$Date arrow-type]
  [:date (date-unit->kw (.getUnit arrow-type))])

;;; time

(defmethod col-type->field-name :time-local [[type-head time-unit]]
  (str (name type-head) "-" (name time-unit)))

(defmethod arrow-type->col-type ArrowType$Time [^ArrowType$Time arrow-type]
  [:time-local (time-unit->kw (.getUnit arrow-type))])

;;; duration

(defmethod col-type->field-name :duration [[type-head time-unit]]
  (str (name type-head) "-" (name time-unit)))

(defmethod arrow-type->col-type ArrowType$Duration [^ArrowType$Duration arrow-type]
  [:duration (time-unit->kw (.getUnit arrow-type))])

;;; interval

(defmethod col-type->field-name :interval [[type-head interval-unit]]
  (str (name type-head) "-" (name interval-unit)))

(defmethod arrow-type->col-type ArrowType$Interval [^ArrowType$Interval arrow-type]
  [:interval (interval-unit->kw (.getUnit arrow-type))])

;;; extension types

(defmethod arrow-type->col-type TsTzRangeType [_ _] :tstz-range)
(defmethod arrow-type->col-type KeywordType [_] :keyword)
(defmethod arrow-type->col-type RegClassType [_] :regclass)
(defmethod arrow-type->col-type RegProcType [_] :regproc)
(defmethod arrow-type->col-type UriType [_] :uri)
(defmethod arrow-type->col-type UuidType [_] :uuid)
(defmethod arrow-type->col-type TransitType [_] :transit)
(defmethod arrow-type->col-type IntervalMDMType [_] [:interval :month-day-micro])

;;; LUB

(defmulti least-upper-bound2
  (fn [x-type y-type] [(col-type-head x-type) (col-type-head y-type)])
  :hierarchy #'col-type-hierarchy)

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

(defmethod least-upper-bound2 [:num :num] [x-type y-type]
  (let [x-head-type (col-type-head x-type)
        y-head-type (col-type-head y-type)]
    (cond
      (isa? widening-hierarchy x-head-type y-head-type) y-type
      (isa? widening-hierarchy y-head-type x-head-type) x-type
      :else :any)))

(defmethod least-upper-bound2 [:duration :duration] [[_ x-unit] [_ y-unit]]
  [:duration (smallest-ts-unit x-unit y-unit)])

(defmethod least-upper-bound2 [:date :date] [_ _] [:date :day])

(defmethod least-upper-bound2 [:timestamp-local :timestamp-local] [[_ x-unit] [_ y-unit]]
  [:timestamp-local (smallest-ts-unit x-unit y-unit)])

(defmethod least-upper-bound2 [:time-local :time-local] [[_ x-unit] [_ y-unit]]
  [:time-local (smallest-ts-unit x-unit y-unit)])

(defmethod least-upper-bound2 [:timestamp-tz :timestamp-tz] [[_ x-unit x-tz] [_ y-unit y-tz]]
  [:timestamp-tz (smallest-ts-unit x-unit y-unit) (if (= x-tz y-tz) x-tz "Z")])

(defmethod least-upper-bound2 :default [_ _] :any)

(defn least-upper-bound [col-types]
  (reduce (fn
            ([] :null)
            ([lub col-type]
             (if (= lub col-type)
               lub
               (least-upper-bound2 lub col-type))))
          col-types))

(defn with-nullable-fields [fields]
  (->> fields
       (into {} (map (juxt key (fn [[k v]]
                                 (field-with-name (merge-fields v null-field) (str k))))))))

(defn remove-nulls
  [typ]
  (zmatch typ
    [:union inner-types] (let [res (->> (disj inner-types :null)
                                        (into #{} (map remove-nulls)))]
                           (if (<= (count res) 1)
                             (first res)
                             [:union res]))
    [:list inner-type] [:list (remove-nulls inner-type)]
    [:set inner-type] [:set (remove-nulls inner-type)]
    [:struct field-map] [:struct (zipmap (keys field-map)
                                         (map remove-nulls (vals field-map)))]
    typ))
