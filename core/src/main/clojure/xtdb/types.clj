(ns xtdb.types
  (:require [clojure.set :as set]
            [clojure.string :as str]
            xtdb.api
            [xtdb.rewrite :refer [zmatch]]
            [xtdb.util :as util])
  (:import (clojure.lang MapEntry)
           (java.nio ByteBuffer)
           java.nio.charset.StandardCharsets
           (java.time Duration Instant LocalDate LocalTime Period ZoneId)
           java.util.concurrent.ConcurrentHashMap
           java.util.function.Function
           (org.apache.arrow.vector BitVector DateDayVector DateMilliVector DecimalVector Decimal256Vector IntervalDayVector IntervalMonthDayNanoVector IntervalYearVector TimeMicroVector TimeMilliVector TimeNanoVector TimeSecVector TimeStampMicroTZVector TimeStampMilliTZVector TimeStampNanoTZVector TimeStampSecTZVector ValueVector VarBinaryVector VarCharVector)
           (org.apache.arrow.vector.complex DenseUnionVector FixedSizeListVector ListVector StructVector)
           (org.apache.arrow.vector.holders NullableIntervalDayHolder NullableIntervalMonthDayNanoHolder)
           (org.apache.arrow.vector.types DateUnit FloatingPointPrecision IntervalUnit TimeUnit Types$MinorType UnionMode)
           (org.apache.arrow.vector.types.pojo ArrowType ArrowType$Binary ArrowType$Bool ArrowType$Date ArrowType$Decimal ArrowType$Duration ArrowType$FixedSizeBinary ArrowType$FixedSizeList ArrowType$FloatingPoint ArrowType$Int ArrowType$Interval ArrowType$List ArrowType$Null ArrowType$Struct ArrowType$Time ArrowType$Time ArrowType$Timestamp ArrowType$Union ArrowType$Utf8 Field FieldType)
           (xtdb.types IntervalDayTime IntervalMonthDayNano IntervalYearMonth)
           (xtdb.vector.extensions AbsentType AbsentVector ClojureFormType KeywordType SetType SetVector UriType UuidType)))



(set! *unchecked-math* :warn-on-boxed)

(def struct-type (.getType Types$MinorType/STRUCT))
(def dense-union-type (ArrowType$Union. UnionMode/Dense nil))
(def list-type (.getType Types$MinorType/LIST))

(def temporal-col-type [:timestamp-tz :micro "UTC"])

(defprotocol ArrowReadable
  (get-object [value-vector idx]))

(extend-protocol ArrowReadable
  ;; NOTE: Vectors not explicitly listed here have useful getObject methods and are handled by `ValueVector`.
  ValueVector (get-object [this idx] (.getObject this ^int idx))

  AbsentVector (get-object [_this _idx] :xtdb/absent)

  BitVector
  (get-object [this idx]
    ;; `BitVector/getObject` returns `new Boolean(...)` rather than `Boolean/valueOf`
    (when-not (.isNull this idx)
      (= (.get this ^int idx) 1)))

  VarBinaryVector
  (get-object [this idx]
    (when-not (.isNull this idx)
      (ByteBuffer/wrap (.getObject this ^int idx))))

  VarCharVector
  (get-object [this idx]
    (when-not (.isNull this idx)
      (String. (.get this ^int idx) StandardCharsets/UTF_8))))

(let [zones (ConcurrentHashMap.)]
  (defn- zone-id ^java.time.ZoneId [^ValueVector v]
    (.computeIfAbsent zones (.getTimezone ^ArrowType$Timestamp (.getType (.getField v)))
                      (reify Function
                        (apply [_ zone-str]
                          (ZoneId/of zone-str))))))

(extend-protocol ArrowReadable
  TimeStampSecTZVector
  (get-object [this idx]
    (when-not (.isNull this idx)
      (-> (Instant/ofEpochSecond (.get this idx))
          (.atZone (zone-id this)))))

  TimeStampMilliTZVector
  (get-object [this idx]
    (when-not (.isNull this idx)
      (-> (Instant/ofEpochMilli (.get this idx))
          (.atZone (zone-id this)))))

  TimeStampMicroTZVector
  (get-object [this idx]
    (when-not (.isNull this idx)
      (-> ^Instant (util/micros->instant (.get this ^int idx))
          (.atZone (zone-id this)))))

  TimeStampNanoTZVector
  (get-object [this idx]
    (-> (Instant/ofEpochSecond 0 (.get this idx))
        (.atZone (zone-id this)))))

(extend-protocol ArrowReadable
  DateDayVector
  (get-object [this idx]
    (when-not (.isNull this idx)
      (LocalDate/ofEpochDay (.get this idx))))

  DateMilliVector
  (get-object [this idx]
    (when-not (.isNull this idx)
      (LocalDate/ofEpochDay (quot (.get this idx) 86400000)))))

(extend-protocol ArrowReadable
  TimeNanoVector
  (get-object [this idx]
    (when-not (.isNull this idx)
      (LocalTime/ofNanoOfDay (.get this idx))))

  TimeMicroVector
  (get-object [this idx]
    (when-not (.isNull this idx)
      (LocalTime/ofNanoOfDay (* (.get this idx) 1e3))))

  TimeMilliVector
  (get-object [this idx]
    (when-not (.isNull this idx)
      (LocalTime/ofNanoOfDay (* (long (.get this idx)) 1e6))))

  TimeSecVector
  (get-object [this idx]
    (when-not (.isNull this idx)
      (LocalTime/ofSecondOfDay (.get this idx)))))

(extend-protocol ArrowReadable
  ;; we are going to override the get-object function
  ;; to unify the representation on read for non nanovectors
  IntervalYearVector
  (get-object [this idx]
    (when-not (.isNull this idx)
      (IntervalYearMonth. (Period/ofMonths (.get this idx)))))

  IntervalDayVector
  (get-object [this idx]
    (when-not (.isNull this idx)
      (let [holder (NullableIntervalDayHolder.)
            _ (.get this idx holder)
            period (Period/ofDays (.-days holder))
            duration (Duration/ofMillis (.-milliseconds holder))]
        (IntervalDayTime. period duration))))

  IntervalMonthDayNanoVector
  (get-object [this idx]
    (when-not (.isNull this idx)
      (let [holder (NullableIntervalMonthDayNanoHolder.)
            _ (.get this idx holder)
            period (Period/of 0 (.-months holder) (.-days holder))
            duration (Duration/ofNanos (.-nanoseconds holder))]
        (IntervalMonthDayNano. period duration)))))

(extend-protocol ArrowReadable
  ListVector
  (get-object [this idx]
    (when-not (.isNull this idx)
      (let [data-vec (.getDataVector this)
            x (loop [element-idx (.getElementStartIndex this idx)
                     acc (transient [])]
                (if (= (.getElementEndIndex this idx) element-idx)
                  acc
                  (recur (inc element-idx)
                         (conj! acc (get-object data-vec element-idx)))))]
        (persistent! x))))

  FixedSizeListVector
  (get-object [this idx]
    (let [data-vec (.getDataVector this)
          x (loop [element-idx (.getElementStartIndex this idx)
                   acc (transient [])]
              (if (= (.getElementEndIndex this idx) element-idx)
                acc
                (recur (inc element-idx)
                       (conj! acc (get-object data-vec element-idx)))))]
      (persistent! x)))

  SetVector
  (get-object [this idx]
    (when-not (.isNull this idx)
      (let [^ListVector uvec (.getUnderlyingVector this)
            data-vec (.getDataVector uvec)
            x (loop [element-idx (.getElementStartIndex uvec idx)
                     acc (transient #{})]
                (if (= (.getElementEndIndex uvec idx) element-idx)
                  acc
                  (recur (inc element-idx)
                         (conj! acc (get-object data-vec element-idx)))))]
        (persistent! x))))

  StructVector
  (get-object [this idx]
    (when-not (.isNull this idx)
      (-> (reduce (fn [acc k]
                    (let [child-vec (.getChild this k ValueVector)
                          v (get-object child-vec idx)]
                      (cond-> acc
                        (not= v :xtdb/absent) (assoc! (keyword k) v))))
                  (transient {})
                  (.getChildFieldNames this))
          (persistent!))))

  DenseUnionVector
  (get-object [this idx]
    (get-object (.getVectorByType this (.getTypeId this idx))
                (.getOffset this idx))))

(defn ->field ^org.apache.arrow.vector.types.pojo.Field [^String field-name ^ArrowType arrow-type nullable & children]
  (Field. field-name (FieldType. nullable arrow-type nil nil) children))

;;;; col-types

(defn col-type-head [col-type]
  (if (vector? col-type)
    (first col-type)
    col-type))

(defn union? [col-type]
  (= :union (col-type-head col-type)))

(def col-type-hierarchy
  (-> (make-hierarchy)
      (derive :null :any) (derive :absent :null)
      (derive :bool :any)

      (derive :f32 :float) (derive :f64 :float)
      (derive :i8 :int) (derive :i16 :int) (derive :i32 :int) (derive :i64 :int)
      (derive :u8 :uint) (derive :u16 :uint) (derive :u32 :uint) (derive :u64 :uint)

      (derive :uint :num) (derive :int :num) (derive :float :num) (derive :decimal :num)
      (derive :num :any)

      (derive :date-time :any)

      (derive :timestamp-tz :date-time) (derive :timestamp-local :date-time) (derive :date :date-time)
      (derive :time-local :any) (derive :interval :any) (derive :duration :any)
      (derive :varbinary :any) (derive :utf8 :any)

      (derive :keyword :any) (derive :uri :any) (derive :uuid :any) (derive :clj-form :any)

      (derive :list :any) (derive :struct :any)))

(def num-types (descendants col-type-hierarchy :num))
(def date-time-types (descendants col-type-hierarchy :date-time))

(defn flatten-union-types [col-type]
  (if (= :union (col-type-head col-type))
    (second col-type)
    #{col-type}))

(defn merge-col-types [& col-types]
  (letfn [(merge-col-type* [acc col-type]
            (zmatch col-type
              [:union inner-types] (reduce merge-col-type* acc inner-types)
              [:list inner-type] (update acc :list merge-col-type* inner-type)
              [:fixed-size-list el-count inner-type] (update acc [:fixed-size-list el-count] merge-col-type* inner-type)
              [:struct struct-col-types] (update acc :struct
                                                 (fn [acc]
                                                   (let [default-col-type (if acc {:absent nil} nil)]
                                                     (as-> acc acc
                                                       (reduce-kv (fn [acc col-name col-type]
                                                                    (update acc col-name (fnil merge-col-type* default-col-type) col-type))
                                                                  acc
                                                                  struct-col-types)
                                                       (reduce (fn [acc absent-k]
                                                                 (update acc absent-k merge-col-type* :absent))
                                                               acc
                                                               (set/difference (set (keys acc))
                                                                               (set (keys struct-col-types))))))))
              (assoc acc col-type nil)))

          (kv->col-type [[head opts]]
            (case (if (vector? head) (first head) head)
              :list [:list (map->col-type opts)]
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

;;; time units

(defn ts-units-per-second ^long [time-unit]
  (case time-unit
    :second 1
    :milli #=(long 1e3)
    :micro #=(long 1e6)
    :nano #=(long 1e9)))

(defn smallest-ts-unit [x-unit y-unit]
  (if (> (ts-units-per-second x-unit) (ts-units-per-second y-unit))
    x-unit
    y-unit))

;;; multis

;; HACK not ideal that end-users would need to extend all of these
;; to add their own extension types

(defmulti ^String col-type->field-name col-type-head, :default ::default, :hierarchy #'col-type-hierarchy)
(defmethod col-type->field-name ::default [col-type] (name (col-type-head col-type)))

#_{:clj-kondo/ignore [:unused-binding]}
(defmulti col-type->field*
  (fn [col-name nullable? col-type]
    (col-type-head col-type))
  :default ::default, :hierarchy #'col-type-hierarchy)

(alter-meta! #'col-type->field* assoc :tag Field)

(defmethod col-type->field* ::default [col-name nullable? col-type]
  (let [^Types$MinorType
        minor-type (case (col-type-head col-type)
                     :null Types$MinorType/NULL, :bool Types$MinorType/BIT
                     :f32 Types$MinorType/FLOAT4, :f64 Types$MinorType/FLOAT8
                     :i8 Types$MinorType/TINYINT, :i16 Types$MinorType/SMALLINT, :i32 Types$MinorType/INT, :i64 Types$MinorType/BIGINT
                     :utf8 Types$MinorType/VARCHAR, :varbinary Types$MinorType/VARBINARY
                     :decimal Types$MinorType/DECIMAL)]
    (->field col-name (.getType minor-type) (or nullable? (= col-type :null)))))

(defmethod col-type->field* :decimal [col-name nullable? _col-type]
  ;; TODO decide on how deal with precision that is out of this range but still fits into
  ;; a 128 bit decimal
  (->field col-name (ArrowType$Decimal/createDecimal 38 19 (int 128)) nullable?))

(defmethod col-type->field* :keyword [col-name nullable? _col-type]
  (->field col-name KeywordType/INSTANCE nullable?))

(defmethod col-type->field* :uuid [col-name nullable? _col-type]
  (->field col-name UuidType/INSTANCE nullable?))

(defmethod col-type->field* :uri [col-name nullable? _col-type]
  (->field col-name UriType/INSTANCE nullable?))

(defmethod col-type->field* :clj-form [col-name nullable? _col-type]
  (->field col-name ClojureFormType/INSTANCE nullable?))

(defmethod col-type->field* :absent [col-name nullable? _col-type]
  (->field col-name AbsentType/INSTANCE nullable?))

(defn col-type->field
  (^org.apache.arrow.vector.types.pojo.Field [col-type] (col-type->field (col-type->field-name col-type) col-type))
  (^org.apache.arrow.vector.types.pojo.Field [col-name col-type] (col-type->field* (str col-name) false col-type)))

(defn col-type->duv-leg-key [col-type]
  (let [head (col-type-head col-type)]
    (case head
      (:struct :list :set) head
      col-type)))

#_{:clj-kondo/ignore [:unused-binding]}
(defmulti arrow-type->col-type
  (fn [arrow-type & child-fields]
    (class arrow-type)))

(defmethod arrow-type->col-type ArrowType$Null [_] :null)
(defmethod arrow-type->col-type ArrowType$Bool [_] :bool)
(defmethod arrow-type->col-type ArrowType$Utf8 [_] :utf8)
(defmethod arrow-type->col-type ArrowType$Binary [_] :varbinary)
(defmethod arrow-type->col-type ArrowType$Decimal  [_] :decimal)

(defn- col-type->nullable-col-type [col-type]
  (zmatch col-type
    [:union inner-types] [:union (conj inner-types :null)]
    :null :null
    [:union #{:null col-type}]))

(defn field->col-type [^Field field]
  (let [inner-type (apply arrow-type->col-type (.getType field) (.getChildren field))]
    (cond-> inner-type
      (.isNullable field) col-type->nullable-col-type)))

;;; fixed size binary

(defmethod arrow-type->col-type ArrowType$FixedSizeBinary [^ArrowType$FixedSizeBinary fsb-type]
  [:fixed-size-binary (.getByteWidth fsb-type)])

(defmethod col-type->field* :fixed-size-binary [col-name nullable? [_ byte-width]]
  (->field col-name (ArrowType$FixedSizeBinary. byte-width) nullable?))

;;; list

(defmethod col-type->field* :list [col-name nullable? [_ inner-col-type]]
  (->field col-name ArrowType$List/INSTANCE nullable?
           (col-type->field "$data" inner-col-type)))

(defmethod arrow-type->col-type ArrowType$List [_ data-field]
  [:list (field->col-type data-field)])

(defmethod col-type->field* :fixed-size-list [col-name nullable? [_ list-size inner-col-type]]
  (->field col-name (ArrowType$FixedSizeList. list-size) nullable?
           (col-type->field "$data" inner-col-type)))

(defmethod arrow-type->col-type ArrowType$FixedSizeList [^ArrowType$FixedSizeList list-type, data-field]
  [:fixed-size-list (.getListSize list-type) (field->col-type data-field)])

(defmethod col-type->field* :set [col-name nullable? [_ inner-col-type]]
  (->field col-name SetType/INSTANCE nullable?
           (col-type->field "$data" inner-col-type)))

(defmethod arrow-type->col-type SetType [_ data-field]
  [:set (field->col-type data-field)])

;;; struct

(defmethod col-type->field* :struct [col-name nullable? [_ inner-col-types]]
  (apply ->field col-name ArrowType$Struct/INSTANCE nullable?
         (for [[col-name col-type] inner-col-types]
           (col-type->field col-name col-type))))

(defmethod arrow-type->col-type ArrowType$Struct [_ & child-fields]
  [:struct (->> (for [^Field child-field child-fields]
                  [(symbol (.getName child-field)) (field->col-type child-field)])
                (into {}))])

;;; union

(defmethod col-type->field* :union [col-name nullable? col-type]
  (let [col-types (cond-> (flatten-union-types col-type)
                    nullable? (conj :null))
        nullable? (contains? col-types :null)
        without-null (disj col-types :null)]
    (case (count without-null)
      0 (col-type->field* col-name true :null)
      1 (col-type->field* col-name nullable? (first without-null))

      (apply ->field col-name (.getType Types$MinorType/DENSEUNION) false
             (map-indexed (fn [idx col-type]
                            (col-type->field (str (col-type->field-name col-type) "-" idx) col-type))
                          col-types)))))

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

(defn- time-unit->kw [unit]
  (util/case-enum unit
    TimeUnit/SECOND :second
    TimeUnit/MILLISECOND :milli
    TimeUnit/MICROSECOND :micro
    TimeUnit/NANOSECOND :nano))

(defn- kw->time-unit [kw]
  (case kw
    :second TimeUnit/SECOND
    :milli TimeUnit/MILLISECOND
    :micro TimeUnit/MICROSECOND
    :nano TimeUnit/NANOSECOND))

(defmethod col-type->field-name :timestamp-tz [[type-head time-unit tz]]
  (str (name type-head) "-" (name time-unit) "-" (-> (str/lower-case tz) (str/replace #"[/:]" "_"))))

(defmethod col-type->field* :timestamp-tz [col-name nullable? [_type-head time-unit tz]]
  (assert (string? tz) )
  (->field col-name (ArrowType$Timestamp. (kw->time-unit time-unit) tz) nullable?))

(defmethod col-type->field-name :timestamp-local [[type-head time-unit]]
  (str (name type-head) "-" (name time-unit)))

(defmethod col-type->field* :timestamp-local [col-name nullable? [_type-head time-unit]]
  (->field col-name (ArrowType$Timestamp. (kw->time-unit time-unit) nil) nullable?))

(defmethod arrow-type->col-type ArrowType$Timestamp [^ArrowType$Timestamp arrow-type]
  (let [time-unit (time-unit->kw (.getUnit arrow-type))]
    (if-let [tz (.getTimezone arrow-type)]
      [:timestamp-tz time-unit tz]
      [:timestamp-local time-unit])))

;;; date

(defn- date-unit->kw [unit]
  (util/case-enum unit
    DateUnit/DAY :day
    DateUnit/MILLISECOND :milli))

(defn- kw->date-unit [kw]
  (case kw
    :day DateUnit/DAY
    :milli DateUnit/MILLISECOND))

(defmethod col-type->field-name :date [[type-head date-unit]]
  (str (name type-head) "-" (name date-unit)))

(defmethod col-type->field* :date [col-name nullable? [_type-head date-unit]]
  (->field col-name
           (ArrowType$Date. (kw->date-unit date-unit))
           nullable?))

(defmethod arrow-type->col-type ArrowType$Date [^ArrowType$Date arrow-type]
  [:date (date-unit->kw (.getUnit arrow-type))])

;;; time

(defmethod col-type->field-name :time-local [[type-head time-unit]]
  (str (name type-head) "-" (name time-unit)))

(defmethod col-type->field* :time-local [col-name nullable? [_type-head time-unit]]
  (->field col-name
           (ArrowType$Time. (kw->time-unit time-unit)
                            (case time-unit (:second :milli) 32, (:micro :nano) 64))
           nullable?))

(defmethod arrow-type->col-type ArrowType$Time [^ArrowType$Time arrow-type]
  [:time-local (time-unit->kw (.getUnit arrow-type))])

;;; duration

(defmethod col-type->field-name :duration [[type-head time-unit]]
  (str (name type-head) "-" (name time-unit)))

(defmethod col-type->field* :duration [col-name nullable? [_type-head time-unit]]
  (->field col-name (ArrowType$Duration. (kw->time-unit time-unit)) nullable?))

(defmethod arrow-type->col-type ArrowType$Duration [^ArrowType$Duration arrow-type]
  [:duration (time-unit->kw (.getUnit arrow-type))])

;;; interval

(defn- interval-unit->kw [unit]
  (util/case-enum unit
    IntervalUnit/DAY_TIME :day-time
    IntervalUnit/MONTH_DAY_NANO :month-day-nano
    IntervalUnit/YEAR_MONTH :year-month))

(defn- kw->interval-unit [kw]
  (case kw
    :day-time IntervalUnit/DAY_TIME
    :month-day-nano IntervalUnit/MONTH_DAY_NANO
    :year-month IntervalUnit/YEAR_MONTH))

(defmethod col-type->field-name :interval [[type-head interval-unit]]
  (str (name type-head) "-" (name interval-unit)))

(defmethod col-type->field* :interval [col-name nullable? [_type-head interval-unit]]
  (->field col-name (ArrowType$Interval. (kw->interval-unit interval-unit)) nullable?))

(defmethod arrow-type->col-type ArrowType$Interval [^ArrowType$Interval arrow-type]
  [:interval (interval-unit->kw (.getUnit arrow-type))])

;;; extension types

(defmethod arrow-type->col-type KeywordType [_] :keyword)
(defmethod arrow-type->col-type UriType [_] :uri)
(defmethod arrow-type->col-type UuidType [_] :uuid)
(defmethod arrow-type->col-type ClojureFormType [_] :clj-form)
(defmethod arrow-type->col-type AbsentType [_] :absent)

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
  (cond
    (isa? widening-hierarchy x-type y-type) y-type
    (isa? widening-hierarchy y-type x-type) x-type
    :else :any))

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

(defn with-nullable-cols [col-types]
  (->> col-types
       (into {} (map (juxt key (comp #(merge-col-types % :null) val))))))
