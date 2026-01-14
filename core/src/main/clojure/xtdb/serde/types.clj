(ns xtdb.serde.types
  (:require [clojure.pprint :as pp])
  (:import (java.io Writer)
           (org.apache.arrow.vector.types DateUnit FloatingPointPrecision IntervalUnit TimeUnit Types$MinorType UnionMode)
           (org.apache.arrow.vector.types.pojo ArrowType ArrowType$Binary ArrowType$Bool ArrowType$Date ArrowType$Decimal ArrowType$Duration ArrowType$FixedSizeBinary ArrowType$FixedSizeList ArrowType$FloatingPoint ArrowType$Int ArrowType$Interval ArrowType$List ArrowType$Map ArrowType$Null ArrowType$Struct ArrowType$Time ArrowType$Time ArrowType$Timestamp ArrowType$Union ArrowType$Utf8 Field FieldType Schema)
           (xtdb.arrow ArrowTypes VectorType VectorType$Listy VectorType$Maybe VectorType$Null VectorType$Poly VectorType$Scalar VectorType$Struct)
           (xtdb.vector.extensions IntervalMDMType KeywordType OidType RegClassType RegProcType SetType TransitType TsTzRangeType UriType UuidType)))

(defprotocol FromArrowType
  (<-arrow-type [arrow-type]))

(defmacro case-enum
  "Like `case`, but explicitly dispatch on Java enum ordinals.

  See: https://stackoverflow.com/questions/16777814/is-it-possible-to-use-clojures-case-form-with-a-java-enum"
  {:style/indent 1}
  [e & clauses]
  (letfn [(enum-ordinal [e] `(let [^Enum e# ~e] (.ordinal e#)))]
    `(case ~(enum-ordinal e)
       ~@(concat
           (mapcat (fn [[test result]]
                     [(eval (enum-ordinal test)) result])
                   (partition 2 clauses))
           (when (odd? (count clauses))
             (list (last clauses)))))))

(defn- date-unit->kw [unit]
  (case-enum unit
    DateUnit/DAY :day
    DateUnit/MILLISECOND :milli))

(defn- kw->date-unit [kw]
  (case kw
    :day DateUnit/DAY
    :milli DateUnit/MILLISECOND))

(defn- time-unit->kw [unit]
  (case-enum unit
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

(defn- interval-unit->kw [unit]
  (case-enum unit
    IntervalUnit/DAY_TIME :day-time
    IntervalUnit/MONTH_DAY_NANO :month-day-nano
    IntervalUnit/YEAR_MONTH :year-month))

(defn- kw->interval-unit [kw]
  (case kw
    :day-time IntervalUnit/DAY_TIME
    :month-day-nano IntervalUnit/MONTH_DAY_NANO
    :year-month IntervalUnit/YEAR_MONTH))

(extend-protocol FromArrowType
  ArrowType$Null (<-arrow-type [_] :null)
  ArrowType$Bool (<-arrow-type [_] :bool)

  ArrowType$FloatingPoint
  (<-arrow-type [arrow-type]
    (case-enum (.getPrecision arrow-type)
      FloatingPointPrecision/SINGLE :f32
      FloatingPointPrecision/DOUBLE :f64))

  ArrowType$Int
  (<-arrow-type [arrow-type]
    (if (.getIsSigned arrow-type)
      (case (.getBitWidth arrow-type)
        8 :i8, 16 :i16, 32 :i32, 64 :i64)

      (throw (UnsupportedOperationException. "unsigned ints"))))

  ArrowType$Utf8 (<-arrow-type [_] :utf8)
  ArrowType$Binary (<-arrow-type [_] :varbinary)
  ArrowType$FixedSizeBinary (<-arrow-type [arrow-type] [:fixed-size-binary (.getByteWidth arrow-type)])

  ArrowType$Decimal
  (<-arrow-type [arrow-type]
    [:decimal (.getPrecision arrow-type) (.getScale arrow-type) (.getBitWidth arrow-type)])

  ArrowType$FixedSizeList (<-arrow-type [arrow-type] [:fixed-size-list (.getListSize arrow-type)])

  ArrowType$Timestamp
  (<-arrow-type [arrow-type]
    (let [time-unit (time-unit->kw (.getUnit arrow-type))]
      (if-let [tz (.getTimezone arrow-type)]
        (if (and (= :micro time-unit) (= "UTC" tz))
          :instant
          [:timestamp-tz time-unit tz])
        [:timestamp-local time-unit])))

  ArrowType$Date (<-arrow-type [arrow-type] [:date (date-unit->kw (.getUnit arrow-type))])
  ArrowType$Time (<-arrow-type [arrow-type] [:time-local (time-unit->kw (.getUnit arrow-type))])
  ArrowType$Duration (<-arrow-type [arrow-type] [:duration (time-unit->kw (.getUnit arrow-type))])
  ArrowType$Interval (<-arrow-type [arrow-type] [:interval (interval-unit->kw (.getUnit arrow-type))])
  IntervalMDMType (<-arrow-type [_arrow-type] [:interval :month-day-micro])

  ArrowType$Struct (<-arrow-type [_] :struct)
  ArrowType$List (<-arrow-type [_] :list)
  SetType (<-arrow-type [_] :set)
  ArrowType$Map (<-arrow-type [arrow-type] [:map {:sorted? (.getKeysSorted arrow-type)}])

  ArrowType$Union
  (<-arrow-type [^ArrowType$Union arrow-type]
    (if (= UnionMode/Dense (.getMode arrow-type)) :union :sparse-union))

  TsTzRangeType (<-arrow-type [_] :tstz-range)
  KeywordType (<-arrow-type [_] :keyword)
  OidType (<-arrow-type [_] :oid)
  RegClassType (<-arrow-type [_] :regclass)
  RegProcType (<-arrow-type [_] :regproc)
  UuidType (<-arrow-type [_] :uuid)
  UriType (<-arrow-type [_] :uri)
  TransitType (<-arrow-type [_] :transit))

(defn ->arrow-type ^org.apache.arrow.vector.types.pojo.ArrowType [col-type]
  (case (if (keyword? col-type)
          col-type
          (first col-type))
    :null ArrowTypes/NULL_TYPE
    :bool ArrowTypes/BOOL_TYPE

    :i8 ArrowTypes/I8_TYPE
    :i16 ArrowTypes/I16_TYPE
    :i32 ArrowTypes/I32_TYPE
    :i64 ArrowTypes/I64_TYPE
    :f32 ArrowTypes/F32_TYPE
    :f64 ArrowTypes/F64_TYPE

    :utf8 ArrowTypes/UTF8_TYPE
    :varbinary ArrowTypes/VAR_BINARY_TYPE

    :instant ArrowTypes/INSTANT_TYPE
    :tstz-range TsTzRangeType/INSTANCE
    :keyword KeywordType/INSTANCE
    :oid OidType/INSTANCE
    :regclass RegClassType/INSTANCE
    :regproc RegProcType/INSTANCE
    :uuid UuidType/INSTANCE
    :uri UriType/INSTANCE
    :transit TransitType/INSTANCE

    :struct ArrowTypes/STRUCT_TYPE
    :list ArrowTypes/LIST_TYPE
    :set SetType/INSTANCE
    :union ArrowTypes/UNION_TYPE
    :sparse-union (.getType Types$MinorType/UNION)

    :map (let [[_ {:keys [sorted?]}] col-type]
           (ArrowType$Map. (boolean sorted?)))

    :date (let [[_ date-unit] col-type]
            (ArrowType$Date. (kw->date-unit date-unit)))

    :decimal (let [[_ precision scale bitwidth] col-type]
               (ArrowType$Decimal/createDecimal precision scale (int bitwidth)))

    :timestamp-tz (let [[_ time-unit tz] col-type]
                    (ArrowType$Timestamp. (kw->time-unit time-unit) tz))

    :timestamp-local (let [[_ time-unit] col-type]
                       (ArrowType$Timestamp. (kw->time-unit time-unit) nil))

    :time-local (let [[_ time-unit] col-type]
                  (ArrowType$Time. (kw->time-unit time-unit)
                                   (case time-unit (:second :milli) 32, (:micro :nano) 64)))

    :duration (let [[_ time-unit] col-type]
                (ArrowType$Duration. (kw->time-unit time-unit)))

    :interval (let [[_ interval-unit] col-type]
                (if (= :month-day-micro interval-unit)
                  IntervalMDMType/INSTANCE
                  (ArrowType$Interval. (kw->interval-unit interval-unit))))

    :fixed-size-list (let [[_ list-size] col-type]
                       (ArrowType$FixedSizeList. list-size))

    :fixed-size-binary (let [[_ byte-width] col-type]
                         (ArrowType$FixedSizeBinary. byte-width))

    :iid ArrowTypes/IID_TYPE))

(defmethod print-dup ArrowType [arrow-type, ^Writer w]
  (.write w "#xt.arrow/type ")
  (.write w (pr-str (<-arrow-type arrow-type))))

(defmethod print-method ArrowType [arrow-type w]
  (print-dup arrow-type w))

(defmethod print-dup FieldType [^FieldType field-type, ^Writer w]
  (.write w "#xt.arrow/field-type [")
  (print-method (.getType field-type) w)
  (.write w (if (.isNullable field-type)
              " true" " false"))

  (when-let [dictionary (.getDictionary field-type)]
    (.write w " ")
    (.write w (pr-str dictionary)))

  (when-let [metadata (.getMetadata field-type)]
    (.write w " ")
    (print-method metadata w))

  (.write w "]"))

(defmethod print-method FieldType [field-type w]
  (print-dup field-type w))

(defprotocol RenderType
  (render-type [vec-type]))

(extend-protocol RenderType
  VectorType$Null 
  (render-type [_] :null)

  VectorType$Scalar
  (render-type [scalar]
    (<-arrow-type (.getArrowType scalar)))

  VectorType$Listy
  (render-type [list-type]
    (if (= list-type VectorType/TSTZ_RANGE)
      :tstz-range

      (let [arrow-type (.getArrowType list-type)
            el-type (render-type (.getElType list-type))
            arrow-type-spec (<-arrow-type arrow-type)]
        (as-> [] type-spec
          (if (keyword? arrow-type-spec)
            (conj type-spec arrow-type-spec)
            (into type-spec arrow-type-spec))

          (conj type-spec el-type)))))

  VectorType$Struct
  (render-type [struct-type]
    [:struct (-> (.getChildren struct-type) (update-vals render-type))])

  VectorType$Maybe
  (render-type [maybe-type]
    (let [base-type (render-type (.getMono maybe-type))]
      (if (keyword? base-type)
        [:? base-type]
        (into [:?] base-type))))
  
  VectorType$Poly
  (render-type [vec-type]
    (into #{} (map render-type) (.getLegs vec-type))))

(defn render-field-type 
  ([^Field field]
   (render-field-type (<-arrow-type (.getType field)) (.isNullable field) (.getChildren field)))

  ([arrow-type-spec nullable? children]
   (if (and (not nullable?) (empty? children))
     arrow-type-spec

     (as-> [] type-spec
       (cond-> type-spec nullable? (conj :?))
       (if (keyword? arrow-type-spec)
         (conj type-spec arrow-type-spec)
         (into type-spec arrow-type-spec))

       (into type-spec 
             (let [{matching true, non-matching false}
                   (group-by (fn [^Field child]
                               (let [child-name (.getName child)]
                                 (case arrow-type-spec
                                   (:union :sparse-union) (= child-name (ArrowTypes/toLeg (.getType child)))
                                   (:list :set) (= "$data$" child-name)
                                   false)))
                             children)]

               (concat
                 (when (seq non-matching)
                   [(into {} (map (fn [^Field f] [(.getName f) (render-field-type f)]) non-matching))])
                 (map render-field-type matching))))))))

(defn render-field [^Field field]
  (let [field-name (.getName field)
        arrow-type (.getType field)
        arrow-type-spec (<-arrow-type arrow-type)
        type-spec (render-field-type arrow-type-spec (.isNullable field) (.getChildren field))]
    (if (= field-name (ArrowTypes/toLeg arrow-type))
      type-spec
      {field-name type-spec})))

(defmethod print-dup Field [f, ^Writer w]
  (.write w "#xt/field ")
  (.write w (pr-str (render-field f))))

(defmethod print-method Field [f w]
  (print-dup f w))

(declare ->field ->field*)

(defn- ->child-fields
  "Converts child specs to fields
   Accepts: bare types (use leg name), singleton maps, or multi-entry maps."
  [child-specs ctx]
  (->> child-specs
       (into []
             (mapcat (fn [child-spec]
                       (cond
                         (instance? Field child-spec) [child-spec]

                         (map? child-spec)
                         (for [[k v] child-spec]
                           (->field* k v))

                         :else
                         [(->field* (case ctx
                                      (:set :list) "$data$"
                                      nil)
                                    child-spec)]))))))

(defn ->field* ^org.apache.arrow.vector.types.pojo.Field [field-name type-spec]
  (if (instance? VectorType type-spec)
    (if field-name 
      (VectorType/.toField type-spec field-name)
      (VectorType/.getAsLegField type-spec))
    
    (let [[arrow-type nullable? children]
          (if (keyword? type-spec)
            (case type-spec
              :tstz-range [#xt.arrow/type :tstz-range false [(->field :instant)]]
              [(->arrow-type type-spec) false []])

            (let [[first-elem & more-opts] type-spec
                  [nullable? more-opts] (if (= :? first-elem)
                                          [true more-opts]
                                          [false (cons first-elem more-opts)])
                  [arrow-type-head & more-opts] more-opts]
              (case arrow-type-head
                :union [#xt.arrow/type :union false (->child-fields more-opts arrow-type-head)]

                (:set :list :struct :sparse-union :tstz-range)
                [(->arrow-type arrow-type-head) nullable? (->child-fields more-opts arrow-type-head)]

                [(->arrow-type (cons arrow-type-head more-opts)) nullable? []])))]

      (Field. (or field-name (ArrowTypes/toLeg arrow-type))
              (FieldType. nullable? arrow-type nil) 
              children))))

(defn ->field ^org.apache.arrow.vector.types.pojo.Field [field-spec]
  (cond
    (instance? Field field-spec) field-spec

    :else (let [[nm type-spec] (if (and (map? field-spec) (= 1 (count field-spec)))
                                 (first field-spec)
                                 [nil field-spec])]
            (->field* nm type-spec))))

(defmethod print-dup Schema [^Schema s, ^Writer w]
  (.write w "#xt/schema ")
  (print-dup (map render-field (.getFields s)) w))

(defmethod print-method Schema [s w]
  (print-dup s w))

(defmethod pp/simple-dispatch Schema [^Schema s]
  (.write *out* "#xt/schema ")
  (pp/write-out (map render-field (.getFields s))))

#_{:clojure-lsp/ignore [:clojure-lsp/unused-public-var]} ; reader-macro
(defn ->schema [field-specs]
  (Schema. (mapv ->field field-specs)))

(defmethod print-dup VectorType [t, ^Writer w]
  (.write w "#xt/type ")
  (.write w (pr-str (render-type t))))

(defmethod pp/simple-dispatch VectorType [^VectorType v]
  (.write *out* "#xt/type ")
  (pp/write-out (render-type v)))

(defmethod print-method VectorType [t w]
  (print-dup t w))

#_{:clojure-lsp/ignore [:clojure-lsp/unused-public-var]} ; reader-macro
(defn ->type ^xtdb.arrow.VectorType [type-spec]
  (cond
    (instance? VectorType type-spec) type-spec
    (instance? Field type-spec) (VectorType/fromField type-spec)
    (instance? ArrowType type-spec) (VectorType/scalar type-spec)

    (keyword? type-spec) (case type-spec
                           :tstz-range VectorType/TSTZ_RANGE
                           :null VectorType$Null/INSTANCE
                           (VectorType/scalar (->arrow-type type-spec)))

    (set? type-spec) (VectorType/fromLegs (into #{} (map ->type) type-spec))

    :else (let [[first-elem & more-opts] type-spec
                [nullable? more-opts] (if (= :? first-elem)
                                        [true more-opts]
                                        [false (cons first-elem more-opts)])
                [arrow-type-head & more-opts] more-opts]
            (case arrow-type-head
              :null VectorType$Null/INSTANCE
              :struct (-> (VectorType/structOf (->> (first more-opts)
                                                    (into {} (map (fn [[k v]]
                                                                    [(str k) (->type v)])))))
                          (VectorType/maybe nullable?))

              :tstz-range (-> VectorType/TSTZ_RANGE
                              (VectorType/maybe nullable?))

              (:set :list :fixed-size-list)
              (-> (VectorType/listy (->arrow-type arrow-type-head) (->type (first more-opts)))
                  (VectorType/maybe nullable?))

              (-> (VectorType/scalar (->arrow-type (cons arrow-type-head more-opts)))
                  (VectorType/maybe nullable?))))))

