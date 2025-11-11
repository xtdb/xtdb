(ns xtdb.serde.types
  (:require [clojure.pprint :as pp])
  (:import (java.io Writer)
           [java.util List]
           (org.apache.arrow.vector.types DateUnit FloatingPointPrecision IntervalUnit TimeUnit Types$MinorType UnionMode)
           (org.apache.arrow.vector.types.pojo ArrowType ArrowType$Binary ArrowType$Bool ArrowType$Date ArrowType$Decimal ArrowType$Duration ArrowType$FixedSizeBinary ArrowType$FixedSizeList ArrowType$FloatingPoint ArrowType$Int ArrowType$Interval ArrowType$List ArrowType$Map ArrowType$Null ArrowType$Struct ArrowType$Time ArrowType$Time ArrowType$Timestamp ArrowType$Union ArrowType$Utf8 Field FieldType Schema)
           xtdb.arrow.VectorType
           (xtdb.vector.extensions IntervalMDMType KeywordType RegClassType RegProcType SetType TransitType TsTzRangeType UriType UuidType)))

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
        [:timestamp-tz time-unit tz]
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
  RegClassType (<-arrow-type [_] :regclass)
  RegProcType (<-arrow-type [_] :regproc)
  UuidType (<-arrow-type [_] :uuid)
  UriType (<-arrow-type [_] :uri)
  TransitType (<-arrow-type [_] :transit))

(defn ->arrow-type ^org.apache.arrow.vector.types.pojo.ArrowType [col-type]
  (case col-type
    :null ArrowType$Null/INSTANCE
    :bool ArrowType$Bool/INSTANCE

    :i8 (.getType Types$MinorType/TINYINT)
    :i16 (.getType Types$MinorType/SMALLINT)
    :i32 (.getType Types$MinorType/INT)
    :i64 (.getType Types$MinorType/BIGINT)
    :f32 (.getType Types$MinorType/FLOAT4)
    :f64 (.getType Types$MinorType/FLOAT8)

    :utf8 (.getType Types$MinorType/VARCHAR)
    :varbinary (.getType Types$MinorType/VARBINARY)

    :temporal (.getArrowType VectorType/TEMPORAL)
    :tstz-range TsTzRangeType/INSTANCE
    :keyword KeywordType/INSTANCE
    :regclass RegClassType/INSTANCE
    :regproc RegProcType/INSTANCE
    :uuid UuidType/INSTANCE
    :uri UriType/INSTANCE
    :transit TransitType/INSTANCE

    :struct ArrowType$Struct/INSTANCE
    :list ArrowType$List/INSTANCE
    :set SetType/INSTANCE
    :union (.getType Types$MinorType/DENSEUNION)
    :sparse-union (.getType Types$MinorType/UNION)

    (case (first col-type)
      :struct ArrowType$Struct/INSTANCE
      :list ArrowType$List/INSTANCE
      :set SetType/INSTANCE
      :map (let [[_ {:keys [sorted?]}] col-type]
             (ArrowType$Map. (boolean sorted?)))
      :union (.getType Types$MinorType/DENSEUNION)
      :sparse-union (.getType Types$MinorType/UNION)

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
                           (ArrowType$FixedSizeBinary. byte-width)))))

(defn render-field [^Field field]
  (into (cond-> [(.getName field) (<-arrow-type (.getType field))]
          (.isNullable field) (conj :?))
        (map render-field)
        (.getChildren field)))

(defn render-type [^VectorType type]
  (let [rendered (into (cond-> [(<-arrow-type (.getArrowType type))]
                         (.isNullable type) (conj :?))
                       (map render-field)
                       (.getChildren type))]
    (if (= 1 (count rendered))
      (first rendered)
      rendered)))

(defmethod print-dup Field [f, ^Writer w]
  (.write w "#xt/field ")
  (.write w (pr-str (render-field f))))

(defmethod print-method Field [f w]
  (print-dup f w))

(defn ->field ^org.apache.arrow.vector.types.pojo.Field [field-spec]
  (if (instance? Field field-spec)
    field-spec

    (let [[field-name arrow-type & more-opts] field-spec
          [nullable? children] (if (= :? (first more-opts))
                                 [true (rest more-opts)]
                                 [false more-opts])]
      (Field. field-name
              (FieldType. nullable? (->arrow-type arrow-type) nil nil)
              (mapv ->field children)))))

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

(defmethod print-method VectorType [t w]
  (print-dup t w))

#_{:clojure-lsp/ignore [:clojure-lsp/unused-public-var]} ; reader-macro
(defn ->type ^xtdb.arrow.VectorType [type-spec]
  (cond
    (instance? VectorType type-spec) type-spec
    (keyword? type-spec) (VectorType. (->arrow-type type-spec) false ^List (vector))

    :else (let [[arrow-type & more-opts] type-spec
                [arrow-type more-opts] (case arrow-type
                                         ;; TODO to be finished, need to reduce duplication here with what's above
                                         :timestamp-tz (let [[_ time-unit tz & rest-opts] type-spec]
                                                         [[:timestamp-tz time-unit tz] rest-opts])
                                         :duration (let [[_ time-unit & rest-opts] type-spec]
                                                     [[:duration time-unit] rest-opts])
                                         :interval (let [[_ interval-unit & rest-opts] type-spec]
                                                     [[:interval interval-unit] rest-opts])
                                         :timestamp-local (let [[_ time-unit & rest-opts] type-spec]
                                                            [[:timestamp-local time-unit] rest-opts])
                                         :fixed-size-list (let [[_ list-size & rest-opts] type-spec]
                                                            [[:fixed-size-list list-size] rest-opts])
                                         :fixed-size-binary (let [[_ byte-width & rest-opts] type-spec]
                                                              [[:fixed-size-binary byte-width] rest-opts])
                                         [arrow-type more-opts])
                [nullable? children] (if (= :? (first more-opts))
                                       [true (rest more-opts)]
                                       [false more-opts])]
            (VectorType. (->arrow-type arrow-type)
                         ^boolean nullable?
                         ^List (mapv ->field children)))))

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

