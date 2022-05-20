(ns core2.expression.temporal
  (:require [core2.expression :as expr]
            [core2.expression.macro :as emacro]
            [core2.expression.metadata :as expr.meta]
            [core2.expression.walk :as ewalk]
            [core2.temporal :as temporal]
            [core2.types :as types]
            [core2.util :as util])
  (:import java.time.Instant
           [org.apache.arrow.vector.types.pojo ArrowType$Duration ArrowType$Int ArrowType$Timestamp ArrowType$Interval ArrowType$Date]
           org.apache.arrow.vector.types.TimeUnit
           [java.time LocalDate Period Duration]
           [org.apache.arrow.vector PeriodDuration]
           [org.apache.arrow.vector.types IntervalUnit]))

(set! *unchecked-math* :warn-on-boxed)

;; SQL:2011 Time-related-predicates
;; FIXME: these don't take different granularities of timestamp into account

(defmethod expr/codegen-mono-call [:overlaps ArrowType$Timestamp ArrowType$Timestamp ArrowType$Timestamp ArrowType$Timestamp] [_]
  {:continue-call (fn [f [x-start x-end y-start y-end]]
                    (f types/bool-type
                       `(and (< ~x-start ~y-end) (> ~x-end ~y-start))))
   :return-type types/bool-type})

(defmethod expr/codegen-mono-call [:contains ArrowType$Timestamp ArrowType$Timestamp ArrowType$Timestamp] [_]
  {:continue-call (fn [f [x-start x-end y]]
                    (f types/bool-type
                       `(let [y# ~y]
                          (and (<= ~x-start y#) (> ~x-end y#)))))
   :return-type types/bool-type})

(defmethod expr/codegen-mono-call [:contains ArrowType$Timestamp ArrowType$Timestamp ArrowType$Timestamp ArrowType$Timestamp] [_]
  {:continue-call (fn [f [x-start x-end y-start y-end]]
                    (f types/bool-type
                       `(and (<= ~x-start ~y-start) (>= ~x-end ~y-end))))
   :return-type types/bool-type})

(defmethod expr/codegen-mono-call [:precedes ArrowType$Timestamp ArrowType$Timestamp ArrowType$Timestamp ArrowType$Timestamp] [_]
  {:continue-call (fn [f [_x-start x-end y-start _y-end]]
                    (f types/bool-type
                       `(<= ~x-end ~y-start)))
   :return-type types/bool-type})

(defmethod expr/codegen-mono-call [:succeeds ArrowType$Timestamp ArrowType$Timestamp ArrowType$Timestamp ArrowType$Timestamp] [_]
  {:continue-call (fn [f [x-start _x-end _y-start y-end]]
                    (f types/bool-type
                       `(>= ~x-start ~y-end)))
   :return-type types/bool-type})

(defmethod expr/codegen-mono-call [:immediately-precedes ArrowType$Timestamp ArrowType$Timestamp ArrowType$Timestamp ArrowType$Timestamp] [_]
  {:continue-call (fn [f [_x-start x-end y-start _y-end]]
                    (f types/bool-type
                       `(= ~x-end ~y-start)))
   :return-type types/bool-type})

(defmethod expr/codegen-mono-call [:immediately-succeeds ArrowType$Timestamp ArrowType$Timestamp ArrowType$Timestamp ArrowType$Timestamp] [_]
  {:continue-call (fn [f [x-start _x-end _y-start y-end]]
                    (f types/bool-type
                       `(= ~x-start ~y-end)))
   :return-type types/bool-type})

;; SQL:2011 Operations involving datetimes and intervals

(defn- units-per-second ^long [^TimeUnit time-unit]
  (case (.name time-unit)
    "SECOND" 1
    "MILLISECOND" 1000
    "MICROSECOND" 1000000
    "NANOSECOND" 1000000000))

(defn- smallest-unit [x-unit y-unit]
  (if (> (units-per-second x-unit) (units-per-second y-unit))
    x-unit
    y-unit))

(defn- with-conversion [form from-unit to-unit]
  (if (= from-unit to-unit)
    form
    `(Math/multiplyExact ~form ~(quot (units-per-second to-unit) (units-per-second from-unit)))))

(defmethod expr/codegen-mono-call [:+ ArrowType$Timestamp ArrowType$Duration] [{[^ArrowType$Timestamp x-type, ^ArrowType$Duration y-type] :arg-types}]
  (let [x-unit (.getUnit x-type)
        y-unit (.getUnit y-type)
        res-unit (smallest-unit x-unit y-unit)
        return-type (ArrowType$Timestamp. res-unit (.getTimezone x-type))]
    {:return-type return-type
     :continue-call (fn [f [x-arg y-arg]]
                      (f return-type
                         `(Math/addExact ~(-> x-arg (with-conversion x-unit res-unit))
                                         ~(-> y-arg (with-conversion y-unit res-unit)))))}))

(defmethod expr/codegen-mono-call [:+ ArrowType$Duration ArrowType$Timestamp] [{[^ArrowType$Duration x-type, ^ArrowType$Timestamp y-type] :arg-types}]
  (let [x-unit (.getUnit x-type)
        y-unit (.getUnit y-type)
        res-unit (smallest-unit x-unit y-unit)
        return-type (ArrowType$Timestamp. res-unit (.getTimezone y-type))]
    {:return-type return-type
     :continue-call (fn [f [x-arg y-arg]]
                      (f return-type
                         `(Math/addExact ~(-> x-arg (with-conversion x-unit res-unit))
                                         ~(-> y-arg (with-conversion y-unit res-unit)))))}))

(defmethod expr/codegen-mono-call [:+ ArrowType$Duration ArrowType$Duration] [{[^ArrowType$Duration x-type, ^ArrowType$Duration y-type] :arg-types}]
  (let [x-unit (.getUnit x-type)
        y-unit (.getUnit y-type)
        res-unit (smallest-unit x-unit y-unit)
        return-type (ArrowType$Duration. res-unit)]
    {:return-type return-type
     :continue-call (fn [f [x-arg y-arg]]
                      (f return-type
                         `(Math/addExact ~(-> x-arg (with-conversion x-unit res-unit))
                                         ~(-> y-arg (with-conversion y-unit res-unit)))))}))

(defmethod expr/codegen-mono-call [:+ ArrowType$Date ArrowType$Interval] [{[^ArrowType$Date x-type, ^ArrowType$Interval y-type] :arg-types}]
  (let [return-type x-type]
    {:return-type return-type
     :continue-call (fn [f [x-arg y-arg]]
                      (f return-type `(.toEpochDay (.plus (LocalDate/ofEpochDay ~x-arg) (.getPeriod ~y-arg)))))}))

(defmethod expr/codegen-mono-call [:- ArrowType$Timestamp ArrowType$Timestamp] [{[^ArrowType$Timestamp x-type, ^ArrowType$Timestamp y-type] :arg-types}]
  (let [x-unit (.getUnit x-type)
        y-unit (.getUnit y-type)
        res-unit (smallest-unit x-unit y-unit)
        return-type (ArrowType$Duration. res-unit)]
    {:return-type return-type
     :continue-call (fn [f [x-arg y-arg]]
                      (f return-type
                         `(Math/subtractExact ~(-> x-arg (with-conversion x-unit res-unit))
                                              ~(-> y-arg (with-conversion y-unit res-unit)))))}))

(defmethod expr/codegen-mono-call [:- ArrowType$Timestamp ArrowType$Duration] [{[^ArrowType$Timestamp x-type, ^ArrowType$Duration y-type] :arg-types}]
  (let [x-unit (.getUnit x-type)
        y-unit (.getUnit y-type)
        res-unit (smallest-unit x-unit y-unit)
        return-type (ArrowType$Timestamp. res-unit (.getTimezone x-type))]
    {:return-type return-type
     :continue-call (fn [f [x-arg y-arg]]
                      (f return-type
                         `(Math/subtractExact ~(-> x-arg (with-conversion x-unit res-unit))
                                              ~(-> y-arg (with-conversion y-unit res-unit)))))}))

(defmethod expr/codegen-mono-call [:- ArrowType$Duration ArrowType$Duration] [{[^ArrowType$Duration x-type, ^ArrowType$Duration y-type] :arg-types}]
  (let [x-unit (.getUnit x-type)
        y-unit (.getUnit y-type)
        res-unit (smallest-unit x-unit y-unit)
        return-type (ArrowType$Duration. res-unit)]
    {:return-type return-type
     :continue-call (fn [f [x-arg y-arg]]
                      (f return-type
                         `(Math/subtractExact ~(-> x-arg (with-conversion x-unit res-unit))
                                              ~(-> y-arg (with-conversion y-unit res-unit)))))}))

(defmethod expr/codegen-mono-call [:- ArrowType$Date ArrowType$Interval] [{[^ArrowType$Date x-type, ^ArrowType$Interval y-type] :arg-types}]
  (let [return-type x-type]
    {:return-type return-type
     :continue-call (fn [f [x-arg y-arg]]
                      (f return-type `(.toEpochDay (.minus (LocalDate/ofEpochDay ~x-arg) (.getPeriod ~y-arg)))))}))

(defmethod expr/codegen-mono-call [:* ArrowType$Duration ArrowType$Int] [{[x-type _y-type] :arg-types}]
  {:continue-call (fn [f emitted-args]
                    (f x-type
                       `(Math/multiplyExact ~@emitted-args)))
   :return-type x-type})

(defmethod expr/codegen-mono-call [:* ArrowType$Duration ::types/Number] [{[x-type _y-type] :arg-types}]
  {:continue-call (fn [f emitted-args]
                    (f x-type
                       `(* ~@emitted-args)))
   :return-type x-type})

(defmethod expr/codegen-mono-call [:* ArrowType$Int ArrowType$Duration] [{[_x-type y-type] :arg-types}]
  {:continue-call (fn [f emitted-args]
                    (f y-type
                       `(Math/multiplyExact ~@emitted-args)))
   :return-type y-type})

(defmethod expr/codegen-mono-call [:* ::types/Number ArrowType$Duration] [{[_x-type y-type] :arg-types}]
  {:continue-call (fn [f emitted-args]
                    (f y-type
                       `(long (* ~@emitted-args))))
   :return-type y-type})

(defmethod expr/codegen-mono-call [:/ ArrowType$Duration ::types/Number] [{[x-type] :arg-types}]
  {:continue-call (fn [f emitted-args]
                    (f x-type
                       `(quot ~@emitted-args)))
   :return-type x-type})

(defn pd-add ^PeriodDuration [^PeriodDuration pd1 ^PeriodDuration pd2]
  (let [p1 (.getPeriod pd1)
        p2 (.getPeriod pd2)
        d1 (.getDuration pd1)
        d2 (.getDuration pd2)]
    (PeriodDuration. (.plus p1 p2) (.plus d1 d2))))

(defn pd-sub ^PeriodDuration [^PeriodDuration pd1 ^PeriodDuration pd2]
  (let [p1 (.getPeriod pd1)
        p2 (.getPeriod pd2)
        d1 (.getDuration pd1)
        d2 (.getDuration pd2)]
    (PeriodDuration. (.minus p1 p2) (.minus d1 d2))))

(defn- choose-interval-arith-return
  "Given two interval types, return an interval type that can represent the result of an binary arithmetic expression
  over those types, i.e + or -.

  If you add two YearMonth intervals, you can use an YearMonth representation for the result, if you add a YearMonth
  and a MonthDayNano, you must use MonthDayNano to represent the result."
  [^ArrowType$Interval type-a ^ArrowType$Interval type-b]
  (if (= (.getUnit type-a) (.getUnit type-b))
    type-a
    ;; we could be smarter about the return type here to allow a more compact representation
    ;; for day time cases
    types/interval-month-day-nano-type))

(defmethod expr/codegen-mono-call [:+ ArrowType$Interval ArrowType$Interval] [{:keys [arg-types]}]
  (let [[type-a type-b] arg-types
        return-type (choose-interval-arith-return type-a type-b)]
    {:return-type return-type
     :continue-call (fn [f [a b]] (f return-type `(pd-add ~a ~b)))}))

(defmethod expr/codegen-mono-call [:- ArrowType$Interval ArrowType$Interval] [{:keys [arg-types]}]
  (let [[type-a type-b] arg-types
        return-type (choose-interval-arith-return type-a type-b)]
    {:return-type return-type
     :continue-call (fn [f [a b]] (f return-type `(pd-sub ~a ~b)))}))

(defn pd-scale ^PeriodDuration [^PeriodDuration pd ^long factor]
  (let [p (.getPeriod pd)
        d (.getDuration pd)]
    (PeriodDuration. (.multipliedBy p factor) (.multipliedBy d factor))))

(defmethod expr/codegen-mono-call [:* ArrowType$Interval ArrowType$Int] [_]
  {:return-type types/interval-month-day-nano-type
   :continue-call (fn [f [a b]] (f types/interval-month-day-nano-type `(pd-scale ~a ~b)))})

(defmethod expr/codegen-mono-call [:* ArrowType$Int ArrowType$Interval] [_]
  {:return-type types/interval-month-day-nano-type
   :continue-call (fn [f [a b]] (f types/interval-month-day-nano-type `(pd-scale ~b ~a)))})

;; numeric division for intervals
;; can only be supported for Year_Month and Day_Time interval units without
;; ambguity, e.g if I was to divide an interval containing P1M20D, I could return P10D for the day component, but I have
;; to truncate the month component to zero (as it is not divisible into days).

(defn pd-year-month-div ^PeriodDuration [^PeriodDuration pd ^long divisor]
  (let [p (.getPeriod pd)]
    (PeriodDuration.
      (Period/of 0 (quot (.toTotalMonths p) divisor) 0)
      Duration/ZERO)))

(defn pd-day-time-div ^PeriodDuration [^PeriodDuration pd ^long divisor]
  (let [p (.getPeriod pd)
        d (.getDuration pd)]
    (PeriodDuration.
      (Period/ofDays (quot (.getDays p) divisor))
      (Duration/ofSeconds (quot (.toSeconds d) divisor)))))

(defmethod expr/codegen-mono-call [:/ ArrowType$Interval ArrowType$Int] [{:keys [arg-types]}]
  (let [[^ArrowType$Interval itype] arg-types]
    (util/case-enum (.getUnit itype)
      IntervalUnit/YEAR_MONTH
      {:return-type types/interval-year-month-type
       :continue-call (fn [f [a b]] (f types/interval-year-month-type `(pd-year-month-div ~a ~b)))}
      IntervalUnit/DAY_TIME
      {:return-type types/interval-day-time-type
       :continue-call (fn [f [a b]] (f types/interval-day-time-type `(pd-day-time-div ~a ~b)))}
      (throw (UnsupportedOperationException. "Cannot divide mixed period / duration intervals")))))

(defn interval-abs-ym
  "In SQL the ABS function can be applied to intervals, negating them if they are below some definition of 'zero' for the components
  of the intervals.

  We only support abs on YEAR_MONTH and DAY_TIME typed vectors at the moment, This seems compliant with the standard
  which only talks about ABS being applied to a single component interval.

  For YEAR_MONTH, we define where ZERO as 0 months.
  For DAY_TIME we define ZERO as 0 seconds (interval-abs-dt)."
  ^PeriodDuration [^PeriodDuration pd]
  (let [p (.getPeriod pd)]
    (if (<= 0 (.toTotalMonths p))
      pd
      (PeriodDuration. (Period/ofMonths (- (.toTotalMonths p))) Duration/ZERO))))

(defn interval-abs-dt ^PeriodDuration [^PeriodDuration pd]
  (let [p (.getPeriod pd)
        d (.getDuration pd)

        days (.getDays p)
        secs (.toSeconds d)]
    ;; cast to long to avoid overflow during calc
    (if (<= 0 (+ (long secs) (* (long days) 24 60 60)))
      pd
      (PeriodDuration. (Period/ofDays (- days)) (Duration/ofSeconds (- secs))))))

(defmethod expr/codegen-mono-call [:abs ArrowType$Interval] [{[^ArrowType$Interval arg-type] :arg-types}]
  (util/case-enum (.getUnit arg-type)
    IntervalUnit/YEAR_MONTH
    (expr/mono-fn-call arg-type #(do `(interval-abs-ym ~@%)))
    IntervalUnit/DAY_TIME
    (expr/mono-fn-call arg-type #(do `(interval-abs-dt ~@%)))
    (throw (UnsupportedOperationException. "Can only ABS YEAR_MONTH / DAY_TIME intervals"))))

(defn apply-constraint [^longs min-range ^longs max-range
                        f col-name ^Instant time]
  (let [range-idx (temporal/->temporal-column-idx col-name)
        time-μs (util/instant->micros time)]
    (case f
      < (aset max-range range-idx (min (dec time-μs)
                                       (aget max-range range-idx)))
      <= (aset max-range range-idx (min time-μs
                                        (aget max-range range-idx)))
      > (aset min-range range-idx (max (inc time-μs)
                                       (aget min-range range-idx)))
      >= (aset min-range range-idx (max time-μs
                                        (aget min-range range-idx)))
      nil)))

(defn ->temporal-min-max-range [selects srcs]
  (let [min-range (temporal/->min-range)
        max-range (temporal/->max-range)
        col-names (into #{} (map symbol) (keys temporal/temporal-fields))]
    (doseq [[col-name select-form] selects
            :when (temporal/temporal-column? col-name)]
      (->> (expr/form->expr select-form {:params srcs, :col-names col-names})
           (emacro/macroexpand-all)
           (ewalk/postwalk-expr expr/lit->param)
           (expr.meta/meta-expr)
           (ewalk/prewalk-expr
            (fn [{:keys [op] :as expr}]
              (case op
                :call (when (not= 'or (:f expr))
                        expr)

                :metadata-vp-call
                (let [{:keys [f param-expr]} expr]
                  (apply-constraint min-range max-range
                                    f col-name
                                    (util/->instant (some-> (or (find param-expr :literal)
                                                                (find srcs (get param-expr :param)))
                                                            val))))

                expr)))))
    [min-range max-range]))
