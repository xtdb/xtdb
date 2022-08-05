(ns core2.expression.temporal
  (:require [core2.expression :as expr]
            [core2.expression.metadata :as expr.meta]
            [core2.expression.walk :as ewalk]
            [core2.temporal :as temporal]
            [core2.types :as types]
            [core2.util :as util])
  (:import [java.time Duration LocalDate Period]
           java.time.Instant
           [org.apache.arrow.vector PeriodDuration]
           [org.apache.arrow.vector.types.pojo ArrowType$Date ArrowType$Interval]))

(set! *unchecked-math* :warn-on-boxed)

;; SQL:2011 Time-related-predicates
;; FIXME: these don't take different granularities of timestamp into account

(defmethod expr/codegen-mono-call [:overlaps :timestamp-tz :timestamp-tz :timestamp-tz :timestamp-tz] [_]
  {:return-type :bool
   :->call-code (fn [[x-start x-end y-start y-end]]
                  `(and (< ~x-start ~y-end) (> ~x-end ~y-start)))})

(defmethod expr/codegen-mono-call [:contains :timestamp-tz :timestamp-tz :timestamp-tz] [_]
  {:return-type :bool
   :->call-code (fn [[x-start x-end y]]
                  `(let [y# ~y]
                     (and (<= ~x-start y#) (> ~x-end y#))))})

(defmethod expr/codegen-mono-call [:contains :timestamp-tz :timestamp-tz :timestamp-tz :timestamp-tz] [_]
  {:return-type :bool
   :->call-code (fn [[x-start x-end y-start y-end]]
                  `(and (<= ~x-start ~y-start) (>= ~x-end ~y-end)))})

(defmethod expr/codegen-mono-call [:precedes :timestamp-tz :timestamp-tz :timestamp-tz :timestamp-tz] [_]
  {:return-type :bool
   :->call-code (fn [[_x-start x-end y-start _y-end]]
                  `(<= ~x-end ~y-start))})

(defmethod expr/codegen-mono-call [:succeeds :timestamp-tz :timestamp-tz :timestamp-tz :timestamp-tz] [_]
  {:return-type :bool
   :->call-code (fn [[x-start _x-end _y-start y-end]]
                  `(>= ~x-start ~y-end))})

(defmethod expr/codegen-mono-call [:immediately-precedes :timestamp-tz :timestamp-tz :timestamp-tz :timestamp-tz] [_]
  {:return-type :bool
   :->call-code (fn [[_x-start x-end y-start _y-end]]
                  `(= ~x-end ~y-start))})

(defmethod expr/codegen-mono-call [:immediately-succeeds :timestamp-tz :timestamp-tz :timestamp-tz :timestamp-tz] [_]
  {:return-type :bool
   :->call-code (fn [[x-start _x-end _y-start y-end]]
                  `(= ~x-start ~y-end))})

;; SQL:2011 Operations involving datetimes and intervals

(defn- units-per-second ^long [time-unit]
  (case time-unit
    :second 1
    :milli 1000
    :micro 1000000
    :nano 1000000000))

(defn- smallest-unit [x-unit y-unit]
  (if (> (units-per-second x-unit) (units-per-second y-unit))
    x-unit
    y-unit))

(defn- with-conversion [form from-unit to-unit]
  (if (= from-unit to-unit)
    form
    `(Math/multiplyExact ~form ~(quot (units-per-second to-unit) (units-per-second from-unit)))))

(defmethod expr/codegen-mono-call [:+ :timestamp-tz :duration] [{[[_ts ts-time-unit tz], [_dur dur-time-unit]] :arg-types}]
  (let [res-unit (smallest-unit ts-time-unit dur-time-unit)]
    {:return-type [:timestamp-tz res-unit tz]
     :->call-code (fn [[ts-arg dur-arg]]
                    `(Math/addExact ~(-> ts-arg (with-conversion ts-time-unit res-unit))
                                    ~(-> dur-arg (with-conversion dur-time-unit res-unit))))}))

(defmethod expr/codegen-mono-call [:+ :duration :timestamp-tz] [{[[_dur dur-time-unit], [_ts ts-time-unit tz]] :arg-types}]
  (let [res-unit (smallest-unit ts-time-unit dur-time-unit)]
    {:return-type [:timestamp-tz res-unit tz]
     :->call-code (fn [[dur-arg ts-arg]]
                    `(Math/addExact ~(-> dur-arg (with-conversion dur-time-unit res-unit))
                                    ~(-> ts-arg (with-conversion ts-time-unit res-unit))))}))

(defmethod expr/codegen-mono-call [:+ :duration :duration] [{[[_x x-unit] [_y y-unit]] :arg-types}]
  (let [res-unit (smallest-unit x-unit y-unit)]
    {:return-type [:duration res-unit]
     :->call-code (fn [[x-arg y-arg]]
                    `(Math/addExact ~(-> x-arg (with-conversion x-unit res-unit))
                                    ~(-> y-arg (with-conversion y-unit res-unit))))}))

(defmethod expr/codegen-mono-call [:+ :date :interval] [{[^ArrowType$Date x-type, ^ArrowType$Interval y-type] :arg-types}]
  {:return-type x-type
   :->call-code (fn [[x-arg y-arg]]
                  `(.toEpochDay (.plus (LocalDate/ofEpochDay ~x-arg) (.getPeriod ~y-arg))))})

(defmethod expr/codegen-mono-call [:- :timestamp-tz :timestamp-tz] [{[[_x-ts x-unit _x-tz], [_y-ts y-unit _y-tz]] :arg-types}]
  (let [res-unit (smallest-unit x-unit y-unit)]
    {:return-type [:duration res-unit]
     :->call-code (fn [[x-arg y-arg]]
                    `(Math/subtractExact ~(-> x-arg (with-conversion x-unit res-unit))
                                         ~(-> y-arg (with-conversion y-unit res-unit))))}))

(defmethod expr/codegen-mono-call [:- :timestamp-tz :duration] [{[[_ts ts-time-unit tz], [_dur dur-time-unit]] :arg-types}]
  (let [res-unit (smallest-unit ts-time-unit dur-time-unit)]
    {:return-type [:timestamp-tz res-unit tz]
     :->call-code (fn [[ts-arg dur-arg]]
                    `(Math/subtractExact ~(-> ts-arg (with-conversion ts-time-unit res-unit))
                                         ~(-> dur-arg (with-conversion dur-time-unit res-unit))))}))

(defmethod expr/codegen-mono-call [:- :duration :duration] [{[[_x x-unit] [_y y-unit]] :arg-types}]
  (let [res-unit (smallest-unit x-unit y-unit)]
    {:return-type [:duration res-unit]
     :->call-code (fn [[x-arg y-arg]]
                    `(Math/subtractExact ~(-> x-arg (with-conversion x-unit res-unit))
                                         ~(-> y-arg (with-conversion y-unit res-unit))))}))

(defmethod expr/codegen-mono-call [:- :date :interval] [{[dt-type, _i-type] :arg-types}]
  {:return-type dt-type
   :->call-code (fn [[x-arg y-arg]]
                  `(.toEpochDay (.minus (LocalDate/ofEpochDay ~x-arg) (.getPeriod ~y-arg))))})

(defmethod expr/codegen-mono-call [:* :duration :int] [{[x-type _y-type] :arg-types}]
  {:return-type x-type
   :->call-code (fn [emitted-args]
                  `(Math/multiplyExact ~@emitted-args))})

(defmethod expr/codegen-mono-call [:* :duration :num] [{[x-type _y-type] :arg-types}]
  {:return-type x-type
   :->call-code (fn [emitted-args]
                  `(* ~@emitted-args))})

(defmethod expr/codegen-mono-call [:* :int :duration] [{[_x-type y-type] :arg-types}]
  {:return-type y-type
   :->call-code (fn [emitted-args]
                  `(Math/multiplyExact ~@emitted-args))})

(defmethod expr/codegen-mono-call [:* :num :duration] [{[_x-type y-type] :arg-types}]
  {:return-type y-type
   :->call-code (fn [emitted-args]
                  `(long (* ~@emitted-args)))})

(defmethod expr/codegen-mono-call [:/ :duration :num] [{[x-type] :arg-types}]
  {:return-type x-type
   :->call-code (fn [emitted-args]
                  `(quot ~@emitted-args))})

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
  [[_interval l-unit] [_interval r-unit]]
  (if (= l-unit r-unit)
    l-unit
    ;; we could be smarter about the return type here to allow a more compact representation
    ;; for day time cases
    :month-day-nano))

(defmethod expr/codegen-mono-call [:+ :interval :interval] [{[l-type r-type] :arg-types}]
  (let [return-type (choose-interval-arith-return l-type r-type)]
    {:return-type [:interval return-type]
     :->call-code (fn [[l r]] `(pd-add ~l ~r))}))

(defmethod expr/codegen-mono-call [:- :interval :interval] [{[l-type r-type] :arg-types}]
  (let [return-type (choose-interval-arith-return l-type r-type)]
    {:return-type [:interval return-type]
     :->call-code (fn [[l r]] `(pd-sub ~l ~r))}))

(defn pd-scale ^PeriodDuration [^PeriodDuration pd ^long factor]
  (let [p (.getPeriod pd)
        d (.getDuration pd)]
    (PeriodDuration. (.multipliedBy p factor) (.multipliedBy d factor))))

(defmethod expr/codegen-mono-call [:* :interval :int] [_]
  {:return-type [:interval :month-day-nano]
   :->call-code (fn [[a b]] `(pd-scale ~a ~b))})

(defmethod expr/codegen-mono-call [:* :int :interval] [_]
  {:return-type [:interval :month-day-nano]
   :->call-code (fn [[a b]] `(pd-scale ~b ~a))})

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

(defmethod expr/codegen-mono-call [:/ :interval :int] [{[[_interval iunit] _]:arg-types}]
  (case iunit
    :year-month {:return-type [:interval :year-month]
                 :->call-code (fn [[a b]] `(pd-year-month-div ~a ~b))}
    :day-time {:return-type [:interval :day-time]
               :->call-code (fn [[a b]] `(pd-day-time-div ~a ~b))}
    (throw (UnsupportedOperationException. "Cannot divide mixed period / duration intervals"))))

(defmethod expr/codegen-mono-call [:min :timestamp-tz :timestamp-tz] [{:keys [arg-types]}]
  (assert (apply = arg-types))
  {:return-type (types/least-upper-bound arg-types)
   :->call-code #(do `(Math/min ~@%))})

(defmethod expr/codegen-mono-call [:max :timestamp-tz :timestamp-tz] [{:keys [arg-types]}]
  (assert (apply = arg-types))
  {:return-type (types/least-upper-bound arg-types)
   :->call-code #(do `(Math/max ~@%))})

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

(defmethod expr/codegen-mono-call [:abs :interval] [{[[_interval iunit :as itype]] :arg-types}]
  {:return-type itype,
   :->call-code (case iunit
                  :year-month #(do `(interval-abs-ym ~@%))
                  :day-time #(do `(interval-abs-dt ~@%))
                  (throw (UnsupportedOperationException. "Can only ABS YEAR_MONTH / DAY_TIME intervals")))})

(defn apply-constraint [^longs min-range ^longs max-range
                        f col-name ^Instant time]
  (let [range-idx (temporal/->temporal-column-idx col-name)
        time-μs (util/instant->micros time)]
    (case f
      :< (aset max-range range-idx (min (dec time-μs)
                                        (aget max-range range-idx)))
      :<= (aset max-range range-idx (min time-μs
                                         (aget max-range range-idx)))
      :> (aset min-range range-idx (max (inc time-μs)
                                        (aget min-range range-idx)))
      :>= (aset min-range range-idx (max time-μs
                                         (aget min-range range-idx)))
      nil)))

(defn ->temporal-min-max-range [selects params]
  (let [min-range (temporal/->min-range)
        max-range (temporal/->max-range)
        col-types (zipmap (map symbol (keys temporal/temporal-fields))
                          (repeat temporal/temporal-col-type))]
    (doseq [[col-name select-form] selects
            :when (temporal/temporal-column? col-name)]
      (->> (expr/form->expr select-form {:param-types (expr/->param-types params) :col-types col-types})
           (expr/prepare-expr)
           (expr.meta/meta-expr)
           (ewalk/prewalk-expr
            (fn [{:keys [op] :as expr}]
              (case op
                :call (when (not= :or (:f expr))
                        expr)

                :metadata-vp-call
                (let [{:keys [f param-expr]} expr]
                  (apply-constraint min-range max-range
                                    f col-name
                                    (util/->instant (some-> (or (find param-expr :literal)
                                                                (find params (get param-expr :param)))
                                                            val))))

                expr)))))
    [min-range max-range]))
