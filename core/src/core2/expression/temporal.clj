(ns core2.expression.temporal
  (:require [clojure.string :as str]
            [core2.expression :as expr]
            [core2.expression.metadata :as expr.meta]
            [core2.expression.walk :as ewalk]
            [core2.temporal :as temporal]
            [core2.types :as types]
            [core2.util :as util])
  (:import (java.time Duration Instant LocalDate Period ZoneId ZoneOffset ZonedDateTime)
           (java.time.temporal ChronoField ChronoUnit)
           (org.apache.arrow.vector PeriodDuration)
           (org.apache.arrow.vector.types.pojo ArrowType$Date ArrowType$Interval)))

(set! *unchecked-math* :warn-on-boxed)

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
                                    (util/sql-temporal->instant (some-> (or (find param-expr :literal)
                                                                            (find params (get param-expr :param)))
                                                                        val)
                                                                (.getZone expr/*clock*))))

                expr)))))
    [min-range max-range]))

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

(defn- ts-offset-zone-seconds-form [form unit offset-secs]
  (if (= 0 offset-secs)
    form
    (case unit
      :second `(Math/subtractExact ~form ~offset-secs)
      :milli `(Math/subtractExact ~form (Math/multiplyExact 1000 ~offset-secs))
      :micro `(Math/subtractExact ~form (Math/multiplyExact 1000000 ~offset-secs))
      :nano `(Math/subtractExact ~form (Math/multiplyExact 1000000000 ~offset-secs)))))

(def ^:private ts-unit-second-scale
  {:second 1
   :milli 1000
   :micro 1000000
   :nano 1000000000})

(defn- ts-scale-factors-for-comparison [unit1 unit2]
  (let [biggest-unit (max-key ts-unit-second-scale unit1 unit2)]
    (if (= biggest-unit unit1)
      [1 (/ (long (ts-unit-second-scale biggest-unit)) (long (ts-unit-second-scale unit2)))]
      [(/ (long (ts-unit-second-scale biggest-unit)) (long (ts-unit-second-scale unit1))) 1])))

(defn- ts-eq-form [col-type1 col-type2]
  ;; much of this logic is common with CAST later, so expect it to be ported out, and = may be implemented in terms of CAST.
  ;; the same scaling and offset rules ought to apply to ord comparison too
  (let [[_ unit1 tz-id1] col-type1
        [_ unit2 tz-id2] col-type2

        offset-tz1 (when-not tz-id1 (.getZone expr/*clock*))
        offset-tz2 (when-not tz-id2 (.getZone expr/*clock*))

        ;; TODO these casts to ZoneOffset are actually unsafe
        ;; there needs to be a slow path here probably deferring to java.time math
        ;; in the case the zones are not resolvable to offsets
        ;; though I'm not sure you can even do arithemtic with them in that case (needs investigation!)
        offset-secs1 (when offset-tz1 (.getTotalSeconds ^ZoneOffset (.normalized offset-tz1)))
        offset-secs2 (when offset-tz2 (.getTotalSeconds ^ZoneOffset (.normalized offset-tz2)))

        unscaled1 (if offset-secs1 #(ts-offset-zone-seconds-form % unit1 offset-secs1) identity)
        unscaled2 (if offset-secs2 #(ts-offset-zone-seconds-form % unit2 offset-secs2) identity)

        ;; we use the smallest possible scaling param to avoid overflow where possible, overflow behaviour will currently
        ;; be to crash - this may change.
        [scale-factor1 scale-factor2] (ts-scale-factors-for-comparison unit1 unit2)

        form1 (if (= 1 scale-factor1) unscaled1 #(do `(Math/multiplyExact ~(unscaled1 %) scale-factor1)))
        form2 (if (= 1 scale-factor2) unscaled2 #(do `(Math/multiplyExact ~(unscaled2 %) scale-factor2)))]
    {:return-type :bool
     :->call-code (fn [[a b]] `(== ~(form1 a) ~(form2 b)))}))

(defmethod expr/codegen-mono-call [:= :timestamp-tz :timestamp-local]
  [{:keys [arg-types]}]
  (apply ts-eq-form arg-types))

(defmethod expr/codegen-mono-call [:= :timestamp-local :timestamp-tz]
  [{:keys [arg-types]}]
  (apply ts-eq-form arg-types))

(defn interval-eq
  "Override equality for intervals as we want 1 year to = 12 months, and this is not true by for Period.equals."
  ;; we might be able to enforce this differently (e.g all constructs, reads and calcs only use the month component).
  [^PeriodDuration pd1 ^PeriodDuration pd2]
  (let [p1 (.getPeriod pd1)
        p2 (.getPeriod pd2)]
    (and (= (.toTotalMonths p1) (.toTotalMonths p2))
         (= (.getDays p1) (.getDays p2))
         (= (.getDuration pd1) (.getDuration pd2)))))

;; interval equality specialisation, see interval-eq.
(defmethod expr/codegen-mono-call [:= :interval :interval] [_]
  {:return-type :bool, :->call-code #(do `(boolean (interval-eq ~@%)))})

(defn- ensure-interval-precision-valid [^long precision]
  (cond
    (< precision 1)
    (throw (IllegalArgumentException. "The minimum leading field precision is 1."))

    (< 8 precision)
    (throw (IllegalArgumentException. "The maximum leading field precision is 8."))))

(defn- ensure-interval-fractional-precision-valid [^long fractional-precision]
  (cond
    (< fractional-precision 0)
    (throw (IllegalArgumentException. "The minimum fractional seconds precision is 0."))

    (< 9 fractional-precision)
    (throw (IllegalArgumentException. "The maximum fractional seconds precision is 9."))))

(defmethod expr/codegen-mono-call [:single-field-interval :int :utf8 :int :int] [{:keys [args]}]
  (let [[_ unit precision fractional-precision] (map :literal args)]
    (ensure-interval-precision-valid precision)
    (when (= "SECOND" unit)
      (ensure-interval-fractional-precision-valid fractional-precision))

    (case unit
      "YEAR" {:return-type [:interval :year-month]
              :->call-code #(do `(PeriodDuration. (Period/ofYears ~(first %)) Duration/ZERO))}
      "MONTH" {:return-type [:interval :year-month]
               :->call-code #(do `(PeriodDuration. (Period/ofMonths ~(first %)) Duration/ZERO))}
      "DAY" {:return-type [:interval :day-time]
             :->call-code #(do `(PeriodDuration. (Period/ofDays ~(first %)) Duration/ZERO))}
      "HOUR" {:return-type [:interval :day-time]
              :->call-code #(do `(PeriodDuration. Period/ZERO (Duration/ofHours ~(first %))))}
      "MINUTE" {:return-type [:interval :day-time]
                :->call-code #(do `(PeriodDuration. Period/ZERO (Duration/ofMinutes ~(first %))))}
      "SECOND" {:return-type [:interval :day-time]
                :->call-code #(do `(PeriodDuration. Period/ZERO (Duration/ofSeconds ~(first %))))})))

(defn ensure-single-field-interval-int
  "Takes a string or UTF8 ByteBuffer and returns an integer, throws a parse error if the string does not contain an integer.

  This is used to parse INTERVAL literal strings, e.g INTERVAL '3' DAY, as the grammar has been overriden to emit a plain string."
  [string-or-buf]
  (try
    (Integer/valueOf (expr/resolve-string string-or-buf))
    (catch NumberFormatException _
      (throw (IllegalArgumentException. "Parse error. Single field INTERVAL string must contain a positive or negative integer.")))))

(defn second-interval-fractional-duration
  "Takes a string or UTF8 ByteBuffer and returns Duration for a fractional seconds INTERVAL literal.

  e.g INTERVAL '3.14' SECOND

  Throws a parse error if the string does not contain an integer / decimal. Throws on overflow."
  ^Duration [string-or-buf]
  (try
    (let [s (expr/resolve-string string-or-buf)
          bd (bigdec s)
          ;; will throw on overflow, is a custom error message needed?
          secs (.setScale bd 0 BigDecimal/ROUND_DOWN)
          nanos (.longValueExact (.setScale (.multiply (.subtract bd secs) 1e9M) 0 BigDecimal/ROUND_DOWN))]
      (Duration/ofSeconds (.longValueExact secs) nanos))
    (catch NumberFormatException _
      (throw (IllegalArgumentException. "Parse error. SECOND INTERVAL string must contain a positive or negative integer or decimal.")))))

(defmethod expr/codegen-mono-call [:single-field-interval :utf8 :utf8 :int :int] [{:keys [args]}]
  (let [[_ unit precision fractional-precision] (map :literal args)]
    (ensure-interval-precision-valid precision)
    (when (= "SECOND" unit)
      (ensure-interval-fractional-precision-valid fractional-precision))

    (case unit
      "YEAR" {:return-type [:interval :year-month]
              :->call-code #(do `(PeriodDuration. (Period/ofYears (ensure-single-field-interval-int ~(first %))) Duration/ZERO))}
      "MONTH" {:return-type [:interval :year-month]
               :->call-code #(do `(PeriodDuration. (Period/ofMonths (ensure-single-field-interval-int ~(first %))) Duration/ZERO))}
      "DAY" {:return-type [:interval :day-time]
             :->call-code #(do `(PeriodDuration. (Period/ofDays (ensure-single-field-interval-int ~(first %))) Duration/ZERO))}
      "HOUR" {:return-type [:interval :day-time]
              :->call-code #(do `(PeriodDuration. Period/ZERO (Duration/ofHours (ensure-single-field-interval-int ~(first %)))))}
      "MINUTE" {:return-type [:interval :day-time]
                :->call-code #(do `(PeriodDuration. Period/ZERO (Duration/ofMinutes (ensure-single-field-interval-int ~(first %)))))}
      "SECOND" {:return-type [:interval :day-time]
                :->call-code #(do `(PeriodDuration. Period/ZERO (second-interval-fractional-duration ~(first %))))})))

(defn- parse-year-month-literal [s]
  (let [[match plus-minus part1 part2] (re-find #"^([-+]|)(\d+)\-(\d+)" s)]
    (when match
      (let [months (+ (* 12 (Integer/parseInt part1)) (Integer/parseInt part2))
            months' (if (= plus-minus "-") (- months) months)]
        (PeriodDuration. (Period/ofMonths months') Duration/ZERO)))))

(defn- fractional-secs-to->nanos ^long [fractional-secs]
  (if fractional-secs
    (let [num-digits (if (str/starts-with? fractional-secs "-")
                       (unchecked-dec-int (count fractional-secs))
                       (count fractional-secs))
          exp (- 9 num-digits)]
      (* (Math/pow 10 exp) (Long/parseLong fractional-secs)))
    0))

(defn- parse-day-to-second-literal [s unit1 unit2]
  (letfn [(negate-if-minus [plus-minus ^PeriodDuration pd]
            (if (= "-" plus-minus)
              (PeriodDuration.
               (.negated (.getPeriod pd))
               (.negated (.getDuration pd)))
              pd))]
    (case [unit1 unit2]
      ["DAY" "SECOND"]
      (let [re #"^([-+]|)(\d+) (\d+)\:(\d+):(\d+)(\.(\d+)){0,1}$"
            [match plus-minus day hour min sec _ fractional-secs] (re-find re s)]
        (when match
          (negate-if-minus
           plus-minus
           (PeriodDuration. (Period/of 0 0 (Integer/parseInt day))
                            (Duration/ofSeconds (+ (* 60 60 (Integer/parseInt hour))
                                                   (* 60 (Integer/parseInt min))
                                                   (Integer/parseInt sec))
                                                (fractional-secs-to->nanos fractional-secs))))))
      ["DAY" "MINUTE"]
      (let [re #"^([-+]|)(\d+) (\d+)\:(\d+)$"
            [match plus-minus day hour min] (re-find re s)]
        (when match
          (negate-if-minus
           plus-minus
           (PeriodDuration. (Period/of 0 0 (Integer/parseInt day))
                            (Duration/ofMinutes (+ (* 60 (Integer/parseInt hour))
                                                   (Integer/parseInt min)))))))

      ["DAY" "HOUR"]
      (let [re #"^([-+]|)(\d+) (\d+)$"
            [match plus-minus day hour] (re-find re s)]
        (when match
          (negate-if-minus
           plus-minus
           (PeriodDuration. (Period/of 0 0 (Integer/parseInt day))
                            (Duration/ofHours (Integer/parseInt hour))))))

      ["HOUR" "SECOND"]
      (let [re #"^([-+]|)(\d+)\:(\d+):(\d+)(\.(\d+)){0,1}$"
            [match plus-minus hour min sec _ fractional-secs] (re-find re s)]
        (when match
          (negate-if-minus
           plus-minus
           (PeriodDuration. Period/ZERO
                            (Duration/ofSeconds (+ (* 60 60 (Integer/parseInt hour))
                                                   (* 60 (Integer/parseInt min))
                                                   (Integer/parseInt sec))
                                                (fractional-secs-to->nanos fractional-secs))))))

      ["HOUR" "MINUTE"]
      (let [re #"^([-+]|)(\d+)\:(\d+)$"
            [match plus-minus hour min] (re-find re s)]
        (when match
          (negate-if-minus
           plus-minus
           (PeriodDuration. Period/ZERO
                            (Duration/ofMinutes (+ (* 60 (Integer/parseInt hour))
                                                   (Integer/parseInt min)))))))

      ["MINUTE" "SECOND"]
      (let [re #"^([-+]|)(\d+):(\d+)(\.(\d+)){0,1}$"
            [match plus-minus min sec _ fractional-secs] (re-find re s)]
        (when match
          (negate-if-minus
           plus-minus
           (PeriodDuration. Period/ZERO
                            (Duration/ofSeconds (+ (* 60 (Integer/parseInt min))
                                                   (Integer/parseInt sec))
                                                (fractional-secs-to->nanos fractional-secs)))))))))

(defn parse-multi-field-interval
  "This function is used to parse a 2 field interval literal into a PeriodDuration, e.g '12-03' YEAR TO MONTH."
  ^PeriodDuration [s unit1 unit2]
  ; This function overwhelming likely to be applied as a const-expr so not concerned about vectorized perf.

  ;; these rules are not strictly necessary
  ;; be are specified by SQL2011
  ;; if year, end field must be month.
  (when (= unit1 "YEAR")
    (when-not (= unit2 "MONTH")
      (throw (IllegalArgumentException. "If YEAR specified as the interval start field, MONTH must be the end field."))))

  ;; start cannot = month rule.
  (when (= unit1 "MONTH")
    (throw (IllegalArgumentException. "MONTH is not permitted as the interval start field.")))

  ;; less significance rule.
  (when-not (or (= unit1 "YEAR")
                (and (= unit1 "DAY") (#{"HOUR" "MINUTE" "SECOND"} unit2))
                (and (= unit1 "HOUR") (#{"MINUTE" "SECOND"} unit2))
                (and (= unit1 "MINUTE") (#{"SECOND"} unit2)))
    (throw (IllegalArgumentException. "Interval end field must have less significance than the start field.")))

  (or (if (= "YEAR" unit1)
        (parse-year-month-literal s)
        (parse-day-to-second-literal s unit1 unit2))
      (throw (IllegalArgumentException. "Cannot parse interval, incorrect format."))))

(defmethod expr/codegen-mono-call [:multi-field-interval :utf8 :utf8 :int :utf8 :int] [{:keys [args]}]
  (let [[_ unit1 precision unit2 fractional-precision] (map :literal args)]

    (ensure-interval-precision-valid precision)
    (when (= "SECOND" unit2)
      (ensure-interval-fractional-precision-valid fractional-precision))

    ;; TODO choose a more specific representation when possible
    {:return-type [:interval :month-day-nano]
     :->call-code (fn [[s & _]]
                    `(parse-multi-field-interval (expr/resolve-string ~s) ~unit1 ~unit2))}))

(defmethod expr/codegen-mono-call [:extract :utf8 :timestamp-tz] [{[{field :literal} _] :args}]
  {:return-type :i32
   :->call-code (fn [[_ ts-code]]
                  `(.get (.atOffset ^Instant (util/micros->instant ~ts-code) ZoneOffset/UTC)
                         ~(case field
                            "YEAR" `ChronoField/YEAR
                            "MONTH" `ChronoField/MONTH_OF_YEAR
                            "DAY" `ChronoField/DAY_OF_MONTH
                            "HOUR" `ChronoField/HOUR_OF_DAY
                            "MINUTE" `ChronoField/MINUTE_OF_HOUR)))})

(defmethod expr/codegen-mono-call [:extract :utf8 :date] [{[{field :literal} _] :args}]
  ;; FIXME this assumes date-unit :day
  {:return-type :i32
   :->call-code (fn [[_ epoch-day-code]]
                  (case field
                    ;; we could inline the math here, but looking at sources, there is some nuance.
                    "YEAR" `(.getYear (LocalDate/ofEpochDay ~epoch-day-code))
                    "MONTH" `(.getMonthValue (LocalDate/ofEpochDay ~epoch-day-code))
                    "DAY" `(.getDayOfMonth (LocalDate/ofEpochDay ~epoch-day-code))
                    "HOUR" `(int 0)
                    "MINUTE" `(int 0)))})

(defmethod expr/codegen-mono-call [:date-trunc :utf8 :timestamp-tz] [{[{field :literal} _] :args, [_ [_tstz _time-unit tz :as ts-type]] :arg-types}]
  ;; FIXME this assumes micros
  (let [zone-id-sym (gensym 'zone-id)]
    {:return-type ts-type
     :batch-bindings [[zone-id-sym `(ZoneId/of ~tz)]]
     :->call-code (fn [[_ x]]
                    `(util/instant->micros (-> (util/micros->instant ~x)
                                               (ZonedDateTime/ofInstant ~zone-id-sym)
                                               ~(case field
                                                  "YEAR" `(-> (.truncatedTo ChronoUnit/DAYS) (.withDayOfYear 1))
                                                  "MONTH" `(-> (.truncatedTo ChronoUnit/DAYS) (.withDayOfMonth 1))
                                                  `(.truncatedTo ~(case field
                                                                    "DAY" `ChronoUnit/DAYS
                                                                    "HOUR" `ChronoUnit/HOURS
                                                                    "MINUTE" `ChronoUnit/MINUTES
                                                                    "SECOND" `ChronoUnit/SECONDS
                                                                    "MILLISECOND" `ChronoUnit/MILLIS
                                                                    "MICROSECOND" `ChronoUnit/MICROS)))
                                               (.toInstant))))}))

(defmethod expr/codegen-mono-call [:date-trunc :utf8 :date] [{[{field :literal} _] :args, [_ [_date _date-unit :as date-type]] :arg-types}]
  ;; FIXME this assumes epoch-day
  {:return-type date-type
   :->call-code (fn [[_ epoch-day-code]]
                  (case field
                    "YEAR" `(-> ~epoch-day-code LocalDate/ofEpochDay (.withDayOfYear 1) .toEpochDay)
                    "MONTH" `(-> ~epoch-day-code LocalDate/ofEpochDay (.withDayOfMonth 1) .toEpochDay)
                    "DAY" epoch-day-code
                    "HOUR" epoch-day-code
                    "MINUTE" epoch-day-code))})

(defn- bound-precision ^long [^long precision]
  (-> precision (max 0) (min 9)))

(def ^:private precision-timeunits
  [:second
   :milli :milli :milli
   :micro :micro :micro
   :nano :nano :nano])

(def ^:private seconds-multiplier (mapv long [1e0 1e3 1e3 1e3 1e6 1e6 1e6 1e9 1e9 1e9]))
(def ^:private nanos-divisor (mapv long [1e9 1e6 1e6 1e6 1e3 1e3 1e3 1e0 1e0 1e0]))
(def ^:private precision-modulus (mapv long [1e0 1e2 1e1 1e0 1e2 1e1 1e0 1e2 1e1 1e0]))

(defn- truncate-for-precision [code precision]
  (let [^long modulus (precision-modulus precision)]
    (if (= modulus 1)
      code
      `(* ~modulus (quot ~code ~modulus)))))

(defn- current-timestamp [^long precision]
  (let [precision (bound-precision precision)
        zone-id (.getZone expr/*clock*)]
    {:return-type [:timestamp-tz (precision-timeunits precision) (str zone-id)]
     :->call-code (fn [_]
                    (-> `(long (let [inst# (.instant expr/*clock*)]
                                 (+ (* ~(seconds-multiplier precision) (.getEpochSecond inst#))
                                    (quot (.getNano inst#) ~(nanos-divisor precision)))))
                        (truncate-for-precision precision)))}))

(def ^:private default-time-precision 6)

(defmethod expr/codegen-mono-call [:current-timestamp] [_]
  (current-timestamp default-time-precision))

(defmethod expr/codegen-mono-call [:current-timestamp :int] [{[{precision :literal}] :args}]
  (assert (integer? precision) "precision must be literal for now")
  (current-timestamp precision))

(defmethod expr/codegen-mono-call [:current-date] [_]
  ;; TODO check the specs on this one - I read the SQL spec as being returned in local,
  ;; but Arrow expects Dates to be in UTC.
  ;; we then turn DateDays into LocalDates, which confuses things further.
  {:return-type [:date :day]
   :->call-code (fn [_]
                  `(long (-> (ZonedDateTime/ofInstant (.instant expr/*clock*) ZoneOffset/UTC)
                             (.toLocalDate)
                             (.toEpochDay))))})

(defn- current-time [^long precision]
  ;; TODO check the specs on this one - I read the SQL spec as being returned in local,
  ;; but Arrow expects Times to be in UTC.
  ;; we then turn times into LocalTimes, which confuses things further.
  (let [precision (bound-precision precision)]
    {:return-type [:time (precision-timeunits precision)]
     :->call-code (fn [_]
                    (-> `(long (-> (ZonedDateTime/ofInstant (.instant expr/*clock*) ZoneOffset/UTC)
                                   (.toLocalTime)
                                   (.toNanoOfDay)
                                   (quot ~(nanos-divisor precision))))
                        (truncate-for-precision precision)))}))

(defmethod expr/codegen-mono-call [:current-time] [_]
  (current-time default-time-precision))

(defmethod expr/codegen-mono-call [:current-time :int] [{[{precision :literal}] :args}]
  (assert (integer? precision) "precision must be literal for now")
  (current-time precision))

(defn- local-timestamp [^long precision]
  (let [precision (bound-precision precision)]
    {:return-type [:timestamp-local (precision-timeunits precision)]
     :->call-code (fn [_]
                    (-> `(long (let [ldt# (-> (ZonedDateTime/ofInstant (.instant expr/*clock*) (.getZone expr/*clock*))
                                              (.toLocalDateTime))]
                                 (+ (* (.toEpochSecond ldt# ZoneOffset/UTC) ~(seconds-multiplier precision))
                                    (quot (.getNano ldt#) ~(nanos-divisor precision)))))
                        (truncate-for-precision precision)))}))

(defmethod expr/codegen-mono-call [:local-timestamp] [_]
  (local-timestamp default-time-precision))

(defmethod expr/codegen-mono-call [:local-timestamp :num] [{[{precision :literal}] :args}]
  (assert (integer? precision) "precision must be literal for now")
  (local-timestamp precision))

(defn- local-time [^long precision]
  (let [precision (bound-precision precision)
        time-unit (precision-timeunits precision)]
    {:return-type [:time time-unit]
     :->call-code (fn [_]
                    (-> `(long (-> (ZonedDateTime/ofInstant (.instant expr/*clock*) (.getZone expr/*clock*))
                                   (.toLocalTime)
                                   (.toNanoOfDay)
                                   (quot ~(nanos-divisor precision))))
                        (truncate-for-precision precision)))}))

(defmethod expr/codegen-mono-call [:local-time] [_]
  (local-time default-time-precision))

(defmethod expr/codegen-mono-call [:local-time :int] [{[{precision :literal}] :args}]
  (assert (integer? precision) "precision must be literal for now")
  (local-time precision))

(defmethod expr/codegen-mono-call [:abs :num] [{[numeric-type] :arg-types}]
  {:return-type numeric-type
   :->call-code #(do `(Math/abs ~@%))})
