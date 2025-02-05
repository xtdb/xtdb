(ns xtdb.expression.temporal
  (:require [clojure.string :as str]
            [xtdb.error :as err]
            [xtdb.expression :as expr]
            [xtdb.expression.macro :as macro]
            [xtdb.time :as time]
            [xtdb.types :as types])
  (:import (java.nio ByteBuffer)
           (java.nio.charset StandardCharsets)
           (java.time Duration Instant LocalDate LocalDateTime LocalTime Period ZoneId ZoneOffset ZonedDateTime)
           (java.time.format DateTimeParseException)
           (java.time.temporal ChronoField ChronoUnit Temporal)
           (org.apache.arrow.vector PeriodDuration)
           [xtdb DateTruncator]
           (xtdb.arrow ListValueReader ValueBox ValueReader)
           (xtdb.time LocalDateTimeUtil)))

(set! *unchecked-math* :warn-on-boxed)

;;;; units

(defn multiply-for-conversion ^long [^long ts ^long quotient]
  (Math/multiplyExact ts quotient))

(defn divide-for-conversion ^long [^long ts ^long quotient]
  (quot ts quotient))

(defn- with-conversion [form from-unit to-unit]
  (if (= from-unit to-unit)
    form
    (let [from-hz (types/ts-units-per-second from-unit)
          to-hz (types/ts-units-per-second to-unit)]
      (if (> to-hz from-hz)
        `(multiply-for-conversion ~form ~(quot to-hz from-hz))
        `(divide-for-conversion ~form ~(quot from-hz to-hz))))))

(defn- with-arg-unit-conversion [unit1 unit2 ->ret-type ->call-code]
  (if (= unit1 unit2)
    {:return-type (->ret-type unit1), :->call-code ->call-code}

    (let [res-unit (types/smallest-ts-unit unit1 unit2)]
      {:return-type (->ret-type res-unit),
       :->call-code (fn [[arg1 arg2]]
                      (->call-code [(with-conversion arg1 unit1 res-unit) (with-conversion arg2 unit2 res-unit)]))})))

(defn- with-first-arg-unit-conversion [arg-unit unit-lower-bound ->ret-type ->call-code]
  (if (= arg-unit unit-lower-bound)
    {:return-type (->ret-type arg-unit),
     :->call-code #(->call-code arg-unit %)}

    (let [res-unit (types/smallest-ts-unit arg-unit unit-lower-bound)]
      {:return-type (->ret-type res-unit),
       :->call-code (fn [[arg1 & args]]
                      (->call-code res-unit (into [(with-conversion arg1 arg-unit res-unit)] args)))})))

(defn- ensure-interval-precision-valid [^long precision]
  (cond
    (< precision 1)
    (throw (err/illegal-arg :xtdb.expression/invalid-interval-precision
                            {::err/message "The minimum leading field precision is 1."
                             :precision precision}))

    (< 8 precision)
    (throw (err/illegal-arg :xtdb.expression/invalid-interval-precision
                            {::err/message "The maximum leading field precision is 8."
                             :precision precision}))))

(defn- ensure-interval-fractional-precision-valid [^long fractional-precision]
  (cond
    (< fractional-precision 0)
    (throw (err/illegal-arg :xtdb.expression/invalid-interval-fractional-precision
                            {::err/message "The minimum fractional seconds precision is 0."
                             :fractional-precision fractional-precision}))

    (< 9 fractional-precision)
    (throw (err/illegal-arg :xtdb.expression/invalid-interval-fractional-precision
                            {::err/message "The maximum fractional seconds precision is 9."
                             :fractional-precision fractional-precision}))))

(defn- ensure-interval-units-valid [unit1 unit2]
  ;; This function overwhelming likely to be applied as a const-expr so not concerned about vectorized perf.
  ;; these rules are not strictly necessary but are specified by SQL2011
  (letfn [(->iae [msg]
                 (err/illegal-arg :xtdb.expression/invalid-interval-units
                                  {::err/message msg
                                   :start-unit unit1
                                   :end-unit unit2}))]
    (when (and (= unit1 "YEAR") (not= unit2 "MONTH"))
      (throw (->iae "If YEAR specified as the interval start field, MONTH must be the end field.")))

    (when (= unit1 "MONTH")
      (throw (->iae "MONTH is not permitted as the interval start field.")))

    ;; less significance rule.
    (when-not (or (= unit1 "YEAR")
                  (and (= unit1 "DAY") (#{"HOUR" "MINUTE" "SECOND"} unit2))
                  (and (= unit1 "HOUR") (#{"MINUTE" "SECOND"} unit2))
                  (and (= unit1 "MINUTE") (#{"SECOND"} unit2)))
      (throw (->iae "Interval end field must have less significance than the start field.")))))

(defn- ts->inst [form ts-unit]
  (case ts-unit
    :second `(Instant/ofEpochSecond ~form)
    :milli `(Instant/ofEpochMilli ~form)
    :micro `(time/micros->instant ~form)
    :nano `(time/nanos->instant ~form)))

(defn- inst->ts [form ts-unit]
  (case ts-unit
    :second `(.getEpochSecond ~form)
    :milli `(.toEpochMilli ~form)
    :micro `(time/instant->micros ~form)
    :nano `(time/instant->nanos ~form)))

(defn- ts->zdt [form ts-unit tz-sym]
  `(ZonedDateTime/ofInstant ~(ts->inst form ts-unit) ~tz-sym))

(defn epoch-second-from-zdt ^long [^ZonedDateTime zdt]
  (.toEpochSecond zdt))

(defn nano-from-zdt ^long [^ZonedDateTime zdt]
  (.getNano zdt))

(defn- zdt->ts [form ts-unit]
  (if (= ts-unit :second)
    `(epoch-second-from-zdt ~form)
    `(let [form# ~form]
       (Math/addExact (Math/multiplyExact (epoch-second-from-zdt form#) ~(types/ts-units-per-second ts-unit))
                      (quot (nano-from-zdt form#) ~(quot (types/ts-units-per-second :nano) (types/ts-units-per-second ts-unit)))))))

(defn- ldt->ts [form ts-unit]
  (case ts-unit
    :second `(LocalDateTimeUtil/getAsSeconds ~form)
    :milli `(LocalDateTimeUtil/getAsMillis ~form)
    :micro `(LocalDateTimeUtil/getAsMicros ~form)
    :nano `(LocalDateTimeUtil/getAsNanos ~form)))

(defn- ts->ldt [form ts-unit]
  (case ts-unit
    :second `(LocalDateTimeUtil/fromSeconds ~form)
    :milli `(LocalDateTimeUtil/fromMillis ~form)
    :micro `(LocalDateTimeUtil/fromMicros ~form)
    :nano `(LocalDateTimeUtil/fromNanos ~form)))

;;;; `CAST`

(defmethod expr/codegen-cast [:date :date] [{:keys [target-type]}]
  ;; date-days and date-millis are both just represented as days throughout the EE,
  ;; conversion is done when we read from/write to the vector.
  {:return-type target-type, :->call-code first})

(defmethod expr/codegen-cast [:time-local :time-local] [{[_ src-tsunit] :source-type, [_ tgt-tsunit :as target-type] :target-type}]
  {:return-type target-type,
   :->call-code (comp #(with-conversion % src-tsunit tgt-tsunit) first)})

(defmethod expr/codegen-cast [:timestamp-local :timestamp-local] [{[_ src-tsunit] :source-type, [_ tgt-tsunit :as target-type] :target-type}]
  {:return-type target-type
   :->call-code (comp #(with-conversion % src-tsunit tgt-tsunit) first)})

(defmethod expr/codegen-cast [:timestamp-tz :timestamp-tz] [{[_ src-tsunit _] :source-type, [_ tgt-tsunit _ :as target-type] :target-type}]
  {:return-type target-type
   :->call-code (comp #(with-conversion % src-tsunit tgt-tsunit) first)})

(defmethod expr/codegen-cast [:duration :duration] [{[_ src-tsunit] :source-type, [_ tgt-tsunit :as target-type] :target-type}]
  {:return-type target-type, :->call-code (comp #(with-conversion % src-tsunit tgt-tsunit) first)})

(defmethod expr/codegen-cast [:date :timestamp-local] [{[_ tgt-tsunit :as target-type] :target-type}]
  {:return-type target-type
   :->call-code (fn [[dt]]
                  `(-> (LocalDate/ofEpochDay ~dt)
                       (.atStartOfDay ZoneOffset/UTC)
                       (.toEpochSecond)
                       (Math/multiplyExact ~(types/ts-units-per-second tgt-tsunit))))})

(defmethod expr/codegen-cast [:date :timestamp-tz] [{[_ tgt-tsunit _tgt-tz :as target-type] :target-type}]
  {:return-type target-type
   :->call-code (fn [[dt]]
                  (-> `(-> (LocalDate/ofEpochDay ~dt)
                           (.atStartOfDay expr/*default-tz*)
                           (.toInstant))
                      (inst->ts tgt-tsunit)))})

(defmethod expr/codegen-cast [:time-local :timestamp-local] [{[_ src-tsunit] :source-type, [_ tgt-tsunit :as target-type] :target-type}]
  {:return-type target-type
   :->call-code (fn [[tm]]
                  (-> `(LocalDateTime/of (LocalDate/ofInstant (expr/current-time) expr/*default-tz*)
                                         (LocalTime/ofNanoOfDay ~(with-conversion tm src-tsunit :nano)))
                      (ldt->ts tgt-tsunit)))})

(defmethod expr/codegen-cast [:time-local :timestamp-tz] [{[_ src-tsunit] :source-type, [_ tgt-tsunit _tgt-tz :as target-type] :target-type}]
  {:return-type target-type
   :->call-code (fn [[tm]]
                  (-> `(-> (ZonedDateTime/of (LocalDate/ofInstant (expr/current-time) expr/*default-tz*)
                                             (LocalTime/ofNanoOfDay ~(with-conversion tm src-tsunit :nano))
                                             expr/*default-tz*)
                           (.toInstant))
                      (inst->ts tgt-tsunit)))})

(defmethod expr/codegen-cast [:timestamp-local :date] [{[_ src-tsunit] :source-type, :keys [target-type]}]
  {:return-type target-type
   :->call-code (fn [[ts]]
                  `(-> ~(ts->ldt ts src-tsunit)
                       (.toLocalDate)
                       (.toEpochDay)))})

(defmethod expr/codegen-cast [:timestamp-local :time-local] [{[_ src-tsunit _] :source-type, [_ tgt-tsunit _ :as target-type] :target-type}]
  {:return-type target-type,
   :->call-code (fn [[ts]]
                  (-> `(-> ~(ts->ldt ts src-tsunit)
                           (.toLocalTime)
                           (.toNanoOfDay))
                      (with-conversion :nano tgt-tsunit)))})

(defmethod expr/codegen-cast [:timestamp-local :timestamp-tz] [{[_ src-tsunit] :source-type, [_ tgt-tsunit _tgt-tz :as target-type] :target-type}]
  {:return-type target-type,
   :->call-code (fn [[ts]]
                  (-> `(-> ~(ts->ldt ts src-tsunit)
                           (.atZone expr/*default-tz*)
                           (.toInstant))
                      (inst->ts tgt-tsunit)))})

(defmethod expr/codegen-cast [:timestamp-tz :date] [{[_ src-tsunit src-tz] :source-type, :keys [target-type]}]
  (let [src-tz-sym (gensym 'src-tz)]
    {:return-type target-type,
     :batch-bindings [[src-tz-sym `(ZoneId/of ~src-tz)]]
     :->call-code (fn [[tstz]]
                    `(-> ~(ts->zdt tstz src-tsunit src-tz-sym)
                         (.withZoneSameInstant expr/*default-tz*)
                         (.toLocalDate)
                         (.toEpochDay)))}))

(defmethod expr/codegen-cast [:timestamp-tz :time-local] [{[_ src-tsunit src-tz] :source-type, [_ tgt-tsunit :as target-type] :target-type}]
  (let [src-tz-sym (gensym 'src-tz)]
    {:return-type target-type,
     :batch-bindings [[src-tz-sym `(ZoneId/of ~src-tz)]]
     :->call-code (fn [[tstz]]
                    (-> `(-> ~(ts->zdt tstz src-tsunit src-tz-sym)
                             (.withZoneSameInstant expr/*default-tz*)
                             (.toLocalTime)
                             (.toNanoOfDay))
                        (with-conversion :nano tgt-tsunit)))}))

(defmethod expr/codegen-cast [:timestamp-tz :timestamp-local] [{[_ src-tsunit src-tz] :source-type, [_ tgt-tsunit :as target-type] :target-type}]
  (let [src-tz-sym (gensym 'src-tz)]
    {:return-type target-type,
     :batch-bindings [[src-tz-sym `(ZoneId/of ~src-tz)]]
     :->call-code (fn [[tstz]]
                    (-> `(-> ~(ts->zdt tstz src-tsunit src-tz-sym)
                             (.withZoneSameInstant expr/*default-tz*)
                             (.toLocalDateTime))
                        (ldt->ts tgt-tsunit)))}))

(defmethod expr/codegen-cast [:time-local :duration] [{[_ src-tsunit] :source-type, [_ tgt-tsunit :as target-type] :target-type}]
  {:return-type target-type, :->call-code (comp #(with-conversion % src-tsunit tgt-tsunit) first)})

(defn- ensure-fractional-precision-valid [^long fractional-precision]
  (cond
    (< fractional-precision 0)
    (throw (err/illegal-arg :xtdb.expression/invalid-fractional-precision
                            {::err/message "The minimum fractional seconds precision is 0."
                             :fractional-precision fractional-precision}))

    (< 9 fractional-precision)
    (throw (err/illegal-arg :xtdb.expression/invalid-fractional-precision
                            {::err/message "The maximum fractional seconds precision is 9."
                             :fractional-precision fractional-precision}))))

(defn parse-with-error-handling
  ([date-type parse-fn s]
   (parse-with-error-handling date-type parse-fn nil s))
  ([date-type parse-fn fallback-parse-fn s]
   (or (try
         (parse-fn s)
         (catch DateTimeParseException _))
       (when fallback-parse-fn
         (try
           (fallback-parse-fn s)
           (catch DateTimeParseException _)))
       (throw (err/runtime-err :xtdb.expression/invalid-temporal-string
                               {::err/message (format "String '%s' has invalid format for type %s" s date-type)})))))

(defn alter-precision [^long precision ^Temporal temporal]
  (if (= precision 0)
    (.with temporal ChronoField/NANO_OF_SECOND 0)
    (.with temporal ChronoField/NANO_OF_SECOND (let [nanos (.get temporal ChronoField/NANO_OF_SECOND)
                                                     factor (Math/pow 10 (- 9 precision))]
                                                 (* (Math/floor (/ nanos factor)) factor)))))

(defn gen-alter-precision [precision]
  (if precision (list `(alter-precision ~precision)) '()))

(defn parse-ts-local [ts-str]
  (parse-with-error-handling "timestamp without timezone"
                             (fn [s]
                               (let [res (time/parse-sql-timestamp-literal s)]
                                 (when (instance? LocalDateTime res)
                                   res)))
                             ts-str))

(defmethod expr/codegen-cast [:utf8 :timestamp-local] [{[_ tgt-tsunit :as target-type] :target-type {:keys [precision]} :cast-opts}]
  (when precision (ensure-fractional-precision-valid precision))
  {:return-type target-type
   :->call-code (fn [[s]]
                  (-> `(->> (expr/resolve-string ~s)
                            parse-ts-local
                            ~@(gen-alter-precision precision))
                      (ldt->ts tgt-tsunit)))})

(defn parse-tstz [ts-str]
  (parse-with-error-handling "timestamp with timezone"
                             (fn [s]
                               (let [res (time/parse-sql-timestamp-literal s)]
                                 (when (instance? ZonedDateTime res)
                                   res)))
                             ts-str))

(defmethod expr/codegen-cast [:utf8 :timestamp-tz] [{[_ tgt-tsunit :as target-type] :target-type  {:keys [precision]} :cast-opts}]
  (when precision (ensure-fractional-precision-valid precision))
  {:return-type target-type
   :->call-code (fn [[s]]
                  (-> `(->> (expr/resolve-string ~s)
                            (parse-with-error-handling "timestamp with timezone" parse-tstz)
                            ~@(gen-alter-precision precision))
                      (zdt->ts tgt-tsunit)))})

(defn local-date->epoch-day ^long [^LocalDate d]
  (.toEpochDay d))

(defmethod expr/codegen-cast [:utf8 :date] [{:keys [target-type]}]
  ;; FIXME this assumes date-unit :day
  {:return-type target-type
   :->call-code (fn [[s]]
                  `(->> (expr/resolve-string ~s)
                        (parse-with-error-handling "date" #(LocalDate/parse %))
                        (local-date->epoch-day)))})

(defn local-time->nano ^long [^LocalTime t]
  (.toNanoOfDay t))

(defmethod expr/codegen-cast [:utf8 :time-local] [{[_ tgt-tsunit :as target-type] :target-type  {:keys [precision]} :cast-opts}]
  (when precision (ensure-fractional-precision-valid precision))
  {:return-type target-type
   :->call-code (fn [[s]]
                  (-> `(->> (expr/resolve-string ~s)
                            (parse-with-error-handling "time without timezone" #(LocalTime/parse %))
                            ~@(gen-alter-precision precision)
                            (local-time->nano))
                      (with-conversion :nano tgt-tsunit)))})

(defn alter-duration-precision ^Duration [^long precision ^Duration duration]
  (if (= precision 0)
    (.withNanos duration 0)
    (.withNanos duration (let [nanos (.getNano duration)
                               factor (Math/pow 10 (- 9 precision))]
                           (* (Math/floor (/ nanos factor)) factor)))))

(defn duration->nano ^long [^Duration d]
  (.toNanos d))

(defn string->duration [input]
  (when-let [[_ amount unit] (re-matches #"^'?(?i)([0-9.]+)\s+(microsecond|millisecond|second|minute|hour|day|week|month|year)s?'?$"
                                         (or input ""))]
    (let [amt (Double/parseDouble amount)
          millis (long
                  (case (str/lower-case unit)
                    "microsecond" (/ amt 1000)
                    "millisecond" amt
                    "second" (* 1000 amt)
                    "minute" (* 60 1000 amt)
                    "hour" (* 3600 1000 amt)
                    "day" (* 86400 1000 amt)
                    "week" (* 604800 1000 amt)
                    "month" (* 2628000 1000 amt)
                    "year" (* 31536000 1000 amt)))]
      (Duration/ofMillis millis))))

(defmethod expr/codegen-cast [:utf8 :duration] [{[_ tgt-tsunit :as target-type] :target-type  {:keys [precision]} :cast-opts}]
  (when precision (ensure-fractional-precision-valid precision))
  {:return-type target-type
   :->call-code (fn [[s]]
                  (-> `(->> (expr/resolve-string ~s)
                            (parse-with-error-handling "duration" #(Duration/parse %) string->duration)
                            ~@(if precision (list `(alter-duration-precision ~precision)) '())
                            (duration->nano))
                      (with-conversion :nano tgt-tsunit)))})

(defn string->byte-buffer [^String s]
  (ByteBuffer/wrap (.getBytes s StandardCharsets/UTF_8)))

(defmethod expr/codegen-cast [:timestamp-local :utf8] [{[_ts ts-unit] :source-type}]
  {:return-type :utf8
   :->call-code (fn [[ts]]
                  `(-> ~(ts->ldt ts ts-unit)
                       (.toString)
                       (string->byte-buffer)))})

(defmethod expr/codegen-cast [:timestamp-tz :utf8] [{[_ts ts-unit tz] :source-type}]
  (let [zone-id-sym (gensym 'zone-id)]
    {:return-type :utf8
     :batch-bindings [[zone-id-sym (ZoneId/of tz)]]
     :->call-code (fn [[ts]]
                    `(-> ~(ts->zdt ts ts-unit zone-id-sym)
                         (.toString)
                         (string->byte-buffer)))}))

(defmethod expr/codegen-cast [:date :utf8] [_]
  ;; FIXME this assumes date-unit :day
  {:return-type :utf8
   :->call-code (fn [[x]]
                  `(-> (LocalDate/ofEpochDay ~x)
                       (.toString)
                       (string->byte-buffer)))})

(defmethod expr/codegen-cast [:time-local :utf8] [{[_ t-unit] :source-type}]
  {:return-type :utf8
   :->call-code (fn [[t]]
                  `(-> ~(with-conversion t t-unit :nano)
                       (LocalTime/ofNanoOfDay)
                       (.toString)
                       (string->byte-buffer)))})

(defmethod expr/codegen-cast [:duration :utf8] [{[_ t-unit] :source-type}]
  {:return-type :utf8
   :->call-code (fn [[t]]
                  `(-> ~(with-conversion t t-unit :nano)
                       (Duration/ofNanos)
                       (.toString)
                       (string->byte-buffer)))})

;; TODO - finish this
(defmethod expr/parse-list-form  'cast-tstz [[_ expr opts] env]
  (let [unit (or (:unit opts) :micro)]
    {:op :call
     :f :cast
     :args [(expr/form->expr expr env)]
     :target-type [:timestamp-tz unit (str expr/*default-tz*)]
     :cast-opts opts}))

(defn mdn-interval->duration [^PeriodDuration x]
  (let [period (.getPeriod x)]
    (if (> (.toTotalMonths period) 0)
      (throw (err/runtime-err :xtdb.expression/cannot-cast-mdn-interval-with-months
                              {::err/message "Cannot cast month-day-nano intervals when month component is non-zero."}))
      (.plusDays (.getDuration x) (.getDays period)))))

(defmethod expr/codegen-cast [:interval :duration] [{[_ iunit] :source-type [_ tgt-tsunit :as target-type] :target-type {:keys [precision]} :cast-opts}]
  (when (not= iunit :month-day-nano)
    (throw (UnsupportedOperationException. (format "Cannot cast a %s interval to a duration" (name iunit)))))

  (when precision (ensure-fractional-precision-valid precision))

  {:return-type target-type
   :->call-code (fn [[x]]
                  (-> `(->> (mdn-interval->duration ~x)
                            ~@(if precision (list `(alter-duration-precision ~precision)) '())
                            (duration->nano))
                      (with-conversion :nano tgt-tsunit)))})


(defn duration->mdn-interval [^Duration d]
  (PeriodDuration. Period/ZERO d))

;; Used for DAY as lone start-field
(defn ->day-mdn-interval [^Period p ^Duration d]
  (PeriodDuration. (Period/ofDays (+ (.getDays p) (.toDays d))) Duration/ZERO))

;; Used for DAY as start-field and HOUR as end-field
(defn ->day-hour-mdn-interval [^Period p ^Duration d]
  (PeriodDuration. (Period/ofDays (+ (.getDays p) (.toDays d))) (Duration/ofHours (rem (.toHours d) 24))))

;; Used for DAY as start-field and MINUTE as end-field
(defn ->day-minute-mdn-interval [^Period p ^Duration d]
  (PeriodDuration. (Period/ofDays (+ (.getDays p) (.toDays d))) (Duration/ofMinutes (rem (.toMinutes d) 1440))))

;; Used for DAY as start-field and SECOND as end-field
(defn ->day-second-mdn-interval [^Period p ^Duration d ^long fractional-precision]
  (let [^Duration altered-precision-duration (alter-duration-precision fractional-precision d)]
    (PeriodDuration. (Period/ofDays (+ (.getDays p) (.toDays d)))
                     (.minusDays altered-precision-duration (.toDays d)))))

;; Used for HOUR as lone start-field or as end-field
(defn ->hour-mdn-interval [^Period p ^Duration d]
  (PeriodDuration. Period/ZERO (.plusDays (Duration/ofHours (.toHours d)) (.getDays p))))

;; Used for MINUTE as lone start-field or as end-field
(defn ->minute-mdn-interval [^Period p ^Duration d]
  (PeriodDuration. Period/ZERO (.plusDays (Duration/ofMinutes (.toMinutes d)) (.getDays p))))

;; Used for SECOND as lone start-field or as end-field
(defn ->second-mdn-interval [^Period p ^Duration d ^long fractional-precision]
  (let [^Duration altered-precision-duration (alter-duration-precision fractional-precision d)]
    (PeriodDuration. Period/ZERO (.plusDays altered-precision-duration (.getDays p)))))

(defn normalize-interval-to-mdn-iq [^PeriodDuration pd {:keys [start-field end-field fractional-precision]}]
  (let [period (.getPeriod pd)
        duration (.getDuration pd)]

    (when (> (.toTotalMonths period) 0)
      (throw (throw (err/runtime-err :xtdb.expression/cannot-normalize-mdn-interval-with-months
                                     {::err/message "Cannot normalize month-day-nano interval with non-zero month component"}))))

    (case [start-field end-field]
      ["DAY" nil] (->day-mdn-interval period duration)
      ["DAY" "HOUR"] (->day-hour-mdn-interval period duration)
      ["DAY" "MINUTE"] (->day-minute-mdn-interval period duration)
      ["DAY" "SECOND"] (->day-second-mdn-interval period duration fractional-precision)
      ["HOUR" nil] (->hour-mdn-interval period duration)
      ["HOUR" "MINUTE"] (->minute-mdn-interval period duration)
      ["HOUR" "SECOND"] (->second-mdn-interval period duration fractional-precision)
      ["MINUTE" nil] (->minute-mdn-interval period duration)
      ["MINUTE" "SECOND"] (->second-mdn-interval period duration fractional-precision)
      ["SECOND" nil] (->second-mdn-interval period duration fractional-precision))))

(defn gen-normalize-call [interval-qualifier]
  (if interval-qualifier (list `(normalize-interval-to-mdn-iq ~interval-qualifier)) '()))

(defmethod expr/codegen-cast [:duration :interval]
  [{[_ d-unit] :source-type {:keys [start-field end-field leading-precision fractional-precision] :as interval-qualifier} :cast-opts}]

  (when interval-qualifier
    (when (or (= "YEAR" start-field) (= "MONTH" start-field))
      (throw (UnsupportedOperationException. "Cannot cast a duration to a year-month interval")))
    (ensure-interval-precision-valid leading-precision)
    (when end-field (ensure-interval-units-valid start-field end-field))
    (when (= "SECOND" end-field) (ensure-interval-fractional-precision-valid fractional-precision)))

  {:return-type [:interval :month-day-nano]
   :->call-code (fn [[d]]
                  `(-> ~(with-conversion d d-unit :nano)
                       (Duration/ofNanos)
                       (duration->mdn-interval)
                       ~@(gen-normalize-call interval-qualifier)))})


(defn normalize-interval-to-ym-iq [^PeriodDuration pd {:keys [start-field end-field]}]
  (let [period (.getPeriod pd)
        total-months-in-period (.toTotalMonths period)
        years-in-period (quot total-months-in-period 12)]
    (case [start-field end-field]
      ["YEAR" nil] (PeriodDuration. (Period/ofYears years-in-period) Duration/ZERO)
      ["YEAR" "MONTH"] (PeriodDuration. (Period/of years-in-period (mod total-months-in-period 12) 0) Duration/ZERO)
      ["MONTH" nil] (PeriodDuration. (Period/ofMonths total-months-in-period) Duration/ZERO))))

(defmethod expr/codegen-cast [:interval :interval] [{source-type :source-type interval-qualifier :cast-opts}]
  (if (empty? interval-qualifier)
    {:return-type source-type, :->call-code first}
    (let [{:keys [start-field end-field leading-precision fractional-precision]} interval-qualifier
          ym-cast? (some? (#{"YEAR" "MONTH"} start-field))]
      ;; Assertions against precision and units are not strictly necessary but are specified by SQL2011
      (ensure-interval-precision-valid leading-precision)
      (when end-field (ensure-interval-units-valid start-field end-field))
      (when (= "SECOND" end-field) (ensure-interval-fractional-precision-valid fractional-precision))
      ;; Assert that we are not casting year-month intervals to month-day-nano/day-time intervals and vice versa
      (when (and ym-cast? (not= source-type [:interval :year-month])) (throw (UnsupportedOperationException. "Cannot cast a non Year-Month interval with a Year-Month interval qualifier")))
      (when (and (not ym-cast?) (= source-type [:interval :year-month])) (throw (UnsupportedOperationException. "Cannot cast a Year-Month interval with a non Year-Month interval qualifier")))

      (if ym-cast?
        {:return-type [:interval :year-month], :->call-code (fn [[pd]] `(normalize-interval-to-ym-iq ~pd ~interval-qualifier))}
        {:return-type source-type, :->call-code (fn [[pd]] `(normalize-interval-to-mdn-iq ~pd ~interval-qualifier))}))))

(defmethod expr/codegen-cast [:int :interval] [{{:keys [start-field end-field]} :cast-opts}]
  (when end-field (throw (err/illegal-arg :xtdb.expression/attempting-to-cast-int-to-multi-field-interval
                                          {::err/message "Cannot cast integer to a multi field interval"
                                           :start-field start-field
                                           :end-field end-field})))
  {:return-type (if (#{"YEAR" "MONTH"} start-field)
                  [:interval :year-month]
                  [:interval :month-day-nano])
   :->call-code (fn [[x]]
                  (case start-field
                    "YEAR" `(PeriodDuration. (Period/ofYears ~x) Duration/ZERO)
                    "MONTH" `(PeriodDuration. (Period/ofMonths ~x) Duration/ZERO)
                    "DAY" `(PeriodDuration. (Period/ofDays ~x) Duration/ZERO)
                    "HOUR" `(PeriodDuration. Period/ZERO (Duration/ofHours ~x))
                    "MINUTE" `(PeriodDuration. Period/ZERO (Duration/ofMinutes ~x))
                    "SECOND" `(PeriodDuration. Period/ZERO (Duration/ofSeconds ~x))))})

(defn iso8601-string-to-period-duration [iso-string]
  (try
    (let [[_ p d] (re-find #"P((?:-?\d+Y)?(?:-?\d+M)?(?:-?\d+W)?(?:-?\d+D)?)?T?((?:-?\d+H)?(?:-?\d+M)?(?:-?\d+\.?\d*?S)?)?" iso-string)]
      (PeriodDuration. (if (str/blank? p) Period/ZERO (Period/parse (str "P" p)))
                       (if (str/blank? d) Duration/ZERO (Duration/parse (str "PT" d)) )))
    (catch DateTimeParseException e
      (throw (err/runtime-err :xtdb.expression/invalid-iso-interval-string
                              {::err/message (format "Invalid ISO 8601 string '%s' for interval" iso-string)
                               ::err/parse-exception e})))))

(defn ->single-field-interval-call [{{:keys [start-field leading-precision fractional-precision]} :cast-opts}]
  (let [expr {:op :call
              :f :single_field_interval
              :args [{} {:literal start-field} {:literal leading-precision} {:literal fractional-precision}]
              :arg-types [:utf8 :utf8 :int :int]}]
    (expr/codegen-call expr)))

(defn ->multi-field-interval-call [{{:keys [start-field end-field leading-precision fractional-precision]} :cast-opts}]
  (let [expr {:op :call
              :f :multi_field_interval
              :args [{} {:literal start-field} {:literal leading-precision} {:literal end-field} {:literal fractional-precision}]
              :arg-types [:utf8 :utf8 :int :utf8 :int]}]
    (expr/codegen-call expr)))

(defn duration-from-seconds [^double seconds unit]
  (long (* seconds (types/ts-units-per-second unit))))

(defmethod expr/codegen-cast [:f64 :duration] [{[_duration unit] :target-type}]
  {:return-type [:duration unit]
   :->call-code (fn [[x]]
                  `(duration-from-seconds ~x ~unit))})

(defmethod expr/codegen-cast [:utf8 :interval] [{interval-opts :cast-opts :as expr}]
  (if (empty? interval-opts)
    {:return-type [:interval :month-day-nano]
     :->call-code (fn [[x]]
                    `(iso8601-string-to-period-duration (expr/resolve-string ~x)))}

    (if (nil? (:end-field interval-opts))
      (->single-field-interval-call expr)
      (->multi-field-interval-call expr))))

(defn interval->iso-string [^PeriodDuration x]
  (let [period-str (.toString (.getPeriod x))
        duration-str (.toString (.getDuration x))]
    (str period-str (str/replace duration-str #"^PT" "T"))))

(defmethod expr/codegen-cast [:interval :utf8] [_]
  {:return-type :utf8
   :->call-code (fn [[pd]]
                  `(-> (interval->iso-string ~pd)
                       (string->byte-buffer)))})

;;;; SQL:2011 Operations involving datetimes and intervals
(defn- recall-with-cast2
  ([expr cast1 cast2] (recall-with-cast2 expr cast1 cast2 expr/codegen-call))

  ([{[t1 t2] :arg-types, :as expr} cast1 cast2 f]
   (let [{ret1 :return-type, bb1 :batch-bindings, ->cc1 :->call-code} (expr/codegen-cast {:source-type t1, :target-type cast1})
         {ret2 :return-type, bb2 :batch-bindings, ->cc2 :->call-code} (expr/codegen-cast {:source-type t2, :target-type cast2})
         {ret :return-type, bb :batch-bindings, ->cc :->call-code} (f (assoc expr :arg-types [ret1 ret2]))]
     {:return-type ret
      :batch-bindings (concat bb1 bb2 bb)
      :->call-code (fn [[a1 a2]]
                     (->cc [(->cc1 [a1]) (->cc2 [a2])]))})))

(defn- recall-with-cast3
  ([expr cast1 cast2 cast3] (recall-with-cast3 expr cast1 cast2 cast3 expr/codegen-call))

  ([{[t1 t2 t3] :arg-types, :as expr} cast1 cast2 cast3 f]
   (let [{ret1 :return-type, bb1 :batch-bindings, ->cc1 :->call-code} (expr/codegen-cast {:source-type t1, :target-type cast1})
         {ret2 :return-type, bb2 :batch-bindings, ->cc2 :->call-code} (expr/codegen-cast {:source-type t2, :target-type cast2})
         {ret3 :return-type, bb3 :batch-bindings, ->cc3 :->call-code} (expr/codegen-cast {:source-type t3, :target-type cast3})
         {ret :return-type, bb :batch-bindings, ->cc :->call-code} (f (assoc expr :arg-types [ret1 ret2 ret3]))]
     {:return-type ret
      :batch-bindings (concat bb1 bb2 bb3 bb)
      :->call-code (fn [[a1 a2 a3]]
                     (->cc [(->cc1 [a1]) (->cc2 [a2]) (->cc3 [a3])]))})))

(defn- recall-with-flipped-args [expr]
  (let [{ret :return-type, bb :batch-bindings, ->cc :->call-code} (expr/codegen-call (update expr :arg-types (comp vec rseq)))]
    {:return-type ret, :batch-bindings bb, :->call-code (comp ->cc vec rseq)}))

;;; addition

(defmethod expr/codegen-call [:+ :date :time-local] [{[_ [_ time-unit :as arg2]] :arg-types, :as expr}]
  (-> expr (recall-with-cast2 [:timestamp-local time-unit] arg2)))

(defmethod expr/codegen-call [:+ :timestamp-local :time-local] [{[[_ ts-unit], [_ time-unit]] :arg-types}]
  (with-arg-unit-conversion ts-unit time-unit
    #(do [:timestamp-local %]) #(do `(Math/addExact ~@%))))

(defmethod expr/codegen-call [:+ :timestamp-tz :time-local] [{[[_ ts-unit tz], [_time time-unit]] :arg-types}]
  (with-arg-unit-conversion ts-unit time-unit
    #(do [:timestamp-tz % tz]) #(do `(Math/addExact ~@%))))

(defmethod expr/codegen-call [:+ :date :duration] [{[_ [_ dur-unit :as arg2]] :arg-types, :as expr}]
  (-> expr (recall-with-cast2 [:timestamp-local dur-unit] arg2)))

(defmethod expr/codegen-call [:+ :timestamp-local :duration] [{[[_ ts-unit], [_ dur-unit]] :arg-types}]
  (with-arg-unit-conversion ts-unit dur-unit
    #(do [:timestamp-local %]) #(do `(Math/addExact ~@%))))

(defmethod expr/codegen-call [:+ :timestamp-tz :duration] [{[[_ ts-unit tz], [_ dur-unit]] :arg-types}]
  (with-arg-unit-conversion ts-unit dur-unit
    #(do [:timestamp-tz % tz]) #(do `(Math/addExact ~@%))))

(defmethod expr/codegen-call [:+ :duration :duration] [{[[_ x-unit] [_ y-unit]] :arg-types}]
  (with-arg-unit-conversion x-unit y-unit
    #(do [:duration %]) #(do `(Math/addExact ~@%))))

(doseq [[f-kw method-sym] [[:+ '.plus]
                           [:- '.minus]]]
  (defmethod expr/codegen-call [f-kw :date :interval] [{[dt-type, [_ iunit :as itype]] :arg-types, :as expr}]
    (case iunit
      :year-month {:return-type dt-type
                   :->call-code (fn [[x-arg y-arg]]
                                  `(.toEpochDay (~method-sym (LocalDate/ofEpochDay ~x-arg) (.getPeriod ~y-arg))))}
      :day-time (recall-with-cast2 expr [:timestamp-local :milli] itype)
      :month-day-nano (recall-with-cast2 expr [:timestamp-local :nano] itype)))

  (defmethod expr/codegen-call [f-kw :timestamp-local :interval] [{[[_ ts-unit :as ts-type], [_ iunit]] :arg-types}]
    (letfn [(codegen-call [unit-lower-bound]
              (with-first-arg-unit-conversion ts-unit unit-lower-bound
                #(do [:timestamp-local %])
                (fn [ts-unit [ts-arg i-arg]]
                  (-> `(let [i# ~i-arg]
                         (-> ~(ts->ldt ts-arg ts-unit)
                             (~method-sym (.getPeriod i#))
                             (~method-sym (.getDuration i#))))
                      (ldt->ts ts-unit)))))]
      (case iunit
        :year-month {:return-type ts-type
                     :->call-code (fn [[x-arg y-arg]]
                                    (-> `(let [i# ~y-arg]
                                           (-> ~(ts->ldt x-arg ts-unit)
                                               (~method-sym (.getPeriod i#))
                                               (~method-sym (.getDuration i#))))
                                        (ldt->ts ts-unit)))}

        :day-time (codegen-call :milli)
        :month-day-nano (codegen-call :nano))))

  (defmethod expr/codegen-call [f-kw :timestamp-tz :interval] [{[[_ ts-unit tz :as ts-type], [_ iunit]] :arg-types}]
    (let [zone-id-sym (gensym 'zone-id)]
      (letfn [(codegen-call [unit-lower-bound]
                (with-first-arg-unit-conversion ts-unit unit-lower-bound
                  #(do [:timestamp-tz % tz])
                  (fn [ts-unit [ts-arg i-arg]]
                    (-> `(let [i# ~i-arg]
                           (-> ~(ts->zdt ts-arg ts-unit zone-id-sym)
                               (~method-sym (.getPeriod i#))
                               (~method-sym (.getDuration i#))))
                        (zdt->ts ts-unit)))))]
        (-> (case iunit
              :year-month {:return-type ts-type
                           :->call-code (fn [[x-arg y-arg]]
                                          (-> `(let [y# ~y-arg]
                                                 (-> ~(ts->zdt x-arg ts-unit zone-id-sym)
                                                     (~method-sym (.getPeriod y#))
                                                     (~method-sym (.getDuration y#))))
                                              (zdt->ts ts-unit)))}

              :day-time (codegen-call :milli)
              :month-day-nano (codegen-call :nano))
            (update :batch-bindings (fnil conj []) [zone-id-sym `(ZoneId/of ~(str tz))]))))))

(doseq [[t1 t2] [[:duration :timestamp-tz]
                 [:duration :timestamp-local]
                 [:duration :date]
                 [:time-local :date]
                 [:time-local :timestamp-local]
                 [:time-local :timestamp-tz]
                 [:interval :date]
                 [:interval :timestamp-local]
                 [:interval :timestamp-tz]]]
  (defmethod expr/codegen-call [:+ t1 t2] [expr]
    (recall-with-flipped-args expr)))

;;; subtract

(defmethod expr/codegen-call [:- :timestamp-tz :timestamp-tz] [{[[_ x-unit _], [_ y-unit _]] :arg-types}]
  (with-arg-unit-conversion x-unit y-unit
    #(do [:duration %]) #(do `(Math/subtractExact ~@%))))

(defmethod expr/codegen-call [:- :timestamp-local :timestamp-local] [{[[_ x-unit], [_ y-unit]] :arg-types}]
  (with-arg-unit-conversion x-unit y-unit
    #(do [:duration %]) #(do `(Math/subtractExact ~@%))))

(defmethod expr/codegen-call [:- :timestamp-local :timestamp-tz] [{[x [_ y-unit _]] :arg-types, :as expr}]
  (-> expr (recall-with-cast2 x [:timestamp-local y-unit])))

(defmethod expr/codegen-call [:- :timestamp-tz :timestamp-local] [{[[_ x-unit] y] :arg-types, :as expr}]
  (-> expr (recall-with-cast2 [:timestamp-local x-unit] y)))

(defmethod expr/codegen-call [:- :date :date] [_expr]
  ;; FIXME this assumes date-unit :day
  {:return-type :i32 :->call-code (fn [[x y]] `(Math/subtractExact ~x ~y))})

(doseq [t [:timestamp-tz :timestamp-local]]
  (defmethod expr/codegen-call [:- :date t] [{[_ t2] :arg-types, :as expr}]
    (-> expr (recall-with-cast2 [:timestamp-local :micro] t2)))

  (defmethod expr/codegen-call [:- t :date] [{[t1 _] :arg-types, :as expr}]
    (-> expr (recall-with-cast2 t1 [:timestamp-local :micro]))))

(defmethod expr/codegen-call [:- :time-local :time-local] [{[[_ time-unit1] [_ time-unit2]] :arg-types, :as expr}]
  (-> expr (recall-with-cast2 [:duration time-unit1] [:duration time-unit2])))

(doseq [th [:date :timestamp-tz :timestamp-local]]
  (defmethod expr/codegen-call [:- th :time-local] [{[t [_ time-unit]] :arg-types, :as expr}]
    (-> expr (recall-with-cast2 t [:duration time-unit]))))

(defmethod expr/codegen-call [:- :timestamp-tz :duration] [{[[_ts ts-unit tz], [_dur dur-unit]] :arg-types}]
  (with-arg-unit-conversion ts-unit dur-unit
    #(do [:timestamp-tz % tz]) #(do `(Math/subtractExact ~@%))))

(defmethod expr/codegen-call [:- :date :duration] [{[_ [_dur dur-unit :as arg2]] :arg-types, :as expr}]
  (-> expr (recall-with-cast2 [:timestamp-local dur-unit] arg2)))

(defmethod expr/codegen-call [:- :timestamp-local :duration] [{[[_ts ts-unit], [_dur dur-unit]] :arg-types}]
  (with-arg-unit-conversion ts-unit dur-unit
    #(do [:timestamp-local %]) #(do `(Math/subtractExact ~@%))))

(defmethod expr/codegen-call [:- :duration :duration] [{[[_x x-unit] [_y y-unit]] :arg-types}]
  (with-arg-unit-conversion x-unit y-unit
    #(do [:duration %]) #(do `(Math/subtractExact ~@%))))

(defmethod expr/codegen-call [:- :date :interval] [{[dt-type, [_ iunit :as itype]] :arg-types, :as expr}]
  (case iunit
    :year-month {:return-type dt-type
                 :->call-code (fn [[x-arg y-arg]]
                                `(.toEpochDay (.minus (LocalDate/ofEpochDay ~x-arg) (.getPeriod ~y-arg))))}
    :day-time (recall-with-cast2 expr [:timestamp-local :milli] itype)
    :month-day-nano (recall-with-cast2 expr [:timestamp-local :nano] itype)))

;;; multiply, divide

(defmethod expr/codegen-call [:* :duration :int] [{[x-type _y-type] :arg-types}]
  {:return-type x-type
   :->call-code (fn [emitted-args]
                  `(Math/multiplyExact ~@emitted-args))})

(defmethod expr/codegen-call [:* :duration :num] [{[x-type _y-type] :arg-types}]
  {:return-type x-type
   :->call-code (fn [emitted-args]
                  `(* ~@emitted-args))})

(defmethod expr/codegen-call [:* :int :duration] [{[_x-type y-type] :arg-types}]
  {:return-type y-type
   :->call-code (fn [emitted-args]
                  `(Math/multiplyExact ~@emitted-args))})

(defmethod expr/codegen-call [:* :num :duration] [{[_x-type y-type] :arg-types}]
  {:return-type y-type
   :->call-code (fn [emitted-args]
                  `(long (* ~@emitted-args)))})

(defmethod expr/codegen-call [:/ :duration :num] [{[x-type] :arg-types}]
  {:return-type x-type
   :->call-code (fn [emitted-args]
                  `(quot ~@emitted-args))})

(defmethod expr/codegen-call [:/ :duration :interval] [{[d-type _i-type] :arg-types, :as expr}]
  (recall-with-cast2 expr d-type d-type))

(defmethod expr/codegen-call [:/ :duration :duration] [{[[_ x-unit] [_ y-unit]] :arg-types}]
  (with-arg-unit-conversion x-unit y-unit
    (constantly :i64) #(do `(quot ~@%))))

;;;; Boolean operations

(defmethod expr/codegen-call [:compare :timestamp-tz :timestamp-tz] [{[[_ x-unit _], [_ y-unit _]] :arg-types}]
  (with-arg-unit-conversion x-unit y-unit
    (constantly :i32) #(do `(Long/compare ~@%))))

(defmethod expr/codegen-call [:compare :timestamp-local :timestamp-local] [{[[_ x-unit], [_ y-unit]] :arg-types}]
  (with-arg-unit-conversion x-unit y-unit
    (constantly :i32) #(do `(Long/compare ~@%))))

(defmethod expr/codegen-call [:compare :timestamp-local :timestamp-tz] [{[x [_ y-unit _]] :arg-types, :as expr}]
  (-> expr (recall-with-cast2 x [:timestamp-local y-unit])))

(defmethod expr/codegen-call [:compare :timestamp-tz :timestamp-local] [{[[_ x-unit] y] :arg-types, :as expr}]
  (-> expr (recall-with-cast2 [:timestamp-local x-unit] y)))

(defmethod expr/codegen-call [:compare :date :date] [expr]
  (-> expr (recall-with-cast2 [:timestamp-local :micro] [:timestamp-local :micro])))

(doseq [t [:timestamp-tz :timestamp-local]]
  (defmethod expr/codegen-call [:compare :date t] [{[_ t2] :arg-types, :as expr}]
    (-> expr (recall-with-cast2 [:timestamp-local :micro] t2)))

  (defmethod expr/codegen-call [:compare t :date] [{[t1 _] :arg-types, :as expr}]
    (-> expr (recall-with-cast2 t1 [:timestamp-local :micro]))))

(defmethod expr/codegen-call [:compare :time-local :time-local] [{[[_ time-unit1] [_ time-unit2]] :arg-types, :as expr}]
  (-> expr (recall-with-cast2 [:duration time-unit1] [:duration time-unit2])))

(defmethod expr/codegen-call [:compare :duration :duration] [{[[_x x-unit] [_y y-unit]] :arg-types}]
  (with-arg-unit-conversion x-unit y-unit
    (constantly :i32) #(do `(Long/compare ~@%))))

(defn compare-ym-intervals ^long [^PeriodDuration x ^PeriodDuration y]
  (Long/compare (.toTotalMonths (.getPeriod x)) (.toTotalMonths (.getPeriod y))))

(defn compare-mdn-intervals ^long [^PeriodDuration x ^PeriodDuration y]
  (let [^Period x-period (.getPeriod x)
        ^Period y-period (.getPeriod y)]
    (if (or (> (.toTotalMonths x-period) 0) (> (.toTotalMonths y-period) 0))
      (throw (err/runtime-err :xtdb.expression/cannot-compare-mdn-interval-with-months
                              {::err/message "Cannot compare month-day-nano intervals when month component is non-zero."}))
      (let [x-period-nanos (* (.getDays x-period) 86400000000000)
            x-duration-nanos (.toNanos (.getDuration x))
            y-period-nanos (* (.getDays y-period) 86400000000000)
            y-duration-nanos (.toNanos (.getDuration y))]
        (Long/compare (+ x-period-nanos x-duration-nanos) (+ y-period-nanos y-duration-nanos))))))

(defmethod expr/codegen-call [:compare :interval :interval] [{[[_x x-unit :as xy] [_y y-unit :as yx]] :arg-types}]
  (cond
    (not= x-unit y-unit) (throw (UnsupportedOperationException. "Cannot compare intervals with different units"))
    (= x-unit :day-time)  (throw (UnsupportedOperationException. "Cannot compare day-time intervals")))

  {:return-type :i32
   :->call-code (cond
                  (= x-unit :year-month) (fn [[x y]] `(compare-ym-intervals ~x ~y))
                  (= x-unit :month-day-nano) (fn [[x y]] `(compare-mdn-intervals ~x ~y)))})

(doseq [[f cmp] [[:= #(do `(zero? ~%))]
                 [:< #(do `(neg? ~%))]
                 [:<= #(do `(not (pos? ~%)))]
                 [:> #(do `(pos? ~%))]
                 [:>= #(do `(not (neg? ~%)))]]]
  (doseq [x [:date :timestamp-local :timestamp-tz]
          y [:date :timestamp-local :timestamp-tz]]
    (defmethod expr/codegen-call [f x y] [expr]
      (let [{:keys [batch-bindings ->call-code]} (expr/codegen-call (assoc expr :f :compare))]
        {:return-type :bool,
         :batch-bindings batch-bindings
         :->call-code (comp cmp ->call-code)})))

  (doseq [x [:time-local :duration :interval]]
    (defmethod expr/codegen-call [f x x] [expr]
      (let [{:keys [batch-bindings ->call-code]} (expr/codegen-call (assoc expr :f :compare))]
        {:return-type :bool,
         :batch-bindings batch-bindings
         :->call-code (comp cmp ->call-code)}))))

;;;; Periods

(defmethod expr/codegen-call [:= :tstz-range :tstz-range] [_]
  {:return-type :bool
   :->call-code (fn [[x y]]
                  (let [x-sym (gensym 'x)
                        y-sym (gensym 'y)]
                    `(let [~x-sym ~x
                           ~y-sym ~y]
                       (and (= (from ~x-sym) (from ~y-sym))
                            (= (to ~x-sym) (to ~y-sym))))))})

(defmethod expr/codegen-call [:* :tstz-range :tstz-range] [_]
  {:return-type [:union #{:null :tstz-range}]
   :continue-call (fn [f [x y]]
                    (let [x-sym (gensym 'x)
                          y-sym (gensym 'y)
                          min-to (gensym 'min-to)
                          max-from (gensym 'max)]
                      `(let [~x-sym ~x
                             ~y-sym ~y
                             ~max-from (Math/max (from ~x-sym) (from ~y-sym))
                             ~min-to (Math/min (to ~x-sym) (to ~y-sym))]
                         (if (< ~max-from ~min-to)
                           ~(f :tstz-range `(->period ~max-from ~min-to))
                           ~(f :null nil)))))})

;;;; Intervals

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

(defn pd-neg ^PeriodDuration [^PeriodDuration pd]
  (PeriodDuration. (.negated (.getPeriod pd)) (.negated (.getDuration pd))))

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

(defmethod expr/codegen-call [:+ :interval :interval] [{[l-type r-type] :arg-types}]
  (let [return-type (choose-interval-arith-return l-type r-type)]
    {:return-type [:interval return-type]
     :->call-code (fn [[l r]] `(pd-add ~l ~r))}))

(defmethod expr/codegen-call [:- :interval :interval] [{[l-type r-type] :arg-types}]
  (let [return-type (choose-interval-arith-return l-type r-type)]
    {:return-type [:interval return-type]
     :->call-code (fn [[l r]] `(pd-sub ~l ~r))}))

(defmethod expr/codegen-call [:- :interval] [{[interval-type] :arg-types}]
  {:return-type interval-type
   :->call-code (fn [[i]] `(pd-neg ~i))})

(defn pd-scale ^PeriodDuration [^PeriodDuration pd ^long factor]
  (let [p (.getPeriod pd)
        d (.getDuration pd)]
    (PeriodDuration. (.multipliedBy p factor) (.multipliedBy d factor))))

(defmethod expr/codegen-call [:* :interval :int] [{[l-type _] :arg-types}]
  {:return-type l-type
   :->call-code (fn [[a b]] `(pd-scale ~a ~b))})

(defmethod expr/codegen-call [:* :int :interval] [{[_ r-type] :arg-types}]
  {:return-type r-type
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

(defmethod expr/codegen-call [:/ :interval :int] [{[[_interval iunit] _]:arg-types}]
  (case iunit
    :year-month {:return-type [:interval :year-month]
                 :->call-code (fn [[a b]] `(pd-year-month-div ~a ~b))}
    :day-time {:return-type [:interval :day-time]
               :->call-code (fn [[a b]] `(pd-day-time-div ~a ~b))}
    (throw (UnsupportedOperationException. "Cannot divide mixed period / duration intervals"))))

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

(defmethod expr/codegen-call [:abs :interval] [{[[_interval iunit :as itype]] :arg-types}]
  {:return-type itype,
   :->call-code (case iunit
                  :year-month #(do `(interval-abs-ym ~@%))
                  :day-time #(do `(interval-abs-dt ~@%))
                  (throw (UnsupportedOperationException. "Can only ABS YEAR_MONTH / DAY_TIME intervals")))})

(defmethod expr/codegen-call [:single_field_interval :int :utf8 :int :int] [{:keys [args]}]
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
  (let [interval-str (expr/resolve-string string-or-buf)]
    (try
      (Integer/valueOf interval-str)
      (catch NumberFormatException _
        (throw (err/illegal-arg :xtdb.expression/invalid-interval
                                {::err/message "Parse error. Single field INTERVAL string must contain a positive or negative integer."
                                 :interval interval-str}))))))

(defn second-interval-fractional-duration
  "Takes a string or UTF8 ByteBuffer and returns Duration for a fractional seconds INTERVAL literal.

  e.g INTERVAL '3.14' SECOND

  Throws a parse error if the string does not contain an integer / decimal. Throws on overflow."
  ^Duration [string-or-buf]
  (let [interval-str (expr/resolve-string string-or-buf)]
    (try
      (let [bd (bigdec interval-str)
            ;; will throw on overflow, is a custom error message needed?
            secs (.setScale bd 0 BigDecimal/ROUND_DOWN)
            nanos (.longValueExact (.setScale (.multiply (.subtract bd secs) 1e9M) 0 BigDecimal/ROUND_DOWN))]
        (Duration/ofSeconds (.longValueExact secs) nanos))
      (catch NumberFormatException _
        (throw (err/illegal-arg :xtdb.expression/invalid-interval
                                {::err/message "Parse error. SECOND INTERVAL string must contain a positive or negative integer or decimal."
                                 :interval interval-str}))))))

(defmethod expr/codegen-call [:single_field_interval :utf8 :utf8 :int :int] [{:keys [args]}]
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
  (or (if (= "YEAR" unit1)
        (parse-year-month-literal s)
        (parse-day-to-second-literal s unit1 unit2))
      (throw (err/illegal-arg :xtdb.expression/invalid-interval-string
                              {::err/message "Cannot parse interval, incorrect format."
                               :start-unit unit1
                               :end-unit unit2}))))

(defmethod expr/codegen-call [:multi_field_interval :utf8 :utf8 :int :utf8 :int] [{:keys [args]}]
  (let [[_ unit1 precision unit2 fractional-precision] (map :literal args)]
    (ensure-interval-units-valid unit1 unit2)
    (ensure-interval-precision-valid precision)
    (when (= "SECOND" unit2)
      (ensure-interval-fractional-precision-valid fractional-precision))

    ;; TODO choose a more specific representation when possible
    {:return-type (case [unit1 unit2]
                    ["YEAR" "MONTH"] [:interval :year-month]
                    [:interval :month-day-nano])
     :->call-code (fn [[s & _]]
                    `(parse-multi-field-interval (expr/resolve-string ~s) ~unit1 ~unit2))}))


(defn time-field->ChronoField
  [field]
  (case field
    "YEAR" `ChronoField/YEAR
    "MONTH" `ChronoField/MONTH_OF_YEAR
    "DAY" `ChronoField/DAY_OF_MONTH
    "HOUR" `ChronoField/HOUR_OF_DAY
    "MINUTE" `ChronoField/MINUTE_OF_HOUR
    "SECOND" `ChronoField/SECOND_OF_MINUTE))

(defmethod expr/codegen-call [:extract :utf8 :timestamp-tz] [{[{field :literal} _] :args, [_ [_ts ts-unit tz]] :arg-types}]
  (let [zone-id-sym (gensym 'zone-id)]
    {:return-type :i32
     :batch-bindings [[zone-id-sym (ZoneId/of tz)]]
     :->call-code (fn [[_ x]]
                    `(-> ~(ts->zdt x ts-unit zone-id-sym)
                         ~(case field
                            "TIMEZONE_HOUR" `(-> (.getOffset) (.getTotalSeconds) (/ 3600) (int))
                            "TIMEZONE_MINUTE" `(-> (.getOffset) (.getTotalSeconds) (/ 60) (rem 60) (int))
                            `(.get ~(time-field->ChronoField field)))))}))

(defmethod expr/codegen-call [:extract :utf8 :timestamp-local] [{[{field :literal} _] :args, [_ [_ts ts-unit]] :arg-types}]
  {:return-type :i32
   :->call-code (fn [[_ x]]
                  `(-> ~(ts->ldt x ts-unit)
                       ~(case field
                          "TIMEZONE_HOUR" (throw (UnsupportedOperationException. "Extract \"TIMEZONE_HOUR\" not supported for type timestamp without timezone"))
                          "TIMEZONE_MINUTE" (throw (UnsupportedOperationException. "Extract \"TIMEZONE_MINUTE\" not supported for type timestamp without timezone"))
                          `(.get ~(time-field->ChronoField field)))))})

(defmethod expr/codegen-call [:extract :utf8 :date] [{[{field :literal} _] :args}]
  ;; FIXME this assumes date-unit :day
  {:return-type :i32
   :->call-code (fn [[_ epoch-day-code]]
                  (case field
                    "YEAR" `(.getYear (LocalDate/ofEpochDay ~epoch-day-code))
                    "MONTH" `(.getMonthValue (LocalDate/ofEpochDay ~epoch-day-code))
                    "DAY" `(.getDayOfMonth (LocalDate/ofEpochDay ~epoch-day-code))
                    (throw (UnsupportedOperationException. (format "Extract \"%s\" not supported for type date" field)))))})

(defmethod expr/codegen-call [:extract :utf8 :interval] [{[{field :literal} _] :args}]
  {:return-type :i32
   :->call-code (fn [[_ pd]]
                  (let [period `(.getPeriod ^PeriodDuration ~pd)
                        duration `(.getDuration ^PeriodDuration ~pd)]
                    (case field
                      "YEAR" `(-> (.toTotalMonths ~period) (/ 12) (int))
                      "MONTH" `(-> (.toTotalMonths ~period) (rem 12) (int))
                      "DAY" `(.getDays ~period)
                      "HOUR" `(-> (.toHours ~duration) (int))
                      "MINUTE" `(-> (.toMinutes ~duration) (rem 60) (int))
                      "SECOND" `(-> (.toSeconds ~duration) (rem 60) (int))
                      (throw (UnsupportedOperationException. (format "Extract \"%s\" not supported for type interval" field))))))})

(defmethod expr/codegen-call [:extract :utf8 :time-local] [{[{field :literal} _] :args [_ [_tm tm-unit]] :arg-types}]
  {:return-type :i32
   :->call-code (fn [[_ tm]]
                  (let [local-time `(LocalTime/ofNanoOfDay ~(with-conversion tm tm-unit :nano))]
                    (case field
                      "HOUR" `(.getHour ~local-time)
                      "MINUTE" `(.getMinute ~local-time)
                      "SECOND" `(.getSecond ~local-time)
                      (throw (UnsupportedOperationException. (format "Extract \"%s\" not supported for type time without timezone" field))))))})

(defn field->truncate-fn
  [field]
  (case field
    "MILLENNIUM" `(DateTruncator/truncateYear 1000)
    "CENTURY" `(DateTruncator/truncateYear 100)
    "DECADE" `(DateTruncator/truncateYear 10)
    "YEAR" `(DateTruncator/truncateYear)
    "QUARTER" `(DateTruncator/truncateQuarter)
    "MONTH" `(DateTruncator/truncateMonth)
    "WEEK" `(DateTruncator/truncateWeek)
    `(.truncatedTo ~(case field
                      "DAY" `ChronoUnit/DAYS
                      "HOUR" `ChronoUnit/HOURS
                      "MINUTE" `ChronoUnit/MINUTES
                      "SECOND" `ChronoUnit/SECONDS
                      "MILLISECOND" `ChronoUnit/MILLIS
                      "MICROSECOND" `ChronoUnit/MICROS))))

;; 1. We pass in the arrow timestamp - essentially a Long with some units (ts-unit) and a timezone (tz)
;; 2. Convert the tz to a ZoneId
;; 3. We take the long and the units, and use these to create an Instant
;; 4. We convert the instant to a ZonedDateTime with the ZoneId
;; 5. We truncate the ZonedDateTime, to the equivalent `field` value
;; 6. We convert the ZonedDateTime back to a Long with the same units as the input
;; 7. The generated code returns the Long.
(defmethod expr/codegen-call [:date_trunc :utf8 :timestamp-tz] [{[{field :literal} _] :args, [_ [_tstz ts-unit tz :as ts-type]] :arg-types}]
  (let [zone-id-sym (gensym 'zone-id)]
    {:return-type ts-type
     :batch-bindings [[zone-id-sym (ZoneId/of tz)]]
     :->call-code (fn [[_ x]]
                    (-> `(-> ~(ts->zdt x ts-unit zone-id-sym)
                             ~(field->truncate-fn field))
                        (zdt->ts ts-unit)))}))

(defmethod expr/codegen-call [:date_trunc :utf8 :timestamp-local] [{[{field :literal} _] :args, [_ [_ts ts-unit :as ts-type]] :arg-types}]
    {:return-type ts-type
     :->call-code (fn [[_ x]]
                    (-> `(-> ~(ts->ldt x ts-unit)
                             ~(field->truncate-fn field))
                        (ldt->ts ts-unit)))})

;; 1. We pass in the arrow timestamp - essentially a Long with some units (ts-unit) and a timezone (tz), and we have a provided time_zone argument
;; 2. Convert the time_zone to ZoneId
;; 3. We take the long and the units, and use these to create an Instant
;; 4. We convert the instant to a ZonedDateTime with the time_zone ZoneId
;; 5. We truncate the ZonedDateTime, to the equivalent `field` value
;; 6. We convert the ZonedDateTime back to a Long with the same units as the input
;; 7. The generated code returns the Long, with the original tz from the args.
(defmethod expr/codegen-call [:date_trunc :utf8 :timestamp-tz :utf8] [{[{field :literal} _ {trunc-tz :literal}] :args, [_ [_tstz ts-unit _tz :as ts-type] _] :arg-types}]
  (let [trunc-zone-id-sym (gensym 'zone-id)]
    {:return-type ts-type
     :batch-bindings [[trunc-zone-id-sym (ZoneId/of trunc-tz)]]
     :->call-code (fn [[_ x]]
                    (-> `(-> ~(ts->zdt x ts-unit trunc-zone-id-sym)
                             ~(field->truncate-fn field))
                        (zdt->ts ts-unit)))}))

(defmethod expr/codegen-call [:date_trunc :utf8 :date] [{[{field :literal} _] :args, [_ [_date _date-unit :as date-type]] :arg-types}]
  ;; FIXME this assumes epoch-day
  {:return-type date-type
   :->call-code (fn [[_ epoch-day-code]]
                  `(-> (LocalDate/ofEpochDay ~epoch-day-code)
                       ~(case field
                         "MILLENNIUM" `(DateTruncator/truncateYear 1000)
                         "CENTURY" `(DateTruncator/truncateYear 100)
                         "DECADE" `(DateTruncator/truncateYear 10)
                         "YEAR" `(DateTruncator/truncateYear)
                         "QUARTER" `(DateTruncator/truncateQuarter)
                         "MONTH" `(DateTruncator/truncateMonth)
                         "WEEK" `(DateTruncator/truncateWeek)
                         "DAY" `(identity)
                         "HOUR" `(identity)
                         "MINUTE" `(identity)
                         "SECOND" `(identity)
                         "MILLISECOND" `(identity)
                         "MICROSECOND" `(identity))
                       (.toEpochDay)))})

(defn ->period-duration
  ([^Period p]
   (->period-duration p (Duration/ofSeconds 0)))
  ([^Period p ^Duration d]
   (PeriodDuration. p d)))

(defn truncated-millennium [^Period period]
  (let [months (.toTotalMonths period)]
    (Period/ofMonths (- months (rem months 12000)))))

(defn truncated-century [^Period period]
  (let [months (.toTotalMonths period)]
    (Period/ofMonths (- months (rem months 1200)))))

(defn truncated-decade [^Period period]
  (let [months (.toTotalMonths period)]
    (Period/ofMonths (- months (rem months 120)))))

(defn truncated-year [^Period period]
  (let [months (.toTotalMonths period)]
    (Period/ofMonths (- months (rem months 12)))))

(defn truncated-quarter [^Period period]
  (let [months (.toTotalMonths period)]
    (Period/ofMonths (- months (rem months 3)))))

(defn truncated-month [^Period period]
  (let [months (.toTotalMonths period)]
    (Period/ofMonths months)))

(defn truncated-week [^Period period]
  (let [day (.getDays period)]
    (Period/of (.getYears period) (.getMonths period) (- day (rem day 7)))))

(defmethod expr/codegen-call [:date_trunc :utf8 :interval] [{[{field :literal} _] :args, [_ [_interval _interval-unit :as interval-type]] :arg-types}]
  {:return-type interval-type
   :->call-code (fn [[_ pd]]
                  (let [period `(.getPeriod ^PeriodDuration ~pd)
                        duration `(.getDuration ^PeriodDuration ~pd)]
                    (case field
                      "MILLENNIUM" `(->period-duration (truncated-millennium ~period))
                      "CENTURY" `(->period-duration (truncated-century ~period))
                      "DECADE" `(->period-duration (truncated-decade ~period))
                      "YEAR" `(->period-duration (truncated-year ~period))
                      "QUARTER" `(->period-duration (truncated-quarter ~period))
                      "MONTH" `(->period-duration (truncated-month ~period))
                      "WEEK" `(->period-duration (truncated-week ~period))
                      "DAY" `(->period-duration ~period)
                      "HOUR" `(->period-duration ~period (.truncatedTo ~duration ChronoUnit/HOURS))
                      "MINUTE" `(->period-duration ~period (.truncatedTo ~duration ChronoUnit/MINUTES))
                      "SECOND" `(->period-duration ~period (.truncatedTo ~duration ChronoUnit/SECONDS))
                      "MILLISECOND" `(->period-duration ~period (.truncatedTo ~duration ChronoUnit/MILLIS))
                      "MICROSECOND" `(->period-duration ~period (.truncatedTo ~duration ChronoUnit/MICROS)))))})

(defn ->mdn-interval-between [^LocalDateTime end-dt ^LocalDateTime start-dt]
  (let [period-between (Period/between (.toLocalDate start-dt) (.toLocalDate end-dt))
        period-days (.getDays period-between)
        duration-between (Duration/between start-dt end-dt)
        start-lt (.toLocalTime start-dt)
        end-lt (.toLocalTime end-dt)
        period-day-adjustment (cond
                                (and (.isNegative period-between)
                                     (.isBefore start-lt end-lt))
                                1

                                (and (not (.isNegative period-between))
                                     (not= period-days 0)
                                     (.isAfter start-lt end-lt))
                                -1

                                :else 0)
        adjusted-period (.plusDays period-between period-day-adjustment)
        adjusted-duration (.minusDays duration-between (.toDays duration-between))]
    (PeriodDuration. adjusted-period adjusted-duration)))

(defmethod expr/codegen-call [:age :timestamp-local :timestamp-local] [{[[_ x-unit _], [_ y-unit _]] :arg-types}]
  {:return-type [:interval :month-day-nano]
   :->call-code (fn [[x y]] `(->mdn-interval-between ~(ts->ldt x x-unit) ~(ts->ldt y y-unit)))})

;; Cast and call for timestamp tz and mixed types
(doseq [x [:timestamp-tz :timestamp-local]
        y [:timestamp-tz :timestamp-local]]
  (when-not (= x y :timestamp-local)
    (defmethod expr/codegen-call [:age x y] [{[[_ x-unit _], [_ y-unit _]] :arg-types, :as expr}]
      (-> expr (recall-with-cast2 [:timestamp-local x-unit] [:timestamp-local y-unit])))))

;; Cast and call age for date and mixed types with date
(defmethod expr/codegen-call [:age :date :date] [expr]
  (-> expr (recall-with-cast2 [:timestamp-local :micro] [:timestamp-local :micro])))

(doseq [x [:timestamp-tz :timestamp-local]]
  (defmethod expr/codegen-call [:age x :date] [{[[_ x-unit _] _] :arg-types, :as expr}]
    (-> expr (recall-with-cast2 [:timestamp-local x-unit] [:timestamp-local :micro])))

  (defmethod expr/codegen-call [:age :date x] [{[_ [_ y-unit _]] :arg-types, :as expr}]
    (-> expr (recall-with-cast2 [:timestamp-local :micro] [:timestamp-local y-unit]))))

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

(defmethod expr/codegen-call [:snapshot_time] [_]
  (let [ts-type [:timestamp-tz :micro "UTC"]]
    {:return-type [:union #{:null ts-type}]
     :continue-call (fn [f _]
                      (let [ts (gensym 'ts)]
                        `(if-let [~ts expr/*snapshot-time*]
                           ~(f ts-type `(time/instant->micros ~ts))
                           ~(f :null nil))))}))

(defn- truncate-for-precision [code precision]
  (let [^long modulus (precision-modulus precision)]
    (if (= modulus 1)
      code
      `(* ~modulus (quot ~code ~modulus)))))

(defn- current-timestamp [^long precision]
  (let [precision (bound-precision precision)]
    {:return-type [:timestamp-tz (precision-timeunits precision) (str expr/*default-tz*)]
     :->call-code (fn [_]
                    (-> `(long (let [inst# (expr/current-time)]
                                 (+ (* ~(seconds-multiplier precision) (.getEpochSecond inst#))
                                    (quot (.getNano inst#) ~(nanos-divisor precision)))))
                        (truncate-for-precision precision)))}))

(def ^:private default-time-precision 6)

(defmethod expr/codegen-call [:current_timestamp] [_]
  (current-timestamp default-time-precision))

(defmethod expr/codegen-call [:current_timestamp :int] [{[{precision :literal}] :args}]
  (assert (integer? precision) "precision must be literal for now")
  (current-timestamp precision))

(defmethod expr/codegen-call [:current_date] [_]
  ;; TODO check the specs on this one - I read the SQL spec as being returned in local,
  ;; but Arrow expects Dates to be in UTC.
  ;; we then turn DateDays into LocalDates, which confuses things further.
  {:return-type [:date :day]
   :->call-code (fn [_]
                  `(long (-> (ZonedDateTime/ofInstant (expr/current-time) ZoneOffset/UTC)
                             (.toLocalDate)
                             (.toEpochDay))))})

(defn- current-time [^long precision]
  ;; TODO check the specs on this one - I read the SQL spec as being returned in local,
  ;; but Arrow expects Times to be in UTC.
  ;; we then turn times into LocalTimes, which confuses things further.
  (let [precision (bound-precision precision)]
    {:return-type [:time-local (precision-timeunits precision)]
     :->call-code (fn [_]
                    (-> `(long (-> (ZonedDateTime/ofInstant (expr/current-time) ZoneOffset/UTC)
                                   (.toLocalTime)
                                   (.toNanoOfDay)
                                   (quot ~(nanos-divisor precision))))
                        (truncate-for-precision precision)))}))

(defmethod expr/codegen-call [:current_time] [_]
  (current-time default-time-precision))

(defmethod expr/codegen-call [:current_time :int] [{[{precision :literal}] :args}]
  (assert (integer? precision) "precision must be literal for now")
  (current-time precision))

(defn- local-timestamp [^long precision]
  (let [precision (bound-precision precision)]
    {:return-type [:timestamp-local (precision-timeunits precision)]
     :->call-code (fn [_]
                    (-> `(long (let [ldt# (-> (ZonedDateTime/ofInstant (expr/current-time) expr/*default-tz*)
                                              (.toLocalDateTime))]
                                 (+ (* (.toEpochSecond ldt# ZoneOffset/UTC) ~(seconds-multiplier precision))
                                    (quot (.getNano ldt#) ~(nanos-divisor precision)))))
                        (truncate-for-precision precision)))}))

(defmethod expr/codegen-call [:local_timestamp] [_]
  (local-timestamp default-time-precision))

(defmethod expr/codegen-call [:local_timestamp :num] [{[{precision :literal}] :args}]
  (assert (integer? precision) "precision must be literal for now")
  (local-timestamp precision))

(defn- local-time [^long precision]
  (let [precision (bound-precision precision)
        time-unit (precision-timeunits precision)]
    {:return-type [:time-local time-unit]
     :->call-code (fn [_]
                    (-> `(long (-> (ZonedDateTime/ofInstant (expr/current-time) expr/*default-tz*)
                                   (.toLocalTime)
                                   (.toNanoOfDay)
                                   (quot ~(nanos-divisor precision))))
                        (truncate-for-precision precision)))}))

(defmethod expr/codegen-call [:local_time] [_]
  (local-time default-time-precision))

(defmethod expr/codegen-call [:local_time :int] [{[{precision :literal}] :args}]
  (assert (integer? precision) "precision must be literal for now")
  (local-time precision))

(defmethod expr/codegen-call [:current_timezone] [_]
  {:return-type :utf8
   :->call-code (fn [_]
                  (expr/emit-value String `(str expr/*default-tz*)))})

(defmethod expr/codegen-call [:abs :num] [{[numeric-type] :arg-types}]
  {:return-type numeric-type
   :->call-code #(do `(Math/abs ~@%))})

(defn invalid-period-err [^long from-µs, ^long to-µs]
  (let [from (time/micros->instant from-µs)
        to (time/micros->instant to-µs)]
    (err/runtime-err :xtdb/invalid-period
                     {::err/message (format "From cannot be greater than to when constructing a period - from: %s, to %s" from to)
                      :from from
                      :to to})))

(defn ->period ^xtdb.arrow.ListValueReader [^long from, ^long to]
  (when (>= from to)
    (throw (invalid-period-err from to)))

  (let [from (doto (ValueBox.) (.writeLong from))
        to (doto (ValueBox.) (.writeLong to))]
    (reify ListValueReader
      (size [_] 2)
      (nth [_ idx]
        (case idx 0 from, 1 to)))))

(defmethod expr/codegen-call [:period :timestamp-tz :timestamp-tz] [{[[_ from-tsunit _from-tz] [_ to-tsunit _to-tz]] :arg-types}]
  {:return-type :tstz-range
   :->call-code (fn [[from-code to-code]]
                  `(->period ~(with-conversion from-code from-tsunit :micro)
                             ~(with-conversion to-code to-tsunit :micro)))})

(defmethod expr/codegen-call [:period :timestamp-tz :null] [{[[_ from-tsunit _from-tz]] :arg-types}]
  {:return-type :tstz-range
   :->call-code (fn [[from-code _to-code]]
                  `(->period ~(with-conversion from-code from-tsunit :micro)
                             Long/MAX_VALUE))})

(defmethod expr/codegen-call [:period :date-time :date-time] [expr]
  (recall-with-cast2 expr types/temporal-col-type types/temporal-col-type))

(defmethod expr/codegen-call [:period :date-time :null] [expr]
  (recall-with-cast2 expr types/temporal-col-type :null))

(defn from ^long [^ListValueReader period]
  (.readLong ^ValueReader (.nth period 0)))

(defn to ^long [^ListValueReader period]
  (.readLong ^ValueReader (.nth period 1)))

(defmethod expr/codegen-call [:lower :tstz-range] [_]
  {:return-type types/temporal-col-type
   :->call-code (fn [[arg]]
                  `(from ~arg))})

(defmethod expr/codegen-call [:upper :tstz-range] [_]
  {:return-type types/nullable-temporal-type
   :continue-call (fn [f [arg]]
                    (let [to-sym (gensym 'to)]
                      `(let [~to-sym (to ~arg)]
                         (if (= Long/MAX_VALUE ~to-sym)
                           ~(f :null nil)
                           ~(f types/temporal-col-type to-sym)))))})

(defmethod expr/codegen-call [:lower_inf :tstz-range] [_]
  {:return-type :bool
   :->call-code (fn [[arg]]
                  `(= Long/MIN_VALUE (from ~arg)))})

(defmethod expr/codegen-call [:upper_inf :tstz-range] [_]
  {:return-type :bool
   :->call-code (fn [[arg]]
                  `(= Long/MAX_VALUE (to ~arg)))})

(defn temporal-contains-point? [p1 ^long ts]
  (and (<= (from p1) ts)
       (> (to p1) ts)))

(defmethod expr/codegen-call [:contains? :tstz-range :timestamp-tz] [_]
  {:return-type :bool
   :->call-code (fn [[p1-code ts-code]]
                  `(temporal-contains-point? ~p1-code ~ts-code))})

(defmethod expr/codegen-call [:contains? :tstz-range :timestamp-local] [{[arg1 [_ arg2-unit]] :arg-types, :as expr}]
  (-> expr (recall-with-cast2 arg1 [:timestamp-tz arg2-unit (str expr/*default-tz*)])))

(defmethod expr/codegen-call [:contains? :tstz-range :date] [{[arg1 _arg2] :arg-types, :as expr}]
  (-> expr (recall-with-cast2 arg1 [:timestamp-local :micro])))

(defn temporal-contains? [p1 p2]
  (and (<= (from p1) (from p2))
       (>= (to p1) (to p2))))

(defn temporal-strictly-contains? [p1 p2]
  (and (< (from p1) (from p2))
       (> (to p1) (to p2))))

(defn overlaps? [p1 p2]
  (and (< (from p1) (to p2))
       (> (to p1) (from p2))))

(defn strictly-overlaps? [p1 p2]
  (and (> (from p1) (from p2))
       (< (to p1) (to p2))))

(defn equals? [p1 p2]
  (and (= (from p1) (from p2))
       (= (to p1) (to p2))))

(defn precedes? [p1 p2]
  (<= (to p1) (from p2)))

(defn strictly-precedes? [p1 p2]
  (< (to p1) (from p2)))

(defn immediately-precedes? [p1 p2]
  (= (to p1) (from p2)))

(defn succeeds? [p1 p2]
  (>= (from p1) (to p2)))

(defn strictly-succeeds? [p1 p2]
  (> (from p1) (to p2)))

(defn immediately-succeeds? [p1 p2]
  (= (from p1) (to p2)))

(defn leads? [p1 p2]
  (and (< (from p1) (from p2))
       (< (from p2) (to p1))
       (<= (to p1) (to p2))))

(defn strictly-leads? [p1 p2]
  (and (< (from p1) (from p2))
       (< (from p2) (to p1))
       (< (to p1) (to p2))))

(defn immediately-leads? [p1 p2]
  (and (< (from p1) (from p2))
       (= (to p1) (to p2))))

(defn lags? [p1 p2]
  (and (>= (from p1) (from p2))
       (< (from p2) (to p1))
       (> (to p1) (to p2))))

(defn strictly-lags? [p1 p2]
  (and (> (from p1) (from p2))
       (< (from p2) (to p1))
       (> (to p1) (to p2))))

(defn immediately-lags? [p1 p2]
  (and (= (from p1) (from p2))
       (> (to p1) (to p2))))

(doseq [[pred-name pred-sym] [[:contains `temporal-contains?]
                              [:strictly_contains `temporal-strictly-contains?]
                              [:overlaps `overlaps?]
                              [:strictly_overlaps `strictly-overlaps?]
                              [:equals `equals?]
                              [:precedes `precedes?]
                              [:strictly_precedes `strictly-precedes?]
                              [:immediately_precedes `immediately-precedes?]
                              [:succeeds `succeeds?]
                              [:strictly_succeeds `strictly-succeeds?]
                              [:immediately_succeeds `immediately-succeeds?]
                              [:leads `leads?]
                              [:strictly_leads `strictly-leads?]
                              [:immediately_leads `immediately-leads?]
                              [:lags `lags?]
                              [:strictly_lags `strictly-lags?]
                              [:immediately_lags `immediately-lags?]]]
  (defmethod expr/codegen-call [pred-name :tstz-range :tstz-range] [_]
    {:return-type :bool
     :->call-code (fn [[p1-code p2-code]]
                    `(~pred-sym ~p1-code ~p2-code))})

  ;; add aliases with `?` suffix
  (defmethod expr/codegen-call [(keyword (str (name pred-name) "?")) :tstz-range :tstz-range] [expr]
    (expr/codegen-call (assoc expr :f pred-name))))

(defmethod macro/macroexpand1-call :date_bin [{:keys [args]}]
  (let [[interval src origin] args
        i-sym (gensym 'interval)
        o-sym (gensym 'origin)]
    {:op :let, :local i-sym, :expr interval
     :body {:op :let, :local o-sym, :expr (or origin {:op :literal, :literal Instant/EPOCH})
            :body {:op :call, :f :+
                   :args [{:op :local, :local o-sym}
                          {:op :call, :f :*
                           :args [{:op :local, :local i-sym}
                                  {:op :call, :f :/,
                                   :args [{:op :call, :f :-
                                           :args [src {:op :local, :local o-sym}]}
                                          {:op :local, :local i-sym}]}]}]}}}))


(defmethod macro/macroexpand1-call :range_bins [{:keys [args] :as expr}]
  (cond-> expr
    (= 3 (count args)) (update :args conj {:op :literal, :literal Instant/EPOCH})))

(defn emit-range-bins ^xtdb.arrow.ListValueReader [^long stride, ^long r-from, ^long r-to, ^long origin]
  (let [from-box (ValueBox.)
        to-box (ValueBox.)
        weight-box (ValueBox.)
        box (doto (ValueBox.)
              (.writeObject {"_from" from-box
                             "_to" to-box
                             "_weight" weight-box}))
        r-size (- r-to r-from)

        base (-> (- r-from origin)
                 (quot stride)
                 (* stride)
                 (+ origin))
        top (-> (- r-to origin 1)
                (quot stride)
                inc
                (* stride)
                (+ origin))
        n-bins (quot (- top base) stride)]

    (reify ListValueReader
      (size [_] n-bins)

      (nth [_ idx]
        (let [bin-from (doto (+ base (* idx stride))
                         (->> (.writeLong from-box)))
              bin-to (doto (+ base (* (inc idx) stride))
                       (->> (.writeLong to-box)))]
          (.writeDouble weight-box (/ (double (- (min bin-to r-to) (max bin-from r-from)))
                                      r-size))
          box)))))

(defmethod expr/codegen-call [:range_bins :interval :timestamp-tz :timestamp-tz :timestamp-tz] [{[i-type from-type to-type origin-type] :arg-types}]
  (assert (= (subvec from-type 0 2)
             (subvec to-type 0 2)
             (subvec origin-type 0 2)
             (subvec types/temporal-col-type 0 2))
          (format "TODO: from-type = %s; to-type = %s; origin-type = %s"
                  (pr-str from-type) (pr-str to-type) (pr-str origin-type)))

  (let [{bb1 :batch-bindings, stride->duration :->call-code} (expr/codegen-cast {:source-type i-type, :target-type [:duration :micro]})]
    {:return-type [:list [:struct {'_from types/temporal-col-type,
                                   '_to types/temporal-col-type,
                                   '_weight :f64}]]
     :batch-bindings bb1
     :->call-code (fn [[stride-code from-code to-code origin-code]]
                    `(emit-range-bins ~(stride->duration [stride-code])
                                      ~from-code ~to-code ~origin-code))}))

(defmethod expr/codegen-call [:range_bins :interval :date-time :date-time :date-time] [{[i-type from-type to-type origin-type] :arg-types}]
  (let [{bb-from :batch-bindings, ->from-code :->call-code} (expr/codegen-cast {:source-type from-type, :target-type types/temporal-col-type})
        {bb-to :batch-bindings, ->to-code :->call-code} (expr/codegen-cast {:source-type to-type, :target-type types/temporal-col-type})
        {bb-origin :batch-bindings, ->origin-code :->call-code} (expr/codegen-cast {:source-type origin-type, :target-type types/temporal-col-type})

        {ret :return-type, bb :batch-bindings, ->call-code :->call-code}
        (expr/codegen-call {:f :range_bins
                            :arg-types [i-type types/temporal-col-type types/temporal-col-type types/temporal-col-type]})]

    {:return-type ret
     :batch-bindings (concat bb-from bb-to bb-origin bb)
     :->call-code (fn [[i-code from-code to-code origin-code]]
                    (->call-code [i-code (->from-code [from-code]) (->to-code [to-code]) (->origin-code [origin-code])]))}))

(defn date-series [^LocalDate from, ^LocalDate to, ^PeriodDuration stride]
  (assert (= Duration/ZERO (.getDuration stride))
          "date-series only supports zero-duration strides")

  (let [period (.getPeriod stride)
        months (+ (* (.getYears period) 12) (.getMonths period))
        el-box (ValueBox.)

        ;; we eagerly evaluate here because (unlike the ints version)
        ;; every entry will need to calculate the one before anyway
        res (->> (iterate (fn [^LocalDate acc]
                            (.plusMonths acc months))
                          from)
                 (into [] (take-while #(neg? (compare % to)))))]

    (reify ListValueReader
      (size [_]
        (count res))

      (nth [_ idx]
        (doto el-box
          (.writeLong (.toEpochDay ^LocalDate (nth res idx))))))))

(defmethod expr/codegen-call [:generate_series :date :date :interval] [{[from-type to-type [_interval i-unit :as i-type]] :arg-types, :as expr}]
  (when-not (= from-type [:date :day])
    (throw (UnsupportedOperationException. "generate_series with date-from of type date-milli")))

  (when-not (= to-type [:date :day])
    (throw (UnsupportedOperationException. "generate_series with date-to of type date-milli")))

  (case i-unit
    :year-month {:return-type [:list [:date :day]]
                 :->call-code (fn [[x-arg y-arg stride]]
                                `(date-series (LocalDate/ofEpochDay ~x-arg) (LocalDate/ofEpochDay ~y-arg) ~stride))}
    :day-time (recall-with-cast3 expr [:timestamp-local :milli] [:timestamp-local :milli] i-type)
    :month-day-nano (recall-with-cast3 expr [:timestamp-local :nano] [:timestamp-local :nano] i-type)))

(defn ts-series [^LocalDateTime from, ^LocalDateTime to, ^PeriodDuration stride, write-ldt]
  (let [period (.getPeriod stride)
        duration (.getDuration stride)
        el-box (ValueBox.)

        ;; we eagerly evaluate here because (unlike the ints version)
        ;; every entry will need to calculate the one before anyway
        res (->> (iterate (fn [^LocalDateTime acc]
                            (-> acc (.plus period) (.plus duration)))
                          from)
                 (into [] (take-while #(neg? (compare % to)))))]

    (reify ListValueReader
      (size [_] (count res))

      (nth [_ idx]
        (doto el-box
          (write-ldt (nth res idx)))))))

(defmethod expr/codegen-call [:generate_series :timestamp-local :timestamp-local :interval] [{[[_from-ts from-unit] [_to-ts to-unit] [_interval i-unit]] :arg-types}]
  (let [out-unit (types/smallest-ts-unit from-unit to-unit
                                         (case i-unit
                                           :year-month :second
                                           :day-time :milli
                                           :month-day-nano :nano))]
    {:return-type [:list [:timestamp-local out-unit]]
     :->call-code (fn [[from-arg to-arg i-arg]]
                    (-> `(ts-series ~(ts->ldt from-arg from-unit)
                                    ~(ts->ldt to-arg to-unit)
                                    ~i-arg
                                    ~(let [ldt-sym (gensym 'ldt)]
                                       `(fn ~'write-ldt [^ValueBox box#, ~(-> ldt-sym (expr/with-tag LocalDateTime))]
                                          (.writeLong box# ~(ldt->ts ldt-sym out-unit)))))))}))

(defn tstz-series [^ZonedDateTime from, ^ZonedDateTime to, ^PeriodDuration stride, write-zdt]
  (let [period (.getPeriod stride)
        duration (.getDuration stride)
        el-box (ValueBox.)

        ;; we eagerly evaluate here because (unlike the ints version)
        ;; every entry will need to calculate the one before anyway
        res (->> (iterate (fn [^ZonedDateTime acc]
                            (-> acc (.plus period) (.plus duration)))
                          from)
                 (into [] (take-while #(neg? (compare % to)))))]

    (reify ListValueReader
      (size [_] (count res))

      (nth [_ idx]
        (doto el-box
          (write-zdt (nth res idx)))))))

(defmethod expr/codegen-call [:generate_series :timestamp-tz :timestamp-tz :interval] [{[[_from-ts from-unit from-tz] [_to-ts to-unit to-tz] [_interval i-unit]] :arg-types}]
  (let [out-unit (types/smallest-ts-unit from-unit to-unit
                                         (case i-unit
                                           :year-month :second
                                           :day-time :milli
                                           :month-day-nano :nano))
        out-tz-sym (gensym 'out-tz)
        out-tz (if (= from-tz to-tz) from-tz "UTC")]
    {:return-type [:list [:timestamp-tz out-unit out-tz]]
     :batch-bindings [[out-tz-sym (ZoneId/of out-tz)]]
     :->call-code (fn [[from-arg to-arg i-arg]]
                    (-> `(tstz-series ~(ts->zdt from-arg from-unit out-tz-sym)
                                      ~(ts->zdt to-arg to-unit out-tz-sym)
                                      ~i-arg
                                      ~(let [zdt-sym (gensym 'zdt)]
                                         `(fn ~'write-zdt [^ValueBox box#, ~(-> zdt-sym (expr/with-tag ZonedDateTime))]
                                            (.writeLong box# ~(zdt->ts zdt-sym out-unit)))))))}))


