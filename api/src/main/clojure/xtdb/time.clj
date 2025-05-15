(ns xtdb.time
  (:require [clojure.spec.alpha :as s]
            [xtdb.error :as err])
  (:import (java.time Duration Instant LocalDate LocalDateTime LocalTime OffsetDateTime Period ZoneId ZonedDateTime)
           [java.time.format DateTimeParseException]
           java.time.temporal.ChronoUnit
           (java.util Date)
           (org.apache.arrow.vector PeriodDuration)
           (xtdb.time Interval Time)))

(defn ->duration [d]
  (cond
    (instance? Duration d) d
    (nat-int? d) (Duration/ofMillis d)
    (string? d) (Duration/parse d)
    :else ::s/invalid))

(s/def ::duration
  (s/and (s/conformer ->duration) #(instance? Duration %)))

(s/def ::datetime-value
  (some-fn (partial instance? Date)
           (partial instance? Instant)
           (partial instance? ZonedDateTime)
           (partial instance? LocalDate)
           (partial instance? LocalDateTime)))

(defprotocol TimeConversions
  (->instant
    ^java.time.Instant [v]
    ^java.time.Instant [v {:keys [default-tz]}])

  (^java.time.ZonedDateTime ->zdt [v]))

(defn expect-instant [instant]
  (when-not (or (instance? Instant instant)
                (instance? Date instant)
                (instance? ZonedDateTime instant))
    (throw (err/illegal-arg :xtdb/invalid-date-time
                            {::err/message "expected date-time"
                             :timestamp instant})))

  (->instant instant))

(def ^java.time.ZoneId utc (ZoneId/of "UTC"))

(extend-protocol TimeConversions
  nil
  (->instant ([_] nil) ([_ _] nil))
  (->zdt [_] nil)

  String
  (->instant
    ([s] (Time/asInstant s))
    ([s {:keys [default-tz]}] (Time/asInstant s default-tz)))
  (->zdt [s] (Time/asZonedDateTime s))

  Instant
  (->instant ([i] i) ([i _] i))
  (->zdt [i] (-> i (.atZone utc)))

  Date
  (->instant
    ([d] (.toInstant d))
    ([d _] (.toInstant d)))
  (->zdt [d] (->zdt (->instant d)))

  ZonedDateTime
  (->instant
    ([zdt] (.toInstant zdt))
    ([zdt _] (.toInstant zdt)))
  (->zdt [zdt] zdt)

  OffsetDateTime
  (->instant
    ([odt] (.toInstant odt))
    ([odt _] (.toInstant odt)))

  LocalDate
  (->instant [ld opts] (->instant (.atStartOfDay ld) opts))

  LocalDateTime
  (->instant [ldt {:keys [^ZoneId default-tz] :as opts}]
    (->instant (.atZone ldt default-tz) opts)))

(defn instant->micros ^long [^Instant inst]
  (let [epoch-milli (.toEpochMilli inst)
        sec-part (quot epoch-milli 1000)
        milli-part-in-micros (Math/multiplyExact (long (rem epoch-milli 1000)) 1000)
        micro-part (quot (rem (.getNano inst) (long 1e6)) 1000)]
    (-> (Math/multiplyExact sec-part #=(long 1e6))
        (Math/addExact (Math/addExact milli-part-in-micros micro-part)))))

(defn sql-temporal->micros
  "Given some temporal value (such as a Date, LocalDateTime, OffsetDateTime and so on) will return the corresponding Instant.

   To do this for LocalDate and LocalDateTime, the provided SQL session time zone is assumed to be the implied time zone of the date/time."
  ^long [temporal ^ZoneId session-zone]
  (condp instance? temporal
    LocalDate (sql-temporal->micros (.atTime ^LocalDate temporal LocalTime/MIDNIGHT) session-zone)
    LocalDateTime (instant->micros (.toInstant (.atZone ^LocalDateTime temporal session-zone)))
    (instant->micros (->instant temporal))))

(defn instant->nanos ^long [^Instant inst]
  (-> (Math/multiplyExact (.getEpochSecond inst) #=(long 1e9))
      (Math/addExact (long (.getNano inst)))))

(defn micros->instant ^java.time.Instant [^long μs]
  (.plus Instant/EPOCH μs ChronoUnit/MICROS))

(defn nanos->instant ^java.time.Instant [^long ns]
  (.plus Instant/EPOCH ns ChronoUnit/NANOS))

(def ^java.time.Instant end-of-time
  (micros->instant Long/MAX_VALUE))

(def ^java.time.Instant start-of-time
  (micros->instant Long/MIN_VALUE))

(defn max-tx [l r]
  (if (or (nil? l)
          (and r (neg? (compare l r))))
    r
    l))

(defn seconds-fraction->nanos ^long [seconds-fraction]
  (if seconds-fraction
    (* (Long/parseLong seconds-fraction)
       (long (Math/pow 10 (- 9 (count seconds-fraction)))))
    0))

(defn parse-sql-timestamp-literal [ts-str]
  (try
    (Time/asSqlTimestamp ts-str)
    (catch DateTimeParseException e
      (throw (err/illegal-arg :xtdb/invalid-date-time
                              {::err/message (str "invalid timestamp: " (ex-message e))
                               :timestamp ts-str}
                              e)))))

(defn alter-duration-precision ^Duration [^long precision ^Duration duration]
  (if (= precision 0)
    (.withNanos duration 0)
    (.withNanos duration (let [nanos (.getNano duration)
                               factor (Math/pow 10 (- 9 precision))]
                           (* (Math/floor (/ nanos factor)) factor)))))

(defn <-iso-interval-str [i-str]
  (when-let [[_ neg p-str d-str] (re-matches #"(-)?P([-\dYMWD]+)?(?:T([-\dHMS\.]+)?)?" i-str)]
    (Interval. (if p-str
                 (Period/parse (str neg "P" p-str))
                 Period/ZERO)
               (if d-str
                 (Duration/parse (str neg "PT" d-str))
                 Duration/ZERO))))

(defn alter-md*-interval-precision ^PeriodDuration [^long precision ^PeriodDuration pd]
  (PeriodDuration.
   (.getPeriod pd)
   (alter-duration-precision precision (.getDuration pd))))
