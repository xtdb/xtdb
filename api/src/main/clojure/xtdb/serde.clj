(ns ^{:clojure.tools.namespace.repl/load false}
  xtdb.serde
  (:require [clojure.edn :as edn]
            [cognitect.transit :as transit]
            [time-literals.read-write :as time-literals]
            [xtdb.api :as xt]
            [xtdb.error :as err]
            [xtdb.types :as types])
  (:import java.io.Writer
           [java.time Duration Period]
           (java.time DayOfWeek Duration Instant LocalDate LocalDateTime LocalTime Month MonthDay OffsetDateTime OffsetTime Period Year YearMonth ZoneId ZonedDateTime)
           [org.apache.arrow.vector PeriodDuration]
           xtdb.api.TransactionKey
           (xtdb.types IntervalDayTime IntervalMonthDayNano IntervalYearMonth)
           (xtdb.types ClojureForm IntervalDayTime IntervalMonthDayNano IntervalYearMonth)))

(when-not (or (some-> (System/getenv "XTDB_NO_JAVA_TIME_LITERALS") Boolean/valueOf)
              (some-> (System/getProperty "xtdb.no-java-time-literals") Boolean/valueOf))
  (time-literals/print-time-literals-clj!))

(defn period-duration-reader [[p d]]
  (PeriodDuration. (Period/parse p) (Duration/parse d)))

(defmethod print-dup PeriodDuration [^PeriodDuration pd ^Writer w]
  (.write w (format "#xt/period-duration %s" (pr-str [(str (.getPeriod pd)) (str (.getDuration pd))]))))

(defmethod print-method PeriodDuration [c ^Writer w]
  (print-dup c w))

(defn interval-ym-reader [p]
  (IntervalYearMonth. (Period/parse p)))

(defmethod print-dup IntervalYearMonth [^IntervalYearMonth i, ^Writer w]
  (.write w (format "#xt/interval-ym %s" (pr-str (str (.period i))))))

(defmethod print-method IntervalYearMonth [i ^Writer w]
  (print-dup i w))

(defn interval-dt-reader [[p d]]
  (IntervalDayTime. (Period/parse p) (Duration/parse d)))

(defmethod print-dup IntervalDayTime [^IntervalDayTime i, ^Writer w]
  (.write w (format "#xt/interval-dt %s" (pr-str [(str (.period i)) (str (.duration i))]))))

(defmethod print-method IntervalDayTime [i ^Writer w]
  (print-dup i w))

(defn interval-mdn-reader [[p d]]
  (IntervalMonthDayNano. (Period/parse p) (Duration/parse d)))

(defmethod print-dup IntervalMonthDayNano [^IntervalMonthDayNano i, ^Writer w]
  (.write w (format "#xt/interval-mdn %s" (pr-str [(str (.period i)) (str (.duration i))]))))

(defmethod print-method IntervalMonthDayNano [i ^Writer w]
  (print-dup i w))

(def tj-read-handlers
  (merge (-> time-literals/tags
             (update-keys str)
             (update-vals transit/read-handler))
         {"xtdb/clj-form" (transit/read-handler xt/->ClojureForm)
          "xtdb/tx-key" (transit/read-handler xt/map->TransactionKey)
          "xtdb/illegal-arg" (transit/read-handler err/-iae-reader)
          "xtdb/runtime-err" (transit/read-handler err/-runtime-err-reader)
          "xtdb/exception-info" (transit/read-handler #(ex-info (first %) (second %)))
          "xtdb/period-duration" period-duration-reader
          "xtdb.interval/year-month" interval-ym-reader
          "xtdb.interval/day-time" interval-dt-reader
          "xtdb.interval/month-day-nano" interval-mdn-reader
          "xtdb/list" (transit/read-handler edn/read-string)}))

(def tj-write-handlers
  (merge (-> {Period "time/period"
              LocalDate "time/date"
              LocalDateTime "time/date-time"
              ZonedDateTime "time/zoned-date-time"
              OffsetTime "time/offset-time"
              Instant "time/instant"
              OffsetDateTime "time/offset-date-time"
              ZoneId "time/zone"
              DayOfWeek "time/day-of-week"
              LocalTime "time/time"
              Month "time/month"
              Duration "time/duration"
              Year "time/year"
              YearMonth "time/year-month"
              MonthDay "time/month-day"}
             (update-vals #(transit/write-handler % str)))
         {TransactionKey (transit/write-handler "xtdb/tx-key" #(select-keys % [:tx-id :system-time]))
          xtdb.IllegalArgumentException (transit/write-handler "xtdb/illegal-arg" ex-data)
          xtdb.RuntimeException (transit/write-handler "xtdb/runtime-err" ex-data)
          clojure.lang.ExceptionInfo (transit/write-handler "xtdb/exception-info" #(vector (ex-message %) (ex-data %)))

          ClojureForm (transit/write-handler "xtdb/clj-form" #(.form ^ClojureForm %))

          IntervalYearMonth (transit/write-handler "xtdb.interval/year-month" #(str (.period ^IntervalYearMonth %)))

          IntervalDayTime (transit/write-handler "xtdb.interval/day-time"
                                                 #(vector (str (.period ^IntervalDayTime %))
                                                          (str (.duration ^IntervalDayTime %))))

          IntervalMonthDayNano (transit/write-handler "xtdb.interval/month-day-nano"
                                                      #(vector (str (.period ^IntervalMonthDayNano %))
                                                               (str (.duration ^IntervalMonthDayNano %))))
          clojure.lang.PersistentList (transit/write-handler "xtdb/list" #(pr-str %))}))
