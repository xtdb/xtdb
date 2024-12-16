;; THIRD-PARTY SOFTWARE NOTICE
;; This file is derivative of the 'time-literals' project, under the MIT license.
;; The time-literals license is available at https://github.com/henryw374/time-literals/blob/2a1c7e195a91dcf3638df1a2876445fc7b7872ef/LICENSE
;; https://github.com/henryw374/time-literals

(ns xtdb.mirrors.time-literals
  (:refer-clojure :exclude [time])
  (:require [cognitect.transit :as transit])
  (:import (java.io Writer)
           (java.time DayOfWeek Duration Instant LocalDate LocalDateTime LocalTime Month MonthDay OffsetDateTime OffsetTime Period Year YearMonth ZoneId ZonedDateTime)))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(do
  (defn date [x] (LocalDate/parse x))
  (defn instant [x] (Instant/parse x))
  (defn time [x] (LocalTime/parse x))
  (defn offset-time [x] (OffsetTime/parse x))
  (defn duration [x] (Duration/parse x))
  (defn period [x] (Period/parse x))
  (defn zoned-date-time [x] (ZonedDateTime/parse x))
  (defn offset-date-time [x] (OffsetDateTime/parse x))
  (defn date-time [x] (LocalDateTime/parse x))
  (defn year [x] (Year/parse x))
  (defn year-month [x] (YearMonth/parse x))
  (defn zone [x] (ZoneId/of x))
  (defn day-of-week [x] (DayOfWeek/valueOf x))
  (defn month [x] (Month/valueOf x))
  (defn month-day [x] (MonthDay/parse x)))

(defn- print-to-string [t o]
  (format "#xt/%s \"%s\"" t o))

(def ^String print-period (partial print-to-string "period"))
(def ^String print-date (partial print-to-string "date"))
(def ^String print-date-time (partial print-to-string "date-time"))
(def ^String print-zoned-date-time (partial print-to-string "zoned-date-time"))
(def ^String print-offset-time (partial print-to-string "offset-time"))
(def ^String print-instant (partial print-to-string "instant"))
(def ^String print-offset-date-time (partial print-to-string "offset-date-time"))
(def ^String print-zone (partial print-to-string "zone"))
(def ^String print-day-of-week (partial print-to-string "day-of-week"))
(def ^String print-time (partial print-to-string "time"))
(def ^String print-month (partial print-to-string "month"))
(def ^String print-month-day (partial print-to-string "month-day"))
(def ^String print-duration (partial print-to-string "duration"))
(def ^String print-year (partial print-to-string "year"))
(def ^String print-year-month (partial print-to-string "year-month"))

(when-not (or (Boolean/parseBoolean (System/getenv "XTDB_NO_JAVA_TIME_LITERALS"))
              (Boolean/parseBoolean (System/getProperty "xtdb.no-java-time-literals")))
  (defmethod print-method Period [c ^Writer w] (.write w (print-period c)))
  (defmethod print-method LocalDate [c ^Writer w] (.write w (print-date c)))
  (defmethod print-method LocalDateTime [c ^Writer w] (.write w (print-date-time c)))
  (defmethod print-method ZonedDateTime [c ^Writer w] (.write w (print-zoned-date-time c)))
  (defmethod print-method OffsetTime [c ^Writer w] (.write w (print-offset-time c)))
  (defmethod print-method Instant [c ^Writer w] (.write w (print-instant c)))
  (defmethod print-method OffsetDateTime [c ^Writer w] (.write w (print-offset-date-time c)))
  (defmethod print-method ZoneId [c ^Writer w] (.write w (print-zone c)))
  (defmethod print-method DayOfWeek [c ^Writer w] (.write w (print-day-of-week c)))
  (defmethod print-method LocalTime [c ^Writer w] (.write w (print-time c)))
  (defmethod print-method Month [c ^Writer w] (.write w (print-month c)))
  (defmethod print-method MonthDay [c ^Writer w] (.write w (print-month-day c)))
  (defmethod print-method Duration [c ^Writer w] (.write w (print-duration c)))
  (defmethod print-method Year [c ^Writer w] (.write w (print-year c)))
  (defmethod print-method YearMonth [c ^Writer w] (.write w (print-year-month c)))

  (defmethod print-dup Period [c ^Writer w] (.write w (print-period c)))
  (defmethod print-dup LocalDate [c ^Writer w] (.write w (print-date c)))
  (defmethod print-dup LocalDateTime [c ^Writer w] (.write w (print-date-time c)))
  (defmethod print-dup ZonedDateTime [c ^Writer w] (.write w (print-zoned-date-time c)))
  (defmethod print-dup OffsetTime [c ^Writer w] (.write w (print-offset-time c)))
  (defmethod print-dup Instant [c ^Writer w] (.write w (print-instant c)))
  (defmethod print-dup OffsetDateTime [c ^Writer w] (.write w (print-offset-date-time c)))
  (defmethod print-dup ZoneId [c ^Writer w] (.write w (print-zone c)))
  (defmethod print-dup DayOfWeek [c ^Writer w] (.write w (print-day-of-week c)))
  (defmethod print-dup LocalTime [c ^Writer w] (.write w (print-time c)))
  (defmethod print-dup Month [c ^Writer w] (.write w (print-month c)))
  (defmethod print-dup MonthDay [c ^Writer w] (.write w (print-month-day c)))
  (defmethod print-dup Duration [c ^Writer w] (.write w (print-duration c)))
  (defmethod print-dup Year [c ^Writer w] (.write w (print-year c)))
  (defmethod print-dup YearMonth [c ^Writer w] (.write w (print-year-month c))))

(def transit-read-handlers
  (-> {"time/period" period
       "time/date" date
       "time/date-time" date-time
       "time/zoned-date-time" zoned-date-time
       "time/offset-time" offset-time
       "time/instant" instant
       "time/offset-date-time" offset-date-time
       "time/zone" zone
       "time/day-of-week" day-of-week
       "time/time" time
       "time/month" month
       "time/month-day" month-day
       "time/duration" duration
       "time/year" year
       "time/year-month" year-month}
      (update-vals transit/read-handler)))

(def transit-write-handlers
  (-> {Period "time/period"
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
      (update-vals #(transit/write-handler % str))))
