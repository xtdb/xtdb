(ns core2.transit
  (:require [cognitect.transit :as transit]
            [time-literals.read-write :as time-literals.rw]
            [core2.api :as c2]
            [core2.error :as err])
  (:import core2.api.TransactionInstant
           [java.time Period
            LocalDate
            LocalDateTime
            ZonedDateTime
            OffsetTime
            Instant
            OffsetDateTime
            ZoneId
            DayOfWeek
            LocalTime
            Month
            Duration
            Year
            YearMonth
            MonthDay]))

(def tj-read-handlers
  (-> time-literals.rw/tags
      (update-keys str)
      (update-vals #(transit/read-handler %))
      (merge {"core2/tx-key" (transit/read-handler c2/map->TransactionInstant)
              "core2/illegal-arg" (transit/read-handler err/-iae-reader)})))

(def tj-write-handlers
  (-> {Period "time/period"
       LocalDate "time/date"
       LocalDateTime  "time/date-time"
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
      (update-vals #(transit/write-handler % str))
      (merge
        {TransactionInstant (transit/write-handler "core2/tx-key" #(select-keys % [:tx-id :sys-time]))
         core2.IllegalArgumentException (transit/write-handler "core2/illegal-arg" ex-data)})))
