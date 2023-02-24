(ns core2.transit
  (:require [cognitect.transit :as transit]
            [core2.api :as c2]
            [core2.edn :as c2-edn]
            [core2.error :as err]
            [time-literals.read-write :as time-literals.rw])
  (:import core2.api.TransactionInstant
           (core2.types IntervalDayTime IntervalMonthDayNano IntervalYearMonth)
           [java.time DayOfWeek Duration Instant LocalDate LocalDateTime LocalTime Month MonthDay OffsetDateTime OffsetTime Period Year YearMonth ZoneId ZonedDateTime]))

(def tj-read-handlers
  (merge (-> time-literals.rw/tags
             (update-keys str)
             (update-vals transit/read-handler))
         {"core2/tx-key" (transit/read-handler c2/map->TransactionInstant)
          "core2/illegal-arg" (transit/read-handler err/-iae-reader)
          "core2/runtime-err" (transit/read-handler err/-runtime-err-reader)
          "core2/period-duration" c2-edn/period-duration-reader
          "core2.interval/year-month" c2-edn/interval-ym-reader
          "core2.interval/day-time" c2-edn/interval-dt-reader
          "core2.interval/month-day-nano" c2-edn/interval-mdn-reader}))

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
         {TransactionInstant (transit/write-handler "core2/tx-key" #(select-keys % [:tx-id :sys-time]))
          core2.IllegalArgumentException (transit/write-handler "core2/illegal-arg" ex-data)
          core2.RuntimeException (transit/write-handler "core2/runtime-err" ex-data)

          IntervalYearMonth (transit/write-handler "core2.interval/year-month" #(str (.-period ^IntervalYearMonth %)))

          IntervalDayTime (transit/write-handler "core2.interval/day-time"
                                                 #(vector (str (.-period ^IntervalDayTime %))
                                                          (str (.-duration ^IntervalDayTime %))))

          IntervalMonthDayNano (transit/write-handler "core2.interval/month-day-nano"
                                                      #(vector (str (.-period ^IntervalMonthDayNano %))
                                                               (str (.-duration ^IntervalMonthDayNano %))))}))
