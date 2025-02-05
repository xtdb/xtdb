(ns xtdb.expression.temporal-test
  (:require [clojure.string :as str]
            [clojure.test :as t :refer [deftest]]
            [clojure.test.check.clojure-test :as tct]
            [clojure.test.check.generators :as tcg]
            [clojure.test.check.properties :as tcp]
            [xtdb.expression :as expr]
            [xtdb.expression-test :as et]
            [xtdb.test-util :as tu]
            [xtdb.time :as time])
  (:import (java.time Duration Instant LocalDate LocalDateTime LocalTime Period ZoneId ZoneOffset ZonedDateTime)
           java.time.temporal.ChronoUnit
           (org.apache.arrow.vector PeriodDuration)
           (xtdb.types IntervalDayTime IntervalMonthDayNano IntervalYearMonth)))

(t/use-fixtures :each tu/with-allocator)

;; the goal of this test is simply to demonstrate clock affects the computation
;; equality may well remain incorrect for now, it is not the goal
(t/deftest clock-influences-equality-of-ambiguous-datetimes-test
  (t/are [expected a b zone-id]
      (= expected (-> (tu/query-ra [:project [{'res '(= ?a ?b)}]
                                    [:table [{}]]]
                                   {:args {:a a, :b b}
                                    :current-time Instant/EPOCH
                                    :default-tz (ZoneOffset/of zone-id)})
                      first :res))
    ;; identity
    true #xt/date-time "2021-08-09T15:43:23" #xt/date-time "2021-08-09T15:43:23" "Z"

    ;; obvious inequality
    false #xt/date-time "2022-08-09T15:43:23" #xt/date-time "2021-08-09T15:43:23" "Z"

    ;; added fraction
    false #xt/date-time "2021-08-09T15:43:23" #xt/date-time "2021-08-09T15:43:23.3" "Z"

    ;; trailing zero ok
    true #xt/date-time "2021-08-09T15:43:23" #xt/date-time "2021-08-09T15:43:23.0" "Z"

    ;; equality preserved across tz
    true #xt/date-time "2021-08-09T15:43:23" #xt/date-time "2021-08-09T15:43:23" "+02:00"

    ;; offset equiv
    true #xt/zoned-date-time "2021-08-09T15:43:23+02:00" #xt/date-time "2021-08-09T15:43:23" "+02:00"

    ;; offset inequality
    false #xt/zoned-date-time "2021-08-09T15:43:23+02:00" #xt/date-time "2021-08-09T15:43:23" "+03:00"))

(t/deftest test-cast-temporal
  (let [current-time #xt/instant "2022-08-16T20:04:14.423452Z"]
    (letfn [(test-cast
              ([src-value tgt-type] (test-cast src-value tgt-type {}))
              ([src-value tgt-type {:keys [default-tz], :or {default-tz ZoneOffset/UTC}}]
               (-> (tu/query-ra [:project [{'res (list 'cast '?arg tgt-type)}]
                                 [:table [{}]]]
                                {:current-time current-time
                                 :default-tz default-tz
                                 :args {:arg src-value}})
                   first :res)))]

      (t/testing "date ->"
        (t/testing "date"
          (t/is (= #xt/date "2022-08-01"
                   (test-cast #xt/date "2022-08-01"
                              [:date :milli]))))

        (t/testing "ts"
          (t/is (= #xt/date-time "2022-08-01T00:00:00"
                   (test-cast #xt/date "2022-08-01"
                              [:timestamp-local :milli]
                              {:default-tz (ZoneId/of "Europe/London")}))))

        (t/testing "tstz"
          (t/is (= #xt/zoned-date-time "2022-07-31T16:00-07:00[America/Los_Angeles]"
                   (test-cast #xt/date "2022-08-01"
                              [:timestamp-tz :nano "America/Los_Angeles"]
                              {:default-tz (ZoneId/of "Europe/London")})))))

      (t/testing "tstz ->"
        (t/testing "date"
          (t/is (= #xt/date "2022-07-31"
                   (test-cast #xt/zoned-date-time "2022-08-01T05:34:56.789+01:00[Europe/London]"
                              [:date :day]
                              {:default-tz (ZoneId/of "America/Los_Angeles")})))

          (t/is (nil? (test-cast nil [:date :day]))))

        (t/testing "time"
          (t/is (= #xt/time "12:34:56"
                   (test-cast #xt/zoned-date-time "2022-08-01T12:34:56.789Z"
                              [:time-local :second])))

          (t/is (= #xt/time "13:34:56"
                   (test-cast #xt/zoned-date-time "2022-08-01T12:34:56.789Z"
                              [:time-local :second]
                              {:default-tz (ZoneId/of "Europe/London")})))

          (t/is (= #xt/time "05:34:56"
                   (test-cast #xt/zoned-date-time "2022-08-01T13:34:56.789+01:00[Europe/London]"
                              [:time-local :second]
                              {:default-tz (ZoneId/of "America/Los_Angeles")})))

          (t/is (nil? (test-cast nil [:time-local :second]))))

        (t/testing "ts"
          (t/is (= #xt/date-time "2022-08-01T12:34:56"
                   (test-cast #xt/zoned-date-time "2022-08-01T12:34:56.789Z"
                              [:timestamp-local :second])))

          (t/is (= #xt/date-time "2022-08-01T13:34:56"
                   (test-cast #xt/zoned-date-time "2022-08-01T12:34:56.789Z"
                              [:timestamp-local :second]
                              {:default-tz (ZoneId/of "Europe/London")})))

          (t/is (= #xt/date-time "2022-08-01T05:34:56"
                   (test-cast #xt/zoned-date-time "2022-08-01T13:34:56.789+01:00[Europe/London]"
                              [:timestamp-local :second]
                              {:default-tz (ZoneId/of "America/Los_Angeles")})))

          (t/is (nil? (test-cast nil [:timestamp-local :second])))
          (t/is (nil? (test-cast nil [:timestamp-local :second] {:default-tz (ZoneId/of "America/Los_Angeles")}))))

        (t/testing "tstz"
          (t/is (= #xt/zoned-date-time "2022-08-01T13:34:56+01:00[Europe/London]"
                   (test-cast #xt/zoned-date-time "2022-08-01T12:34:56.789012Z"
                              [:timestamp-tz :second "Europe/London"])))

          (t/is (= #xt/zoned-date-time "2022-08-01T13:34:56+01:00[Europe/London]"
                   (test-cast #xt/zoned-date-time "2022-08-01T12:34:56Z"
                              [:timestamp-tz :nano "Europe/London"])))

          (t/is (nil? (test-cast nil [:timestamp-tz :second "Europe/London"] {:default-tz (ZoneId/of "America/Los_Angeles")})))
          (t/is (nil? (test-cast nil [:timestamp-tz :nano "Europe/London"])))))

      (t/testing "ts ->"
        (t/testing "date"
          (t/is (= #xt/date "2022-08-01"
                   (test-cast #xt/date-time "2022-08-01T05:34:56.789"
                              [:date :day]
                              {:default-tz (ZoneId/of "America/Los_Angeles")}))))

        (t/testing "time"
          (t/is (= #xt/time "05:34:56.789012"
                   (test-cast #xt/date-time "2022-08-01T05:34:56.789012345"
                              [:time-local :micro]
                              {:default-tz (ZoneId/of "America/Los_Angeles")}))))

        (t/testing "ts"
          (t/is (= #xt/date-time "2022-08-01T05:34:56"
                   (test-cast #xt/date-time "2022-08-01T05:34:56.789"
                              [:timestamp-local :second "America/Los_Angeles"]
                              {:default-tz (ZoneId/of "America/Los_Angeles")}))))

        (t/testing "tstz"
          (t/is (= #xt/zoned-date-time "2022-08-01T05:34:56-07:00[America/Los_Angeles]"
                   (test-cast #xt/date-time "2022-08-01T05:34:56.789"
                              [:timestamp-tz :second "America/Los_Angeles"]
                              {:default-tz (ZoneId/of "America/Los_Angeles")})))

          (t/is (= #xt/zoned-date-time "2022-08-01T04:34:56-07:00[America/Los_Angeles]"
                   (test-cast #xt/date-time "2022-08-01T12:34:56.789"
                              [:timestamp-tz :second "America/Los_Angeles"]
                              {:default-tz (ZoneId/of "Europe/London")})))))

      (t/testing "time ->"
        (t/testing "date"
          (t/is (thrown? IllegalArgumentException
                         (test-cast #xt/time "12:34:56.789012345" [:date :day]))))

        (t/testing "time"
          (t/is (= #xt/time "12:34:56.789"
                   (test-cast #xt/time "12:34:56.789012345"
                              [:time-local :milli]))))

        (t/testing "ts"
          (t/is (= #xt/date-time "2022-08-16T12:34:56"
                   (test-cast #xt/time "12:34:56.789012345"
                              [:timestamp-local :second]))))

        (t/testing "tstz"
          (t/is (= #xt/zoned-date-time "2022-08-16T12:34:56-07:00[America/Los_Angeles]"
                   (test-cast #xt/time "12:34:56.789012345"
                              [:timestamp-tz :second "America/Los_Angeles"]
                              {:default-tz (ZoneId/of "America/Los_Angeles")})))

          (t/is (= #xt/zoned-date-time "2022-08-16T04:34:56.789012-07:00[America/Los_Angeles]"
                   (test-cast #xt/time "12:34:56.789012345"
                              [:timestamp-tz :micro "America/Los_Angeles"]
                              {:default-tz (ZoneId/of "Europe/London")}))))))))

(t/deftest test-cast-string-and-temporal
  (let [current-time #xt/instant "2022-08-16T20:04:14.423452Z"]
    (letfn [(test-cast
              ([src-value tgt-type] (test-cast src-value tgt-type nil))
              ([src-value tgt-type cast-opts]
               (-> (tu/query-ra [:project [{'res `(~'cast ~src-value ~tgt-type ~cast-opts)}]
                                 [:table [{}]]]
                                {:current-time current-time})
                   first :res)))]

      (t/testing "string ->"
        (t/testing "date"
          (t/is (= #xt/date "2022-08-01" (test-cast "2022-08-01" [:date :day])))
          (t/is (thrown-with-msg? RuntimeException
                                  #"'2022-08-01T00:00:00Z' has invalid format for type date"
                                  (test-cast "2022-08-01T00:00:00Z" [:date :day]))))

        (t/testing "time"
          (t/is (= #xt/time "12:00:01" (test-cast "12:00:01.111" [:time-local :second])))
          (t/is (= #xt/time "12:00:01.111" (test-cast "12:00:01.111" [:time-local :milli])))
          (t/is (thrown-with-msg? RuntimeException
                                  #"'2022-08-01T12:00:01' has invalid format for type time without timezone"
                                  (test-cast "2022-08-01T12:00:01" [:time-local :second]))))

        (t/testing "ts"
          (t/is (= #xt/date-time "2022-08-01T05:34:56.789" (test-cast "2022-08-01T05:34:56.789" [:timestamp-local :milli])))
          (t/is (= #xt/date-time "2022-08-01T05:34:56" (test-cast "2022-08-01T05:34:56.789" [:timestamp-local :second])))
          (t/is (thrown-with-msg? RuntimeException
                                  #"'2022-08-01T05:34:56.789Z' has invalid format for type timestamp without timezone"
                                  (test-cast "2022-08-01T05:34:56.789Z" [:timestamp-local :milli])))
          (t/is (= #xt/date-time "2022-08-01T05:34:56.789"
                   (test-cast "2022-08-01 05:34:56.789" [:timestamp-local :milli]))))

        (t/testing "tstz"
          (t/is (= #xt/zoned-date-time "2022-08-01T05:34:56.789Z[UTC]" (test-cast "2022-08-01T05:34:56.789Z" [:timestamp-tz :milli "UTC"])))
          (t/is (= #xt/zoned-date-time "2022-08-01T05:34:56Z[UTC]" (test-cast "2022-08-01T05:34:56.789Z" [:timestamp-tz :second "UTC"])))
          (t/is (= #xt/zoned-date-time "2022-08-01T04:04:56Z[UTC]" (test-cast "2022-08-01T05:34:56.789+01:30" [:timestamp-tz :second "UTC"])))
          (t/is (thrown-with-msg? RuntimeException
                                  #"'2022-08-01 05:34:56.789' has invalid format for type timestamp with timezone"
                                  (test-cast "2022-08-01 05:34:56.789" [:timestamp-tz :second "UTC"])))
          (t/is (thrown-with-msg? RuntimeException
                                  #"'2022-08-01T05:34:56.789' has invalid format for type timestamp with timezone"
                                  (test-cast "2022-08-01T05:34:56.789" [:timestamp-tz :second "UTC"]))))
        
        (t/testing "duration"
          (t/is (= #xt/duration "PT13M56.123456S" (test-cast "PT13M56.123456S" [:duration :micro])))
          (t/is (= #xt/duration "PT13M56S" (test-cast "PT13M56.123456S" [:duration :second])))
          (t/is (thrown-with-msg? RuntimeException
                                  #"'2022-08-01T00:00:00Z' has invalid format for type duration"
                                  (test-cast "2022-08-01T00:00:00Z" [:duration :micro]))))

        (t/testing "with precision"
          (t/is (= #xt/date-time "2022-08-01T05:34:56" (test-cast "2022-08-01T05:34:56.1234" [:timestamp-local :micro] {:precision 0})))
          (t/is (= #xt/date-time "2022-08-01T05:34:56.1234" (test-cast "2022-08-01T05:34:56.123456" [:timestamp-local :micro] {:precision 4})))
          (t/is (= #xt/zoned-date-time "2022-08-01T05:34:56.12Z[UTC]" (test-cast  "2022-08-01T05:34:56.123456Z" [:timestamp-tz :micro "UTC"] {:precision 2})))
          (t/is (= #xt/zoned-date-time "2022-08-01T04:04:56.12345678Z[UTC]" (test-cast "2022-08-01T05:34:56.123456789+01:30" [:timestamp-tz :nano "UTC"] {:precision 8})))
          (t/is (= #xt/time "05:34:56.1234567" (test-cast "05:34:56.123456789" [:time-local :nano] {:precision 7})))
          (t/is (= #xt/duration "PT13M56.1234567S" (test-cast "PT13M56.123456789S" [:duration :nano] {:precision 7})))
          (t/is (thrown-with-msg? IllegalArgumentException
                                  #"The minimum fractional seconds precision is 0."
                                  (test-cast "05:34:56.123456789" [:time-local :nano] {:precision -1})))
          (t/is (thrown-with-msg? IllegalArgumentException
                                  #"The maximum fractional seconds precision is 9."
                                  (test-cast "05:34:56.123456789" [:time-local :nano] {:precision 11})))))

      (t/testing "->string"
        (t/testing "date"
          (t/is (= "2022-08-01" (test-cast #xt/date "2022-08-01" :utf8))))

        (t/testing "time"
          (t/is (= "12:00:01" (test-cast #xt/time "12:00:01" :utf8))))

        (t/testing "ts"
          (t/is (= "2022-08-01T05:34:56.789" (test-cast #xt/date-time "2022-08-01T05:34:56.789" :utf8))))

        (t/testing "tstz"
          (t/is (= "2022-08-01T05:34:56.789Z[UTC]" (test-cast #xt/zoned-date-time "2022-08-01T05:34:56.789Z[UTC]" :utf8))))
        
        (t/testing "duration"
          (t/is (= "PT13M56.123S" (test-cast #xt/duration "PT13M56.123S" :utf8))))))))

(t/deftest cast-interval-to-duration
  (letfn [(test-cast
            ([src-value tgt-type] (test-cast src-value tgt-type nil))
            ([src-value tgt-type cast-opts]
             (-> (tu/query-ra [:project [{'res `(~'cast ~src-value ~tgt-type ~cast-opts)}]
                               [:table [{}]]])
                 first :res)))]

    (t/testing "cannot cast year-month interval to duration"
      (t/is (thrown-with-msg? UnsupportedOperationException
                              #"Cannot cast a year-month interval to a duration"
                              (test-cast #xt/interval-ym "P12M" [:duration :micro]))))

    (t/testing "cannot cast day-time interval to duration"
      (t/is (thrown-with-msg? UnsupportedOperationException
                              #"Cannot cast a day-time interval to a duration"
                              (test-cast #xt/interval-dt ["P1D" "PT0S"] [:duration :micro]))))

    (t/testing "cannot cast month-day-nano interval to duration when months > 0"
      (t/is (thrown-with-msg? RuntimeException
                              #"Cannot cast month-day-nano intervals when month component is non-zero."
                              (test-cast #xt/interval-mdn ["P4M8D" "PT0S"] [:duration :micro]))))

    (t/testing "casting month-day-nano intervals -> duration"
      (t/is (= #xt/duration "PT25H1S" (test-cast #xt/interval-mdn ["P1D" "PT1H1S"] [:duration :second])))
      (t/is (= #xt/duration "PT3H1M1S" (test-cast #xt/interval-mdn ["P0D" "PT3H1M1S"] [:duration :second])))
      (t/is (= #xt/duration "PT25H1.111111S" (test-cast #xt/interval-mdn ["P1D" "PT1H1.111111111S"] [:duration :micro]))))

    (t/testing "casting month-day-nano intervals -> duration with precision"
      (t/is (= #xt/duration "PT25H1.11S" (test-cast #xt/interval-mdn ["P1D" "PT1H1.111111111S"] [:duration :milli] {:precision 2})))
      (t/is (= #xt/duration "PT25H1.1111111S" (test-cast #xt/interval-mdn ["P1D" "PT1H1.111111111S"] [:duration :nano] {:precision 7})))
      (t/is (thrown-with-msg? IllegalArgumentException
                              #"The maximum fractional seconds precision is 9."
                              (test-cast #xt/interval-mdn ["P4M8D" "PT0S"] [:duration :nano] {:precision 10})))
      (t/is (thrown-with-msg? IllegalArgumentException
                              #"The minimum fractional seconds precision is 0."
                              (test-cast #xt/interval-mdn ["P4M8D" "PT0S"] [:duration :nano] {:precision -1}))))))


(t/deftest cast-duration-to-interval
  (t/testing "without interval qualifier"
    (letfn [(test-cast
              [src-value tgt-type]
              (-> (tu/query-ra [:project [{'res `(~'cast ~src-value ~tgt-type)}]
                                [:table [{}]]])
                  first :res))]

      (t/is (= #xt/interval-mdn ["P0D" "PT3H1.11S"] (test-cast #xt/duration "PT3H1.11S" :interval)))
      (t/is (= #xt/interval-mdn ["P0D" "PT25H1.11S"] (test-cast #xt/duration "PT25H1.11S" :interval)))
      (t/is (= #xt/interval-mdn ["P0D" "PT842H1.11S"] (test-cast #xt/duration "P35DT2H1.11S" :interval)))
      (t/is (= #xt/interval-mdn ["P0D" "PT1M1.111111S"] (test-cast #xt/duration "PT1M1.111111S" :interval)))))

  (t/testing "with interval qualifier"
    (letfn [(test-cast
              [src-value tgt-type iq]
              (-> (tu/query-ra [:project [{'res `(~'cast ~src-value ~tgt-type ~iq)}]
                                [:table [{}]]])
                  first :res))]

      (t/is (= #xt/interval-mdn ["P0D" "PT36H"] (test-cast #xt/duration "PT36H" :interval {:start-field "HOUR" :leading-precision 2 :fractional-precision 0})))
      (t/is (= #xt/interval-mdn ["P1D" "PT0S"] (test-cast #xt/duration "PT36H" :interval {:start-field "DAY" :leading-precision 2 :fractional-precision 0})))
      (t/is (= #xt/interval-mdn ["P1D" "PT12H"] (test-cast #xt/duration "PT36H10M10S" :interval {:start-field "DAY" :end-field "HOUR" :leading-precision 2 :fractional-precision 0})))
      (t/is (= #xt/interval-mdn ["P1D" "PT12H10M"] (test-cast #xt/duration "PT36H10M10S" :interval {:start-field "DAY" :end-field "MINUTE" :leading-precision 2 :fractional-precision 0})))
      (t/is (= #xt/interval-mdn ["P1D" "PT12H10M10S"] (test-cast #xt/duration "PT36H10M10.111S" :interval {:start-field "DAY" :end-field "SECOND" :leading-precision 2 :fractional-precision 0})))
      (t/is (= #xt/interval-mdn ["P1D" "PT12H10M10.1111S"] (test-cast #xt/duration "PT36H10M10.111111S" :interval {:start-field "DAY" :end-field "SECOND" :leading-precision 2 :fractional-precision 4})))
      (t/is (= #xt/interval-mdn ["P0D" "PT3H"] (test-cast #xt/duration "PT3H1M1S" :interval {:start-field "HOUR" :leading-precision 2 :fractional-precision 0})))
      (t/is (= #xt/interval-mdn ["P0D" "PT3H1M"] (test-cast #xt/duration "PT3H1M1S" :interval {:start-field "HOUR" :end-field "MINUTE" :leading-precision 2 :fractional-precision 0})))
      (t/is (= #xt/interval-mdn ["P0D" "PT3H1M1S"] (test-cast #xt/duration "PT3H1M1.111111S" :interval {:start-field "HOUR" :end-field "SECOND" :leading-precision 2 :fractional-precision 0})))
      (t/is (= #xt/interval-mdn ["P0D" "PT3H1M1.111111S"] (test-cast #xt/duration "PT3H1M1.111111S" :interval {:start-field "HOUR" :end-field "SECOND" :leading-precision 2 :fractional-precision 6})))
      (t/is (= #xt/interval-mdn ["P0D" "PT3H1M1.111S"] (test-cast #xt/duration "PT3H1M1.111111S" :interval {:start-field "HOUR" :end-field "SECOND" :leading-precision 2 :fractional-precision 3})))
      (t/is (= #xt/interval-mdn ["P0D" "PT3H1M"] (test-cast #xt/duration "PT3H1M1.111S" :interval {:start-field "MINUTE" :leading-precision 2 :fractional-precision 0})))
      (t/is (= #xt/interval-mdn ["P0D" "PT3H1M1S"] (test-cast #xt/duration "PT3H1M1.111S" :interval {:start-field "MINUTE" :end-field "SECOND" :leading-precision 2 :fractional-precision 0})))
      (t/is (= #xt/interval-mdn ["P0D" "PT3H1M1.111S"] (test-cast #xt/duration "PT3H1M1.111S" :interval {:start-field "MINUTE" :end-field "SECOND" :leading-precision 2 :fractional-precision 3})))
      (t/is (= #xt/interval-mdn ["P0D" "PT3H1M1S"] (test-cast #xt/duration "PT3H1M1.111S" :interval {:start-field "SECOND" :leading-precision 2 :fractional-precision 0})))
      (t/is (= #xt/interval-mdn ["P0D" "PT3H1M1.111S"] (test-cast #xt/duration "PT3H1M1.111111S" :interval {:start-field "SECOND" :leading-precision 2 :fractional-precision 3})))))

  (t/testing "with invalid interval qualifier"
    (t/is (thrown-with-msg?
           UnsupportedOperationException
           #"Cannot cast a duration to a year-month interval"
           (tu/query-ra [:project [{'res `(~'cast #xt/duration "PT3H1M1.111S" :interval {:start-field "YEAR" :end-field "MONTH" :leading-precision 2 :fractional-precision 0})}]
                         [:table [{}]]])))

    (t/is (thrown-with-msg?
           IllegalArgumentException
           #"The maximum fractional seconds precision is 9."
           (tu/query-ra [:project [{'res `(~'cast #xt/duration "PT3H1M1.111S" :interval {:start-field "DAY" :end-field "SECOND" :leading-precision 2 :fractional-precision 11})}]
                         [:table [{}]]])))))

(t/deftest cast-int-to-interval
  (letfn [(test-cast
            [src-value tgt-type cast-opts]
            (-> (tu/query-ra [:project [{'res `(~'cast ~src-value ~tgt-type ~cast-opts)}]
                              [:table [{}]]]
                             )
                first :res))]

    (t/is (= #xt/interval-ym "P12M" (test-cast 1 :interval {:start-field "YEAR"})))
    (t/is (= #xt/interval-ym "P10M" (test-cast 10 :interval {:start-field "MONTH"})))
    (t/is (= #xt/interval-mdn ["P10D" "PT0S"] (test-cast 10 :interval {:start-field "DAY"})))
    (t/is (= #xt/interval-mdn ["P0D" "PT10H"] (test-cast 10 :interval {:start-field "HOUR"})))
    (t/is (= #xt/interval-mdn ["P0D" "PT10M"] (test-cast 10 :interval {:start-field "MINUTE"})))
    (t/is (= #xt/interval-mdn ["P0D" "PT10S"] (test-cast 10 :interval {:start-field "SECOND"})))
    (t/is (thrown-with-msg?
           IllegalArgumentException
           #"Cannot cast integer to a multi field interval"
           (test-cast 10 :interval {:start-field "DAY"
                                    :end-field "HOUR"})))))

(t/deftest cast-utf8-to-interval-without-qualifier
  (letfn [(test-cast
            [src-value tgt-type]
            (-> (tu/query-ra [:project [{'res `(~'cast ~src-value ~tgt-type)}]
                              [:table [{}]]])
                first :res))]
    
    (t/are [expected src-value] (= expected (test-cast src-value :interval))
      #xt/interval-mdn ["P12M" "PT0S"] "P12M"
      #xt/interval-mdn ["P14M" "PT0S"] "P1Y2M"
      #xt/interval-mdn ["P1D" "PT0S"] "P1D"
      #xt/interval-mdn ["P-1D" "PT0S"] "P-1D"
      #xt/interval-mdn ["P1D" "PT1H1M"] "P1DT1H1M"
      #xt/interval-mdn ["P1M2D" "PT1H"] "P1M2DT1H"
      #xt/interval-mdn ["P0D" "PT10H10M10.111111S"] "PT10H10M10.111111S"
      #xt/interval-mdn ["P0D" "PT-10H-10M-10.111111S"] "PT-10H-10M-10.111111S")))

(t/deftest cast-utf8-to-interval-with-qualifier
  (letfn [(test-cast
            [src-value tgt-type cast-opts]
            (-> (tu/query-ra [:project [{'res `(~'cast ~src-value ~tgt-type ~cast-opts)}]
                              [:table [{}]]])
                first :res))]

    (t/is (= #xt/interval-ym "P12M"
             (test-cast "1" :interval {:start-field "YEAR", :end-field nil, :leading-precision 2 :fractional-precision 0})))

    (t/is (= #xt/interval-dt ["P10D" "PT0S"]
             (test-cast "10" :interval {:start-field "DAY", :end-field nil, :leading-precision 2 :fractional-precision 0})))

    (t/is (= #xt/interval-ym "P22M"
             (test-cast "1-10" :interval {:start-field "YEAR", :end-field "MONTH", :leading-precision 2 :fractional-precision 0})))

    (t/is (= #xt/interval-mdn ["P1D" "PT10H"]
             (test-cast "1 10" :interval {:start-field "DAY", :end-field "HOUR", :leading-precision 2 :fractional-precision 0})))

    (t/is (= #xt/interval-mdn ["P1D" "PT10H10M10S"]
             (test-cast "1 10:10:10" :interval {:start-field "DAY", :end-field "SECOND", :leading-precision 2 :fractional-precision 6})))

    (t/is (= #xt/interval-mdn ["P1D" "PT10H10M10.111111S"]
             (test-cast "1 10:10:10.111111" :interval {:start-field "DAY", :end-field "SECOND", :leading-precision 2 :fractional-precision 6})))

    (t/is (thrown-with-msg?
           IllegalArgumentException
           #"Interval end field must have less significance than the start field."
           (test-cast "1 10:10:10.111111" :interval {:start-field "SECOND", :end-field "DAY", :leading-precision 2 :fractional-precision 6})))))

(t/deftest cast-interval-to-string
  (letfn [(test-cast
            [src-value tgt-type]
            (-> (tu/query-ra [:project [{'res `(~'cast ~src-value ~tgt-type)}]
                              [:table [{}]]])
                first :res))]
    (t/testing "year-month interval -> string"
      (t/are [expected src-value] (= expected (test-cast src-value :utf8))
        "P12MT0S" #xt/interval-ym "P12M" 
        "P-12MT0S" #xt/interval-ym "-P12M"
        "P22MT0S" #xt/interval-ym "P22M"
        "P-22MT0S" #xt/interval-ym "-P22M"
        "P6MT0S" #xt/interval-ym "P6M"))

    (t/testing "month-day-nano interval -> string"
      (t/are [expected src-value] (= expected (test-cast src-value :utf8))
        "P1DT0S" #xt/interval-mdn ["P1D" "PT0S"]
        "P0DT1H" #xt/interval-mdn ["P0D" "PT1H"]
        "P0DT1M" #xt/interval-mdn ["P0D" "PT1M"]
        "P0DT1S" #xt/interval-mdn ["P0D" "PT1S"]
        "P1DT1H" #xt/interval-mdn ["P1D" "PT1H"]
        "P1DT1H1M" #xt/interval-mdn ["P1D" "PT1H1M"]
        "P1DT1H1S" #xt/interval-mdn ["P1D" "PT1H1S"]
        "P1DT1H1M1.111111111S" #xt/interval-mdn ["P1D" "PT1H1M1.111111111S"]
        "P-1DT0S" #xt/interval-mdn ["-P1D" "PT0S"]
        "P0DT-10H" #xt/interval-mdn ["P0D" "PT-10H"]
        "P-1DT-10H-10M-10.111111111S" #xt/interval-mdn ["-P1D" "PT-10H-10M-10.111111111S"]
        "P0DT-10H-10M" #xt/interval-mdn ["P0D" "PT-10H-10M"]))
    
    (t/testing "day-time interval -> string"
      (t/are [expected src-value] (= expected (test-cast src-value :utf8))
        "P1DT0S" #xt/interval-dt ["P1D" "PT0S"]
        "P0DT1H" #xt/interval-dt ["P0D" "PT1H"]
        "P0DT1M" #xt/interval-dt ["P0D" "PT1M"]
        "P0DT1S" #xt/interval-dt ["P0D" "PT1S"]
        "P0DT1.111S" #xt/interval-dt ["P0D" "PT1.111S"]
        "P1DT1H1S" #xt/interval-dt ["P1D" "PT1H1S"]))))

(t/deftest cast-interval-to-interval
  (letfn [(test-cast
            [src-value tgt-type iq]
            (-> (tu/query-ra [:project [{'res `(~'cast ~src-value ~tgt-type ~iq)}]
                              [:table [{}]]])
                first :res))]
    (t/testing "casting interval to interval without qualifier is a no-op"
      (t/is (= #xt/interval-ym "P12M" (test-cast #xt/interval-ym "P12M" :interval {})))
      (t/is (= #xt/interval-mdn ["P0D" "PT1H"] (test-cast #xt/interval-mdn ["P0D" "PT1H"] :interval {})))
      (t/is (= #xt/interval-dt ["P0D" "PT1H"] (test-cast #xt/interval-dt ["P0D" "PT1H"] :interval {}))))

    (t/testing "casting YM interval to non YM interval should fail"
      (t/is (thrown-with-msg?
             UnsupportedOperationException
             #"Cannot cast a Year-Month interval with a non Year-Month interval qualifier"
             (test-cast #xt/interval-ym "P12M" :interval {:start-field "DAY", :leading-precision 2, :fractional-precision 0}))))

    (t/testing "casting non YM interval to YM interval should fail"
      (t/is (thrown-with-msg?
             UnsupportedOperationException
             #"Cannot cast a non Year-Month interval with a Year-Month interval qualifier"
             (test-cast #xt/interval-mdn ["P1M1D" "PT1H"] :interval {:start-field "YEAR", :end-field "MONTH", :leading-precision 2, :fractional-precision 0}))))

    (t/testing "invlaid fractional precision throws exception"
      (t/is (thrown-with-msg?
             IllegalArgumentException
             #"The maximum fractional seconds precision is 9."
             (test-cast #xt/interval-mdn ["P1D" "PT1H"] :interval {:start-field "DAY", :end-field "SECOND", :leading-precision 2, :fractional-precision 11}))))

    (t/testing "casting between interval year month"
      (t/is (= #xt/interval-ym "P12M" (test-cast #xt/interval-ym "P13M" :interval {:start-field "YEAR", :leading-precision 2, :fractional-precision 0})))
      (t/is (= #xt/interval-ym "P0D" (test-cast #xt/interval-ym "P11M" :interval {:start-field "YEAR", :leading-precision 2, :fractional-precision 0})))
      (t/is (= #xt/interval-ym "P23M" (test-cast #xt/interval-ym "P1Y11M" :interval {:start-field "YEAR", :end-field "MONTH", :leading-precision 2, :fractional-precision 0})))
      (t/is (= #xt/interval-ym "P12M" (test-cast #xt/interval-ym "P1Y" :interval {:start-field "MONTH", :leading-precision 2, :fractional-precision 0}))))

    (t/testing "casting between interval month-day-nano"
      (t/is (= #xt/interval-mdn ["P0D" "PT36H"] (test-cast #xt/interval-mdn ["P0D" "PT36H"] :interval {:start-field "HOUR" :leading-precision 2 :fractional-precision 0})))
      (t/is (= #xt/interval-mdn ["P1D" "PT0S"] (test-cast #xt/interval-mdn ["P0D" "PT36H"]  :interval {:start-field "DAY" :leading-precision 2 :fractional-precision 0})))
      (t/is (= #xt/interval-mdn ["P1D" "PT12H"] (test-cast #xt/interval-mdn ["P0D" "PT36H10M10S"] :interval {:start-field "DAY" :end-field "HOUR" :leading-precision 2 :fractional-precision 0})))
      (t/is (= #xt/interval-mdn ["P1D" "PT12H10M"] (test-cast #xt/interval-mdn ["P0D" "PT36H10M10S"] :interval {:start-field "DAY" :end-field "MINUTE" :leading-precision 2 :fractional-precision 0})))
      (t/is (= #xt/interval-mdn ["P1D" "PT12H10M10S"] (test-cast #xt/interval-mdn ["P0D" "PT36H10M10.111S"] :interval {:start-field "DAY" :end-field "SECOND" :leading-precision 2 :fractional-precision 0})))
      (t/is (= #xt/interval-mdn ["P1D" "PT12H10M10.1111S"] (test-cast #xt/interval-mdn ["P0D" "PT36H10M10.111111S"] :interval {:start-field "DAY" :end-field "SECOND" :leading-precision 2 :fractional-precision 4})))
      (t/is (= #xt/interval-mdn ["P0D" "PT3H"] (test-cast #xt/interval-mdn ["P0D" "PT3H1M1S"] :interval {:start-field "HOUR" :leading-precision 2 :fractional-precision 0})))
      (t/is (= #xt/interval-mdn ["P0D" "PT3H1M"] (test-cast #xt/interval-mdn ["P0D" "PT3H1M1S"] :interval {:start-field "HOUR" :end-field "MINUTE" :leading-precision 2 :fractional-precision 0})))
      (t/is (= #xt/interval-mdn ["P0D" "PT3H1M1S"] (test-cast #xt/interval-mdn ["P0D" "PT3H1M1.111111S"] :interval {:start-field "HOUR" :end-field "SECOND" :leading-precision 2 :fractional-precision 0})))
      (t/is (= #xt/interval-mdn ["P0D" "PT3H1M1.111111S"] (test-cast #xt/interval-mdn ["P0D" "PT3H1M1.111111S"] :interval {:start-field "HOUR" :end-field "SECOND" :leading-precision 2 :fractional-precision 6})))
      (t/is (= #xt/interval-mdn ["P0D" "PT3H1M1.111S"] (test-cast #xt/interval-mdn ["P0D" "PT3H1M1.111111S"] :interval {:start-field "HOUR" :end-field "SECOND" :leading-precision 2 :fractional-precision 3})))
      (t/is (= #xt/interval-mdn ["P0D" "PT3H1M"] (test-cast #xt/interval-mdn ["P0D" "PT3H1M1.111S"] :interval {:start-field "MINUTE" :leading-precision 2 :fractional-precision 0})))
      (t/is (= #xt/interval-mdn ["P0D" "PT3H1M1S"] (test-cast #xt/interval-mdn ["P0D" "PT3H1M1.111S"] :interval {:start-field "MINUTE" :end-field "SECOND" :leading-precision 2 :fractional-precision 0})))
      (t/is (= #xt/interval-mdn ["P0D" "PT3H1M1.111S"] (test-cast #xt/interval-mdn ["P0D" "PT3H1M1.111S"] :interval {:start-field "MINUTE" :end-field "SECOND" :leading-precision 2 :fractional-precision 3})))
      (t/is (= #xt/interval-mdn ["P0D" "PT3H1M1S"] (test-cast #xt/interval-mdn ["P0D" "PT3H1M1.111S"] :interval {:start-field "SECOND" :leading-precision 2 :fractional-precision 0})))
      (t/is (= #xt/interval-mdn ["P0D" "PT3H1M1.111S"] (test-cast #xt/interval-mdn ["P0D" "PT3H1M1.111111S"] :interval {:start-field "SECOND" :leading-precision 2 :fractional-precision 3})))
      (t/is (= #xt/interval-mdn ["P0D" "PT36H"] (test-cast #xt/interval-mdn ["P1D" "PT12H"] :interval {:start-field "HOUR" :leading-precision 2 :fractional-precision 0}))))

    (t/testing "casting between interval day-time"
      (t/is (= #xt/interval-dt ["P0D" "PT36H"] (test-cast #xt/interval-dt ["P0D" "PT36H"] :interval {:start-field "HOUR" :leading-precision 2 :fractional-precision 0})))
      (t/is (= #xt/interval-dt ["P1D" "PT12H"] (test-cast #xt/interval-dt ["P0D" "PT36H"] :interval {:start-field "DAY" :end-field "HOUR" :leading-precision 2 :fractional-precision 0})))
      (t/is (= #xt/interval-dt ["P1D" "PT0H"] (test-cast #xt/interval-dt ["P0D" "PT36H"] :interval {:start-field "DAY" :leading-precision 2 :fractional-precision 0}))))))

(defn age [dt1 dt2]
  (-> (tu/query-ra [:project [{'res `(~'age ~dt1 ~dt2)}]
                    [:table [{}]]])
      first :res))

;; Keeping in mind - age(dt1, dt2) is dt1 - dt2.
;; As such, we refer to start time / end time, where age(end time, start time)
(t/deftest test-age-edge-cases
  ;; Same day, different times
  (t/is (= #xt/interval-mdn ["P0D" "PT23H"] (age #xt/date-time "2021-10-31T23:00" #xt/date-time "2021-10-31T00:00")))
  ;; End of month to next month (with and without time differences)
  (t/is (= #xt/interval-mdn ["P0D" "PT2H"] (age #xt/date-time "2021-11-01T01:00" #xt/date-time "2021-10-31T23:00")))
  ;; Leap year
  (t/is (= #xt/interval-mdn ["P2D" "PT0S"] (age #xt/date-time "2020-03-01T12:00" #xt/date-time "2020-02-28T12:00")))
  ;; Different years
  (t/is (= #xt/interval-mdn ["P0D" "PT2H"] (age #xt/date-time "2021-01-01T01:00" #xt/date-time "2020-12-31T23:00")))
  ;; More than a month
  (t/is (= #xt/interval-mdn ["P1M1D" "PT0S"] (age #xt/date-time "2021-02-02T00:00" #xt/date-time "2021-01-01T00:00")))
  ;; More than a year
  (t/is (= #xt/interval-mdn ["P24M" "PT0S"] (age #xt/date-time "2021-01-01T00:00" #xt/date-time "2019-01-01T00:00")))
  ;; Start time after end time (dt1 - dt2)
  (t/is (= #xt/interval-mdn ["P-12M" "PT0S"] (age #xt/date-time "2020-01-01T00:00" #xt/date-time "2021-01-01T00:00")))
  ;; Time within the same day but with start time after end time
  (t/is (= #xt/interval-mdn ["P0D" "PT-1H"] (age #xt/date-time "2021-01-01T11:00" #xt/date-time "2021-01-01T12:00")))
  ;; Exactly the same start and end time
  (t/is (= #xt/interval-mdn ["P0D" "PT0S"] (age #xt/date-time "2021-01-01T00:00" #xt/date-time "2021-01-01T00:00"))))

(t/deftest test-age-fn
  (binding [expr/*default-tz* #xt/zone "UTC"]
    (t/testing "testing with local date times"
      (t/are [expected dt1 dt2] (= expected (age dt1 dt2))
        #xt/interval-mdn ["P0D" "PT2H"] #xt/date-time "2022-05-02T01:00" #xt/date-time "2022-05-01T23:00"
        #xt/interval-mdn ["P6M" "PT0S"] #xt/date-time "2022-11-01T00:00" #xt/date-time "2022-05-01T00:00"
        #xt/interval-mdn ["P-6M" "PT0S"] #xt/date-time "2022-05-01T00:00" #xt/date-time "2022-11-01T00:00"
        #xt/interval-mdn ["P0D" "PT0S"] #xt/date-time "2023-01-01T00:00" #xt/date-time "2023-01-01T00:00"
        #xt/interval-mdn ["P30D" "PT12H"] #xt/date-time "2023-02-01T12:00" #xt/date-time "2023-01-02T00:00"
        #xt/interval-mdn ["P-12M" "PT0S"] #xt/date-time "2022-01-01T00:00" #xt/date-time "2023-01-01T00:00"
        #xt/interval-mdn ["P0D" "PT1H"] #xt/date-time "2023-01-01T01:00" #xt/date-time "2023-01-01T00:00"
        #xt/interval-mdn ["P24M" "PT3H"] #xt/date-time "2025-01-01T03:00" #xt/date-time "2023-01-01T00:00"
        #xt/interval-mdn ["P0D" "PT-23H"] #xt/date-time "2023-01-01T01:00" #xt/date-time "2023-01-02T00:00"
        #xt/interval-mdn ["P48M" "PT6M"] #xt/date-time "2027-01-01T00:06" #xt/date-time "2023-01-01T00:00"
        #xt/interval-mdn ["P-48M" "PT-6M"] #xt/date-time "2023-01-01T00:00" #xt/date-time "2027-01-01T00:06"
        #xt/interval-mdn ["P29D" "PT23H"] #xt/date-time "2023-02-28T23:00" #xt/date-time "2023-01-30T00:00"
        #xt/interval-mdn ["P28D" "PT23H"] #xt/date-time "2023-03-29T23:00" #xt/date-time "2023-03-01T00:00"
        #xt/interval-mdn ["P0D" "PT-1M"] #xt/date-time "2023-04-01T00:00" #xt/date-time "2023-04-01T00:01"
        #xt/interval-mdn ["P0D" "PT0.001S"] #xt/date-time "2023-07-01T12:00:30.501" #xt/date-time "2023-07-01T12:00:30.500"
        #xt/interval-mdn ["P0D" "PT-0.001S"] #xt/date-time "2023-07-01T12:00:30.499" #xt/date-time "2023-07-01T12:00:30.500"
        #xt/interval-mdn ["P25M3D" "PT4H5M6.007S"] #xt/date-time "2025-02-04T18:06:07.007" #xt/date-time "2023-01-01T14:01:01.000"))

    (t/testing "testing with zoned date times"
      (t/are [expected dt1 dt2] (= expected (age dt1 dt2))
        #xt/interval-mdn ["P0D" "PT1H"] #xt/zoned-date-time "2023-06-01T11:00+01:00[Europe/London]" #xt/zoned-date-time "2023-06-01T11:00+02:00[Europe/Berlin]"
        #xt/interval-mdn ["P0D" "PT0H"] #xt/zoned-date-time "2023-06-01T11:00+01:00[Europe/London]" #xt/zoned-date-time "2023-06-01T12:00+02:00[Europe/Berlin]"
        #xt/interval-mdn ["P0D" "PT0S"] #xt/zoned-date-time "2023-06-01T11:00+01:00[Europe/London]" #xt/zoned-date-time "2023-06-01T12:00+02:00[Europe/Berlin]"
        #xt/interval-mdn ["P0D" "PT1M"] #xt/zoned-date-time "2023-03-26T03:00+02:00[Europe/Berlin]" #xt/zoned-date-time "2023-03-26T01:59+01:00[Europe/Berlin]"
        #xt/interval-mdn ["P0D" "PT-1H-59M"] #xt/zoned-date-time "2023-10-29T01:00+01:00[Europe/Berlin]" #xt/zoned-date-time "2023-10-29T02:59+01:00[Europe/Berlin]"
        #xt/interval-mdn ["P-1D" "PT0S"] #xt/zoned-date-time "2023-01-01T00:00+14:00[Pacific/Kiritimati]" #xt/zoned-date-time "2023-01-01T00:00-10:00[Pacific/Honolulu]"
        #xt/interval-mdn ["P1D" "PT0S"] #xt/zoned-date-time "2023-01-01T00:00-10:00[Pacific/Honolulu]" #xt/zoned-date-time "2023-01-01T00:00+14:00[Pacific/Kiritimati]"
        #xt/interval-mdn ["P0D" "PT0S"] #xt/zoned-date-time "2023-01-01T12:00-05:00[America/New_York]" #xt/zoned-date-time "2023-01-01T18:00+01:00[Europe/Paris]"
        #xt/interval-mdn ["P12M" "PT0S"] #xt/zoned-date-time "2024-03-30T01:00+01:00[Europe/Berlin]" #xt/zoned-date-time "2023-03-30T01:00+01:00[Europe/Berlin]"
        #xt/interval-mdn ["P0D" "PT3H"] #xt/zoned-date-time "2023-05-15T15:00+02:00[Europe/Berlin]" #xt/zoned-date-time "2023-05-15T12:00+02:00[Europe/Berlin]"
        #xt/interval-mdn ["P1D" "PT2H"] #xt/zoned-date-time "2023-05-16T02:00+02:00[Europe/Berlin]" #xt/zoned-date-time "2023-05-14T22:00+00:00[Europe/Berlin]"
        #xt/interval-mdn ["P0D" "PT3H"] #xt/zoned-date-time "2023-05-16T03:00+02:00[Europe/Berlin]" #xt/zoned-date-time "2023-05-15T22:00+00:00[Europe/London]"
        #xt/interval-mdn ["P0D" "PT0.001S"] #xt/zoned-date-time "2023-07-01T12:00:30.501+02:00[Europe/Berlin]" #xt/zoned-date-time "2023-07-01T12:00:30.500+02:00[Europe/Berlin]"
        #xt/interval-mdn ["P0D" "PT-0.001S"] #xt/zoned-date-time "2023-07-01T12:00:30.499+02:00[Europe/Berlin]" #xt/zoned-date-time "2023-07-01T12:00:30.500+02:00[Europe/Berlin]"
        #xt/interval-mdn ["P25M3D" "PT3H5M6.007S"] #xt/zoned-date-time "2025-02-04T18:06:07.007+01:00[Europe/Paris]" #xt/zoned-date-time "2023-01-01T14:01:01.000+00:00[Europe/Paris]"))

    (t/testing "testing with dates"
      (t/are [expected dt1 dt2] (= expected (age dt1 dt2))
        #xt/interval-mdn ["P1D" "PT0S"] #xt/date "2023-01-02" #xt/date "2023-01-01"
        #xt/interval-mdn ["P-1D" "PT0S"] #xt/date "2023-01-01" #xt/date "2023-01-02"
        #xt/interval-mdn ["P30D" "PT0S"] #xt/date "2023-01-31" #xt/date "2023-01-01"
        #xt/interval-mdn ["P2D" "PT0S"] #xt/date "2020-03-01" #xt/date "2020-02-28"
        #xt/interval-mdn ["P1D" "PT0S"] #xt/date "2021-03-01" #xt/date "2021-02-28"
        #xt/interval-mdn ["P12M" "PT0S"] #xt/date "2024-01-01" #xt/date "2023-01-01"
        #xt/interval-mdn ["P-12M" "PT0S"] #xt/date "2023-01-01" #xt/date "2024-01-01"
        #xt/interval-mdn ["P13M" "PT0S"] #xt/date "2024-02-01" #xt/date "2023-01-01"
        #xt/interval-mdn ["P-13M" "PT0S"] #xt/date "2023-01-01" #xt/date "2024-02-01"
        #xt/interval-mdn ["P0D" "PT0S"] #xt/date "2023-01-01" #xt/date "2023-01-01"))

    (t/testing "test with mixed types"
      (t/are [expected dt1 dt2] (= expected (age dt1 dt2))
        #xt/interval-mdn ["P1D" "PT0S"] #xt/date "2023-01-02" #xt/date-time "2023-01-01T00:00"
        #xt/interval-mdn ["P-1D" "PT0S"] #xt/date-time "2023-01-01T00:00" #xt/date "2023-01-02"
        #xt/interval-mdn ["P12M" "PT0S"] #xt/date "2024-01-01" #xt/zoned-date-time "2023-01-01T00:00+00:00[UTC]"
        #xt/interval-mdn ["P-12M" "PT0S"] #xt/zoned-date-time "2023-01-01T00:00+00:00[UTC]" #xt/date "2024-01-01"
        #xt/interval-mdn ["P0D" "PT2H"] #xt/date-time "2023-06-01T12:00" #xt/zoned-date-time "2023-06-01T11:00+01:00[Europe/London]"
        #xt/interval-mdn ["P0D" "PT2H"] #xt/zoned-date-time "2023-06-01T09:00-05:00[America/Chicago]" #xt/date-time "2023-06-01T12:00"
        #xt/interval-mdn ["P6M" "PT0S"] #xt/zoned-date-time "2022-11-01T00:00+00:00[Europe/London]" #xt/date "2022-05-01"
        #xt/interval-mdn ["P5M30D" "PT23H"] #xt/zoned-date-time "2022-11-01T00:00+01:00[Europe/Paris]" #xt/date "2022-05-01"
        #xt/interval-mdn ["P-6M" "PT0S"] #xt/date "2022-05-01" #xt/zoned-date-time "2022-11-01T00:00+00:00[Europe/London]"
        #xt/interval-mdn ["P0D" "PT2H0.001S"] #xt/date-time "2023-07-01T12:00:30.501" #xt/zoned-date-time "2023-07-01T12:00:30.500+02:00[Europe/Berlin]"
        #xt/interval-mdn ["P0D" "PT-2H-0.001S"] #xt/zoned-date-time "2023-07-01T12:00:30.499+02:00[Europe/Berlin]" #xt/date-time "2023-07-01T12:00:30.500"))))

(def ^:private instant-gen
  (->> (tcg/tuple (tcg/choose (.getEpochSecond #xt/instant "2020-01-01T00:00:00Z")
                              (.getEpochSecond #xt/instant "2040-01-01T00:00:00Z"))
                  (->> (tcg/choose 0 #=(long 1e6))
                       (tcg/fmap #(* % 1000))))
       (tcg/fmap (fn [[s ns]]
                   (Instant/ofEpochSecond s ns)))))

(def ^:private ldt-gen
  (->> instant-gen
       (tcg/fmap (fn [^Instant inst]
                   (LocalDateTime/ofInstant inst ZoneOffset/UTC)))))

(def ^:private ld-gen
  (->> instant-gen
       (tcg/fmap (fn [^Instant inst]
                   (LocalDate/ofInstant inst ZoneOffset/UTC)))))

(def ^:private lt-gen
  (->> instant-gen
       (tcg/fmap (fn [^Instant inst]
                   (LocalTime/ofInstant inst ZoneOffset/UTC)))))

(def ^:private zone-id-gen
  (->> (tcg/elements (ZoneId/getAvailableZoneIds))
       (tcg/fmap #(ZoneId/of %))))

(def ^:private zdt-gen
  (->> (tcg/tuple instant-gen zone-id-gen)
       (tcg/fmap (fn [[inst zone-id]]
                   (ZonedDateTime/ofInstant inst zone-id)))))

(defprotocol BackToInstant (->inst [t current-timestamp zone-id]))

(extend-protocol BackToInstant
  ZonedDateTime
  (->inst [zdt _now _zone-id]
    (.toInstant zdt))

  LocalDateTime
  (->inst [ldt now ^ZoneId zone-id]
    (-> (.atZone ldt zone-id)
        (->inst now zone-id)))

  LocalDate
  (->inst [ld now zone-id]
    (-> (.atStartOfDay ld)
        (->inst now zone-id)))

  LocalTime
  (->inst [lt now zone-id]
    (-> (LocalDateTime/of (LocalDate/ofInstant now zone-id) lt)
        (->inst now zone-id))))

(tct/defspec test-cast-to-date
  (tcp/for-all [t1 (tcg/one-of [ldt-gen ld-gen zdt-gen])
                default-tz zone-id-gen
                now instant-gen]
    (= (LocalDate/ofInstant (->inst t1 now default-tz) default-tz)
       (->> (tu/query-ra [:project [{'res (list 'cast '?t1 [:date :day])}]
                          [:table [{}]]]
                         {:args {:t1 t1}
                          :default-tz default-tz})
            first :res))))

(tct/defspec test-cast-to-time
  (tcp/for-all [t1 (tcg/one-of [ldt-gen lt-gen zdt-gen])
                default-tz zone-id-gen
                now instant-gen]
    (= (if (instance? LocalTime t1) ; see #372
         t1
         (LocalTime/ofInstant (->inst t1 now default-tz) default-tz))
       (->> (tu/query-ra [:project [{'res (list 'cast '?t1 [:time-local :micro])}]
                          [:table [{}]]]
                         {:args {:t1 t1}
                          :current-time now
                          :default-tz default-tz})
            first :res))))

(tct/defspec test-cast-to-ts
  (tcp/for-all [t1 (tcg/one-of [ldt-gen ld-gen lt-gen zdt-gen])
                default-tz zone-id-gen
                now instant-gen]
    (= (if (instance? LocalDate t1) ; see fix for #371
         (.atStartOfDay ^LocalDate t1)
         (LocalDateTime/ofInstant (->inst t1 now default-tz) default-tz))
       (->> (tu/query-ra [:project [{'res (list 'cast '?t1 [:timestamp-local :micro])}]
                          [:table [{}]]]
                         {:args {:t1 t1}
                          :current-time now
                          :default-tz default-tz})
            first :res))))

(tct/defspec test-cast-to-tstz
  (tcp/for-all [t1 (tcg/one-of [ldt-gen ld-gen lt-gen zdt-gen])
                default-tz zone-id-gen
                zdt-tz zone-id-gen
                now instant-gen]
    (= (ZonedDateTime/ofInstant (->inst t1 now default-tz) zdt-tz)
       (->> (tu/query-ra [:project [{'res (list 'cast '?t1 [:timestamp-tz :micro (str zdt-tz)])}]
                          [:table [{}]]]
                         {:args {:t1 t1}
                          :current-time now
                          :default-tz default-tz})
            first :res))))

(t/deftest test-temporal-arithmetic
  (letfn [(test-arithmetic
            ([f x y] (test-arithmetic f x y {}))
            ([f x y {:keys [default-tz], :or {default-tz #xt/zone "America/Los_Angeles"}}]
             (-> (tu/query-ra [:project [{'res (list f '?x '?y)}]
                               [:table [{}]]]
                              {:default-tz default-tz
                               :args {:x x, :y y}})
                 first :res)))]

    (t/testing "(+ datetime duration)"
      (t/is (= #xt/date-time "2022-08-01T01:15:43.342"
               (test-arithmetic '+ #xt/date "2022-08-01" #xt/duration "PT1H15M43.342S")))

      (t/is (= #xt/date-time "2022-08-01T02:31:26.684"
               (test-arithmetic '+ #xt/date-time "2022-08-01T01:15:43.342" #xt/duration "PT1H15M43.342S")))

      (t/is (= #xt/date-time "2022-08-01T02:31:26.684"
               (test-arithmetic '+ #xt/date-time "2022-08-01T01:15:43.342" #xt/time "01:15:43.342")))

      (t/is (= #xt/zoned-date-time "2022-08-01T02:31:26.684+01:00[Europe/London]"
               (test-arithmetic '+ #xt/zoned-date-time "2022-08-01T01:15:43.342+01:00[Europe/London]" #xt/duration "PT1H15M43.342S")))

      (t/is (nil? (test-arithmetic '+ nil #xt/duration "PT1H15M43.342S"))))

    (t/testing "(+ datetime interval)"
      (t/is (= #xt/date "2023-08-01"
               (test-arithmetic '+ #xt/date "2022-08-01" #xt/interval-ym "P1Y")))

      (t/is (= #xt/date-time "2022-08-04T01:15:43.342"
               (test-arithmetic '+ #xt/date "2022-08-01" #xt/interval-dt ["P3D" "PT1H15M43.342S"])))

      (t/is (= #xt/date-time "2022-09-04T02:31:26.684"
               (test-arithmetic '+ #xt/date-time "2022-08-01T01:15:43.342" #xt/interval-mdn ["P1M3D" "PT1H15M43.342S"])))

      (t/is (= #xt/zoned-date-time "2022-08-01T02:31:26.684+01:00[Europe/London]"
               (test-arithmetic '+ #xt/interval-mdn ["P0D" "PT1H15M43.342S"] #xt/zoned-date-time "2022-08-01T01:15:43.342+01:00[Europe/London]")))

      (t/is (= #xt/zoned-date-time "2022-10-31T12:00+00:00[Europe/London]"
               (test-arithmetic '+ #xt/interval-mdn ["P2D" "PT1H"] #xt/zoned-date-time "2022-10-29T11:00+01:00[Europe/London]"))
            "clock change")

      (t/is (nil? (test-arithmetic '+ #xt/interval-mdn ["P2D" "PT1H"] nil))))

    (t/testing "(- datetime duration)"
      (t/is (= #xt/date-time "2022-07-31T22:44:16.658"
               (test-arithmetic '- #xt/date "2022-08-01" #xt/duration "PT1H15M43.342S")))

      (t/is (= #xt/date-time "2022-08-01T01:15:43.342"
               (test-arithmetic '- #xt/date-time "2022-08-01T02:31:26.684" #xt/duration "PT1H15M43.342S")))

      (t/is (= #xt/date-time "2022-08-01T01:15:43.342"
               (test-arithmetic '- #xt/date-time "2022-08-01T02:31:26.684" #xt/time "01:15:43.342")))

      (t/is (= #xt/zoned-date-time "2022-08-01T01:15:43.342+01:00[Europe/London]"
               (test-arithmetic '- #xt/zoned-date-time "2022-08-01T02:31:26.684+01:00[Europe/London]" #xt/duration "PT1H15M43.342S")))

      (t/is (nil? (test-arithmetic '- nil #xt/duration "PT1H15M43.342S"))
            "end of time"))

    (t/testing "(- datetime interval)"
      (t/is (= #xt/date "2021-05-01"
               (test-arithmetic '- #xt/date "2022-08-01" #xt/interval-ym "P1Y3M")))

      (t/is (= #xt/date-time "2022-07-28T22:44:16.658"
               (test-arithmetic '- #xt/date "2022-08-01" #xt/interval-dt ["P3D" "PT1H15M43.342S"])))

      (t/is (= #xt/date-time "2022-07-29T01:15:43.342"
               (test-arithmetic '- #xt/date-time "2022-08-01T02:31:26.684" #xt/interval-mdn ["P3D" "PT1H15M43.342S"])))

      (t/is (= #xt/zoned-date-time "2022-08-01T01:15:43.342+01:00[Europe/London]"
               (test-arithmetic '- #xt/zoned-date-time "2022-08-01T02:31:26.684+01:00[Europe/London]" #xt/interval-mdn ["P0D" "PT1H15M43.342S"])))

      (t/is (= #xt/zoned-date-time "2022-10-29T11:00+01:00[Europe/London]"
               (test-arithmetic '- #xt/zoned-date-time "2022-10-31T12:00+00:00[Europe/London]" #xt/interval-mdn ["P2D" "PT1H"]))
            "clock change")

      (t/is (nil? (test-arithmetic '- nil #xt/interval-mdn ["P0D" "PT1H15M43.342S"]))))

    (t/testing "(- date date)"
      (t/is (= 1 (test-arithmetic '- #xt/date "2022-08-01" #xt/date "2022-07-31")))

      (t/is (= 3 (test-arithmetic '- #xt/date "2001-10-01" #xt/date "2001-09-28")))

      (t/is (= 1 (test-arithmetic '- #xt/date "2001-03-01" #xt/date "2001-02-28")))

      (t/is (= 2 (test-arithmetic '- #xt/date "2000-03-01" #xt/date "2000-02-28"))))

    (t/testing "(- datetime datetime)" 
      (t/is (= #xt/duration "PT1H15M43.342S"
               (test-arithmetic '- #xt/date "2022-08-01" #xt/date-time "2022-07-31T22:44:16.658")))

      (t/is (= #xt/duration "PT1H15M43.342S"
               (test-arithmetic '- #xt/date-time "2022-08-01T02:31:26.684" #xt/date-time "2022-08-01T01:15:43.342")))

      (t/is (= #xt/duration "PT1H15M43.342S"
               (test-arithmetic '- #xt/zoned-date-time "2022-08-01T02:31:26.684+01:00[Europe/London]" #xt/zoned-date-time "2022-08-01T01:15:43.342+01:00[Europe/London]")))

      (t/is (= #xt/duration "PT6H44M16.658S"
               (test-arithmetic '- #xt/date "2022-08-01" #xt/zoned-date-time "2022-08-01T01:15:43.342+01:00[Europe/London]")))

      (t/is (= #xt/duration "PT-9H-15M-43.342S"
               (test-arithmetic '- #xt/zoned-date-time "2022-08-01T01:15:43.342+01:00[Europe/London]" #xt/date-time "2022-08-01T02:31:26.684")))

      (t/is (nil? (test-arithmetic '- nil  #xt/date-time "2022-08-01T02:31:26.684")))
      (t/is (nil? (test-arithmetic '- #xt/zoned-date-time "2022-08-01T01:15:43.342+01:00[Europe/London]" nil)))
      (t/is (nil? (test-arithmetic '- nil nil))))))

(tct/defspec test-lt
  (tcp/for-all [t1 (tcg/one-of [ldt-gen ld-gen zdt-gen])
                t2 (tcg/one-of [ldt-gen ld-gen zdt-gen])
                default-tz zone-id-gen
                now instant-gen]
    (= (neg? (compare (->inst t1 now default-tz)
                      (->inst t2 now default-tz)))
       (->> (tu/query-ra [:project [{'res '(< ?t1 ?t2)}]
                          [:table [{}]]]
                         {:args {:t1 t1, :t2 t2}
                          :current-time now
                          :default-tz default-tz})
            first :res))))

(tct/defspec test-lte
  (tcp/for-all [t1 (tcg/one-of [ldt-gen ld-gen zdt-gen])
                t2 (tcg/one-of [ldt-gen ld-gen zdt-gen])
                default-tz zone-id-gen
                now instant-gen]
    (= (not (pos? (compare (->inst t1 now default-tz)
                           (->inst t2 now default-tz))))
       (->> (tu/query-ra [:project [{'res '(<= ?t1 ?t2)}]
                          [:table [{}]]]
                         {:args {:t1 t1, :t2 t2}
                          :current-time now
                          :default-tz default-tz})
            first :res))))

(tct/defspec test-eq
  (tcp/for-all [t1 (tcg/one-of [ldt-gen ld-gen zdt-gen])
                t2 (tcg/one-of [ldt-gen ld-gen zdt-gen])
                default-tz zone-id-gen
                now instant-gen]
    (= (= (->inst t1 now default-tz)
          (->inst t2 now default-tz))
       (->> (tu/query-ra [:project [{'res '(= ?t1 ?t2)}]
                          [:table [{}]]]
                         {:args {:t1 t1, :t2 t2}
                          :current-time now
                          :default-tz default-tz})
            first :res))))

(tct/defspec test-neq
  (tcp/for-all [t1 (tcg/one-of [ldt-gen ld-gen zdt-gen])
                t2 (tcg/one-of [ldt-gen ld-gen zdt-gen])
                default-tz zone-id-gen
                now instant-gen]
    (= (not= (->inst t1 now default-tz)
             (->inst t2 now default-tz))
       (->> (tu/query-ra [:project [{'res '(<> ?t1 ?t2)}]
                          [:table [{}]]]
                         {:args {:t1 t1, :t2 t2}
                          :current-time now
                          :default-tz default-tz})
            first :res))))

(tct/defspec test-gte
  (tcp/for-all [t1 (tcg/one-of [ldt-gen ld-gen zdt-gen])
                t2 (tcg/one-of [ldt-gen ld-gen zdt-gen])
                default-tz zone-id-gen
                now instant-gen]
    (= (not (neg? (compare (->inst t1 now default-tz)
                           (->inst t2 now default-tz))))
       (->> (tu/query-ra [:project [{'res '(>= ?t1 ?t2)}]
                          [:table [{}]]]
                         {:args {:t1 t1, :t2 t2}
                          :current-time now
                          :default-tz default-tz})
            first :res))))

(tct/defspec test-gt
  (tcp/for-all [t1 (tcg/one-of [ldt-gen ld-gen zdt-gen])
                t2 (tcg/one-of [ldt-gen ld-gen zdt-gen])
                default-tz zone-id-gen
                now instant-gen]
    (= (pos? (compare (->inst t1 now default-tz)
                      (->inst t2 now default-tz)))
       (->> (tu/query-ra [:project [{'res '(> ?t1 ?t2)}]
                          [:table [{}]]]
                         {:args {:t1 t1, :t2 t2}
                          :current-time now
                          :default-tz default-tz})
            first :res))))

(t/deftest test-interval-constructors
  (t/are [expected expr data] (= expected (et/project1 expr data))

    nil '(single-field-interval nil "YEAR" 2 0) {}
    nil '(single-field-interval nil "MONTH" 2 0) {}
    nil '(single-field-interval nil "DAY" 2 0) {}
    nil '(single-field-interval nil "HOUR" 2 0) {}
    nil '(single-field-interval nil "MINUTE" 2 0) {}
    nil '(single-field-interval nil "SECOND" 2 6) {}

    #xt/interval-ym "P0D" '(single-field-interval 0 "YEAR" 2 0) {}
    #xt/interval-ym "P0D" '(single-field-interval 0 "MONTH" 2 0) {}
    #xt/interval-dt ["P0D" "PT0S"] '(single-field-interval 0 "DAY" 2 0) {}
    #xt/interval-dt ["P0D" "PT0S"] '(single-field-interval 0 "HOUR" 2 0) {}
    #xt/interval-dt ["P0D" "PT0S"] '(single-field-interval 0 "MINUTE" 2 0) {}
    #xt/interval-dt ["P0D" "PT0S"] '(single-field-interval 0 "SECOND" 2 0) {}

    #xt/interval-ym "P0D" '(single-field-interval a "YEAR" 2 0) {:a 0}
    #xt/interval-ym "P0D" '(single-field-interval a "MONTH" 2 0) {:a 0}
    #xt/interval-dt ["P0D" "PT0S"] '(single-field-interval a "DAY" 2 0) {:a 0}
    #xt/interval-dt ["P0D" "PT0S"] '(single-field-interval a "HOUR" 2 0) {:a 0}
    #xt/interval-dt ["P0D" "PT0S"] '(single-field-interval a "MINUTE" 2 0) {:a 0}
    #xt/interval-dt ["P0D" "PT0S"] '(single-field-interval a "SECOND" 2 0) {:a 0}

    ;; Y / M distinction is lost when writing to IntervalYear vectors
    #xt/interval-ym "P12M" '(single-field-interval 1 "YEAR" 2 0) {}
    #xt/interval-ym "P-24M" '(single-field-interval -2 "YEAR" 2 0) {}

    #xt/interval-ym "P1M" '(single-field-interval 1 "MONTH" 2 0) {}
    #xt/interval-ym "P-2M" '(single-field-interval -2 "MONTH" 2 0) {}

    #xt/interval-dt ["P1D" "PT0S"] '(single-field-interval 1 "DAY" 2 0) {}
    #xt/interval-dt ["P-2D" "PT0S"] '(single-field-interval -2 "DAY" 2 0) {}

    #xt/interval-dt ["P0D" "PT1H"] '(single-field-interval 1 "HOUR" 2 0) {}
    #xt/interval-dt ["P0D" "PT-2H"] '(single-field-interval -2 "HOUR" 2 0) {}

    #xt/interval-dt ["P0D" "PT1M"] '(single-field-interval 1 "MINUTE" 2 0) {}
    #xt/interval-dt ["P0D" "PT-2M"] '(single-field-interval -2 "MINUTE" 2 0) {}

    #xt/interval-dt ["P0D" "PT1S"] '(single-field-interval 1 "SECOND" 2 6) {}
    #xt/interval-dt ["P0D" "PT-2S"] '(single-field-interval -2 "SECOND" 2 6) {}

    ;; fractional seconds
    #xt/interval-dt ["P0D" "PT1.34S"] '(single-field-interval "1.34" "SECOND" 2 6) {}

    ;; multi part parsing
    nil '(multi-field-interval nil "YEAR" 2 "MONTH" 2) {}

    #xt/interval-ym "P0D" '(multi-field-interval "0-0" "YEAR" 2 "MONTH" 2) {}
    #xt/interval-ym "P12M" '(multi-field-interval "1-0" "YEAR" 2 "MONTH" 2) {}
    #xt/interval-ym "P12M" '(multi-field-interval "+1-0" "YEAR" 2 "MONTH" 2) {}
    #xt/interval-ym "P-12M" '(multi-field-interval "-1-0" "YEAR" 2 "MONTH" 2) {}
    #xt/interval-ym "P13M" '(multi-field-interval "1-1" "YEAR" 2 "MONTH" 2) {}

    #xt/interval-mdn ["P11D" "PT12H"] '(multi-field-interval "11 12" "DAY" 2 "HOUR" 2) {}
    #xt/interval-mdn ["P-1D" "PT-1S"] '(multi-field-interval "-1 00:00:01" "DAY" 2 "SECOND" 6) {}
    #xt/interval-mdn ["P1D" "PT2M"] '(multi-field-interval "1 00:02" "DAY" 2 "MINUTE" 2) {}
    #xt/interval-mdn ["P1D" "PT23H"] '(multi-field-interval "1 23" "DAY" 2 "HOUR" 2) {}

    #xt/interval-mdn ["P0D" "PT-3H-4M-1S"] '(multi-field-interval "-03:04:01" "HOUR" 2 "SECOND" 6) {}
    #xt/interval-mdn ["P0D" "PT23H2M"] '(multi-field-interval "23:02" "HOUR" 2 "MINUTE" 2) {}

    #xt/interval-mdn ["P0D" "PT44M34S"] '(multi-field-interval "44:34" "MINUTE" 2 "SECOND" 6) {}
    #xt/interval-mdn ["P0D" "PT44M34.123456S"] '(multi-field-interval "44:34.123456" "MINUTE" 2 "SECOND" 6) {}

    #xt/interval-mdn ["P1D" "PT1.334S"] '(multi-field-interval "1 00:00:01.334" "DAY" 2 "SECOND" 6) {}
    #xt/interval-mdn ["P0D" "PT3H4M1.334S"] '(multi-field-interval "03:04:1.334" "HOUR" 2 "SECOND" 6) {}
    #xt/interval-mdn ["P0D" "PT44M34.123456789S"] '(multi-field-interval "44:34.123456789" "MINUTE" 2 "SECOND" 6) {}

    ;; truncates when we can no longer represent the number
    #xt/interval-mdn ["P0D" "PT44M34.123456789S"] '(multi-field-interval "44:34.123456789666" "MINUTE" 2 "SECOND" 6) {}

    #xt/interval-mdn ["P0D" "PT0.123S"] '(multi-field-interval "+00:00.123" "MINUTE" 2 "SECOND" 6) {}
    #xt/interval-mdn ["P0D" "PT0.123S"] '(multi-field-interval "00:00.123" "MINUTE" 2 "SECOND" 6) {}
    #xt/interval-mdn ["P0D" "PT-0.123S"] '(multi-field-interval "-00:00.123" "MINUTE" 2 "SECOND" 6) {}))

(t/deftest test-multi-part-interval-ex-cases
  (letfn [(p [unit1 unit2] (et/project1 (list 'multi-field-interval "0-0" unit1 2 unit2 2) {}))]
    (t/is (thrown-with-msg? IllegalArgumentException #"If YEAR specified as the interval start field, MONTH must be the end field\." (p "YEAR" "DAY")))
    (t/is (thrown-with-msg? IllegalArgumentException #"MONTH is not permitted as the interval start field\." (p "MONTH" "DAY")))
    (t/is (thrown-with-msg? IllegalArgumentException #"Interval end field must have less significance than the start field\." (p "DAY" "DAY")))
    (t/is (thrown-with-msg? IllegalArgumentException #"Interval end field must have less significance than the start field\." (p "MINUTE" "HOUR")))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(tct/defspec single-field-interval-string-parse-same-as-int-prop
  (tcp/for-all [i (tcg/choose -99 99)
                unit (tcg/elements ["YEAR" "MONTH" "DAY" "HOUR" "MINUTE" "SECOND"])
                include-plus tcg/boolean]
    (= (et/project1 (list 'single-field-interval i unit 2 (if (= "SECOND" unit) 6 0)) {})
       (et/project1 (list 'single-field-interval
                          (cond (neg? i) (str i)
                                include-plus (str "+" i)
                                :else (str i))
                          unit
                          2
                          (if (= "SECOND" unit) 6 0))
                    {}))))

(t/deftest test-interval-arithmetic
  (t/are [expected expr] (= expected (et/project1 expr {}))
    nil '(+ (single-field-interval 1 "YEAR" 2 0) nil)
    nil '(+ nil (single-field-interval 1 "YEAR" 2 0))

    nil '(- (single-field-interval 1 "YEAR" 2 0) nil)
    nil '(- nil (single-field-interval 1 "YEAR" 2 0))

    nil '(* (single-field-interval 1 "YEAR" 2 0) nil)
    nil '(* nil (single-field-interval 1 "YEAR" 2 0))

    #xt/interval-ym "P24M" '(+ (single-field-interval 1 "YEAR" 2 0) (single-field-interval 1 "YEAR" 2 0))
    #xt/interval-ym "P13M" '(+ (single-field-interval 1 "YEAR" 2 0) (single-field-interval 1 "MONTH" 2 0))
    #xt/interval-ym "P11M" '(+ (single-field-interval 1 "YEAR" 2 0) (single-field-interval -1 "MONTH" 2 0))

    #xt/interval-mdn ["P12M-1D" "PT0S"] '(+ (single-field-interval 1 "YEAR" 2 0) (single-field-interval -1 "DAY" 2 0))
    #xt/interval-mdn ["P12M" "PT-1S"] '(+ (single-field-interval 1 "YEAR" 2 0) (single-field-interval -1 "SECOND" 2 6))
    #xt/interval-mdn ["P12M" "PT1H1S"] '(+ (single-field-interval 1 "YEAR" 2 0) (single-field-interval 1 "HOUR" 2 0) (single-field-interval 1 "SECOND" 2 6))

    #xt/interval-ym "P0D" '(- (single-field-interval 1 "YEAR" 2 0) (single-field-interval 1 "YEAR" 2 0))
    #xt/interval-ym "P11M" '(- (single-field-interval 1 "YEAR" 2 0) (single-field-interval 1 "MONTH" 2 0))
    #xt/interval-ym "P13M" '(- (single-field-interval 1 "YEAR" 2 0) (single-field-interval -1 "MONTH" 2 0))

    #xt/interval-mdn ["P12M1D" "PT0S"] '(- (single-field-interval 1 "YEAR" 2 0) (single-field-interval -1 "DAY" 2 0))
    #xt/interval-mdn ["P12M" "PT1S"] '(- (single-field-interval 1 "YEAR" 2 0) (single-field-interval -1 "SECOND" 2 6))
    #xt/interval-mdn ["P12M" "PT-1H-1S"] '(- (single-field-interval 1 "YEAR" 2 0) (single-field-interval 1 "HOUR" 2 0) (single-field-interval 1 "SECOND" 2 6))

    #xt/interval-ym "P36M" '(* (single-field-interval 1 "YEAR" 2 0) 3)

    #xt/interval-ym "P6M" '(/ (single-field-interval 1 "YEAR" 2 0) 2)
    #xt/interval-ym "P2M" '(/ (single-field-interval 1 "YEAR" 2 0) 5)

    #xt/interval-ym "P12M" '(/ (+ (single-field-interval 1 "YEAR" 2 0) (single-field-interval 1 "YEAR" 2 0)) 2)

    #xt/interval-ym "P0M" '(/ (single-field-interval 1 "MONTH" 2 0) 2)
    #xt/interval-ym "P1M" '(/ (single-field-interval 6 "MONTH" 2 0) 5)

    #xt/interval-dt ["P0M" "PT0S"] '(/ (single-field-interval 1 "DAY" 2 0) 2)
    #xt/interval-dt ["P1D" "PT0S"] '(/ (single-field-interval 6 "DAY" 2 0) 5)))

(t/deftest test-interval-comparison
  (t/testing "comparing intervals with different types"
    (t/is (thrown-with-msg?
           UnsupportedOperationException
           #"Cannot compare intervals with different units"
           (et/project1 '(= (single-field-interval 1 "YEAR" 2 0) (single-field-interval 365 "DAY" 2 0)) {}))))

  (t/testing "comparing year month intervals"
    (t/are [expected expr] (= expected (et/project1 expr {}))
      ;; = 
      true '(= (single-field-interval 1 "YEAR" 2 0) (single-field-interval 12 "MONTH" 2 0))
      false '(= (single-field-interval 1 "YEAR" 2 0) (single-field-interval 1 "MONTH" 2 0))

      ;; <
      true '(< (single-field-interval 1 "YEAR" 2 0) (single-field-interval 2 "YEAR" 2 0))
      false '(< (single-field-interval 1 "YEAR" 2 0) (single-field-interval 12 "MONTH" 2 0))

      ;; <=
      true '(<= (single-field-interval 1 "YEAR" 2 0) (single-field-interval 12 "MONTH" 2 0))
      false '(<= (single-field-interval 1 "YEAR" 2 0) (single-field-interval 1 "MONTH" 2 0))
      true '(<= (single-field-interval 2 "YEAR" 2 0) (multi-field-interval "2-2" "YEAR" 2 "MONTH" 2))

      ;; >
      false '(> (single-field-interval 1 "YEAR" 2 0) (single-field-interval 12 "MONTH" 2 0))
      true '(> (multi-field-interval "2-2" "YEAR" 2 "MONTH" 2) (single-field-interval 2 "YEAR" 2 0))
      false '(> (single-field-interval 1 "YEAR" 2 0) (single-field-interval 2 "YEAR" 2 0))

      ;; >=
      true '(>= (single-field-interval 1 "YEAR" 2 0) (single-field-interval 12 "MONTH" 2 0))
      true '(>= (multi-field-interval "2-2" "YEAR" 2 "MONTH" 2) (single-field-interval 2 "YEAR" 2 0))
      false '(>= (single-field-interval 1 "YEAR" 2 0) (single-field-interval 2 "YEAR" 2 0))))

  (t/testing "can't compare month-day-nano intervals if months > 0"
    (let [test-doc {:_id :foo,
                    :interval (PeriodDuration. (Period/of 0 4 8) (Duration/parse "PT1H"))}]
      (t/is (thrown-with-msg?
             RuntimeException
             #"Cannot compare month-day-nano intervals when month component is non-zero."
             (et/project1 '(= interval interval) test-doc)))))

  (t/testing "can't comapre day-time intervals"
    (t/is (thrown-with-msg?
           RuntimeException
           #"Cannot compare day-time intervals"
           (et/project1 '(= (single-field-interval 1 "DAY" 2 0) (single-field-interval 1 "DAY" 2 0)) {}))))

  (t/testing "comparing month-day-nano intervals"
    (t/are [expected expr] (= expected (et/project1 expr {}))
      ;; =
      true '(= (multi-field-interval "1 0" "DAY" 2 "HOUR" 2) (multi-field-interval "1 0" "DAY" 2 "HOUR" 2))
      false '(= (multi-field-interval "1 0" "DAY" 2 "HOUR" 2) (multi-field-interval "1 2" "DAY" 2 "HOUR" 2))

      ;; <
      false '(< (multi-field-interval "1 0" "DAY" 2 "HOUR" 2) (multi-field-interval "1 0" "DAY" 2 "HOUR" 2))
      true '(< (multi-field-interval "1 0" "DAY" 2 "HOUR" 2) (multi-field-interval "1 2" "DAY" 2 "HOUR" 2))
      false '(< (multi-field-interval "1 2" "DAY" 2 "HOUR" 2) (multi-field-interval "1 0" "DAY" 2 "HOUR" 2))

      ;; <=
      true '(<= (multi-field-interval "1 0" "DAY" 2 "HOUR" 2) (multi-field-interval "1 0" "DAY" 2 "HOUR" 2))
      true '(<= (multi-field-interval "1 0" "DAY" 2 "HOUR" 2) (multi-field-interval "1 2" "DAY" 2 "HOUR" 2))
      false '(<= (multi-field-interval "1 2" "DAY" 2 "HOUR" 2) (multi-field-interval "1 0" "DAY" 2 "HOUR" 2))

      ;; >
      false '(> (multi-field-interval "1 0" "DAY" 2 "HOUR" 2) (multi-field-interval "1 0" "DAY" 2 "HOUR" 2))
      false '(> (multi-field-interval "1 0" "DAY" 2 "HOUR" 2) (multi-field-interval "1 2" "DAY" 2 "HOUR" 2))
      true '(> (multi-field-interval "1 2" "DAY" 2 "HOUR" 2) (multi-field-interval "1 0" "DAY" 2 "HOUR" 2))

      ;; >=
      true '(>= (multi-field-interval "1 0" "DAY" 2 "HOUR" 2) (multi-field-interval "1 0" "DAY" 2 "HOUR" 2))
      false '(>= (multi-field-interval "1 0" "DAY" 2 "HOUR" 2) (multi-field-interval "1 2" "DAY" 2 "HOUR" 2))
      true '(>= (multi-field-interval "1 2" "DAY" 2 "HOUR" 2) (multi-field-interval "1 0" "DAY" 2 "HOUR" 2)))))

(t/deftest test-uoe-thrown-for-unsupported-div
  (t/is (thrown? UnsupportedOperationException (et/project1 '(/ (+ (single-field-interval 1 "MONTH" 2 0) (single-field-interval 3 "MINUTE" 2 0)) 3) {})))
  (t/is (thrown? UnsupportedOperationException (et/project1 '(/ (+ (single-field-interval 1 "MONTH" 2 0) (single-field-interval 3 "DAY" 2 0)) 3) {}))))

(def period-gen
  (tcg/fmap (fn [[y m d]]
              (Period/of y m d))
            (tcg/tuple
             (tcg/return 0)
             tcg/small-integer
             tcg/small-integer)))

(def small-duration-gen
  "Generates java.time.Duration instances representing -1D to +1D (exclusive), this is useful
  as the fractional component of MonthDayNano is going to hold nanos in this range."
  (tcg/fmap #(Duration/ofNanos %) (tcg/choose -86399999999999 86399999999999)))

(def interval-ym-gen
  (->> tcg/small-integer
       (tcg/fmap #(IntervalYearMonth. (Period/ofMonths %)))))

(def interval-dt-gen
  (->> (tcg/tuple tcg/small-integer (tcg/choose -86399999 86399999))
       (tcg/fmap #(IntervalDayTime. (Period/ofDays (first %))
                                    (Duration/ofMillis (second %))))))

(def interval-mdn-gen
  (->> (tcg/tuple period-gen small-duration-gen)
       (tcg/fmap #(IntervalMonthDayNano. (first %) (second %)))))

;; some basic interval algebraic properties

(defn pd-zero [i]
  (cond
    (instance? IntervalYearMonth i) (IntervalYearMonth. Period/ZERO)
    (instance? IntervalDayTime i) (IntervalDayTime. Period/ZERO Duration/ZERO)
    (instance? IntervalMonthDayNano i) (IntervalMonthDayNano. Period/ZERO Duration/ZERO)))

(tct/defspec interval-add-identity-prop
  (tcp/for-all [i (tcg/one-of [interval-ym-gen interval-dt-gen interval-mdn-gen])]
    (= i (et/project1 '(+ a b) {:a i, :b (pd-zero i)}))))

(deftest bug-interval-day-time-normalization-738
  (let [i #xt/interval-dt ["P0D" "PT-23H-59M-59.001S"]]
    (t/is (=  i
              (et/project1 '(+ a b) {:a i, :b (pd-zero i)}))))
  (let [i #xt/interval-mdn ["P33M244D" "PT48H0.003444443S"]]
    (t/is (=  i
              (et/project1 '(+ a b) {:a i, :b (pd-zero i)})))))

(tct/defspec interval-sub-identity-prop
  (tcp/for-all [i (tcg/one-of [interval-ym-gen interval-dt-gen interval-mdn-gen])]
    (= i (et/project1 '(- a b) {:a i, :b (pd-zero i)}))))

(tct/defspec interval-mul-factor-identity-prop
  (tcp/for-all [i (tcg/one-of [interval-ym-gen interval-dt-gen interval-mdn-gen])]
    (= i (et/project1 '(* a 1) {:a i}))))

(tct/defspec interval-mul-by-zero-prop
  (tcp/for-all [i (tcg/one-of [interval-ym-gen interval-dt-gen interval-mdn-gen])]
    (= (pd-zero i) (et/project1 '(* a 0) {:a i}))))

(tct/defspec interval-add-sub-round-trip-prop
  (tcp/for-all [i (tcg/one-of [interval-ym-gen interval-dt-gen interval-mdn-gen])]
    (= i (et/project1 '(- (+ a a) a) {:a i, :b (pd-zero i)}))))

#_
(tct/defspec interval-mul-by-2-is-equiv-to-sum-self-prop
  (tcp/for-all [pd period-duration-gen]
    (= (et/project1 '(* a 2) {:a pd})
       (et/project1 '(+ a a) {:a pd}))))

#_
(tct/defspec interval-mul-by-neg1-is-equiv-to-sub-self2-prop
  (tcp/for-all [pd period-duration-gen]
    (= (et/project1 '(* a -1) {:a pd})
       (et/project1 '(- a a a) {:a pd}))))

(t/deftest test-interval-abs
  (t/are [expected expr] (= expected (et/project1 expr {}))

    #xt/interval-ym "P0D" '(abs (single-field-interval 0 "YEAR" 2 0))
    #xt/interval-ym "P12M" '(abs (single-field-interval 1 "YEAR" 2 0))
    #xt/interval-ym "P12M" '(abs (single-field-interval -1 "YEAR" 2 0))

    #xt/interval-ym "P11M" '(abs (+ (single-field-interval -1 "YEAR" 2 0) (single-field-interval 1 "MONTH" 2 0)))
    #xt/interval-dt ["P1D" "PT-1S"] '(abs (+ (single-field-interval -1 "DAY" 2 0) (single-field-interval 1 "SECOND" 2 6)))))

(def single-interval-constructor-gen
  (->> (tcg/hash-map
        :unit (tcg/elements ["YEAR" "MONTH" "DAY" "HOUR" "MINUTE" "SECOND"])
        :sign (tcg/elements [nil "-" "+"])
        :force-string tcg/boolean
        :leading-value (tcg/choose 0 999999999)
        :fractional-value (tcg/choose 0 99999999)
        :use-fractional-value tcg/boolean
        :precision (tcg/choose 1 8)
        :use-precision tcg/boolean
        :fractional-precision (tcg/choose 0 9)
        :use-fractional-precision tcg/boolean)
       (tcg/fmap
        (fn [{:keys [unit
                     sign
                     force-string
                     leading-value
                     fractional-value
                     use-fractional-value
                     precision
                     use-precision
                     fractional-precision
                     use-fractional-precision]}]

          (let [precision (if use-precision precision 2)

                fractional-precision
                (cond
                  (not= "SECOND" unit) 0
                  use-fractional-precision fractional-precision
                  :else 2)

                leading-value (mod leading-value (parse-long (str/join (repeat precision "9"))))

                fractional-value
                (if (pos? fractional-precision)
                  (mod fractional-value (parse-long (str/join (repeat fractional-precision "9"))))
                  0)

                use-fractional-value (and (= "SECOND" unit) use-fractional-value)

                v
                (cond
                  force-string (str sign leading-value (when use-fractional-value (str "." fractional-value)))
                  use-fractional-value (str sign leading-value (when use-fractional-value (str "." fractional-value)))
                  :else leading-value)]

            (list 'single-field-interval v unit precision fractional-precision))))))

(def multi-interval-fields-gen
  (tcg/bind
   (tcg/elements ["YEAR" "DAY" "HOUR" "MINUTE"])
   (fn [start]
     (tcg/tuple (tcg/return start)
                (case start
                  "YEAR" (tcg/return "MONTH")
                  "DAY" (tcg/elements ["HOUR" "MINUTE" "SECOND"])
                  "HOUR" (tcg/elements ["MINUTE" "SECOND"])
                  "MINUTE" (tcg/return "SECOND"))))))

(def multi-interval-constructor-gen
  (->> (tcg/hash-map
        :fields multi-interval-fields-gen
        :sign (tcg/elements [nil "-" "+"])
        :time-values (tcg/hash-map
                      "YEAR" (tcg/choose 0 999999999)
                      "MONTH" (tcg/choose 0 999999999)
                      "DAY" (tcg/choose 0 999999999)
                      "HOUR" (tcg/choose 0 23)
                      "MINUTE" (tcg/choose 0 59)
                      "SECOND" (tcg/choose 0 59))
        :fractional-value (tcg/choose 0 99999999)
        :use-fractional-value tcg/boolean
        :precision (tcg/choose 1 8)
        :use-precision tcg/boolean
        :fractional-precision (tcg/choose 0 9)
        :use-fractional-precision tcg/boolean)
       (tcg/fmap
        (fn [{:keys [fields
                     sign
                     time-values
                     fractional-value
                     use-fractional-value
                     precision
                     use-precision
                     ^long fractional-precision
                     use-fractional-precision]}]

          (let [[leading-unit ending-unit] fields

                precision (if use-precision precision 2)

                fractional-precision
                (cond
                  (not= "SECOND" ending-unit) 0
                  use-fractional-precision fractional-precision
                  :else 2)

                fractional-value
                (if (pos? fractional-precision)
                  (mod fractional-value (parse-long (str/join (repeat fractional-precision "9"))))
                  0)

                use-fractional-value (and (= "SECOND" ending-unit) use-fractional-value)

                v (str sign
                       (loop [fields (drop-while #(not= leading-unit %) ["YEAR" "MONTH" "DAY" "HOUR" "MINUTE" "SECOND"])
                              s ""]
                         (if-some [field (first fields)]
                           (let [prefix (case field
                                          "MONTH" "-"
                                          "HOUR" " "
                                          ":")]
                             (if (= ending-unit field)
                               (str s prefix (time-values field))
                               (recur (rest fields) (if (= field leading-unit)
                                                      (mod (time-values leading-unit) (parse-long (str/join (repeat precision "9"))))
                                                      (str s prefix (time-values field))))))
                           (throw (Exception. "Unreachable"))))
                       (when use-fractional-value (str "." fractional-value)))]

            (list 'multi-field-interval v leading-unit precision ending-unit fractional-precision))))))

(def interval-constructor-gen
  (tcg/one-of [single-interval-constructor-gen multi-interval-constructor-gen]))

(tct/defspec all-possible-interval-literals-can-be-constructed-prop
  ;; gonna give this a few more rounds by default due to domain size
  1000
  (tcp/for-all [form interval-constructor-gen]
    (let [res (et/project1 form {})]
      (or (instance? IntervalYearMonth res)
          (instance? IntervalDayTime res)
          (instance? IntervalMonthDayNano res)))))

(deftest test-period-constructor
  (letfn [(f [from to]
            (et/project1 '(period x y)
                         {:x from, :y to}))]
    (let [from #xt/zoned-date-time "2020-01-01T00:00Z"
          to #xt/zoned-date-time "2022-01-01T00:00Z"]
      (t/is (= (tu/->tstz-range from to)
               (f from to))))

    (let [from #xt/zoned-date-time "2030-01-01T00:00Z"
          to #xt/zoned-date-time "2020-01-01T00:00Z"]
      (t/is
       (thrown-with-msg?
        RuntimeException
        #"From cannot be greater than to when constructing a period"
        (f from to))))

    (t/testing "other date-time types"
      (t/is (= #xt/tstz-range [#xt/zoned-date-time "2020-01-01T00:00Z"
                               #xt/zoned-date-time "2020-01-02T00:00Z"]
               (f #xt/date "2020-01-01" #xt/date "2020-01-02"))
            "date/date")

      (t/is (= #xt/tstz-range [#xt/zoned-date-time "2020-01-01T00:00Z" nil]
               (f #xt/date "2020-01-01" nil))
            "date/nil")

      (t/is (thrown-with-msg? IllegalArgumentException #"period not applicable to types null and date"
                              (f nil #xt/date "2020-01-01"))
            "nil/date")

      (t/is (= #xt/tstz-range [#xt/zoned-date-time "2020-01-01T00:00Z"
                               #xt/zoned-date-time "2020-01-02T01:23:45Z"]
               (f #xt/date "2020-01-01" #xt/date-time "2020-01-02T01:23:45"))
            "date/ts")

      (t/is (= #xt/tstz-range [#xt/zoned-date-time "2020-01-01T01:23:45Z"
                               #xt/zoned-date-time "2020-01-02T00:00Z"]
               (f #xt/date-time "2020-01-01T01:23:45" #xt/date "2020-01-02"))
            "ts/date")

      (t/is (= #xt/tstz-range [#xt/zoned-date-time "2020-01-01T11:07:08Z"
                               #xt/zoned-date-time "2020-01-02T01:23:45Z"]
               (f #xt/zoned-date-time "2020-01-01T06:07:08-05:00[America/New_York]",
                  #xt/date-time "2020-01-02T01:23:45"))
            "tstz/ts"))))

(deftest test-overlaps?-predicate
  (t/is
    (= true
       (et/project1
         '(overlaps? x y)
         {:x (tu/->tstz-range #inst "2020", #inst "2022")
          :y (tu/->tstz-range #inst "2021", #inst "2023")})))

  (t/is
    (= false
       (et/project1
         '(overlaps? x y)
         {:x (tu/->tstz-range #inst "2020", #inst "2021")
          :y (tu/->tstz-range #inst "2021", #inst "2023")}))))

(deftest test-contains?-predicate
  (t/testing "period to period"
    (t/is
      (= true
         (et/project1
           '(contains? x y)
           {:x (tu/->tstz-range #inst "2020", #inst "2025")
            :y (tu/->tstz-range #inst "2021", #inst "2023")})))

    (t/is
      (= false
         (et/project1
           '(contains? x y)
           {:x (tu/->tstz-range #inst "2020", #inst "2022")
            :y (tu/->tstz-range #inst "2021", #inst "2023")}))))

  (t/testing "period to timestamp"
    (t/is (true? (et/project1
                  '(contains? x y)
                  {:x (tu/->tstz-range #inst "2020", #inst "2025")
                   :y #inst "2021"})))

    (t/is (false? (et/project1
                   '(contains? x y)
                   {:x (tu/->tstz-range #inst "2020", #inst "2022")
                    :y #inst "2023"})))

    (t/is (false? (et/project1
                   '(contains? x y)
                   {:x (tu/->tstz-range #inst "2020", #inst "2022")
                    :y #inst "2022"})))))

(deftest test-equals?-predicate
  (t/is
    (= true
       (et/project1
         '(equals? x y)
         {:x (tu/->tstz-range #inst "2020", #inst "2022")
          :y (tu/->tstz-range #inst "2020", #inst "2022")})))

  (t/is
    (= false
       (et/project1
         '(equals? x y)
         {:x (tu/->tstz-range #inst "2020", #inst "2021")
          :y (tu/->tstz-range #inst "2020", #inst "2023")}))))

(deftest test-precedes?-predicate
  (t/is
    (= true
       (et/project1
         '(precedes? x y)
         {:x (tu/->tstz-range #inst "2020", #inst "2022")
          :y (tu/->tstz-range #inst "2023", #inst "2025")})))

  (t/is
    (= false
       (et/project1
         '(precedes? x y)
         {:x (tu/->tstz-range #inst "2020", #inst "2021")
          :y (tu/->tstz-range #inst "2020", #inst "2023")}))))

(deftest test-succeeds?-predicate
  (t/is
    (= true
       (et/project1
         '(succeeds? x y)
         {:x (tu/->tstz-range #inst "2023", #inst "2025")
          :y (tu/->tstz-range #inst "2020", #inst "2022")})))

  (t/is
    (= false
       (et/project1
         '(succeeds? x y)
         {:x (tu/->tstz-range #inst "2020", #inst "2021")
          :y (tu/->tstz-range #inst "2020", #inst "2023")}))))

(deftest test-immediately-precedes?-predicate
  (t/is
    (= true
       (et/project1
         '(immediately-precedes? x y)
         {:x (tu/->tstz-range #inst "2020", #inst "2023")
          :y (tu/->tstz-range #inst "2023", #inst "2025")})))

  (t/is
    (= false
       (et/project1
         '(immediately-precedes? x y)
         {:x (tu/->tstz-range #inst "2020", #inst "2022")
          :y (tu/->tstz-range #inst "2023", #inst "2025")}))))

(deftest test-immediately-succeeds?-predicate
  (t/is
    (= true
       (et/project1
         '(immediately-succeeds? x y)
         {:x (tu/->tstz-range #inst "2022", #inst "2025")
          :y (tu/->tstz-range #inst "2020", #inst "2022")})))

  (t/is
    (= false
       (et/project1
         '(immediately-succeeds? x y)
         {:x (tu/->tstz-range #inst "2023", #inst "2025")
          :y (tu/->tstz-range #inst "2020", #inst "2022")}))))

(deftest test-leads?-predicate
  (t/is
    (= true
       (et/project1
         '(leads? x y)
         {:x (tu/->tstz-range #inst "2020", #inst "2025")
          :y (tu/->tstz-range #inst "2021", #inst "2025")})))

  (t/is
    (= false
       (et/project1
         '(leads? x y)
         {:x (tu/->tstz-range #inst "2021", #inst "2025")
          :y (tu/->tstz-range #inst "2021", #inst "2025")}))))

(deftest test-strictly-leads?-predicate
  (t/is
    (= true
       (et/project1
         '(strictly-leads? x y)
         {:x (tu/->tstz-range #inst "2020", #inst "2024")
          :y (tu/->tstz-range #inst "2021", #inst "2025")})))

  (t/is
    (= false
       (et/project1
         '(strictly-leads? x y)
         {:x (tu/->tstz-range #inst "2020", #inst "2025")
          :y (tu/->tstz-range #inst "2021", #inst "2025")}))))

(deftest test-lags?-predicate
  (t/is
    (= true
       (et/project1
         '(lags? x y)
         {:x (tu/->tstz-range #inst "2021", #inst "2025")
          :y (tu/->tstz-range #inst "2020", #inst "2024")})))

  (t/is
    (= false
       (et/project1
         '(lags? x y)
         {:x (tu/->tstz-range #inst "2021", #inst "2024")
          :y (tu/->tstz-range #inst "2022", #inst "2025")}))))

(deftest test-strictly-lags?-predicate
  (t/is
    (= true
       (et/project1
         '(strictly-lags? x y)
         {:x (tu/->tstz-range #inst "2022", #inst "2025")
          :y (tu/->tstz-range #inst "2021", #inst "2024")})))

  (t/is
    (= false
       (et/project1
         '(strictly-lags? x y)
         {:x (tu/->tstz-range #inst "2021", #inst "2025")
          :y (tu/->tstz-range #inst "2021", #inst "2024")}))))

(deftest test-strictly-overlaps?-predicate
  (t/is
    (= true
       (et/project1
         '(strictly-overlaps? x y)
         {:x (tu/->tstz-range #inst "2022", #inst "2024")
          :y (tu/->tstz-range #inst "2021", #inst "2025")})))

  (t/is
    (= false
       (et/project1
         '(strictly-overlaps? x y)
         {:x (tu/->tstz-range #inst "2021", #inst "2024")
          :y (tu/->tstz-range #inst "2021", #inst "2025")}))))

(deftest test-strictly-contains?-predicate
  (t/is
    (= true
       (et/project1
         '(strictly-contains? x y)
         {:x (tu/->tstz-range #inst "2021", #inst "2025")
          :y (tu/->tstz-range #inst "2022", #inst "2024")})))

  (t/is
    (= false
       (et/project1
         '(strictly-contains? x y)
         {:x (tu/->tstz-range #inst "2021", #inst "2025")
          :y (tu/->tstz-range #inst "2022", #inst "2025")}))))

(deftest test-strictly-precedes?-predicate
  (t/is
    (= true
       (et/project1
         '(strictly-precedes? x y)
         {:x (tu/->tstz-range #inst "2020", #inst "2022")
          :y (tu/->tstz-range #inst "2023", #inst "2025")})))

  (t/is
    (= false
       (et/project1
         '(strictly-precedes? x y)
         {:x (tu/->tstz-range #inst "2020", #inst "2022")
          :y (tu/->tstz-range #inst "2022", #inst "2023")}))))

(deftest test-strictly-succeeds?-predicate
  (t/is
    (= true
       (et/project1
         '(strictly-succeeds? x y)
         {:x (tu/->tstz-range #inst "2023", #inst "2024")
          :y (tu/->tstz-range #inst "2021", #inst "2022")})))

  (t/is
    (= false
       (et/project1
         '(strictly-succeeds? x y)
         {:x (tu/->tstz-range #inst "2022", #inst "2024")
          :y (tu/->tstz-range #inst "2021", #inst "2022")}))))

(deftest test-immediately-leads?-predicate
  (t/is
    (= true
       (et/project1
         '(immediately-leads? x y)
         {:x (tu/->tstz-range #inst "2021", #inst "2024")
          :y (tu/->tstz-range #inst "2022", #inst "2024")})))

  (t/is
    (= false
       (et/project1
         '(immediately-leads? x y)
         {:x (tu/->tstz-range #inst "2021", #inst "2024")
          :y (tu/->tstz-range #inst "2022", #inst "2025")}))))

(deftest test-immediately-lags?-predicate
  (t/is
    (= true
       (et/project1
         '(immediately-lags? x y)
         {:x (tu/->tstz-range #inst "2021", #inst "2025")
          :y (tu/->tstz-range #inst "2021", #inst "2024")})))

  (t/is
    (= false
       (et/project1
         '(immediately-lags? x y)
         {:x (tu/->tstz-range #inst "2021", #inst "2024")
          :y (tu/->tstz-range #inst "2021", #inst "2025")}))))

(deftest test-date-bin
  (t/is (= (time/->zdt #inst "2020")
           (et/project1 '(date-bin i src)
                        {:i #xt/interval-mdn ["P0D" "PT15M"]
                         :src #inst "2020"})))

  (t/is (= (time/->zdt #inst "2020")
           (et/project1 '(date-bin i src)
                        {:i #xt/interval-mdn ["P0D" "PT15M"]
                         :src #inst "2020-01-01T00:10:10Z"})))

  (t/is (= (time/->zdt #inst "2020-01-01T00:15Z")
           (et/project1 '(date-bin i src)
                        {:i #xt/interval-mdn ["P0D" "PT15M"]
                         :src #inst "2020-01-01T00:20:10Z"}))))

(deftest test-range-bins
  (let [base (time/->zdt #inst "2020-01-01")]
    (letfn [(f [start end]
              (for [{:keys [xt/from xt/to xt/weight]} (et/project1 '(range-bins i from to)
                                                                   {:i #xt/interval-mdn ["P0D" "PT15M"]
                                                                    :from (.plusMinutes base start)
                                                                    :to (.plusMinutes base end)})]
                [(.between ChronoUnit/MINUTES base from)
                 (.between ChronoUnit/MINUTES base to)
                 weight]))]

      (t/is (= [[0 15 1.0]] (f 0 10))
            "starts")

      (t/is (= [[0 15 1.0]] (f 10 15))
            "finishes")

      (t/is (= [[0 15 0.75] [15 30 0.25]]
               (f 0 20))
            "started by")

      (t/is (= [[0 15 0.25] [15 30 0.75]]
               (f 10 30))
            "finished by")

      (t/is (= [[0 15 1.0]] (f 0 15))
            "equals")

      (t/is (= [[0 15 0.4] [15 30 0.6]]
               (f 13 18))
            "overlaps")

      (t/is (= [[0 15 0.08] [15 30 0.6] [30 45 0.32]]
               (f 13 38))
            "contains"))))

(t/deftest test-period-intersection-3493
  (t/is (= (tu/->tstz-range #xt/zoned-date-time "2021-01-01T00:00Z"
                            #xt/zoned-date-time "2022-01-01T00:00Z")
           (et/project1 '(* p1 p2)
                        {:p1 (tu/->tstz-range #inst "2020-01-01", #inst "2022-01-01")
                         :p2 (tu/->tstz-range #inst "2021-01-01", #inst "2023-01-01")}))
        "overlaps")

  (t/is (= (tu/->tstz-range #xt/zoned-date-time "2020-01-01T00:00Z"
                            #xt/zoned-date-time "2022-01-01T00:00Z")
           (et/project1 '(* p1 p2)
                        {:p1 (tu/->tstz-range #inst "2020-01-01", #inst "2022-01-01")
                         :p2 (tu/->tstz-range #inst "2020-01-01", #inst "2022-01-01")}))
        "equals")

  (t/is (= (tu/->tstz-range #xt/zoned-date-time "2020-01-01T00:00Z"
                            #xt/zoned-date-time "2022-01-01T00:00Z")
           (et/project1 '(* p1 p2)
                        {:p1 (tu/->tstz-range #inst "2020-01-01", #inst "2022-01-01")
                         :p2 (tu/->tstz-range #inst "2020-01-01", #inst "2024-01-01")}))
        "starts")

  (t/is (= (tu/->tstz-range #xt/zoned-date-time "2021-01-01T00:00Z"
                            #xt/zoned-date-time "2022-01-01T00:00Z")
           (et/project1 '(* p1 p2)
                        {:p1 (tu/->tstz-range #inst "2020-01-01", #inst "2023-01-01")
                         :p2 (tu/->tstz-range #inst "2021-01-01", #inst "2022-01-01")}))
        "contains")

  (t/is (nil? (et/project1 '(* p1 p2)
                           {:p1 (tu/->tstz-range #inst "2020-01-01", #inst "2022-01-01")
                            :p2 (tu/->tstz-range #inst "2023-01-01", #inst "2025-01-01")}))
        "succeeds")

  (t/is (nil? (et/project1 '(* p1 p2)
                           {:p1 (tu/->tstz-range #inst "2020-01-01", #inst "2022-01-01")
                            :p2 (tu/->tstz-range #inst "2022-01-01", #inst "2024-01-01")}))
        "immediately succeeds")

  (t/is (nil? (et/project1 '(* p1 p2)
                           {:p1 (tu/->tstz-range #inst "2020-01-01", #inst "2022-01-01")
                            :p2 nil}))
        "one-side nil"))
