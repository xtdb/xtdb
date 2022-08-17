(ns core2.expression.temporal-test
  (:require [clojure.test :as t]
            [core2.test-util :as tu])
  (:import (java.time Instant ZoneId ZoneOffset)))

(t/use-fixtures :each tu/with-allocator)

;; the goal of this test is simply to demonstrate clock affects the computation
;; equality may well remain incorrect for now, it is not the goal
(t/deftest clock-influences-equality-of-ambiguous-datetimes-test
  (t/are [expected a b zone-id]
      (= expected (-> (tu/query-ra [:project [{'res '(= ?a ?b)}]
                                    [:table [{}]]]
                                   {:params {'?a a, '?b b}
                                    :current-time Instant/EPOCH
                                    :default-tz (ZoneOffset/of zone-id)})
                      first :res))
    ;; identity
    true #time/date-time "2021-08-09T15:43:23" #time/date-time "2021-08-09T15:43:23" "Z"

    ;; obvious inequality
    false #time/date-time "2022-08-09T15:43:23" #time/date-time "2021-08-09T15:43:23" "Z"

    ;; added fraction
    false #time/date-time "2021-08-09T15:43:23" #time/date-time "2021-08-09T15:43:23.3" "Z"

    ;; trailing zero ok
    true #time/date-time "2021-08-09T15:43:23" #time/date-time "2021-08-09T15:43:23.0" "Z"

    ;; equality preserved across tz
    true #time/date-time "2021-08-09T15:43:23" #time/date-time "2021-08-09T15:43:23" "+02:00"

    ;; offset equiv
    true #time/offset-date-time "2021-08-09T15:43:23+02:00" #time/date-time "2021-08-09T15:43:23" "+02:00"

    ;; offset inequality
    false #time/offset-date-time "2021-08-09T15:43:23+02:00" #time/date-time "2021-08-09T15:43:23" "+03:00"))

(t/deftest test-cast-temporal
  (let [current-time #time/instant "2022-08-16T20:04:14.423452Z"]
    (letfn [(test-cast
              ([src-value tgt-type] (test-cast src-value tgt-type {}))
              ([src-value tgt-type {:keys [default-tz], :or {default-tz ZoneOffset/UTC}}]
               (-> (tu/query-ra [:project [{'res `(~'cast ~'arg ~tgt-type)}]
                                 [:table [{}]]]
                                {:current-time current-time
                                 :default-tz default-tz
                                 :params {'arg src-value}})
                   first :res)))]

      (t/testing "date ->"
        (t/testing "date"
          (t/is (= #time/date "2022-08-01"
                   (test-cast #time/date "2022-08-01"
                              [:date :milli]))))

        (t/testing "ts"
          (t/is (= #time/date-time "2022-08-01T00:00:00"
                   (test-cast #time/date "2022-08-01"
                              [:timestamp-local :milli]))))

        (t/testing "tstz"
          (t/is (= #time/zoned-date-time "2022-07-31T16:00-07:00[America/Los_Angeles]"
                   (test-cast #time/date "2022-08-01"
                              [:timestamp-tz :nano "America/Los_Angeles"]
                              {:default-tz (ZoneId/of "Europe/London")})))))

      (t/testing "tstz ->"
        (t/testing "date"
          (t/is (= #time/date "2022-07-31"
                   (test-cast #time/zoned-date-time "2022-08-01T05:34:56.789+01:00[Europe/London]"
                              [:date :day]
                              {:default-tz (ZoneId/of "America/Los_Angeles")}))))

        (t/testing "time"
          (t/is (= #time/time "12:34:56"
                   (test-cast #time/zoned-date-time "2022-08-01T12:34:56.789Z"
                              [:time-local :second])))

          (t/is (= #time/time "13:34:56"
                   (test-cast #time/zoned-date-time "2022-08-01T12:34:56.789Z"
                              [:time-local :second]
                              {:default-tz (ZoneId/of "Europe/London")})))

          (t/is (= #time/time "05:34:56"
                   (test-cast #time/zoned-date-time "2022-08-01T13:34:56.789+01:00[Europe/London]"
                              [:time-local :second]
                              {:default-tz (ZoneId/of "America/Los_Angeles")}))))

        (t/testing "ts"
          (t/is (= #time/date-time "2022-08-01T12:34:56"
                   (test-cast #time/zoned-date-time "2022-08-01T12:34:56.789Z"
                              [:timestamp-local :second])))

          (t/is (= #time/date-time "2022-08-01T13:34:56"
                   (test-cast #time/zoned-date-time "2022-08-01T12:34:56.789Z"
                              [:timestamp-local :second]
                              {:default-tz (ZoneId/of "Europe/London")})))

          (t/is (= #time/date-time "2022-08-01T05:34:56"
                   (test-cast #time/zoned-date-time "2022-08-01T13:34:56.789+01:00[Europe/London]"
                              [:timestamp-local :second]
                              {:default-tz (ZoneId/of "America/Los_Angeles")}))))

        (t/testing "tstz"
          (t/is (= #time/zoned-date-time "2022-08-01T13:34:56+01:00[Europe/London]"
                   (test-cast #time/zoned-date-time "2022-08-01T12:34:56.789012Z"
                              [:timestamp-tz :second "Europe/London"])))

          (t/is (= #time/zoned-date-time "2022-08-01T13:34:56+01:00[Europe/London]"
                   (test-cast #time/zoned-date-time "2022-08-01T12:34:56Z"
                              [:timestamp-tz :nano "Europe/London"])))))

      (t/testing "ts ->"
        (t/testing "date"
          (t/is (= #time/date "2022-08-01"
                   (test-cast #time/date-time "2022-08-01T05:34:56.789"
                              [:date :day]
                              {:default-tz (ZoneId/of "America/Los_Angeles")}))))

        (t/testing "time"
          (t/is (= #time/time "05:34:56.789012"
                   (test-cast #time/date-time "2022-08-01T05:34:56.789012345"
                              [:time-local :micro]
                              {:default-tz (ZoneId/of "America/Los_Angeles")}))))

        (t/testing "ts"
          (t/is (= #time/date-time "2022-08-01T05:34:56"
                   (test-cast #time/date-time "2022-08-01T05:34:56.789"
                              [:timestamp-local :second "America/Los_Angeles"]
                              {:default-tz (ZoneId/of "America/Los_Angeles")}))))

        (t/testing "tstz"
          (t/is (= #time/zoned-date-time "2022-08-01T05:34:56-07:00[America/Los_Angeles]"
                   (test-cast #time/date-time "2022-08-01T05:34:56.789"
                              [:timestamp-tz :second "America/Los_Angeles"]
                              {:default-tz (ZoneId/of "America/Los_Angeles")})))

          (t/is (= #time/zoned-date-time "2022-08-01T04:34:56-07:00[America/Los_Angeles]"
                   (test-cast #time/date-time "2022-08-01T12:34:56.789"
                              [:timestamp-tz :second "America/Los_Angeles"]
                              {:default-tz (ZoneId/of "Europe/London")})))))

      (t/testing "time ->"
        (t/testing "date"
          (t/is (thrown? IllegalArgumentException
                         (test-cast #time/time "12:34:56.789012345" [:date :day]))))

        (t/testing "time"
          (t/is (= #time/time "12:34:56.789"
                   (test-cast #time/time "12:34:56.789012345"
                              [:time-local :milli]))))

        (t/testing "ts"
          (t/is (= #time/date-time "2022-08-16T12:34:56"
                   (test-cast #time/time "12:34:56.789012345"
                              [:timestamp-local :second]))))

        (t/testing "tstz"
          (t/is (= #time/zoned-date-time "2022-08-16T12:34:56-07:00[America/Los_Angeles]"
                   (test-cast #time/time "12:34:56.789012345"
                              [:timestamp-tz :second "America/Los_Angeles"]
                              {:default-tz (ZoneId/of "America/Los_Angeles")})))

          (t/is (= #time/zoned-date-time "2022-08-16T04:34:56.789012-07:00[America/Los_Angeles]"
                   (test-cast #time/time "12:34:56.789012345"
                              [:timestamp-tz :micro "America/Los_Angeles"]
                              {:default-tz (ZoneId/of "Europe/London")}))))))))
