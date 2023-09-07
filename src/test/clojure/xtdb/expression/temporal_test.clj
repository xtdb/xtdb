(ns xtdb.expression.temporal-test
  (:require [clojure.string :as str]
            [clojure.test :as t :refer [deftest]]
            [clojure.test.check.clojure-test :as tct]
            [clojure.test.check.generators :as tcg]
            [clojure.test.check.properties :as tcp]
            [xtdb.expression-test :as et]
            [xtdb.test-util :as tu])
  (:import (java.time Duration Instant LocalDate LocalDateTime LocalTime Period ZoneId ZoneOffset ZonedDateTime)
           (xtdb.types IntervalDayTime IntervalMonthDayNano IntervalYearMonth)))

(t/use-fixtures :each tu/with-allocator)

;; the goal of this test is simply to demonstrate clock affects the computation
;; equality may well remain incorrect for now, it is not the goal
(t/deftest clock-influences-equality-of-ambiguous-datetimes-test
  (t/are [expected a b zone-id]
      (= expected (-> (tu/query-ra [:project [{'res '(= ?a ?b)}]
                                    [:table [{}]]]
                                   {:params {'?a a, '?b b}
                                    :basis {:current-time Instant/EPOCH}
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
    true #time/zoned-date-time "2021-08-09T15:43:23+02:00" #time/date-time "2021-08-09T15:43:23" "+02:00"

    ;; offset inequality
    false #time/zoned-date-time "2021-08-09T15:43:23+02:00" #time/date-time "2021-08-09T15:43:23" "+03:00"))

(t/deftest test-cast-temporal
  (let [current-time #time/instant "2022-08-16T20:04:14.423452Z"]
    (letfn [(test-cast
              ([src-value tgt-type] (test-cast src-value tgt-type {}))
              ([src-value tgt-type {:keys [default-tz], :or {default-tz ZoneOffset/UTC}}]
               (-> (tu/query-ra [:project [{'res `(~'cast ~'arg ~tgt-type)}]
                                 [:table [{}]]]
                                {:basis {:current-time current-time}
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
                              [:timestamp-local :milli]
                              {:default-tz (ZoneId/of "Europe/London")}))))

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

(def ^:private instant-gen
  (->> (tcg/tuple (tcg/choose (.getEpochSecond #time/instant "2020-01-01T00:00:00Z")
                              (.getEpochSecond #time/instant "2040-01-01T00:00:00Z"))
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
                         {:params {'?t1 t1}
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
                         {:params {'?t1 t1}
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
                         {:params {'?t1 t1}
                          :basis {:current-time now}
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
                         {:params {'?t1 t1}
                          :basis {:current-time now}
                          :default-tz default-tz})
            first :res))))

(t/deftest test-temporal-arithmetic
  (letfn [(test-arithmetic
            ([f x y] (test-arithmetic f x y {}))
            ([f x y {:keys [default-tz], :or {default-tz #time/zone "America/Los_Angeles"}}]
             (-> (tu/query-ra [:project [{'res `(~f ~'x ~'y)}]
                               [:table [{}]]]
                              {:default-tz default-tz
                               :params {'x x, 'y y}})
                 first :res)))]

    (t/testing "(+ datetime duration)"
      (t/is (= #time/date-time "2022-08-01T01:15:43.342"
               (test-arithmetic '+ #time/date "2022-08-01" #time/duration "PT1H15M43.342S")))

      (t/is (= #time/date-time "2022-08-01T02:31:26.684"
               (test-arithmetic '+ #time/date-time "2022-08-01T01:15:43.342" #time/duration "PT1H15M43.342S")))

      (t/is (= #time/date-time "2022-08-01T02:31:26.684"
               (test-arithmetic '+ #time/date-time "2022-08-01T01:15:43.342" #time/time "01:15:43.342")))

      (t/is (= #time/zoned-date-time "2022-08-01T02:31:26.684+01:00[Europe/London]"
               (test-arithmetic '+ #time/zoned-date-time "2022-08-01T01:15:43.342+01:00[Europe/London]" #time/duration "PT1H15M43.342S"))))

    (t/testing "(+ datetime interval)"
      (t/is (= #time/date "2023-08-01"
               (test-arithmetic '+ #time/date "2022-08-01" #xt/interval-ym "P1Y")))

      (t/is (= #time/date-time "2022-08-04T01:15:43.342"
               (test-arithmetic '+ #time/date "2022-08-01" #xt/interval-dt ["P3D" "PT1H15M43.342S"])))

      (t/is (= #time/date-time "2022-09-04T02:31:26.684"
               (test-arithmetic '+ #time/date-time "2022-08-01T01:15:43.342" #xt/interval-mdn ["P1M3D" "PT1H15M43.342S"])))

      (t/is (= #time/zoned-date-time "2022-08-01T02:31:26.684+01:00[Europe/London]"
               (test-arithmetic '+ #xt/interval-mdn ["P0D" "PT1H15M43.342S"] #time/zoned-date-time "2022-08-01T01:15:43.342+01:00[Europe/London]")))

      (t/is (= #time/zoned-date-time "2022-10-31T12:00+00:00[Europe/London]"
               (test-arithmetic '+ #xt/interval-mdn ["P2D" "PT1H"] #time/zoned-date-time "2022-10-29T11:00+01:00[Europe/London]"))
            "clock change"))

    (t/testing "(- datetime duration)"
      (t/is (= #time/date-time "2022-07-31T22:44:16.658"
               (test-arithmetic '- #time/date "2022-08-01" #time/duration "PT1H15M43.342S")))

      (t/is (= #time/date-time "2022-08-01T01:15:43.342"
               (test-arithmetic '- #time/date-time "2022-08-01T02:31:26.684" #time/duration "PT1H15M43.342S")))

      (t/is (= #time/date-time "2022-08-01T01:15:43.342"
               (test-arithmetic '- #time/date-time "2022-08-01T02:31:26.684" #time/time "01:15:43.342")))

      (t/is (= #time/zoned-date-time "2022-08-01T01:15:43.342+01:00[Europe/London]"
               (test-arithmetic '- #time/zoned-date-time "2022-08-01T02:31:26.684+01:00[Europe/London]" #time/duration "PT1H15M43.342S"))))

    (t/testing "(- datetime interval)"
      (t/is (= #time/date "2021-05-01"
               (test-arithmetic '- #time/date "2022-08-01" #xt/interval-ym "P1Y3M")))

      (t/is (= #time/date-time "2022-07-28T22:44:16.658"
               (test-arithmetic '- #time/date "2022-08-01" #xt/interval-dt ["P3D" "PT1H15M43.342S"])))

      (t/is (= #time/date-time "2022-07-29T01:15:43.342"
               (test-arithmetic '- #time/date-time "2022-08-01T02:31:26.684" #xt/interval-mdn ["P3D" "PT1H15M43.342S"])))

      (t/is (= #time/zoned-date-time "2022-08-01T01:15:43.342+01:00[Europe/London]"
               (test-arithmetic '- #time/zoned-date-time "2022-08-01T02:31:26.684+01:00[Europe/London]" #xt/interval-mdn ["P0D" "PT1H15M43.342S"])))

      (t/is (= #time/zoned-date-time "2022-10-29T11:00+01:00[Europe/London]"
               (test-arithmetic '- #time/zoned-date-time "2022-10-31T12:00+00:00[Europe/London]" #xt/interval-mdn ["P2D" "PT1H"] ))
            "clock change"))

    (t/testing "(- datetime datetime)"
      (t/is (= #time/duration "PT24H"
               (test-arithmetic '- #time/date "2022-08-01" #time/date "2022-07-31")))

      (t/is (= #time/duration "PT1H15M43.342S"
               (test-arithmetic '- #time/date "2022-08-01" #time/date-time "2022-07-31T22:44:16.658")))

      (t/is (= #time/duration "PT1H15M43.342S"
               (test-arithmetic '- #time/date-time "2022-08-01T02:31:26.684" #time/date-time "2022-08-01T01:15:43.342")))

      (t/is (= #time/duration "PT1H15M43.342S"
               (test-arithmetic '- #time/zoned-date-time "2022-08-01T02:31:26.684+01:00[Europe/London]" #time/zoned-date-time "2022-08-01T01:15:43.342+01:00[Europe/London]")))

      (t/is (= #time/duration "PT6H44M16.658S"
               (test-arithmetic '- #time/date "2022-08-01" #time/zoned-date-time "2022-08-01T01:15:43.342+01:00[Europe/London]")))

      (t/is (= #time/duration "PT-9H-15M-43.342S"
               (test-arithmetic '- #time/zoned-date-time "2022-08-01T01:15:43.342+01:00[Europe/London]" #time/date-time "2022-08-01T02:31:26.684"))))))

(tct/defspec test-lt
  (tcp/for-all [t1 (tcg/one-of [ldt-gen ld-gen zdt-gen])
                t2 (tcg/one-of [ldt-gen ld-gen zdt-gen])
                default-tz zone-id-gen
                now instant-gen]
    (= (neg? (compare (->inst t1 now default-tz)
                      (->inst t2 now default-tz)))
       (->> (tu/query-ra [:project [{'res '(< ?t1 ?t2)}]
                          [:table [{}]]]
                         {:params {'?t1 t1, '?t2 t2}
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
                         {:params {'?t1 t1, '?t2 t2}
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
                         {:params {'?t1 t1, '?t2 t2}
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
                         {:params {'?t1 t1, '?t2 t2}
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
                         {:params {'?t1 t1, '?t2 t2}
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
                         {:params {'?t1 t1, '?t2 t2}
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
  (let [i #xt/interval-mdn ["P33M244D" "PT48H0.003444443S"] ]
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

(t/deftest test-interval-equality-quirks
  (t/are [expr expected]
      (= expected (et/project1 expr {}))

    '(= (single-field-interval 0 "YEAR" 2 0)) true
    '(= (single-field-interval 0 "YEAR" 2 0) (single-field-interval 0 "DAY" 2 0)) true
    '(= (single-field-interval 0 "YEAR" 2 0) (single-field-interval 1 "DAY" 2 0)) false

    '(= (single-field-interval 1 "YEAR" 2 0) (single-field-interval 12 "MONTH" 2 0)) true))

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
  (let [from #time/zoned-date-time "2020-01-01T00:00Z[UTC]"
        to #time/zoned-date-time "2022-01-01T00:00Z[UTC]"]
    (t/is (= {:from from :to to}
             (et/project1 '(period x y)
                          {:x from, :y to}))))

  (let [from #time/zoned-date-time "2030-01-01T00:00Z[UTC]"
        to #time/zoned-date-time "2020-01-01T00:00Z[UTC]"]
    (t/is
      (thrown-with-msg?
        RuntimeException
        #"From cannot be greater than to when constructing a period"
        (et/project1 '(period x y)
                     {:x from, :y to})))))

(deftest test-overlaps?-predicate
  (t/is
    (= true
       (et/project1
         '(overlaps? x y)
         {:x {:from #inst "2020", :to #inst "2022"}
          :y {:from #inst "2021", :to #inst "2023"}})))

  (t/is
    (= false
       (et/project1
         '(overlaps? x y)
         {:x {:from #inst "2020", :to #inst "2021"}
          :y {:from #inst "2021", :to #inst "2023"}}))))

(deftest test-contains?-predicate
  (t/testing "period to period"
    (t/is
      (= true
         (et/project1
           '(contains? x y)
           {:x {:from #inst "2020", :to #inst "2025"}
            :y {:from #inst "2021", :to #inst "2023"}})))

    (t/is
      (= false
         (et/project1
           '(contains? x y)
           {:x {:from #inst "2020", :to #inst "2022"}
            :y {:from #inst "2021", :to #inst "2023"}}))))

  (t/testing "period to timestamp"
    (t/is
      (= true
         (et/project1
           '(contains? x y)
           {:x {:from #inst "2020", :to #inst "2025"}
            :y #inst "2021"})))

    (t/is
      (= false
         (et/project1
           '(contains? x y)
           {:x {:from #inst "2020", :to #inst "2022"}
            :y #inst "2023"})))))

(deftest test-equals?-predicate
  (t/is
    (= true
       (et/project1
         '(equals? x y)
         {:x {:from #inst "2020", :to #inst "2022"}
          :y {:from #inst "2020", :to #inst "2022"}})))

  (t/is
    (= false
       (et/project1
         '(equals? x y)
         {:x {:from #inst "2020", :to #inst "2021"}
          :y {:from #inst "2020", :to #inst "2023"}}))))

(deftest test-precedes?-predicate
  (t/is
    (= true
       (et/project1
         '(precedes? x y)
         {:x {:from #inst "2020", :to #inst "2022"}
          :y {:from #inst "2023", :to #inst "2025"}})))

  (t/is
    (= false
       (et/project1
         '(precedes? x y)
         {:x {:from #inst "2020", :to #inst "2021"}
          :y {:from #inst "2020", :to #inst "2023"}}))))

(deftest test-succeeds?-predicate
  (t/is
    (= true
       (et/project1
         '(succeeds? x y)
         {:x {:from #inst "2023", :to #inst "2025"}
          :y {:from #inst "2020", :to #inst "2022"}})))

  (t/is
    (= false
       (et/project1
         '(succeeds? x y)
         {:x {:from #inst "2020", :to #inst "2021"}
          :y {:from #inst "2020", :to #inst "2023"}}))))

(deftest test-immediately-precedes?-predicate
  (t/is
    (= true
       (et/project1
         '(immediately-precedes? x y)
         {:x {:from #inst "2020", :to #inst "2023"}
          :y {:from #inst "2023", :to #inst "2025"}})))

  (t/is
    (= false
       (et/project1
         '(immediately-precedes? x y)
         {:x {:from #inst "2020", :to #inst "2022"}
          :y {:from #inst "2023", :to #inst "2025"}}))))

(deftest test-immediately-succeeds?-predicate
  (t/is
    (= true
       (et/project1
         '(immediately-succeeds? x y)
         {:x {:from #inst "2022", :to #inst "2025"}
          :y {:from #inst "2020", :to #inst "2022"}})))

  (t/is
    (= false
       (et/project1
         '(immediately-succeeds? x y)
         {:x {:from #inst "2023", :to #inst "2025"}
          :y {:from #inst "2020", :to #inst "2022"}}))))

(deftest test-leads?-predicate
  (t/is
    (= true
       (et/project1
         '(leads? x y)
         {:x {:from #inst "2020", :to #inst "2025"}
          :y {:from #inst "2021", :to #inst "2025"}})))

  (t/is
    (= false
       (et/project1
         '(leads? x y)
         {:x {:from #inst "2021", :to #inst "2025"}
          :y {:from #inst "2021", :to #inst "2025"}}))))

(deftest test-strictly-leads?-predicate
  (t/is
    (= true
       (et/project1
         '(strictly-leads? x y)
         {:x {:from #inst "2020", :to #inst "2024"}
          :y {:from #inst "2021", :to #inst "2025"}})))

  (t/is
    (= false
       (et/project1
         '(strictly-leads? x y)
         {:x {:from #inst "2020", :to #inst "2025"}
          :y {:from #inst "2021", :to #inst "2025"}}))))

(deftest test-lags?-predicate
  (t/is
    (= true
       (et/project1
         '(lags? x y)
         {:x {:from #inst "2021", :to #inst "2025"}
          :y {:from #inst "2020", :to #inst "2024"}})))

  (t/is
    (= false
       (et/project1
         '(lags? x y)
         {:x {:from #inst "2021", :to #inst "2024"}
          :y {:from #inst "2022", :to #inst "2025"}}))))

(deftest test-strictly-lags?-predicate
  (t/is
    (= true
       (et/project1
         '(strictly-lags? x y)
         {:x {:from #inst "2022", :to #inst "2025"}
          :y {:from #inst "2021", :to #inst "2024"}})))

  (t/is
    (= false
       (et/project1
         '(strictly-lags? x y)
         {:x {:from #inst "2021", :to #inst "2025"}
          :y {:from #inst "2021", :to #inst "2024"}}))))

(deftest test-strictly-overlaps?-predicate
  (t/is
    (= true
       (et/project1
         '(strictly-overlaps? x y)
         {:x {:from #inst "2022", :to #inst "2024"}
          :y {:from #inst "2021", :to #inst "2025"}})))

  (t/is
    (= false
       (et/project1
         '(strictly-overlaps? x y)
         {:x {:from #inst "2021", :to #inst "2024"}
          :y {:from #inst "2021", :to #inst "2025"}}))))

(deftest test-strictly-contains?-predicate
  (t/is
    (= true
       (et/project1
         '(strictly-contains? x y)
         {:x {:from #inst "2021", :to #inst "2025"}
          :y {:from #inst "2022", :to #inst "2024"}})))

  (t/is
    (= false
       (et/project1
         '(strictly-contains? x y)
         {:x {:from #inst "2021", :to #inst "2025"}
          :y {:from #inst "2022", :to #inst "2025"}}))))

(deftest test-strictly-precedes?-predicate
  (t/is
    (= true
       (et/project1
         '(strictly-precedes? x y)
         {:x {:from #inst "2020", :to #inst "2022"}
          :y {:from #inst "2023", :to #inst "2025"}})))

  (t/is
    (= false
       (et/project1
         '(strictly-precedes? x y)
         {:x {:from #inst "2020", :to #inst "2022"}
          :y {:from #inst "2022", :to #inst "2023"}}))))

(deftest test-strictly-succeeds?-predicate
  (t/is
    (= true
       (et/project1
         '(strictly-succeeds? x y)
         {:x {:from #inst "2023", :to #inst "2024"}
          :y {:from #inst "2021", :to #inst "2022"}})))

  (t/is
    (= false
       (et/project1
         '(strictly-succeeds? x y)
         {:x {:from #inst "2022", :to #inst "2024"}
          :y {:from #inst "2021", :to #inst "2022"}}))))

(deftest test-immediately-leads?-predicate
  (t/is
    (= true
       (et/project1
         '(immediately-leads? x y)
         {:x {:from #inst "2021", :to #inst "2024"}
          :y {:from #inst "2022", :to #inst "2024"}})))

  (t/is
    (= false
       (et/project1
         '(immediately-leads? x y)
         {:x {:from #inst "2021", :to #inst "2024"}
          :y {:from #inst "2022", :to #inst "2025"}}))))

(deftest test-immediately-lags?-predicate
  (t/is
    (= true
       (et/project1
         '(immediately-lags? x y)
         {:x {:from #inst "2021", :to #inst "2025"}
          :y {:from #inst "2021", :to #inst "2024"}})))

  (t/is
    (= false
       (et/project1
         '(immediately-lags? x y)
         {:x {:from #inst "2021", :to #inst "2024"}
          :y {:from #inst "2021", :to #inst "2025"}}))))
