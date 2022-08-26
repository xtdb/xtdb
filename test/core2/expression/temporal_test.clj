(ns core2.expression.temporal-test
  (:require [clojure.test :as t]
            [clojure.test.check.clojure-test :as tct]
            [clojure.test.check.properties :as tcp]
            [clojure.test.check.generators :as tcg]
            [core2.test-util :as tu])
  (:import (java.time Instant LocalDate LocalDateTime LocalTime ZonedDateTime ZoneId ZoneOffset)))

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
    (= (LocalTime/ofInstant (->inst t1 now default-tz) default-tz)
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
                         {:params {'?t1 t1}
                          :current-time now
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

    (t/testing "(- datetime duration)"
      (t/is (= #time/date-time "2022-08-01T01:15:43.342"
               (test-arithmetic '+ #time/date "2022-08-01" #time/duration "PT1H15M43.342S")))

      (t/is (= #time/date-time "2022-08-01T02:31:26.684"
               (test-arithmetic '+ #time/date-time "2022-08-01T01:15:43.342" #time/duration "PT1H15M43.342S")))

      (t/is (= #time/date-time "2022-08-01T02:31:26.684"
               (test-arithmetic '+ #time/date-time "2022-08-01T01:15:43.342" #time/time "01:15:43.342")))

      (t/is (= #time/zoned-date-time "2022-08-01T02:31:26.684+01:00[Europe/London]"
               (test-arithmetic '+ #time/zoned-date-time "2022-08-01T01:15:43.342+01:00[Europe/London]" #time/duration "PT1H15M43.342S"))))

    (t/testing "(- datetime duration)"
      (t/is (= #time/date-time "2022-07-31T22:44:16.658"
               (test-arithmetic '- #time/date "2022-08-01" #time/duration "PT1H15M43.342S")))

      (t/is (= #time/date-time "2022-08-01T01:15:43.342"
               (test-arithmetic '- #time/date-time "2022-08-01T02:31:26.684" #time/duration "PT1H15M43.342S")))

      (t/is (= #time/date-time "2022-08-01T01:15:43.342"
               (test-arithmetic '- #time/date-time "2022-08-01T02:31:26.684" #time/time "01:15:43.342")))

      (t/is (= #time/zoned-date-time "2022-08-01T01:15:43.342+01:00[Europe/London]"
               (test-arithmetic '- #time/zoned-date-time "2022-08-01T02:31:26.684+01:00[Europe/London]" #time/duration "PT1H15M43.342S"))))

    (t/testing "(- datetime datetime)"
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
