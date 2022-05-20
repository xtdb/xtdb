(ns core2.types-test
  (:require [clojure.test :as t]
            [core2.test-util :as tu]
            [core2.types :as types]
            [core2.util :as util]
            [core2.vector.writer :as vw])
  (:import [core2.vector.extensions KeywordVector UuidVector UriVector]
           java.net.URI
           java.nio.ByteBuffer
           [java.time Instant OffsetDateTime ZonedDateTime ZoneId ZoneOffset LocalDate LocalTime Period Duration]
           [org.apache.arrow.vector BigIntVector BitVector Float4Vector Float8Vector IntVector NullVector SmallIntVector TimeStampMicroTZVector TinyIntVector VarBinaryVector VarCharVector DateDayVector DateMilliVector TimeNanoVector TimeSecVector TimeMilliVector TimeMicroVector PeriodDuration IntervalMonthDayNanoVector IntervalYearVector IntervalDayVector]
           [org.apache.arrow.vector.complex DenseUnionVector ListVector StructVector]
           [core2.types LegType]
           [org.apache.arrow.vector.types.pojo ArrowType$Date ArrowType$Time ArrowType$Interval]
           [org.apache.arrow.vector.types DateUnit TimeUnit IntervalUnit]
           [core2.vector IVectorWriter]))

(t/use-fixtures :each tu/with-allocator)

(defn- test-read [leg-type-fn write-fn vs]
  (with-open [duv (DenseUnionVector/empty "" tu/*allocator*)]
    (let [duv-writer (.asDenseUnion (vw/vec->writer duv))]
      (doseq [v vs]
        (.startValue duv-writer)
        (doto (.writerForType duv-writer (leg-type-fn v))
          (.startValue)
          (write-fn v)
          (.endValue))
        (.endValue duv-writer)))
    {:vs (vec (for [idx (range (count vs))]
                (types/get-object duv idx)))
     :vec-types (vec (for [idx (range (count vs))]
                       (class (.getVectorByType duv (.getTypeId duv idx)))))}))

(defn- test-round-trip [vs]
  (test-read types/value->leg-type #(types/write-value! %2 %1) vs))

(t/deftest round-trips-values

  (t/is (= {:vs [false nil 2 1 6 4 3.14 2.0]
            :vec-types [BitVector NullVector BigIntVector TinyIntVector SmallIntVector IntVector Float8Vector Float4Vector]}
           (test-round-trip [false nil (long 2) (byte 1) (short 6) (int 4) (double 3.14) (float 2)]))
        "primitives")

  (t/is (= {:vs ["Hello"
                 (ByteBuffer/wrap (byte-array [1, 2, 3]))
                 (ByteBuffer/wrap (byte-array [1, 2, 3]))]
            :vec-types [VarCharVector VarBinaryVector VarBinaryVector]}
           (test-round-trip ["Hello"
                             (byte-array [1 2 3])
                             (ByteBuffer/wrap (byte-array [1 2 3]))]))
        "binary types")

  (t/is (= {:vs [(util/->zdt #inst "1999")
                 (util/->zdt #inst "2021-09-02T13:54:35.809Z")
                 (ZonedDateTime/ofInstant (util/->instant #inst "2021-09-02T13:54:35.809Z") (ZoneId/of "Europe/Stockholm"))
                 (ZonedDateTime/ofInstant (util/->instant #inst "2021-09-02T13:54:35.809Z") (ZoneOffset/ofHours 2))
                 (ZonedDateTime/ofInstant (Instant/ofEpochSecond 3600 1000) (ZoneId/of "UTC"))]
            :vec-types (repeat 5 TimeStampMicroTZVector)}
           (test-round-trip [#inst "1999"
                             (util/->instant #inst "2021-09-02T13:54:35.809Z")
                             (ZonedDateTime/ofInstant (util/->instant #inst "2021-09-02T13:54:35.809Z") (ZoneId/of "Europe/Stockholm"))
                             (OffsetDateTime/ofInstant (util/->instant #inst "2021-09-02T13:54:35.809Z") (ZoneOffset/ofHours 2))
                             (Instant/ofEpochSecond 3600 1234)]))
        "timestamp types")

  (let [vs [[]
            [2 3.14 [false nil]]
            {}
            {:B 2, :C 1, :F false}
            {:B 2, :C 1, :F false}
            [1 {:B [2]}]
            [1 {:B [2]}]
            {:B 3.14, :D {:E ["hello" -1]}, :F nil}]]
    (t/is (= {:vs vs
              :vec-types [ListVector ListVector StructVector StructVector StructVector ListVector ListVector StructVector]}
             (test-round-trip vs))
          "nested types"))

  (let [vs [:foo :foo/bar #uuid "97a392d5-5e3f-406f-9651-a828ee79b156" (URI/create "https://xtdb.com")]]
    (t/is (= {:vs vs
              :vec-types [KeywordVector KeywordVector UuidVector UriVector]}
             (test-round-trip vs))
          "extension types")))

(t/deftest date-vector-test
  (let [vs [(LocalDate/of 2007 12 11)]]
    (->> "LocalDate can be round tripped through DAY date vectors"
         (t/is (= {:vs vs
                   :vec-types [DateDayVector]}
                  (test-round-trip vs))))

    (->> "LocalDate can be read from MILLISECOND date vectors"
         (t/is (= vs (:vs (test-read (constantly (LegType. (ArrowType$Date. DateUnit/MILLISECOND)))
                                     (fn [^IVectorWriter w ^LocalDate v]
                                       (.setSafe ^DateMilliVector (.getVector w) (.getPosition w) (long (* 86400000 (.toEpochDay v)))))
                                     vs)))))))

(t/deftest time-vector-test
  (let [secs [(LocalTime/of 13 1 14 0)]
        micros [(LocalTime/of 13 1 14 1e3)]
        millis [(LocalTime/of 13 1 14 1e6)]
        nanos [(LocalTime/of 13 1 14 1e8)]
        all (concat secs millis micros nanos)]
    (->> "LocalTime can be round tripped through NANO time vectors"
         (t/is (= {:vs all
                   :vec-types (map (constantly TimeNanoVector) all)}
                  (test-round-trip all))))

    (->> "LocalTime can be read from SECOND time vectors"
         (t/is (= secs (:vs (test-read (constantly (LegType. (ArrowType$Time. TimeUnit/SECOND 32)))
                                       (fn [^IVectorWriter w, ^LocalTime v]
                                         (.setSafe ^TimeSecVector (.getVector w) (.getPosition w) (.toSecondOfDay v)))
                                       secs)))))

    (let [millis+ (concat millis secs)]
      (->> "LocalTime can be read from MILLI time vectors"
           (t/is (= millis+ (:vs (test-read (constantly (LegType. (ArrowType$Time. TimeUnit/MILLISECOND 32)))
                                            (fn [^IVectorWriter w, ^LocalTime v]
                                              (.setSafe ^TimeMilliVector (.getVector w) (.getPosition w) (int (quot (.toNanoOfDay v) 1e6))))
                                            millis+))))))

    (let [micros+ (concat micros millis secs)]
      (->> "LocalTime can be read from MICRO time vectors"
           (t/is (= micros+ (:vs (test-read (constantly (LegType. (ArrowType$Time. TimeUnit/MICROSECOND 64)))
                                            (fn [^IVectorWriter w, ^LocalTime v]
                                              (.setSafe ^TimeMicroVector (.getVector w) (.getPosition w) (long (quot (.toNanoOfDay v) 1e3))))
                                            micros+))))))))

(t/deftest interval-vector-test
  ;; need to normalize for round trip poses a problem
  ;; will need to consider a canonical vector that preserves every component in PeriodDuration
  ;; for years/months we lose the years as a separate component, it has to be folded into months.
  (let [npd (fn [years months days nanos] (PeriodDuration. (.normalized (Period/of years months days)) (Duration/ofNanos nanos)))
        renorm (fn [^PeriodDuration pd] (PeriodDuration. (.normalized (.getPeriod pd)) (.getDuration pd)))
        renorm-rt (fn [{:keys [vs] :as result}] (assoc result :vs (mapv renorm vs)))

        full-period (npd 0 33 244 3444443)
        period-year-month (npd 1 23 0 0)
        period-day-ms (npd 0 0 1434 23e6)

        test-round-trip (comp renorm-rt test-round-trip)
        test-read (comp renorm-rt test-read)]

    (->> "(normalized) PeriodDuration objects can be round tripped"
         (t/is (= {:vs [full-period]
                   :vec-types [IntervalMonthDayNanoVector]}
                  (test-round-trip [full-period]))))

    (->> "PeriodDuration can be read from YEAR vectors"
         (t/is (= [period-year-month] (:vs (test-read (constantly (LegType. (ArrowType$Interval. IntervalUnit/YEAR_MONTH)))
                                                   (fn [^IVectorWriter w, ^PeriodDuration v]
                                                     (.setSafe ^IntervalYearVector (.getVector w) (.getPosition w) (.toTotalMonths (.getPeriod v))))
                                                   [period-year-month])))))

    (->> "PeriodDuration can be read from DAY vectors"
         (t/is (= [period-day-ms] (:vs (test-read (constantly (LegType. (ArrowType$Interval. IntervalUnit/DAY_TIME)))
                                                   (fn [^IVectorWriter w, ^PeriodDuration v]
                                                     (.setSafe ^IntervalDayVector (.getVector w) (.getPosition w) (.getDays (.getPeriod v)) (.toMillis (.getDuration v))))
                                                   [period-day-ms])))))))

(t/deftest test-merge-fields
  (let [varchar-field (types/->field "foo" types/varchar-type false)
        bigint-field (types/->field "foo" types/bigint-type false)
        float8-field (types/->field "float8" types/float8-type false)]
    (t/is (= varchar-field
             (types/merge-fields varchar-field varchar-field)))

    (t/is (= (types/->field "foo" types/dense-union-type false
                            (types/field-with-name varchar-field "varchar")
                            (types/field-with-name bigint-field "bigint"))
             (types/merge-fields varchar-field bigint-field)))

    (t/is (= (types/->field "foo" types/dense-union-type false
                            (types/field-with-name varchar-field "varchar")
                            (types/field-with-name bigint-field "bigint")
                            (types/field-with-name float8-field "float8"))
             (types/merge-fields (types/->field "foo" types/dense-union-type false
                                                (types/field-with-name varchar-field "varchar")
                                                (types/field-with-name bigint-field "bigint"))
                                 float8-field))))

  (t/testing "merges list types"
    (t/is (= (types/->field "foo" types/list-type false
                            (types/->field "$data" types/varchar-type false))
             (types/merge-fields (types/->field "foo" types/list-type false
                                                (types/->field "$data" types/varchar-type false))
                                 (types/->field "foo" types/list-type false
                                                (types/->field "$data" types/varchar-type false)))))

    (t/is (= (types/->field "foo" types/list-type false
                            (types/->field "$data" types/dense-union-type false
                                           (types/->field "varchar" types/varchar-type false)
                                           (types/->field "bigint" types/bigint-type false)))
             (types/merge-fields (types/->field "foo" types/list-type false
                                                (types/->field "$data" types/varchar-type false))
                                 (types/->field "foo" types/list-type false
                                                (types/->field "$data" types/bigint-type false))))))

  (t/testing "merges struct types"
    (let [struct-field (types/->field "foo" types/struct-type false
                                      (types/->field "a" types/varchar-type false)
                                      (types/->field "b" types/varchar-type false))]
      (t/is (= struct-field
               (types/merge-fields struct-field struct-field))))

    (t/is (= (types/->field "foo" types/struct-type false
                            (types/->field "a" types/varchar-type false)
                            (types/->field "b" types/dense-union-type false
                                           (types/->field "varchar" types/varchar-type false)
                                           (types/->field "bigint" types/bigint-type false)))

             (types/merge-fields (types/->field "foo" types/struct-type false
                                                (types/->field "a" types/varchar-type false)
                                                (types/->field "b" types/varchar-type false))
                                 (types/->field "foo" types/struct-type false
                                                (types/->field "a" types/varchar-type false)
                                                (types/->field "b" types/bigint-type false)))))

    (let [struct0 (types/->field "foo" types/struct-type false
                                 (types/->field "a" types/varchar-type false)
                                 (types/->field "b" types/varchar-type false))
          struct1 (types/->field "foo" types/struct-type false
                                 (types/->field "b" types/varchar-type false)
                                 (types/->field "c" types/bigint-type false))]
      (t/is (= (types/->field "foo" types/dense-union-type false
                              (types/field-with-name struct0 "struct0")
                              (types/field-with-name struct1 "struct1"))
               (types/merge-fields struct0 struct1))))))
