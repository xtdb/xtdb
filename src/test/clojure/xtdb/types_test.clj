(ns xtdb.types-test
  (:require [clojure.test :as t]
            [xtdb.test-util :as tu]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.writer :as vw])
  (:import (xtdb.types IntervalDayTime IntervalYearMonth)
           (xtdb.vector IVectorWriter)
           (xtdb.vector.extensions ClojureFormVector KeywordVector UriVector UuidVector)
           java.net.URI
           java.nio.ByteBuffer
           (java.time Instant LocalDate LocalTime OffsetDateTime ZonedDateTime)
           (org.apache.arrow.vector BigIntVector BitVector DateDayVector DateMilliVector Float4Vector Float8Vector IntVector IntervalDayVector IntervalMonthDayNanoVector IntervalYearVector NullVector SmallIntVector TimeMicroVector TimeMilliVector TimeNanoVector TimeSecVector TimeStampMicroTZVector TinyIntVector VarBinaryVector VarCharVector)
           (org.apache.arrow.vector.complex DenseUnionVector ListVector StructVector)))

(t/use-fixtures :each tu/with-allocator)

(defn- test-read [col-type-fn write-fn vs]
  (with-open [duv (DenseUnionVector/empty "" tu/*allocator*)]
    (let [duv-writer (.asDenseUnion (vw/vec->writer duv))]
      (doseq [v vs]
        (.startValue duv-writer)
        (doto (.writerForType duv-writer (col-type-fn v))
          (.startValue)
          (write-fn v)
          (.endValue))
        (.endValue duv-writer)))
    {:vs (vec (for [idx (range (count vs))]
                (types/get-object duv idx)))
     :vec-types (vec (for [idx (range (count vs))]
                       (class (.getVectorByType duv (.getTypeId duv idx)))))}))

(defn- test-round-trip [vs]
  (test-read types/value->col-type #(types/write-value! %2 %1) vs))

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
                 (ZonedDateTime/ofInstant (util/->instant #inst "2021-09-02T13:54:35.809Z") #time/zone "Europe/Stockholm")
                 (ZonedDateTime/ofInstant (util/->instant #inst "2021-09-02T13:54:35.809Z") #time/zone "+02:00")
                 (ZonedDateTime/ofInstant (Instant/ofEpochSecond 3600 1000) #time/zone "UTC")]
            :vec-types (repeat 5 TimeStampMicroTZVector)}
           (test-round-trip [#inst "1999"
                             (util/->instant #inst "2021-09-02T13:54:35.809Z")
                             (ZonedDateTime/ofInstant (util/->instant #inst "2021-09-02T13:54:35.809Z") #time/zone "Europe/Stockholm")
                             (OffsetDateTime/ofInstant (util/->instant #inst "2021-09-02T13:54:35.809Z") #time/zone "+02:00")
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

  (let [vs [:foo :foo/bar #uuid "97a392d5-5e3f-406f-9651-a828ee79b156" (URI/create "https://xtdb.com") #xt/clj-form (fn [a b] (+ a b))]]
    (t/is (= {:vs vs
              :vec-types [KeywordVector KeywordVector UuidVector UriVector ClojureFormVector]}
             (test-round-trip vs))
          "extension types")))

(t/deftest date-vector-test
  (let [vs [(LocalDate/of 2007 12 11)]]
    (->> "LocalDate can be round tripped through DAY date vectors"
         (t/is (= {:vs vs
                   :vec-types [DateDayVector]}
                  (test-round-trip vs))))

    (->> "LocalDate can be read from MILLISECOND date vectors"
         (t/is (= vs (:vs (test-read (constantly [:date :milli])
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
         (t/is (= secs (:vs (test-read (constantly [:time-local :second])
                                       (fn [^IVectorWriter w, ^LocalTime v]
                                         (.setSafe ^TimeSecVector (.getVector w) (.getPosition w) (.toSecondOfDay v)))
                                       secs)))))

    (let [millis+ (concat millis secs)]
      (->> "LocalTime can be read from MILLI time vectors"
           (t/is (= millis+ (:vs (test-read (constantly [:time-local :milli])
                                            (fn [^IVectorWriter w, ^LocalTime v]
                                              (.setSafe ^TimeMilliVector (.getVector w) (.getPosition w) (int (quot (.toNanoOfDay v) 1e6))))
                                            millis+))))))

    (let [micros+ (concat micros millis secs)]
      (->> "LocalTime can be read from MICRO time vectors"
           (t/is (= micros+ (:vs (test-read (constantly [:time-local :micro])
                                            (fn [^IVectorWriter w, ^LocalTime v]
                                              (.setSafe ^TimeMicroVector (.getVector w) (.getPosition w) (long (quot (.toNanoOfDay v) 1e3))))
                                            micros+))))))))

(t/deftest interval-vector-test
  ;; for years/months we lose the years as a separate component, it has to be folded into months.
  (let [iym #xt/interval-ym "P35M"]
    (t/is (= [iym]
             (:vs (test-read (constantly [:interval :year-month])
                             (fn [^IVectorWriter w, ^IntervalYearMonth v]
                               (.setSafe ^IntervalYearVector (.getVector w) (.getPosition w) (.toTotalMonths (.-period v))))
                             [iym])))))

  (let [idt #xt/interval-dt ["P1434D" "PT0.023S"]]
    (t/is (= [idt]
             (:vs (test-read (constantly [:interval :day-time])
                             (fn [^IVectorWriter w, ^IntervalDayTime v]
                               (.setSafe ^IntervalDayVector (.getVector w) (.getPosition w) (.getDays (.-period v)) (.toMillis (.-duration v))))
                             [idt])))))

  (let [imdn #xt/interval-mdn ["P33M244D" "PT0.003444443S"]]
    (t/is (= {:vs [imdn]
              :vec-types [IntervalMonthDayNanoVector]}
             (test-round-trip [imdn])))))

(t/deftest test-merge-col-types
  (t/is (= :utf8 (types/merge-col-types :utf8 :utf8)))

  (t/is (= [:union #{:utf8 :i64}]
           (types/merge-col-types :utf8 :i64)))

  (t/is (= [:union #{:utf8 :i64 :f64}]
           (types/merge-col-types [:union #{:utf8 :i64}] :f64)))

  (t/testing "merges list types"
    (t/is (= [:list :utf8]
             (types/merge-col-types [:list :utf8] [:list :utf8])))

    (t/is (= [:list [:union #{:utf8 :i64}]]
             (types/merge-col-types [:list :utf8] [:list :i64]))))

  (t/testing "merges struct types"
    (t/is (= '[:struct {a :utf8, b :utf8}]
             (types/merge-col-types '[:struct {a :utf8, b :utf8}]
                                    '[:struct {a :utf8, b :utf8}])))

    (t/is (= '[:struct {a :utf8
                        b [:union #{:utf8 :i64}]}]

             (types/merge-col-types '[:struct {a :utf8, b :utf8}]
                                    '[:struct {a :utf8, b :i64}])))

    (let [struct0 '[:struct {a :utf8, b :utf8}]
          struct1 '[:struct {b :utf8, c :i64}]]
      (t/is (= '[:struct {a [:union #{:utf8 :absent}]
                          b :utf8
                          c [:union #{:i64 :absent}]}]
               (types/merge-col-types struct0 struct1))))

    (t/is (= '[:union #{:f64 [:struct {a [:union #{:i64 :utf8}]}]}]
             (types/merge-col-types '[:union #{:f64, [:struct {a :i64}]}]
                                    '[:struct {a :utf8}])))))
