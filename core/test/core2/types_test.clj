(ns core2.types-test
  (:require [clojure.test :as t]
            [core2.test-util :as tu]
            [core2.types :as types]
            [core2.util :as util]
            [core2.vector.writer :as vw])
  (:import java.nio.ByteBuffer
           [java.time Instant OffsetDateTime ZonedDateTime ZoneId ZoneOffset]
           [org.apache.arrow.vector BigIntVector BitVector Float4Vector Float8Vector IntVector NullVector SmallIntVector TimeStampMicroTZVector TinyIntVector VarBinaryVector VarCharVector]
           [org.apache.arrow.vector.complex DenseUnionVector ListVector StructVector]))

(t/use-fixtures :each tu/with-allocator)

(t/deftest round-trips-values
  (letfn [(test-round-trip [vs]
            (with-open [duv (DenseUnionVector/empty "" tu/*allocator*)]
              (let [duv-writer (.asDenseUnion (vw/vec->writer duv))]
                (doseq [v vs]
                  (.startValue duv-writer)
                  (doto (.writerForType duv-writer (types/value->leg-type v))
                    (.startValue)
                    (->> (types/write-value! v))
                    (.endValue))
                  (.endValue duv-writer)))

              {:vs (vec (for [idx (range (count vs))]
                          (types/get-object duv idx)))
               :vec-types (vec (for [idx (range (count vs))]
                                 (class (.getVectorByType duv (.getTypeId duv idx)))))}))]

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
            "nested types"))))
