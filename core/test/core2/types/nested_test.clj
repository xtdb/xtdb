(ns core2.types.nested-test
  (:require [clojure.test :as t]
            [clojure.spec.alpha :as s]
            [core2.types.nested :as tn]
            [core2.util :as util])
  (:import [org.apache.arrow.vector.complex DenseUnionVector]
           [org.apache.arrow.vector.util Text VectorBatchAppender]
           [org.apache.arrow.vector VectorLoader VectorSchemaRoot]
           [org.apache.arrow.vector.types.pojo Schema]
           [org.apache.arrow.memory RootAllocator]
           [java.time Duration LocalDate LocalTime ZonedDateTime ZoneId]
           [java.nio ByteBuffer]))

(t/deftest can-build-sparse-union-vector
  (with-open [allocator (RootAllocator.)
              v (DenseUnionVector/empty "" allocator)]

    (doseq [x [false
               nil
               (long 2)
               (byte 1)
               (short 6)
               (int 4)
               (double 3.14)
               (float 2)
               "Hello"
               \F
               1.41M
               2.718281828459045M
               #inst "1999"
               (.toInstant #inst "2021-09-02T13:54:35.809Z")
               (ZonedDateTime/ofInstant (.toInstant #inst "2021-09-02T13:54:35.809Z") (ZoneId/of "Europe/Stockholm"))
               (.plusDays (Duration/ofMillis 1234) 1)
               (LocalDate/of 1999 05 01)
               (LocalTime/of 14 05 10)
               (byte-array [1 2 3])
               (ByteBuffer/wrap (byte-array [1 2 3]))
               :foo/bar
               'foo/baz
               #uuid "7b9f641c-254d-4d9a-a5d9-be8bac9fb0fd"
               []
               [2 3.14 [false nil]]
               #{3 "foo"}
               '(foo 10)
               {}
               {:B 2 :C 1 :F false}
               [1 {:B [2]}]
               {:B 3.14 :D {:E ["hello" -1]} :F nil}]]
      (tn/append-value x v))

    (t/is (.getField v))

    (t/testing "nested data"
      (t/is (= [false, nil, 2, 1, 6, 4, 3.14, 2.0, "Hello", \F, 1.41M, 2.718281828459045M, (.toInstant #inst "1999-01-01T00:00"), (.toInstant #inst "2021-09-02T13:54:35.809Z"), (ZonedDateTime/ofInstant (.toInstant #inst "2021-09-02T13:54:35.809Z") (ZoneId/of "Europe/Stockholm")), (.plusDays (Duration/ofMillis 1234) 1), (LocalDate/of 1999 05 01), (LocalTime/of 14 05 10), (ByteBuffer/wrap (byte-array [1, 2, 3])), (ByteBuffer/wrap (byte-array [1, 2, 3])), :foo/bar, 'foo/baz, #uuid "7b9f641c-254d-4d9a-a5d9-be8bac9fb0fd", [], [2, 3.14, [false, nil]], #{3 "foo"}, '(foo 10) {}, {:B 2, :C 1, :F false}, [1, {:B [2]}], {:B 3.14, :D {:E ["hello", -1]} :F nil}]
               (for [x (range (.getValueCount v))]
                 (tn/get-value v x)))))

    (t/testing "types"
      (t/is (= [org.apache.arrow.vector.BitVector
                nil
                org.apache.arrow.vector.BigIntVector
                org.apache.arrow.vector.TinyIntVector
                org.apache.arrow.vector.SmallIntVector
                org.apache.arrow.vector.IntVector
                org.apache.arrow.vector.Float8Vector
                org.apache.arrow.vector.Float4Vector
                org.apache.arrow.vector.VarCharVector
                org.apache.arrow.vector.UInt2Vector
                org.apache.arrow.vector.DecimalVector
                org.apache.arrow.vector.DecimalVector
                org.apache.arrow.vector.TimeStampMicroVector
                org.apache.arrow.vector.TimeStampMicroVector
                org.apache.arrow.vector.TimeStampMicroTZVector
                org.apache.arrow.vector.DurationVector
                org.apache.arrow.vector.DateMilliVector
                org.apache.arrow.vector.TimeMicroVector
                org.apache.arrow.vector.VarBinaryVector
                org.apache.arrow.vector.VarBinaryVector
                org.apache.arrow.vector.VarCharVector
                org.apache.arrow.vector.VarCharVector
                org.apache.arrow.vector.FixedSizeBinaryVector
                org.apache.arrow.vector.complex.ListVector
                org.apache.arrow.vector.complex.ListVector
                org.apache.arrow.vector.complex.ListVector
                org.apache.arrow.vector.complex.ListVector
                org.apache.arrow.vector.complex.StructVector
                org.apache.arrow.vector.complex.StructVector
                org.apache.arrow.vector.complex.ListVector
                org.apache.arrow.vector.complex.StructVector]
               (for [x (range (.getValueCount v))]
                 (class (.getVectorByType v (.getTypeId v (long x))))))))

    (t/testing "IPC round-trip"
      (with-open [in-root (VectorSchemaRoot/of (into-array [v]))
                  buf (util/->arrow-buf-view allocator (util/root->arrow-ipc-byte-buffer in-root :file))]
        (let [footer (util/read-arrow-footer buf)]
          (t/is (= (.getField v) (first (.getFields (.getSchema footer)))))
          (with-open [record-batch (util/->arrow-record-batch-view (first (.getRecordBatches footer)) buf)
                      out-root (VectorSchemaRoot/create (.getSchema footer) allocator)]
            (.load (VectorLoader. out-root) record-batch)
            (let [reloaded-duv (.getVector out-root 0)]
              (t/is (= (.getField v) (.getField reloaded-duv)))
              (t/is (= (for [x (range (.getValueCount v))]
                         (tn/get-value v x))
                       (for [x (range (.getValueCount reloaded-duv))]
                         (tn/get-value reloaded-duv x)))))))))))
