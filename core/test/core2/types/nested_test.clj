(ns core2.types.nested-test
  (:require [clojure.test :as t]
            [clojure.spec.alpha :as s]
            [core2.types.nested :as tn]
            [core2.util :as util])
  (:import [org.apache.arrow.vector.complex UnionVector]
           [org.apache.arrow.vector.util Text]
           [org.apache.arrow.vector VectorLoader VectorSchemaRoot]
           [org.apache.arrow.memory RootAllocator]
           [java.time Duration LocalDate LocalTime]
           [java.nio ByteBuffer]))

(t/deftest can-build-sparse-union-vector
  (with-open [allocator (RootAllocator.)
              v (UnionVector/empty "" allocator)
              writer (.getWriter v)]
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
               #inst "1999"
               (.toInstant #inst "2021-09-02T13:54:35.809Z")
               (.plusDays (Duration/ofMillis 1234) 1)
               (LocalDate/of 1999 05 01)
               (LocalTime/of 14 05 10)
               (byte-array [1 2 3])
               (ByteBuffer/wrap (byte-array [1 2 3]))
               []
               [2 3.14 [false nil]]
               {}
               {:B 2 :C 1 :F false}
               [1 {:B [2]}]
               ;; NOTE: should contain :F
               {:B 3.14 :D {:E ["hello" -1]} :F nil}]]
      (tn/append-value x writer allocator))
    (.setValueCount v (.getPosition writer))

    (t/testing "nested data"
      (t/is (= [false, nil, 2, 1, 6, 4, 3.14, 2.0, (Text. "Hello"), (Text. "F"), (util/date->local-date-time #inst "1999-01-01T00:00"), (util/date->local-date-time #inst "2021-09-02T13:54:35.809Z"), (.plusDays (Duration/ofMillis 1234) 1), (util/date->local-date-time #inst "1999-05-01"), 50710000000, [1, 2, 3], [1, 2, 3], [], [2,3.14,[false,nil]], {}, {"B" 2,"C" 1,"F" false}, [1,{"B" [2]}], {"B" 3.14,"D" {"E" [(Text. "hello"),-1]}}]
               (for [x (range (.getValueCount v))
                     :let [v (.getObject v (long x))]]
                 (if (bytes? v)
                   (vec v)
                   v)))))

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
                org.apache.arrow.vector.VarCharVector
                org.apache.arrow.vector.TimeStampMicroVector
                org.apache.arrow.vector.TimeStampMicroVector
                org.apache.arrow.vector.IntervalDayVector
                org.apache.arrow.vector.DateMilliVector
                org.apache.arrow.vector.TimeMicroVector
                org.apache.arrow.vector.VarBinaryVector
                org.apache.arrow.vector.VarBinaryVector
                org.apache.arrow.vector.complex.ListVector
                org.apache.arrow.vector.complex.ListVector
                org.apache.arrow.vector.complex.StructVector
                org.apache.arrow.vector.complex.StructVector
                org.apache.arrow.vector.complex.ListVector
                org.apache.arrow.vector.complex.StructVector]
               (for [x (range (.getValueCount v))]
                 (class (.getVector v (long x)))))))

    (t/testing "sparse to dense union"
      (with-open [duv (tn/sparse->dense-union v allocator)]
        (t/is (= (for [x (range (.getValueCount v))
                       :let [v (.getObject v (long x))]]
                   (if (bytes? v)
                     (vec v)
                     v))
                 (for [x (range (.getValueCount duv))
                       :let [v (.getObject duv (long x))]]
                   (if (bytes? v)
                     (vec v)
                     v))))
        (t/is (.getField duv))

        (t/is (< (.capacity (util/root->arrow-ipc-byte-buffer (VectorSchemaRoot/of (into-array [duv])) :file))
                 (.capacity (util/root->arrow-ipc-byte-buffer (VectorSchemaRoot/of (into-array [v])) :file))))

        (t/testing "IPC round-trip"
          (with-open [in-root (VectorSchemaRoot/of (into-array [duv]))
                      buf (util/->arrow-buf-view allocator (util/root->arrow-ipc-byte-buffer in-root :file))]
            (let [footer (util/read-arrow-footer buf)]
              (t/is (= (.getField duv) (first (.getFields (.getSchema footer)))))
              (with-open [record-batch (util/->arrow-record-batch-view (first (.getRecordBatches footer)) buf)
                          out-root (VectorSchemaRoot/create (.getSchema footer) allocator)]
                (.load (VectorLoader. out-root) record-batch)
                (let [reloaded-duv (.getVector out-root 0)]
                  (t/is (= (.getField duv) (.getField reloaded-duv)))
                  (t/is (= (for [x (range (.getValueCount v))
                                 :let [v (.getObject v (long x))]]
                             (if (bytes? v)
                               (vec v)
                               v))
                           (for [x (range (.getValueCount reloaded-duv))
                                 :let [v (.getObject reloaded-duv (long x))]]
                             (if (bytes? v)
                               (vec v)
                               v)))))))))))))
