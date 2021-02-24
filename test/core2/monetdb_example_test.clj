(ns core2.monetdb-example-test
  (:require [clojure.test :as t]
            [core2.test-util :as tu])
  (:import [org.apache.arrow.memory BufferAllocator RootAllocator]
           org.apache.arrow.memory.util.ArrowBufPointer
           [org.apache.arrow.vector.util VectorBatchAppender]
           [org.apache.arrow.vector BaseIntVector BaseVariableWidthVector BigIntVector BitVector ElementAddressableVector
            FloatingPointVector Float8Vector TimeStampVector ValueVector]
           [org.apache.arrow.vector.complex StructVector]
           org.apache.arrow.vector.types.pojo.FieldType
           [java.util.function Function Predicate LongPredicate DoublePredicate]
           [java.util ArrayList List HashMap Map]))

;; Follows Figure 4.1 in "The Design and Implementation of Modern
;; Column-Oriented Database Systems", Abadi

(set! *unchecked-math* :warn-on-boxed)

(defn- append-bigint-vector ^org.apache.arrow.vector.BigIntVector [^BigIntVector v ^long x]
  (let [idx (.getValueCount v)]
    (.set v idx x)
    (.setValueCount v (inc idx))
    v))

(defn- ensure-capacity ^org.apache.arrow.vector.ValueVector [^ValueVector v ^long capacity]
  (doto v
    (.setInitialCapacity capacity)
    (.allocateNew)))

(defn- populate-bigint-vector ^org.apache.arrow.vector.BigIntVector [^BigIntVector v values]
  (ensure-capacity v (count values))
  (doseq [^long n values]
    (append-bigint-vector v n))
  v)

(defn- copy-vector ^org.apache.arrow.vector.ValueVector [^ValueVector from ^ValueVector to]
  (VectorBatchAppender/batchAppend to (into-array [from]))
  to)

(defn- head ^org.apache.arrow.vector.ValueVector [^StructVector v]
  (.getChildByOrdinal v 0))

(defn- tail ^org.apache.arrow.vector.ValueVector [^StructVector v]
  (.getChildByOrdinal v 1))

(defn- ->bat ^org.apache.arrow.vector.complex.StructVector [^BufferAllocator a ^FieldType head-type ^FieldType tail-type]
  (doto (StructVector/empty "" a)
    (.addOrGet "head" head-type ValueVector)
    (.addOrGet "tail" tail-type ValueVector)))

(defn- field-type ^org.apache.arrow.vector.types.pojo.FieldType [^ValueVector v]
  (.getFieldType (.getField v)))

(defn- reconstruct ^org.apache.arrow.vector.complex.StructVector [^BufferAllocator a ^ValueVector v ^BigIntVector selection-vector]
  (let [reconstructed-struct (->bat a (field-type selection-vector) (field-type v))
        value-count (.getValueCount selection-vector)
        new-head (head reconstructed-struct)
        new-tail (doto (tail reconstructed-struct)
                   (ensure-capacity value-count))]
    (copy-vector selection-vector new-head)
    (dotimes [idx value-count]
      (.copyFrom new-tail (dec (.get selection-vector idx)) idx v))
    (.setValueCount reconstructed-struct value-count)
    reconstructed-struct))

(definterface ArrowBufPointerPredicate
  (^boolean test [^org.apache.arrow.memory.util.ArrowBufPointer pointer]))

;; NOTE: uses one-based indexes
(defn- select ^org.apache.arrow.vector.BigIntVector [^BufferAllocator a ^ValueVector v pred]
  (let [value-count (.getValueCount v)
        selection-vector (doto (BigIntVector. "" a)
                           (ensure-capacity value-count))]
    (cond
      (and (instance? BaseIntVector v)
           (instance? LongPredicate pred))
      (dotimes [idx value-count]
        (when (.test ^LongPredicate pred (.getValueAsLong ^BaseIntVector v idx))
          (append-bigint-vector selection-vector (inc idx))))

      (and (instance? TimeStampVector v)
           (instance? LongPredicate pred))
      (dotimes [idx value-count]
        (when (.test ^LongPredicate pred (.get ^TimeStampVector v idx))
          (append-bigint-vector selection-vector (inc idx))))

      (and (instance? FloatingPointVector v)
           (instance? DoublePredicate pred))
      (dotimes [idx value-count]
        (when (.test ^DoublePredicate pred (.getValueAsDouble ^FloatingPointVector v idx))
          (append-bigint-vector selection-vector (inc idx))))

      (instance? StructVector v)
      (with-open [tail-selection-vector (select a (tail v) pred)]
        (let [value-count (.getValueCount tail-selection-vector)
              from-head (head v)]
          (dotimes [idx value-count]
            (.copyFrom selection-vector (dec (.get tail-selection-vector idx)) idx from-head))
          (.setValueCount selection-vector value-count)))

      (and (instance? ElementAddressableVector v)
           (instance? ArrowBufPointerPredicate v))
      (let [pointer (ArrowBufPointer.)]
        (dotimes [idx value-count]
          (.getDataPointer ^ElementAddressableVector v idx pointer)
          (when (.test ^ArrowBufPointerPredicate pred pointer)
            (append-bigint-vector selection-vector (inc idx)))))

      (instance? Predicate v)
      (dotimes [idx value-count]
        (when (.test ^Predicate pred (.getObject v idx))
          (append-bigint-vector selection-vector (inc idx))))

      :else
      (dotimes [idx value-count]
        (when (pred (.getObject v idx))
          (append-bigint-vector selection-vector (inc idx)))))

    selection-vector))

(defn- reverse-vec ^org.apache.arrow.vector.complex.StructVector [^BufferAllocator a ^StructVector v]
  (let [old-head (head v)
        old-tail (tail v)
        reversed-struct (->bat a (field-type old-tail) (field-type old-head))]
    (copy-vector old-tail (head reversed-struct))
    (copy-vector old-head (tail reversed-struct))
    (.setValueCount reversed-struct (.getValueCount v))
    reversed-struct))

(defn- hash-join-build ^java.util.Map [^BufferAllocator a ^ValueVector build-indexes ^ElementAddressableVector build-values]
  (let [join-map (HashMap.)]
    (dotimes [idx (.getValueCount build-values)]
      (let [pointer (.getDataPointer build-values idx)
            ^BigIntVector hash-vec (.computeIfAbsent join-map pointer (reify Function
                                                                        (apply [_ k]
                                                                          (BigIntVector. "" a))))
            value-count (.getValueCount hash-vec)]
        (.copyFromSafe hash-vec idx value-count build-indexes)
        (.setValueCount hash-vec (inc value-count))))
    join-map))

(defn- hash-join-probe ^org.apache.arrow.vector.complex.StructVector [^BufferAllocator a ^Map join-map ^ValueVector probe-indexes ^ElementAddressableVector probe-values]
  (let [join-struct (->bat a (field-type probe-indexes) (field-type probe-indexes))

        join-head (head join-struct)
        join-tail (tail join-struct)

        pointer (ArrowBufPointer.)]
    (dotimes [probe-idx (.getValueCount probe-values)]
      (.getDataPointer probe-values probe-idx pointer)
      (when-let [^BigIntVector build-head (.get join-map pointer)]
        (dotimes [build-idx (.getValueCount build-head)]
          (let [value-count (.getValueCount join-struct)]
            (.copyFromSafe join-head probe-idx value-count probe-indexes)
            (.copyFromSafe join-tail build-idx value-count build-head)
            (.setValueCount join-struct (inc value-count))))))

    join-struct))

(defn- hash-join ^org.apache.arrow.vector.complex.StructVector [^BufferAllocator a ^StructVector left ^StructVector right]
  (let [join-map (hash-join-build a (tail right) (head right))]
    (try
      (hash-join-probe a join-map (head left) (tail left))
      (finally
        (doseq [^BigIntVector hash-vec (vals join-map)]
          (.close hash-vec))))))

(defn- nested-loop-join ^org.apache.arrow.vector.complex.StructVector [^BufferAllocator a ^StructVector left ^StructVector right]
  (let [left-head (head left)
        ^ElementAddressableVector left-tail (tail left)
        left-pointer (ArrowBufPointer.)

        ^ElementAddressableVector right-head (head right)
        right-tail (tail right)
        right-pointer (ArrowBufPointer.)

        join-struct (->bat a (field-type left-head) (field-type right-tail))
        join-head (head join-struct)
        join-tail (tail join-struct)]

    ;; NOTE: nested loop join.
    (dotimes [left-idx (.getValueCount left-tail)]
      (.getDataPointer left-tail left-idx left-pointer)
      (dotimes [right-idx (.getValueCount right-head)]
        (.getDataPointer right-head right-idx right-pointer)
        (when (= left-pointer right-pointer)
          (let [value-count (.getValueCount join-struct)]
            (.copyFromSafe join-head left-idx value-count left-head)
            (.copyFromSafe join-tail right-idx value-count right-tail)
            (.setValueCount join-struct (inc value-count))))))

    join-struct))

(defn- join ^org.apache.arrow.vector.complex.StructVector [^BufferAllocator a ^StructVector left ^StructVector right]
  (hash-join a left right))

(defn- void-tail ^org.apache.arrow.vector.ValueVector [^BufferAllocator a ^StructVector v]
  (copy-vector (head v) (.createVector (.getField (head v)) a)))

(defmacro ^:private scalar-fixed-width-vec [v value]
  `(doto ~v
     (ensure-capacity 1)
     (.set 0 ~value)
     (.setValueCount 1)))

(defmacro ^:private reduce-fixed-width-vec [v ret reduce-fn access-fn]
  `(let [v# ~v
         ret# ~ret
         value-count# (.getValueCount v#)]
     (if (zero? value-count#)
       ret#
       (loop [acc# (~access-fn v# (int 0))
              idx# (int 1)]
         (if (= idx# value-count#)
           (scalar-fixed-width-vec ret# acc#)
           (recur (~reduce-fn acc# (~access-fn v# idx#)) (inc idx#)))))))

(defmacro ^:private scalar-variable-width-vec [v value]
  `(let [value# ~value]
     (doto ~v
       (ensure-capacity 1)
       (.set 0 (.getOffset value#) (.getLength value#) (.getBuf value#))
       (.setValueCount 1))))

(defmacro ^:private min-max-variable-width-vec [v ret diff-fn]
  `(let [v# ~v
         ret# ~ret
         value-count# (.getValueCount v#)]
     (if (zero? value-count#)
       ret#
       (loop [acc# (.getDataPointer v# (int 0))
              pointer# (ArrowBufPointer.)
              idx# (int 1)]
        (if (= idx# value-count#)
          (scalar-variable-width-vec ret# acc#)
          (do (.getDataPointer v# idx# pointer#)
              (if (~diff-fn (.compareTo acc# pointer#))
                (recur acc#
                       pointer#
                       (inc idx#))
                (recur pointer#
                       acc#
                       (inc idx#)))))))))

(defn- ^org.apache.arrow.vector.BigIntVector count-vec [^BufferAllocator a ^ValueVector v]
  (scalar-fixed-width-vec (BigIntVector. "" a) (.getValueCount v)))

(defn- sum-vec ^org.apache.arrow.vector.ValueVector [^BufferAllocator a ^ValueVector v]
  (cond
    (instance? StructVector v)
    (sum-vec a (tail v))

    (instance? BaseIntVector v)
    (reduce-fixed-width-vec ^BaseIntVector v (BigIntVector. "" a) + .getValueAsLong)

    (instance? FloatingPointVector v)
    (reduce-fixed-width-vec ^FloatingPointVector v (Float8Vector. "" a) + .getValueAsDouble)

    :else
    (throw (UnsupportedOperationException.))))

(defn- min-vec ^org.apache.arrow.vector.ValueVector [^BufferAllocator a ^ValueVector v]
  (cond
    (instance? StructVector v)
    (min-vec a (tail v))

    (instance? BitVector v)
    (reduce-fixed-width-vec ^BitVector v (BitVector. "" a) min .get)

    (instance? BaseIntVector v)
    (reduce-fixed-width-vec ^BaseIntVector v (BigIntVector. "" a) min .getValueAsLong)

    (instance? FloatingPointVector v)
    (reduce-fixed-width-vec ^FloatingPointVector v (Float8Vector. "" a) min .getValueAsDouble)

    (instance? TimeStampVector v)
    (reduce-fixed-width-vec ^TimeStampVector v ^TimeStampVector (.createVector (.getField v) a) min .get)

    (instance? BaseVariableWidthVector v)
    (min-max-variable-width-vec ^BaseVariableWidthVector v ^BaseVariableWidthVector (.createVector (.getField v) a) neg?)

    :else
    (throw (UnsupportedOperationException.))))

(defn- max-vec ^org.apache.arrow.vector.ValueVector [^BufferAllocator a ^ValueVector v]
  (cond
    (instance? StructVector v)
    (max-vec a (tail v))

    (instance? BitVector v)
    (reduce-fixed-width-vec ^BitVector v (BitVector. "" a) max .get)

    (instance? BaseIntVector v)
    (reduce-fixed-width-vec ^BaseIntVector v (BigIntVector. "" a) max .getValueAsLong)

    (instance? FloatingPointVector v)
    (reduce-fixed-width-vec ^FloatingPointVector v (Float8Vector. "" a) max .getValueAsDouble)

    (instance? TimeStampVector v)
    (reduce-fixed-width-vec ^TimeStampVector v ^TimeStampVector (.createVector (.getField v) a) max .get)

    (instance? BaseVariableWidthVector v)
    (min-max-variable-width-vec ^BaseVariableWidthVector v ^BaseVariableWidthVector (.createVector (.getField v) a) pos?)

    :else
    (throw (UnsupportedOperationException.))))


;; Corresponding SQL, note that the last range is off compared to the
;; selected indexes in the figure, so we use 49 < S.a < 65

;; select sum(R.a) from R, S where R.c = S.b and 5 < R.a < 20 and 40 <
;; R.b < 50 and 30 < S.a < 40

(t/deftest mal-algebra-abadi-figure-4_1-example
  (with-open [a (RootAllocator.)
              ra (BigIntVector. "r.a" a)
              rb (BigIntVector. "r.b" a)
              rc (BigIntVector. "r.c" a)
              sa (BigIntVector. "s.a" a)
              sb (BigIntVector. "s.b" a)]

    (populate-bigint-vector ra [ 3 16 56  9 11 27  8 41 19 35])
    (populate-bigint-vector rb [12 34 75 45 49 58 97 75 42 55])
    (populate-bigint-vector rc [12 34 53 23 78 65 33 21 29  0])

    (populate-bigint-vector sa [17 49 58 99 64 37 53 61 32 50])
    (populate-bigint-vector sb [11 35 62 44 29 78 19 81 26 23])

    (doseq [^ValueVector v [ra rb rc sa sb]]
      (t/is (= 10 (.getValueCount v))))

    (with-open [^BigIntVector inter1 (select a ra (reify LongPredicate
                                                    (test [_ x]
                                                      (< 5 x 20))))]
      (t/is (= [2 4 5 7 9] (tu/->list inter1)))

      (with-open [inter2 (reconstruct a rb inter1)]
        (t/is (= [2 4 5 7 9] (tu/->list (head inter2))))
        (t/is (= [34 45 49 97 42] (tu/->list (tail inter2))))

        ;; NOTE: this range is different from figure and SQL.
        (with-open [^BigIntVector inter3 (select a inter2 (reify LongPredicate
                                                            (test [_ x]
                                                              (< 40 x 50))))]
          (t/is (= [4 5 9] (tu/->list inter3)))

          (with-open [join-input-r (reconstruct a rc inter3)]
            (t/is (= [4 5 9] (tu/->list (head join-input-r))))
            (t/is (= [23 78 29] (tu/->list (tail join-input-r ))))

            ;; NOTE: this range is different from figure and SQL, and
            ;; neither return the displayed indexes, which the below
            ;; does.
            (with-open [^BigIntVector inter4 (select a sa (reify LongPredicate
                                                            (test [_ x]
                                                              (< 49 x 65))))]
              (t/is (= [3 5 7 8 10] (tu/->list inter4)))

              (with-open [inter5 (reconstruct a sb inter4)]
                (t/is (= [3 5 7 8 10] (tu/->list (head inter5))))
                (t/is (= [62 29 19 81 23] (tu/->list (tail inter5))))

                (with-open [join-input-s (reverse-vec a inter5)]
                  (t/is (= [62 29 19 81 23] (tu/->list (head join-input-s))))
                  (t/is (= [3 5 7 8 10] (tu/->list (tail join-input-s))))

                  (with-open [join-res-r-s (join a join-input-r join-input-s)]
                    (t/is (= [4 9] (tu/->list (head join-res-r-s))))
                    (t/is (= [10 5] (tu/->list (tail join-res-r-s))))

                    (with-open [inter6 (void-tail a join-res-r-s)]
                      (t/is (= [4 9] (tu/->list inter6)))

                      ;; NOTE: in paper this is displayed as a single
                      ;; vector, not a pair.
                      (with-open [inter7 (reconstruct a ra inter6)]
                        (t/is (= [4 9] (tu/->list (head inter7))))
                        (t/is (= [9 19] (tu/->list (tail inter7))))

                        ;; NOTE: scalar is represented as a single
                        ;; element vector in paper.
                        (with-open [result (sum-vec a inter7)]
                          (t/is (= [28] (tu/->list result))))

                        ;; NOTE: other aggregations, not in example.
                        (with-open [result (count-vec a inter7)]
                          (t/is (= [2] (tu/->list result))))

                        (with-open [result (min-vec a inter7)]
                          (t/is (= [9] (tu/->list result))))

                        (with-open [result (max-vec a inter7)]
                          (t/is (= [19] (tu/->list result))))))))))))))))
