(ns core2.monetdb-example-test
  (:refer-clojure :exclude [reverse])
  (:require [clojure.test :as t])
  (:import [org.apache.arrow.memory BufferAllocator RootAllocator]
           org.apache.arrow.memory.util.ArrowBufPointer
           [org.apache.arrow.vector.util VectorBatchAppender]
           [org.apache.arrow.vector BaseIntVector BigIntVector ElementAddressableVector FloatingPointVector Float8Vector ValueVector]
           [org.apache.arrow.vector.complex StructVector]
           org.apache.arrow.vector.types.pojo.FieldType
           [java.util.function Predicate IntPredicate LongPredicate DoublePredicate]
           [java.util ArrayList List]))

;; Follows Figure 4.1 in "The Design and Implementation of Modern
;; Column-Oriented Database Systems", Abadi

(set! *unchecked-math* :warn-on-boxed)

(defn- append-bigint-vector ^org.apache.arrow.vector.BigIntVector [^BigIntVector v ^long x]
  (let [idx (.getValueCount v)]
    (.set v idx x)
    (.setValueCount v (inc idx))
    v))

(defn- ensure-capacity [^ValueVector v ^long capacity]
  (.setInitialCapacity v capacity)
  (.allocateNew v))

(defn- populate-bigint-vector ^org.apache.arrow.vector.BigIntVector [^BigIntVector v values]
  (ensure-capacity v (count values))
  (doseq [^long n values]
    (append-bigint-vector v n))
  v)

(defn- copy-vector [^ValueVector from ^ValueVector to]
  (VectorBatchAppender/batchAppend to (into-array [from]))
  to)

(defn- ->list ^java.util.List [^ValueVector v]
  (let [acc (ArrayList.)]
    (dotimes [n (.getValueCount v)]
      (.add acc (.getObject v n)))
    acc))

(defn- head ^org.apache.arrow.vector.ValueVector [^StructVector v]
  (.getChildByOrdinal v 0))

(defn- tail ^org.apache.arrow.vector.ValueVector [^StructVector v]
  (.getChildByOrdinal v 1))

(defn- ->bat ^org.apache.arrow.vector.complex.StructVector [^BufferAllocator a ^FieldType head-type ^FieldType tail-type]
  (doto (StructVector/empty "" a)
    (.addOrGet "head" head-type ValueVector)
    (.addOrGet "tail" tail-type ValueVector)))

(defn- ^org.apache.arrow.vector.types.pojo.FieldType field-type [^ValueVector v]
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

      (and (instance? FloatingPointVector v)
           (instance? DoublePredicate pred))
      (dotimes [idx value-count]
        (when (.test ^DoublePredicate pred (.getValueAsDouble ^FloatingPointVector v idx))
          (append-bigint-vector selection-vector (inc idx))))

      (instance? StructVector v)
      (with-open [^BigIntVector tail-selection-vector (select a (tail v) pred)]
        (let [value-count (.getValueCount tail-selection-vector)
              from-head (head v)]
          (dotimes [idx value-count]
            (.copyFrom selection-vector (dec (.get tail-selection-vector idx)) idx from-head))
          (.setValueCount selection-vector value-count)))

      (instance? Predicate v)
      (dotimes [idx value-count]
        (when (.test ^Predicate pred (.getObject v idx))
          (append-bigint-vector selection-vector (inc idx))))

      :else
      (dotimes [idx value-count]
        (when (pred (.getObject v idx))
          (append-bigint-vector selection-vector (inc idx)))))

    selection-vector))

(defn- reverse ^org.apache.arrow.vector.complex.StructVector [^BufferAllocator a ^StructVector v]
  (let [old-head (head v)
        old-tail (tail v)
        reversed-struct (->bat a (field-type old-tail) (field-type old-head))]
    (copy-vector old-tail (head reversed-struct))
    (copy-vector old-head (tail reversed-struct))
    (.setValueCount reversed-struct (.getValueCount v))
    reversed-struct))

(defn- join ^org.apache.arrow.vector.complex.StructVector [^BufferAllocator a ^StructVector left ^StructVector right]
  (let [^BigIntVector left-head (head left)
        ^ElementAddressableVector left-tail (tail left)
        left-pointer (ArrowBufPointer.)

        ^ElementAddressableVector right-head (head right)
        ^BigIntVector right-tail (tail right)
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

(defn- void-tail ^org.apache.arrow.vector.ValueVector [^BufferAllocator a ^StructVector v]
  (copy-vector (head v) (.createVector (.getField (head v)) a)))

(defn- sum ^org.apache.arrow.vector.ValueVector [^BufferAllocator a ^ValueVector v]
  (cond
    (instance? StructVector v)
    (sum a (tail v))

    (instance? BaseIntVector v)
    (let [^BaseIntVector v v
          value-count (.getValueCount v)]
      (loop [acc 0
             idx 0]
        (if (= idx value-count)
          (doto (BigIntVector. "" a)
            (ensure-capacity 1)
            (.set 0 acc)
            (.setValueCount 1))
          (recur (+ acc (.getValueAsLong v idx)) (inc idx)))))

    (instance? FloatingPointVector v)
    (let [^FloatingPointVector v v
          value-count (.getValueCount v)]
      (loop [acc 0.0
             idx 0]
        (if (= idx value-count)
          (doto (Float8Vector. "" a)
            (ensure-capacity 1)
            (.set 0 acc)
            (.setValueCount 1))
          (recur (+ acc (.getValueAsDouble v idx)) (inc idx)))))

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
      (t/is (= [2 4 5 7 9] (->list inter1)))

      (with-open [inter2 (reconstruct a rb inter1)]
        (t/is (= [2 4 5 7 9] (->list (head inter2))))
        (t/is (= [34 45 49 97 42] (->list (tail inter2))))

        ;; NOTE: this range is different from figure and SQL.
        (with-open [^BigIntVector inter3 (select a inter2 (reify LongPredicate
                                                            (test [_ x]
                                                              (< 40 x 50))))]
          (t/is (= [4 5 9] (->list inter3)))

          (with-open [join-input-r (reconstruct a rc inter3)]
            (t/is (= [4 5 9] (->list (head join-input-r))))
            (t/is (= [23 78 29] (->list (tail join-input-r ))))

            ;; NOTE: this range is different from figure and SQL, and
            ;; neither return the displayed indexes, which the below
            ;; does.
            (with-open [^BigIntVector inter4 (select a sa (reify LongPredicate
                                                            (test [_ x]
                                                              (< 49 x 65))))]
              (t/is (= [3 5 7 8 10] (->list inter4)))

              (with-open [inter5 (reconstruct a sb inter4)]
                (t/is (= [3 5 7 8 10] (->list (head inter5))))
                (t/is (= [62 29 19 81 23] (->list (tail inter5))))

                (with-open [join-input-s (reverse a inter5)]
                  (t/is (= [62 29 19 81 23] (->list (head join-input-s))))
                  (t/is (= [3 5 7 8 10] (->list (tail join-input-s))))

                  (with-open [join-res-r-s (join a join-input-r join-input-s)]
                    (t/is (= [4 9] (->list (head join-res-r-s))))
                    (t/is (= [10 5] (->list (tail join-res-r-s))))

                    (with-open [inter6 (void-tail a join-res-r-s)]
                      (t/is (= [4 9] (->list inter6)))

                      ;; NOTE: in paper this is displayed as a single
                      ;; vector, not a pair.
                      (with-open [inter7 (reconstruct a ra inter6)]
                        (t/is (= [4 9] (->list (head inter7))))
                        (t/is (= [9 19] (->list (tail inter7))))

                        ;; NOTE: scalar is represented as a single
                        ;; element vector in paper.
                        (with-open [result (sum a inter7)]
                          (t/is (= [28] (->list result))))))))))))))))
