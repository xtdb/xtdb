(ns core2.monetdb-example-test
  (:refer-clojure :exclude [reverse])
  (:require [clojure.test :as t])
  (:import [org.apache.arrow.memory BufferAllocator RootAllocator]
           [org.apache.arrow.vector BigIntVector ElementAddressableVector ValueVector]
           [org.apache.arrow.vector.complex StructVector]
           org.apache.arrow.vector.types.pojo.FieldType
           [java.util ArrayList List]))

;; Follows Figure 4.1 in "The Design and Implementation of Modern
;; Column-Oriented Database Systems", Abadi

(set! *unchecked-math* :warn-on-boxed)

(defn- append-bigint-vector ^org.apache.arrow.vector.BigIntVector [^BigIntVector v ^long x]
  (let [idx (.getValueCount v)]
    (.setSafe v idx x)
    (.setValueCount v (inc idx))
    v))

(defn- populate-bigint-vector ^org.apache.arrow.vector.BigIntVector [^BigIntVector v values]
  (doseq [^long n values]
    (append-bigint-vector v n))
  v)

(defn- copy-vector [^ValueVector from ^ValueVector to]
  (let [value-count (.getValueCount from)]
    (dotimes [idx value-count]
      (.copyFromSafe to idx idx from))
    (.setValueCount to value-count)
    to))

(defn- ->list ^java.util.List [^ValueVector v]
  (let [acc (ArrayList.)]
    (dotimes [n (.getValueCount v)]
      (.add acc (.getObject v n)))
    acc))

(defn- head ^org.apache.arrow.vector.ValueVector [^StructVector v]
  (.getChild v "head"))

(defn- tail ^org.apache.arrow.vector.ValueVector [^StructVector v]
  (.getChild v "tail"))

(defn- ->bat ^org.apache.arrow.vector.complex.StructVector [^BufferAllocator a ^FieldType head-type ^FieldType tail-type]
  (doto (StructVector/empty "" a)
    (.addOrGet "head" head-type ValueVector)
    (.addOrGet "tail" tail-type ValueVector)))

(defn- ^org.apache.arrow.vector.types.pojo.FieldType field-type [^ValueVector v]
  (.getFieldType (.getField v)))

(defn- reconstruct ^org.apache.arrow.vector.complex.StructVector [^BufferAllocator a ^ValueVector v ^BigIntVector selection-vector]
  (let [reconstructed-struct (->bat a (field-type selection-vector) (field-type v))
        new-head (head reconstructed-struct)
        new-tail (tail reconstructed-struct)
        value-count (.getValueCount selection-vector)]
    (dotimes [idx value-count]
      (.copyFromSafe new-head idx idx selection-vector))
    (.setValueCount new-head value-count)
    (dotimes [idx value-count]
      (.copyFromSafe new-tail (dec (.get selection-vector idx)) idx v))
    (.setValueCount new-tail value-count)
    (.setValueCount reconstructed-struct value-count)
    reconstructed-struct))

;; NOTE: uses one-based indexes
(defn- select ^org.apache.arrow.vector.BigIntVector [^BufferAllocator a ^ValueVector v min-exclusive max-exclusive]
  (let [selection-vector (BigIntVector. "" a)]
    (cond
      (instance? BigIntVector v)
      (let [^long min-exclusive min-exclusive
            ^long max-exclusive max-exclusive]
        (dotimes [idx (.getValueCount v)]
          (when (< min-exclusive (.get ^BigIntVector v idx) max-exclusive)
            (append-bigint-vector selection-vector (inc idx)))))

      (instance? StructVector v)
      (with-open [^BigIntVector tail-selection-vector (select a (tail v) min-exclusive max-exclusive)]
        (let [value-count (.getValueCount tail-selection-vector)
              from-head (head v)]
          (dotimes [idx value-count]
            (.copyFromSafe selection-vector (dec (.get tail-selection-vector idx)) idx from-head))
          (.setValueCount selection-vector value-count)))

      :else
      (throw (UnsupportedOperationException.)))

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

        ^ElementAddressableVector right-head (head right)
        ^BigIntVector right-tail (tail right)

        join-struct (->bat a (field-type left-head) (field-type right-tail))
        join-head (head join-struct)
        join-tail (tail join-struct)]

    ;; NOTE: nested loop join.
    (dotimes [left-idx (.getValueCount left-tail)]
      (let [l (.getDataPointer left-tail left-idx)]
        (dotimes [right-idx (.getValueCount right-head)]
          (let [r (.getDataPointer right-head right-idx)]
            (when (= l r)
              (let [value-count (.getValueCount join-struct)]
                (.copyFromSafe join-head left-idx value-count left-head)
                (.copyFromSafe join-tail right-idx value-count right-tail)
                (.setValueCount join-struct (inc value-count))))))))

    join-struct))

(defn- void-tail ^org.apache.arrow.vector.ValueVector [^BufferAllocator a ^StructVector v]
  (copy-vector (.getChild v "head") (.createVector (.getField (.getChild v "head")) a)))

(defn- sum ^org.apache.arrow.vector.ValueVector [^BufferAllocator a ^ValueVector v]
  (cond
    (instance? StructVector v)
    (sum a (.getChild ^StructVector v "tail"))

    (instance? BigIntVector v)
    (let [^BigIntVector v v
          value-count (.getValueCount v)]
      (loop [acc 0
             idx 0]
        (if (= idx value-count)
          (append-bigint-vector (BigIntVector. "" a) acc)
          (recur (+ acc (.get v idx)) (inc idx)))))

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

    (with-open [^BigIntVector inter1 (select a ra 5 20)]
      (t/is (= [2 4 5 7 9] (->list inter1)))

      (with-open [inter2 (reconstruct a rb inter1)]
        (t/is (= [2 4 5 7 9] (->list (.getChild inter2 "head"))))
        (t/is (= [34 45 49 97 42] (->list (.getChild inter2 "tail"))))

        ;; NOTE: this range is different from figure and SQL.
        (with-open [^BigIntVector inter3 (select a inter2 40 50)]
          (t/is (= [4 5 9] (->list inter3)))

          (with-open [join-input-r (reconstruct a rc inter3)]
            (t/is (= [4 5 9] (->list (.getChild join-input-r "head"))))
            (t/is (= [23 78 29] (->list (.getChild join-input-r "tail"))))

            ;; NOTE: this range is different from figure and SQL, and
            ;; neither return the displayed indexes, which the below
            ;; does.
            (with-open [^BigIntVector inter4 (select a sa 49 65)]
              (t/is (= [3 5 7 8 10] (->list inter4)))

              (with-open [inter5 (reconstruct a sb inter4)]
                (t/is (= [3 5 7 8 10] (->list (.getChild inter5 "head"))))
                (t/is (= [62 29 19 81 23] (->list (.getChild inter5 "tail"))))

                (with-open [join-input-s (reverse a inter5)]
                  (t/is (= [62 29 19 81 23] (->list (.getChild join-input-s "head"))))
                  (t/is (= [3 5 7 8 10] (->list (.getChild join-input-s "tail"))))

                  (with-open [join-res-r-s (join a join-input-r join-input-s)]
                    (t/is (= [4 9] (->list (.getChild join-res-r-s "head"))))
                    (t/is (= [10 5] (->list (.getChild join-res-r-s "tail"))))

                    (with-open [inter6 (void-tail a join-res-r-s)]
                      (t/is (= [4 9] (->list inter6)))

                      ;; NOTE: in paper this is displayed as a single
                      ;; vector, not a pair.
                      (with-open [inter7 (reconstruct a ra inter6)]
                        (t/is (= [4 9] (->list (.getChild inter7 "head"))))
                        (t/is (= [9 19] (->list (.getChild inter7 "tail"))))

                        ;; NOTE: scalar is represented as a single
                        ;; element vector in paper.
                        (with-open [result (sum a inter7)]
                          (t/is (= [28] (->list result))))))))))))))))
