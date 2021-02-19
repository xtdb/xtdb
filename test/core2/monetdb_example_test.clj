(ns core2.monetdb-example-test
  (:refer-clojure :exclude [reverse])
  (:require [clojure.test :as t])
  (:import [org.apache.arrow.memory BufferAllocator RootAllocator]
           [org.apache.arrow.vector BigIntVector ValueVector]
           [org.apache.arrow.vector.complex StructVector]
           [java.util ArrayList List]))

;; Follows Figure 4.1 in "The Design and Implementation of Modern
;; Column-Oriented Database Systems", Abadi

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

(defn- reconstruct ^org.apache.arrow.vector.complex.StructVector [^BufferAllocator a ^BigIntVector v ^BigIntVector selection-vector]
  (let [reconstructed-struct (StructVector/empty "" a)
        value-count (.getValueCount selection-vector)
        head (.addOrGet reconstructed-struct "head" (.getFieldType (.getField selection-vector)) ValueVector)
        tail (.addOrGet reconstructed-struct "tail" (.getFieldType (.getField v)) ValueVector)]
    (dotimes [idx value-count]
      (.copyFromSafe head idx idx selection-vector))
    (.setValueCount head value-count)
    (dotimes [idx (.getValueCount selection-vector)]
      (append-bigint-vector tail (.get v (dec (.get selection-vector idx)))))
    (.setValueCount tail value-count)
    (.setValueCount reconstructed-struct value-count)
    reconstructed-struct))

;; NOTE: uses one-based indexes
(defn- select ^org.apache.arrow.vector.BigIntVector [^BufferAllocator a ^ValueVector v ^long min ^long max]
  (let [selection-vector (BigIntVector. "" a)]
    (cond
      (instance? BigIntVector v)
      (dotimes [idx (.getValueCount v)]
        (when (< min (.get ^BigIntVector v idx) max)
          (append-bigint-vector selection-vector (inc idx))))

      (instance? StructVector v)
      (let [^StructVector v v
            ^BigIntVector head (.getChild v "head")
            ^BigIntVector tail (.getChild v "tail")]
        (with-open [^BigIntVector tail-selection-vector (select a tail min max)]
          (dotimes [idx (.getValueCount tail-selection-vector)]
            (append-bigint-vector selection-vector (.get head (dec (.get tail-selection-vector idx)))))))

      :else
      (throw (UnsupportedOperationException.)))

    selection-vector))

(defn- reverse ^org.apache.arrow.vector.complex.StructVector [^BufferAllocator a ^StructVector v]
  (let [reversed-struct (StructVector/empty "" a)
        head (.getChild v "head")
        tail (.getChild v "tail")
        new-head (.addOrGet reversed-struct "head" (.getFieldType (.getField head)) ValueVector)
        new-tail (.addOrGet reversed-struct "tail" (.getFieldType (.getField tail)) ValueVector)]
    (copy-vector tail new-head)
    (copy-vector head new-tail)
    (.setValueCount reversed-struct (.getValueCount v))
    reversed-struct))

(defn- join ^org.apache.arrow.vector.complex.StructVector [^BufferAllocator a ^StructVector left ^StructVector right]
  (let [join-struct (StructVector/empty "" a)
        ^BigIntVector left-head (.getChild left "head")
        ^BigIntVector left-tail (.getChild left "tail")

        ^BigIntVector right-head (.getChild right "head")
        ^BigIntVector right-tail (.getChild right "tail")

        join-head (.addOrGet join-struct "head" (.getFieldType (.getField left-head)) ValueVector)
        join-tail (.addOrGet join-struct "tail" (.getFieldType (.getField right-tail)) ValueVector)]

    ;; NOTE: nested loop join.
    (dotimes [left-idx (.getValueCount left-tail)]
      (let [l (.get left-tail left-idx)]
        (dotimes [right-idx (.getValueCount right-head)]
          (let [r (.get right-head right-idx)]
            (when (= l r)
              (append-bigint-vector join-head (.get left-head left-idx))
              (append-bigint-vector join-tail (.get right-tail right-idx))
              (.setValueCount join-struct (inc (.getValueCount join-struct))))))))

    join-struct))

(defn- void-tail ^org.apache.arrow.vector.BigIntVector [^BufferAllocator a ^StructVector v]
  (copy-vector (.getChild v "head") (BigIntVector. "" a)))

(defn- sum ^org.apache.arrow.vector.BigIntVector [^BufferAllocator a ^BigIntVector v]
  (let [value-count (.getValueCount v)]
    (loop [acc 0
           idx 0]
      (if (= idx value-count)
        (append-bigint-vector (BigIntVector. "" a) acc)
        (recur (+ acc (.get v idx)) (inc idx))))))

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
                        (t/is (= [9 19] (->list (.getChild inter7 "tail"))))

                        ;; NOTE: scalar is represented as a single
                        ;; element vector in paper.
                        (with-open [result (sum a (.getChild inter7 "tail"))]
                          (t/is (= [28] (->list result))))))))))))))))
