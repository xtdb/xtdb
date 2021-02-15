(ns core2.select
  (:require [core2.types :as t])
  (:import core2.select.IVectorPredicate
           core2.types.ReadWrite
           java.util.Comparator
           java.util.function.IntPredicate
           org.apache.arrow.memory.BufferAllocator
           [org.apache.arrow.vector BitVector FieldVector]
           org.apache.arrow.vector.holders.ValueHolder))

(defmacro ^:private def-pred [sym [binding] & body]
  `(def ~(-> sym (with-meta {:tag IntPredicate}))
     (reify IntPredicate (test [_ ~binding] ~@body))))

(def-pred pred< [n] (neg? n))
(def-pred pred<= [n] (not (pos? n)))
(def-pred pred> [n] (pos? n))
(def-pred pred>= [n] (not (neg? n)))
(def-pred pred= [n] (zero? n))

(deftype VectorPredicate [^ReadWrite rw
                          ^Comparator comparator
                          ^IntPredicate compare-pred
                          ^ValueHolder holder
                          comparison-value]
  IVectorPredicate
  (test [_ field-vec idx]
    (.read rw field-vec idx holder)
    (.test compare-pred (.compare comparator holder comparison-value))))

(defn ->vec-pred [compare-pred ^ValueHolder comparison-value]
  (let [minor-type (t/holder-minor-type comparison-value)
        ^ReadWrite rw (t/type->rw minor-type)
        comparator (t/type->comp minor-type)]
    (VectorPredicate. rw comparator compare-pred (.newHolder rw) comparison-value)))

(defn first-index-of ^long [^FieldVector field-vec, ^IntPredicate predicate, ^ValueHolder needle-holder]
  (let [minor-type (.getMinorType field-vec)
        ^ReadWrite rw (t/type->rw minor-type)
        ^Comparator comparator (t/type->comp minor-type)
        val-holder (.newHolder rw)
        value-count (.getValueCount field-vec)]
    (loop [low 0
           high (dec value-count)]
      (if (< high low)
        (if (.test predicate 1)
          low
          -1)

        (let [mid (+ low (quot (- high low) 2))]
          (.read rw field-vec mid val-holder)
          (case (Long/signum (.compare comparator val-holder needle-holder))
            -1 (recur (inc mid) high)
            0 (if (.test predicate 0)
                (loop [idx mid]
                  (if (and (pos? idx)
                           (do
                             (.read rw field-vec (dec idx) val-holder)
                             (zero? (.compare comparator val-holder needle-holder))))
                    (recur (dec idx))
                    idx))

                ;; pred> case
                (loop [idx mid]
                  (if (< idx value-count)
                    (do
                      (.read rw field-vec idx val-holder)
                      (if (.test predicate (.compare comparator val-holder needle-holder))
                        idx
                        (recur (inc idx))))
                    idx)))

            1 (recur low (dec mid))))))))

(defn last-index-of ^long [^FieldVector field-vec, ^ValueHolder needle-holder, ^long start-idx]
  (let [minor-type (.getMinorType field-vec)
        ^ReadWrite rw (t/type->rw minor-type)
        ^Comparator comparator (t/type->comp minor-type)
        val-holder (.newHolder rw)
        value-count (.getValueCount field-vec)]
    (loop [idx start-idx]
      (if (< idx value-count)
        (do
          (.read rw field-vec idx val-holder)
          (if (zero? (.compare comparator val-holder needle-holder))
            (recur (inc idx))
            idx))
        idx))))

(definterface ISelect
  (^int select [^org.apache.arrow.vector.FieldVector fieldVector
                ^org.apache.arrow.vector.BitVector indexesOut])
  (^int select [^org.apache.arrow.vector.FieldVector fieldVector
                ^int start
                ^org.apache.arrow.vector.BitVector indexesOut])
  (^int select [^org.apache.arrow.vector.FieldVector fieldVector
                ^int start, ^int end
                ^org.apache.arrow.vector.BitVector indexesOut]))

(deftype Selector [^IVectorPredicate vec-predicate, ^boolean continue-on-mismatch?]
  ISelect
  (select [this field-vec idxs-out]
    (.select this field-vec 0 idxs-out))

  (select [this field-vec start idxs-out]
    (.select this field-vec start (.getValueCount field-vec) idxs-out))

  (select [_this field-vec start end idxs-out]
    (assert (<= 0 start (.getValueCount field-vec)))
    (assert (<= 0 end (.getValueCount field-vec)))
    (assert (<= start end))

    (loop [idx start
           res-count 0]
      (if-not (< idx end)
        res-count

        (let [res-idx (- idx start)]
          (if (and (pos? (.get idxs-out res-idx))
                   (.test vec-predicate field-vec idx))
            (recur (inc idx) (inc res-count))
            (do
              (.set idxs-out res-idx 0)
              (if continue-on-mismatch?
                (recur (inc idx) res-count)
                res-count))))))))

(defn ^core2.select.ISelect ->selector
  "on-mismatch :: #{:take-while :filter}"
  ([^IVectorPredicate vec-pred] (->selector vec-pred :filter))
  ([^IVectorPredicate vec-pred, mismatch-behaviour] (Selector. vec-pred (= mismatch-behaviour :filter))))

(defn open-result-vec ^org.apache.arrow.vector.BitVector [^BufferAllocator allocator, len]
  (doto (BitVector. "res" allocator)
    (.setValueCount len)
    (.setRangeToOne 0 len)))
