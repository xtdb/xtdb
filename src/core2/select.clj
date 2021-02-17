(ns core2.select
  (:require [core2.types :as t])
  (:import [core2.select IVectorCompare IVectorPredicate]
           core2.types.ReadWrite
           java.nio.charset.StandardCharsets
           java.util.Comparator
           [java.util.function IntConsumer IntPredicate]
           java.util.stream.IntStream
           [org.apache.arrow.memory.util ArrowBufPointer ByteFunctionHelpers]
           [org.apache.arrow.vector FieldVector VarCharVector]
           org.apache.arrow.vector.holders.ValueHolder
           org.roaringbitmap.RoaringBitmap))

(defmacro ^:private def-pred [sym [binding] & body]
  `(def ~(-> sym (with-meta {:tag IntPredicate}))
     (reify IntPredicate (test [_ ~binding] ~@body))))

(def-pred pred< [n] (neg? n))
(def-pred pred<= [n] (not (pos? n)))
(def-pred pred> [n] (pos? n))
(def-pred pred>= [n] (not (neg? n)))
(def-pred pred= [n] (zero? n))

(defn ->vec-compare ^core2.select.IVectorCompare [^ValueHolder comparison-value]
  (let [minor-type (t/holder-minor-type comparison-value)
        ^ReadWrite rw (t/type->rw minor-type)
        ^Comparator comparator (t/type->comp minor-type)
        holder (.newHolder rw)]
    (reify IVectorCompare
      (compareIdx [_ field-vec idx]
        (.read rw field-vec idx holder)
        (.compare comparator holder comparison-value)))))

(defn- compare->pred [^IntPredicate compare-pred ^IVectorCompare cmp]
  (reify IVectorPredicate
    (test [_ field-vec idx]
      (.test compare-pred (.compareIdx cmp field-vec idx)))))

(defn ->vec-pred [^IntPredicate compare-pred ^ValueHolder comparison-value]
  (compare->pred compare-pred (->vec-compare comparison-value)))

(defn ->str-compare ^core2.select.IVectorCompare [^String comparison-value]
  (let [vc-bytes (.getBytes comparison-value StandardCharsets/UTF_8)
        buf-pointer (ArrowBufPointer.)]
    (reify IVectorCompare
      (compareIdx [_ field-vec idx]
        (.getDataPointer ^VarCharVector field-vec idx buf-pointer)
        (ByteFunctionHelpers/compare (.getBuf buf-pointer) (.getOffset buf-pointer) (+ (.getOffset buf-pointer) (.getLength buf-pointer))
                                     vc-bytes 0 (alength vc-bytes))))))

(defn ->str-pred [^IntPredicate compare-pred ^String comparison-value]
  (compare->pred compare-pred (->str-compare comparison-value)))

(defn search ^org.roaringbitmap.RoaringBitmap [^FieldVector field-vec, ^IVectorCompare vec-compare]
  (let [value-count (.getValueCount field-vec)
        idx-bitmap (RoaringBitmap.)]
    (loop [low 0
           high (dec value-count)]
      (if (< high low)
        idx-bitmap

        (let [mid (+ low (quot (- high low) 2))]
          (case (Long/signum (.compareIdx vec-compare field-vec mid))
            -1 (recur (inc mid) high)
            0 (let [first-idx (loop [idx mid]
                                (if (and (pos? idx)
                                         (zero? (.compareIdx vec-compare field-vec (dec idx))))
                                  (recur (dec idx))
                                  idx))
                    last-idx (loop [idx mid]
                               (if (< idx value-count)
                                 (if (zero? (.compareIdx vec-compare field-vec idx))
                                   (recur (inc idx))
                                   idx)
                                 idx))]
                (doto idx-bitmap
                  (.add ^int first-idx ^int last-idx)))

            1 (recur low (dec mid))))))))

(defn select
  (^org.roaringbitmap.RoaringBitmap [^FieldVector field-vec, ^IVectorPredicate vec-predicate]
   (select nil field-vec vec-predicate))

  (^org.roaringbitmap.RoaringBitmap [^RoaringBitmap idx-bitmap, ^FieldVector field-vec, ^IVectorPredicate vec-predicate]
   (assert (or (nil? idx-bitmap) (< (.last idx-bitmap) (.getValueCount field-vec))))
   (let [res (RoaringBitmap.)]
     (-> ^IntStream (if idx-bitmap
                      (.stream idx-bitmap)
                      (IntStream/range 0 (.getValueCount field-vec)))
         (.forEach (reify IntConsumer
                     (accept [_ idx]
                       (when (.test vec-predicate field-vec idx)
                         (.add res idx))))))

     res)))
