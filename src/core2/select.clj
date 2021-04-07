(ns core2.select
  (:require [core2.types :as t])
  (:import [core2.select IVectorCompare IVectorPredicate IVectorSelector]
           core2.types.ReadWrite
           java.nio.charset.StandardCharsets
           java.util.Comparator
           [java.util.function IntConsumer IntPredicate]
           java.util.stream.IntStream
           [org.apache.arrow.memory.util ArrowBufPointer ByteFunctionHelpers]
           [org.apache.arrow.vector ElementAddressableVector FieldVector]
           org.apache.arrow.vector.complex.DenseUnionVector
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

;; NOTE the IVectorCompare and IVectorPredicate instances here aren't thread-safe
(defn ->vec-compare ^core2.select.IVectorCompare [^ValueHolder comparison-value]
  (let [minor-type (t/holder-minor-type comparison-value)
        ^ReadWrite rw (t/type->rw minor-type)
        ^Comparator comparator (t/type->comp minor-type)
        holder (.newHolder rw)]
    (reify IVectorCompare
      (compareIdx [_ field-vec idx]
        (.read rw field-vec idx holder)
        (.compare comparator holder comparison-value)))))

(defn compare->pred [^IntPredicate compare-pred ^IVectorCompare cmp]
  (reify
    IVectorPredicate
    (test [_ field-vec idx]
      (.test compare-pred (.compareIdx cmp field-vec idx)))

    IVectorCompare
    (compareIdx [_ field-vec idx] (.compareIdx cmp field-vec idx))

    IntPredicate
    (test [_ n] (.test compare-pred n))))

(defn ->vec-pred [^IntPredicate compare-pred ^ValueHolder comparison-value]
  (compare->pred compare-pred (->vec-compare comparison-value)))

(defn ->bytes-compare ^core2.select.IVectorCompare [^bytes comparison-value]
  (reify IVectorCompare
    (compareIdx [_ field-vec idx]
      (let [buf-pointer (.getDataPointer ^ElementAddressableVector field-vec idx)]
        (ByteFunctionHelpers/compare (.getBuf buf-pointer) (.getOffset buf-pointer) (+ (.getOffset buf-pointer) (.getLength buf-pointer))
                                     comparison-value 0 (alength comparison-value))))))

(defn ->bytes-pred [^IntPredicate compare-pred ^bytes comparison-value]
  (compare->pred compare-pred (->bytes-compare comparison-value)))

(defn ->str-compare ^core2.select.IVectorCompare [^String comparison-value]
  (->bytes-compare (.getBytes comparison-value StandardCharsets/UTF_8)))

(defn ->str-pred [^IntPredicate compare-pred ^String comparison-value]
  (compare->pred compare-pred (->str-compare comparison-value)))

(deftype DenseUnionPredicate [^IVectorPredicate vec-pred, ^long type-id]
  IVectorPredicate
  (test [_ field-vec idx]
    (let [^DenseUnionVector field-vec field-vec]
      (and (= (.getTypeId field-vec idx) type-id)
           (.test vec-pred (.getVectorByType field-vec type-id) (.getOffset field-vec idx))))))

(defn ->dense-union-pred [^IVectorPredicate vec-pred, ^long type-id]
  (->DenseUnionPredicate vec-pred type-id))

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

(defn- dense-union-with-single-child? [^FieldVector field-vec ^IVectorPredicate vec-predicate]
  (and (instance? DenseUnionVector field-vec)
       (instance? DenseUnionPredicate vec-predicate)
       (= (.getValueCount field-vec)
          (.getValueCount (.getVectorByType ^DenseUnionVector field-vec (.type-id ^DenseUnionPredicate vec-predicate))))))

(defn select
  (^org.roaringbitmap.RoaringBitmap [^FieldVector field-vec, ^IVectorPredicate vec-predicate]
   (select nil field-vec vec-predicate))

  (^org.roaringbitmap.RoaringBitmap [^RoaringBitmap idx-bitmap, ^FieldVector field-vec, vec-predicate-or-selector]
   (cond
     (instance? IVectorSelector vec-predicate-or-selector)
     (.select ^IVectorSelector vec-predicate-or-selector field-vec)

     (dense-union-with-single-child? field-vec vec-predicate-or-selector)
     (recur idx-bitmap
            (.getVectorByType ^DenseUnionVector field-vec (.type-id ^DenseUnionPredicate vec-predicate-or-selector))
            (.vec-pred ^DenseUnionPredicate vec-predicate-or-selector))

     :else
     (do (assert (or (nil? idx-bitmap) (.isEmpty idx-bitmap) (< (.last idx-bitmap) (.getValueCount field-vec))))
         (let [res (RoaringBitmap.)
               ^IVectorPredicate vec-predicate vec-predicate-or-selector]

           (-> ^IntStream (if idx-bitmap
                            (.stream idx-bitmap)
                            (IntStream/range 0 (.getValueCount field-vec)))
               (.forEach (reify IntConsumer
                           (accept [_ idx]
                             (when (.test vec-predicate field-vec idx)
                               (.add res idx))))))

           res)))))
