(ns core2.select
  (:require [core2.types :as t])
  (:import [core2.select IVectorCompare IVectorPredicate]
           core2.types.ReadWrite
           java.nio.charset.StandardCharsets
           [java.util Comparator List]
           [java.util.function IntConsumer IntPredicate]
           java.util.stream.IntStream
           org.apache.arrow.memory.BufferAllocator
           [org.apache.arrow.memory.util ArrowBufPointer ByteFunctionHelpers]
           [org.apache.arrow.vector BigIntVector ElementAddressableVector FieldVector VarCharVector VectorSchemaRoot]
           org.apache.arrow.vector.complex.DenseUnionVector
           [org.apache.arrow.vector.holders NullableBigIntHolder ValueHolder]
           org.apache.arrow.vector.util.Text
           org.roaringbitmap.longlong.Roaring64Bitmap
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
  (let [buf-pointer (ArrowBufPointer.)]
    (reify IVectorCompare
      (compareIdx [_ field-vec idx]
        (.getDataPointer ^ElementAddressableVector field-vec idx buf-pointer)
        (ByteFunctionHelpers/compare (.getBuf buf-pointer) (.getOffset buf-pointer) (+ (.getOffset buf-pointer) (.getLength buf-pointer))
                                     comparison-value 0 (alength comparison-value))))))

(defn ->bytes-pred [^IntPredicate compare-pred ^bytes comparison-value]
  (compare->pred compare-pred (->bytes-compare comparison-value)))

(defn ->str-compare ^core2.select.IVectorCompare [^String comparison-value]
  (->bytes-compare (.getBytes comparison-value StandardCharsets/UTF_8)))

(defn ->str-pred [^IntPredicate compare-pred ^String comparison-value]
  (compare->pred compare-pred (->str-compare comparison-value)))

(defn ->dense-union-pred [^IVectorPredicate vec-pred, ^long type-id]
  (reify IVectorPredicate
    (test [_ field-vec idx]
      (let [^DenseUnionVector field-vec field-vec]
        (and (= (.getTypeId field-vec idx) type-id)
             (.test vec-pred (.getVectorByType field-vec type-id) (.getOffset field-vec idx)))))))

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

(defn ->row-id-bitmap ^org.roaringbitmap.longlong.Roaring64Bitmap [^RoaringBitmap idxs ^BigIntVector row-id-vec]
  (let [res (Roaring64Bitmap.)]
    (-> (.stream idxs)
        (.forEach (reify IntConsumer
                    (accept [_ n]
                      (.addLong res (.get row-id-vec n))))))
    res))

(defn- <-row-id-bitmap ^org.roaringbitmap.RoaringBitmap [^Roaring64Bitmap row-ids ^BigIntVector row-id-vec]
  (let [res (RoaringBitmap.)]
    (dotimes [idx (.getValueCount row-id-vec)]
      (when (.contains row-ids (.get row-id-vec idx))
        (.add res idx)))
    res))


(defn- project-vec ^org.apache.arrow.vector.FieldVector [^FieldVector field-vec ^RoaringBitmap idxs ^FieldVector out-vec]
  (.setInitialCapacity out-vec (.getLongCardinality idxs))
  (.allocateNew out-vec)
  (-> (.stream idxs)
      (.forEach (reify IntConsumer
                  (accept [_ idx]
                    (let [out-idx (.getValueCount out-vec)]
                      (.setValueCount out-vec (inc out-idx))
                      (.copyFrom out-vec idx out-idx field-vec))))))
  out-vec)

(defn align-vectors ^org.apache.arrow.vector.VectorSchemaRoot [^BufferAllocator allocator, ^List vsrs, ^Roaring64Bitmap row-id-bitmap]
  (let [out-vecs (reduce (fn [acc ^VectorSchemaRoot vsr]
                           (let [row-id-vec (.getVector vsr 0)
                                 in-vec (.getVector vsr 1)
                                 idxs (<-row-id-bitmap row-id-bitmap row-id-vec)]

                             (cond-> acc
                               (empty? acc) (conj (project-vec row-id-vec idxs (BigIntVector. "_row-id" allocator)))
                               :always (conj (project-vec in-vec idxs
                                                          (.createVector (.getField in-vec) allocator))))))
                         []
                         vsrs)

        res-vsr (VectorSchemaRoot. ^java.util.List out-vecs)]
    (.setRowCount res-vsr (.getLongCardinality row-id-bitmap))
    res-vsr))
