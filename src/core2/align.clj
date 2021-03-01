(ns core2.align
  (:require [core2.types :as t]
            [core2.util :as util])
  (:import java.util.function.IntConsumer
           java.util.List
           java.util.stream.IntStream
           [org.apache.arrow.vector BigIntVector FieldVector VectorSchemaRoot]
           org.apache.arrow.vector.complex.DenseUnionVector
           org.apache.arrow.vector.types.pojo.Schema
           org.roaringbitmap.longlong.Roaring64Bitmap
           org.roaringbitmap.RoaringBitmap))

(defn ->row-id-bitmap
  (^org.roaringbitmap.longlong.Roaring64Bitmap [^BigIntVector row-id-vec]
   (->row-id-bitmap nil row-id-vec))

  (^org.roaringbitmap.longlong.Roaring64Bitmap [^RoaringBitmap idxs ^BigIntVector row-id-vec]
   (let [res (Roaring64Bitmap.)]
     (-> (or (some-> idxs .stream)
             (IntStream/range 0 (.getValueCount row-id-vec)))
         (.forEach (reify IntConsumer
                     (accept [_ n]
                       (.addLong res (.get row-id-vec n))))))
     res)))

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
      (.forEach (if (instance? DenseUnionVector field-vec)
                  (reify IntConsumer
                    (accept [_ idx]
                      (let [field-vec ^DenseUnionVector field-vec
                            out-idx (.getValueCount out-vec)
                            type-id (.getTypeId field-vec idx)
                            offset (util/write-type-id out-vec (.getValueCount out-vec) type-id)]
                        (util/set-value-count out-vec (inc out-idx))
                        (.copyFrom (.getVectorByType ^DenseUnionVector out-vec type-id)
                                   (.getOffset field-vec idx)
                                   offset
                                   (.getVectorByType field-vec type-id)))))
                  (reify IntConsumer
                    (accept [_ idx]
                      (let [out-idx (.getValueCount out-vec)]
                        (util/set-value-count out-vec (inc out-idx))
                        (.copyFrom out-vec idx out-idx field-vec)))))))
  out-vec)

(defn align-schemas ^org.apache.arrow.vector.types.pojo.Schema [^List schemas]
  (Schema. (reduce (fn [acc ^Schema schema]
                     (cond-> acc
                       (empty? acc) (conj t/row-id-field)
                       :always (conj (-> (.getFields schema) (.get 1)))))
                   []
                   schemas)))

(defn align-vectors ^org.apache.arrow.vector.VectorSchemaRoot [^List roots, ^Roaring64Bitmap row-id-bitmap ^VectorSchemaRoot out-root]
  (.clear out-root)

  (doseq [^VectorSchemaRoot root roots
          :let [row-id-vec (.getVector root 0)
                in-vec (.getVector root 1)
                idxs (<-row-id-bitmap row-id-bitmap row-id-vec)]]
    (when (identical? root (first roots))
      (project-vec row-id-vec idxs (.getVector out-root t/row-id-field)))
    (project-vec in-vec idxs (.getVector out-root (.getField in-vec))))

  (doto out-root
    (util/set-vector-schema-root-row-count (.getLongCardinality row-id-bitmap))))
