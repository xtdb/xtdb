(ns core2.align
  (:import [java.util ArrayList LinkedList List Map]
           java.util.function.IntConsumer
           java.util.stream.IntStream
           [org.apache.arrow.vector BigIntVector VectorSchemaRoot]
           org.roaringbitmap.longlong.Roaring64Bitmap
           org.roaringbitmap.RoaringBitmap)
  (:require [core2.vector.indirect :as iv]))

(set! *unchecked-math* :warn-on-boxed)

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

(defn- <-row-id-bitmap ^ints [^Roaring64Bitmap row-ids ^BigIntVector row-id-vec]
  (let [res (RoaringBitmap.)]
    (dotimes [idx (.getValueCount row-id-vec)]
      (when (.contains row-ids (.get row-id-vec idx))
        (.add res idx)))
    (.toArray res)))

(defn- <-row-id-bitmap-with-repetitions ^ints [^Map row-id->repeat-count ^BigIntVector row-id-vec]
  (let [res (ArrayList.)]
    (dotimes [idx (.getValueCount row-id-vec)]
      (let [row-id (.get row-id-vec idx)]
        (when-let [ns (.get row-id->repeat-count row-id)]
          (dotimes [_ ns]
            (.add res idx)))))
    (int-array res)))

(defn align-vectors ^core2.vector.IIndirectRelation [^List roots, ^Roaring64Bitmap row-id-bitmap ^Map row-id->repeat-count]
  (let [read-cols (LinkedList.)]
    (doseq [^VectorSchemaRoot root roots
            :let [row-id-vec (.getVector root 0)
                  in-vec (.getVector root 1)]]
      (.add read-cols
            (iv/->indirect-vec in-vec
                               (if row-id->repeat-count
                                 (<-row-id-bitmap-with-repetitions row-id->repeat-count row-id-vec)
                                 (<-row-id-bitmap row-id-bitmap row-id-vec)))))

    (iv/->indirect-rel read-cols)))
