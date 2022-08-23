(ns core2.align
  (:require [core2.vector.indirect :as iv])
  (:import [java.util ArrayList LinkedList List Map]
           [org.apache.arrow.vector BigIntVector VectorSchemaRoot]
           org.roaringbitmap.longlong.Roaring64Bitmap
           org.roaringbitmap.RoaringBitmap))

(set! *unchecked-math* :warn-on-boxed)

(defn ->row-id-bitmap
  (^org.roaringbitmap.longlong.Roaring64Bitmap [^BigIntVector row-id-vec]
   (->row-id-bitmap nil row-id-vec))

  (^org.roaringbitmap.longlong.Roaring64Bitmap [^ints idxs ^BigIntVector row-id-vec]
   (let [res (Roaring64Bitmap.)]
     (if idxs
       (dotimes [idx (alength idxs)]
         (.addLong res (.get row-id-vec (aget idxs idx))))
       (dotimes [idx (.getValueCount row-id-vec)]
         (.addLong res (.get row-id-vec idx))))
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

(defn align-vectors ^core2.vector.IIndirectRelation [^List roots, ^Roaring64Bitmap row-id-bitmap
                                                     {:keys [^Map row-id->repeat-count, with-row-id-vec?]}]
  (let [read-cols (LinkedList.)]
    (doseq [^VectorSchemaRoot root roots
            :let [row-id-vec (.getVector root 0)
                  in-vec (.getVector root 1)
                  idxs (if row-id->repeat-count
                         (<-row-id-bitmap-with-repetitions row-id->repeat-count row-id-vec)
                         (<-row-id-bitmap row-id-bitmap row-id-vec))]]
      (when (and with-row-id-vec? (empty? read-cols))
        (.add read-cols (iv/->indirect-vec row-id-vec idxs)))

      (.add read-cols (iv/->indirect-vec in-vec idxs)))

    (iv/->indirect-rel read-cols)))
