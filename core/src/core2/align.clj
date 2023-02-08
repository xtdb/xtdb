(ns core2.align
  (:require [core2.temporal :as temporal]
            [core2.vector.indirect :as iv]
            [core2.vector :as vec])
  (:import (core2.vector IIndirectRelation IIndirectVector)
           [java.util HashMap LinkedList List Map]
           java.util.function.BiFunction
           java.util.stream.IntStream
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

(defn- ->row-id->repeat-count ^java.util.Map [^IIndirectVector row-id-col]
  (let [res (HashMap.)
        ^BigIntVector row-id-vec (.getVector row-id-col)]
    (dotimes [idx (.getValueCount row-id-col)]
      (let [row-id (.get row-id-vec (.getIndex row-id-col idx))]
        (.compute res row-id (reify BiFunction
                               (apply [_ _k v]
                                 (if v
                                   (inc (long v))
                                   1))))))
    res))

(defn- align-vector ^core2.vector.IIndirectVector [^VectorSchemaRoot content-root, ^Map row-id->repeat-count]
  (let [^BigIntVector row-id-vec (.getVector content-root 0)
        in-vec (.getVector content-root 1)
        res (IntStream/builder)]
    (dotimes [idx (.getValueCount row-id-vec)]
      (let [row-id (.get row-id-vec idx)]
        (when-let [ns (.get row-id->repeat-count row-id)]
          (dotimes [_ ns]
            (.add res idx)))))

    (iv/->indirect-vec in-vec (.toArray (.build res)))))

(defn align-vectors ^core2.vector.IIndirectRelation [^List content-roots, ^IIndirectRelation temporal-rel]
  ;; assumption: temporal-rel is sorted by row-id
  (let [read-cols (LinkedList. (seq temporal-rel))
        temporal-row-id-col (.vectorForName temporal-rel "_row-id")]
    (assert temporal-row-id-col)

    (let [row-id->repeat-count (->row-id->repeat-count temporal-row-id-col)]
      (doseq [^VectorSchemaRoot content-root content-roots]
        (.add read-cols (align-vector content-root row-id->repeat-count))))

    (iv/->indirect-rel read-cols)))
