(ns core2.align
  (:require [core2.vector.indirect :as iv]
            [core2.vector :as vec])
  (:import (core2.vector IIndirectRelation IIndirectVector)
           [java.util HashMap LinkedList List Map]
           java.util.function.BiFunction
           java.util.stream.IntStream
           org.roaringbitmap.longlong.Roaring64Bitmap))

(set! *unchecked-math* :warn-on-boxed)

(defn ->row-id-bitmap
  (^org.roaringbitmap.longlong.Roaring64Bitmap [^IIndirectVector row-id-col]
   (->row-id-bitmap nil row-id-col))

  (^org.roaringbitmap.longlong.Roaring64Bitmap [^ints idxs ^IIndirectVector row-id-col]
   (let [res (Roaring64Bitmap.)
         row-id-rdr (.monoReader row-id-col :i64)]
     (if idxs
       (dotimes [idx (alength idxs)]
         (.addLong res (.readLong row-id-rdr (aget idxs idx))))
       (dotimes [idx (.getValueCount row-id-col)]
         (.addLong res (.readLong row-id-rdr idx))))
     res)))

(defn- ->row-id->repeat-count ^java.util.Map [^IIndirectVector row-id-col]
  (let [res (HashMap.)
        row-id-rdr (.monoReader row-id-col :i64)]
    (dotimes [idx (.getValueCount row-id-col)]
      (let [row-id (.readLong row-id-rdr idx)]
        (.compute res row-id (reify BiFunction
                               (apply [_ _k v]
                                 (if v
                                   (inc (long v))
                                   1))))))
    res))

(defn- align-vector ^core2.vector.IIndirectVector [^IIndirectRelation content-rel, ^Map row-id->repeat-count]
  (let [[^IIndirectVector row-id-col, ^IIndirectVector content-col] (vec content-rel)
        row-id-rdr (.monoReader row-id-col :i64)
        res (IntStream/builder)]
    (dotimes [idx (.getValueCount row-id-col)]
      (let [row-id (.readLong row-id-rdr idx)]
        (when-let [ns (.get row-id->repeat-count row-id)]
          (dotimes [_ ns]
            (.add res idx)))))

    (.select content-col (.toArray (.build res)))))

(defn align-vectors ^core2.vector.IIndirectRelation [^List content-rels, ^IIndirectRelation temporal-rel]
  ;; assumption: temporal-rel is sorted by row-id
  (let [read-cols (LinkedList. (seq temporal-rel))
        temporal-row-id-col (.vectorForName temporal-rel "_row-id")]
    (assert temporal-row-id-col)

    (let [row-id->repeat-count (->row-id->repeat-count temporal-row-id-col)]
      (doseq [^IIndirectRelation content-rel content-rels]
        (.add read-cols (align-vector content-rel row-id->repeat-count))))

    (iv/->indirect-rel read-cols)))
