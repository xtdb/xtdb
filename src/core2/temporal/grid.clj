(ns core2.temporal.grid
  (:require [core2.temporal.kd-tree :as kd])
  (:import core2.temporal.kd_tree.IKdTreePointAccess
           core2.BitUtil
           [java.util ArrayList Arrays Collection HashMap List Map]
           java.util.function.Function
           java.util.stream.LongStream
           java.nio.LongBuffer))

(set! *unchecked-math* :warn-on-boxed)

(declare ->grid-file-point-access)

(def ^:private ^Class longs-class (Class/forName "[J"))

(defn ->longs ^longs [xs]
  (if (instance? longs-class xs)
    xs
    (long-array xs)))

(defn- cartesian-product [colls]
  (if (empty? colls)
    '(())
    (for [more (cartesian-product (rest colls))
          x (first colls)]
      (cons x more))))

(deftype GridFile [^int k ^int block-size ^List scales ^Map directory ^List blocks]
  kd/KdTree
  (kd-tree-insert [this allocator point]
    (let [point (->longs point)
          cell-idx (long-array k)]
      (dotimes [n k]
        (let [x (aget point n)
              ^longs axis-scale (nth scales n)
              splits (alength axis-scale)
              cell-axis-idx (Arrays/binarySearch axis-scale x)
              ^long cell-axis-idx (if (pos? cell-axis-idx)
                                    cell-axis-idx
                                    (dec (- cell-axis-idx)))]
          (aset cell-idx n cell-axis-idx)
          (aset axis-scale 0 (min (aget axis-scale 0) x))
          (aset axis-scale (dec splits) (max (aget axis-scale (dec splits)) x))))
      (let [cell-idx (LongBuffer/wrap cell-idx)
            ^long block-id (.computeIfAbsent directory cell-idx (reify Function
                                                                  (apply [_ k]
                                                                    (.add blocks (ArrayList.))
                                                                    (dec (.size blocks)))))
            ^List block (.get blocks block-id)]
        (.add block point)
        (when (> block-size (.size block))
          (let [^long split-axis (ffirst (sort-by (comp count second) (map-indexed vector scales)))
                [^List old-block ^List new-block] (split-at (quot block-size 2) (sort-by #(aget ^longs % split-axis) block))
                new-axis-idx (inc (.get cell-idx split-axis))
                new-min (aget ^longs (first new-block) split-axis)
                new-cell-idx (doto (long-array (.array cell-idx))
                               (aset split-axis new-axis-idx))]
            (.set blocks block-id (ArrayList. old-block))
            (.add blocks (ArrayList. new-block))
            (.put directory new-cell-idx (.size blocks))
            (.set scales split-axis (long-array (doto (ArrayList. ^List (vec (.get scales split-axis)))
                                                  (.add new-axis-idx new-min))))))
        this)))
  (kd-tree-delete [_ allocator point]
    (throw (UnsupportedOperationException.)))
  (kd-tree-range-search [this min-range max-range]
    (let [min-range (->longs min-range)
          max-range (->longs max-range)
          axis-block-ids (for [n (range k)
                               :let [x (aget min-range n)
                                     y (aget max-range n)
                                     ^longs axis-scale (nth scales n)
                                     splits (alength axis-scale)
                                     cell-axis-idx (Arrays/binarySearch axis-scale x)
                                     ^long cell-axis-idx (if (pos? cell-axis-idx)
                                                           cell-axis-idx
                                                           (dec (- cell-axis-idx)))]]
                           (take-while #(<= ^long % y) (drop (dec cell-axis-idx) axis-scale)))
          blocks (distinct (keep #(.get directory %) (cartesian-product axis-block-ids)))
          ^IKdTreePointAccess access (kd/kd-tree-point-access this)
          axis-mask (kd/range-bitmask min-range max-range)
          acc (LongStream/builder)]
      (loop [[block & blocks] blocks
             idx 0]
        (if-not block
          (.build acc)
          (let [block-size (count block)]
            (loop [n 0]
              (when-not (= block-size n)
                (let [point (nth block n)]
                  (when (.isInRange access k min-range max-range axis-mask)
                    (.add acc (+ n idx)))
                  (recur (inc n)))))
            (recur blocks (+ idx block-size)))))))
  (kd-tree-points [this]
    (LongStream/range 0 (kd/kd-tree-value-count this)))
  (kd-tree-depth [_]
    1)
  (kd-tree-retain [this allocator]
    this)
  (kd-tree-point-access [this]
    (->grid-file-point-access this))
  (kd-tree-size [this]
    (kd/kd-tree-value-count this))
  (kd-tree-value-count [_]
    (reduce + (map count blocks)))
  (kd-tree-dimensions [_]
    k))

(deftype GridFilePointAccess [^GridFile gf ^int block-shift ^int block-mask]
  IKdTreePointAccess
  (getPoint [this idx]
    (vec (.getArrayPoint this idx)))
  (getArrayPoint [this idx]
    (nth (nth (.blocks gf) (BitUtil/unsignedBitShiftRight idx block-shift))
         (bit-and idx block-mask)))
  (getCoordinate [this idx axis]
    (aget (.getArrayPoint this idx) axis))
  (setCoordinate [_ idx axis value]
    (throw (UnsupportedOperationException.)))
  (swapPoint [_ from-idx to-idx]
    (throw (UnsupportedOperationException.)))
  (isDeleted [_ idx]
    false)
  (isInRange [this idx min-range max-range mask]
    (let [point (.getArrayPoint this idx)
          k (.k gf)]
      (loop [n (int 0)]
        (if (= n k)
          true
          (if (BitUtil/isBitSet mask n)
            (let [x (aget point n)]
              (if (and (<= (aget min-range n) x)
                       (<= x (aget max-range n)))
                (recur (inc n))
                false))
            (recur (inc n))))))))

(defn- ->grid-file-point-access [^GridFile gf]
  (GridFilePointAccess. gf (.block-size gf) (dec (.block-size gf))))

(defn ->grid-file ^core2.temporal.grid.GridFile [^long k ^long block-size points]
  (assert (= 1 (Long/bitCount block-size)))
  (let [axis-splits (int-array k)
        scales (ArrayList. ^Collection (repeatedly k #(long-array 1 Long/MAX_VALUE)))
        directory (HashMap.)
        blocks (ArrayList.)
        gf (GridFile. k block-size scales directory blocks)]
    (reduce kd/kd-tree-insert gf points)))
