(ns core2.temporal.grid
  (:require [core2.temporal.kd-tree :as kd])
  (:import core2.temporal.kd_tree.IKdTreePointAccess
           core2.BitUtil
           [java.util ArrayList Collection HashMap List Map]
           [java.util.stream LongStream StreamSupport]))

(set! *unchecked-math* :warn-on-boxed)

(declare ->grid-file-point-access)

(def ^:private ^Class longs-class (Class/forName "[J"))

(defn ->longs ^longs [xs]
  (if (instance? longs-class xs)
    xs
    (long-array xs)))

(deftype GridFile [^int k ^int block-size ^List scales ^Map directory ^List blocks]
  kd/KdTree
  (kd-tree-insert [_ allocator point]
    (let [point (->longs point)]
      (dotimes [n k]
        (let [x (aget point n)
              ^longs axis-scale (nth scales n)
              splits (alength axis-scale)]
          (aset axis-scale 0 (min (aget axis-scale 0) x))
          (aset axis-scale (dec splits) (max (aget axis-scale (dec splits)) x)))))
    (throw (UnsupportedOperationException.)))
  (kd-tree-delete [_ allocator point]
    (throw (UnsupportedOperationException.)))
  (kd-tree-range-search [_ min-range max-range]
    (throw (UnsupportedOperationException.)))
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
