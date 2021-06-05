(ns core2.temporal.grid
  (:require [core2.util :as util]
            [core2.temporal.kd-tree :as kd]
            [core2.temporal.histogram :as hist])
  (:import [core2.temporal.kd_tree IKdTreePointAccess KdTreeVectorPointAccess]
           core2.temporal.histogram.IHistogram
           core2.BitUtil
           org.apache.arrow.memory.BufferAllocator
           org.apache.arrow.vector.complex.FixedSizeListVector
           org.apache.arrow.vector.VectorSchemaRoot
           org.apache.arrow.vector.types.pojo.Field
           [java.util ArrayList Arrays List]
           java.util.function.LongFunction
           java.util.stream.LongStream
           java.io.Closeable))

;; "Learning Multi-dimensional Indexes"
;; https://arxiv.org/pdf/1912.01668.pdf

;; TODO:

;; Try different data distributions in test.

;; Sort cells by a dimension (last in paper) and leave it out from the
;; grid. Can reuse variant of old quick select.

;; Fix boxing inefficiencies in boundary generation and cartesian
;; product.

;; Support points as kd tree idx stream.

;; Write cells to intermediate files and stitch them together as a
;; single Arrow IPC file, one cell per batch (including empty cells).

;; Split out multidimensional index protocols from kd-tree specifics.

(set! *unchecked-math* :warn-on-boxed)

(defn- cartesian-product [colls]
  (if (empty? colls)
    '(())
    (for [more (cartesian-product (rest colls))
          x (first colls)]
      (cons x more))))

(definterface ISimpleGrid
  (^int cellIdx [^longs point]))

(declare ->simple-grid-point-access)

(deftype SimpleGrid [^BufferAllocator allocator
                     ^objects scales
                     ^longs mins
                     ^longs maxs
                     ^objects cells
                     ^int k
                     ^int axis-shift
                     ^int cell-shift
                     ^long total]
  ISimpleGrid
  (cellIdx [_ point]
    (loop [n 0
           idx 0]
      (if (= n k)
        idx
        (let [axis-idx (Arrays/binarySearch ^longs (aget scales n) (aget point n))
              ^long axis-idx (if (neg? axis-idx)
                               (dec (- axis-idx))
                               axis-idx)]
          (recur (inc n) (bit-or (bit-shift-left idx axis-shift) axis-idx))))))

  kd/KdTree
  (kd-tree-insert [this allocator point]
    (throw (UnsupportedOperationException.)))
  (kd-tree-delete [this allocator point]
    (throw (UnsupportedOperationException.)))
  (kd-tree-range-search [this min-range max-range]
    (let [min-range (kd/->longs min-range)
          max-range (kd/->longs max-range)
          axis-cell-idxs (for [n (range k)
                               :let [min-r (aget min-range n)
                                     max-r (aget max-range n)
                                     min-v (aget mins n)
                                     max-v (aget maxs n)]
                               :when (BitUtil/bitNot (or (< max-v min-r) (> min-v max-r)))
                               :let [^longs axis-scale (aget scales n)
                                     min-axis-idx (Arrays/binarySearch axis-scale min-r)
                                     min-axis-idx (if (neg? min-axis-idx)
                                                    (dec (- min-axis-idx))
                                                    min-axis-idx)
                                     max-axis-idx (Arrays/binarySearch axis-scale max-r)
                                     max-axis-idx (if (neg? max-axis-idx)
                                                    (- max-axis-idx)
                                                    (inc max-axis-idx))]
                               :when (not= min-axis-idx max-axis-idx)
                               :let [r (range min-axis-idx max-axis-idx)]]
                           (-> (mapv vector r (repeat true))
                               (assoc-in [0 1] false)
                               (assoc-in [(dec (count r)) 1] false)))
          cell-idxs (when (= k (count axis-cell-idxs))
                      (for [cell-idxs+cell-in-ranges (cartesian-product axis-cell-idxs)
                            :let [cell-idxs (kd/->longs (map first cell-idxs+cell-in-ranges))
                                  cell-in-range? (every? true? (map second cell-idxs+cell-in-ranges))]]
                        (loop [n 0
                               idx 0]
                          (if (= n k)
                            [idx cell-in-range?]
                            (let [axis-idx (aget cell-idxs n)]
                              (recur (inc n) (bit-or (bit-shift-left idx axis-shift) axis-idx)))))))
          axis-mask (kd/range-bitmask min-range max-range)
          acc (LongStream/builder)]
      (loop [[[cell-idx cell-in-range?] & cell-idxs] cell-idxs]
        (if-not cell-idx
          (.build acc)
          (let [^long cell-idx cell-idx]
            (when-let [^FixedSizeListVector cell (aget cells cell-idx)]
              (let [access (KdTreeVectorPointAccess. cell k)
                    start-point-idx (bit-shift-left cell-idx cell-shift)]
                (if cell-in-range?
                  (dotimes [m (.getValueCount cell)]
                    (.add acc (+ start-point-idx m)))
                  (dotimes [m (.getValueCount cell)]
                    (when (.isInRange access m min-range max-range axis-mask)
                      (.add acc (+ start-point-idx m)))))))
            (recur cell-idxs))))))
  (kd-tree-points [this]
    (.flatMap (LongStream/range 0 (alength cells))
              (reify LongFunction
                (apply [_ cell-idx]
                  (if-let [^FixedSizeListVector cell (aget cells cell-idx)]
                    (let [start-point-idx (bit-shift-left cell-idx cell-shift)]
                      (LongStream/range start-point-idx (+ start-point-idx (.getValueCount cell))))
                    (LongStream/empty))))))
  (kd-tree-depth [_] 0)
  (kd-tree-retain [this _] this)
  (kd-tree-point-access [this]
    (->simple-grid-point-access this))
  (kd-tree-size [_] total)
  (kd-tree-value-count [_] total)
  (kd-tree-dimensions [_] k)

  Closeable
  (close [_]
    (doseq [cell cells]
      (util/try-close cell))))

(deftype SimpleGridPointAccess [^objects cells ^int cell-shift ^int cell-mask ^int k]
  IKdTreePointAccess
  (getPoint [this idx]
    (.getPoint (KdTreeVectorPointAccess. (aget cells (BitUtil/unsignedBitShiftRight idx cell-shift)) k)
               (bit-and idx cell-mask))
    (ArrayList. ^List (seq (.getArrayPoint this idx))))
  (getArrayPoint [this idx]
    (.getArrayPoint (KdTreeVectorPointAccess. (aget cells (BitUtil/unsignedBitShiftRight idx cell-shift)) k)
                    (bit-and idx cell-mask)))
  (getCoordinate [this idx axis]
    (.getCoordinate (KdTreeVectorPointAccess. (aget cells (BitUtil/unsignedBitShiftRight idx cell-shift)) k)
                    (bit-and idx cell-mask)
                    axis))
  (setCoordinate [_ idx axis value]
    (throw (UnsupportedOperationException.)))
  (swapPoint [_ from-idx to-idx]
    (throw (UnsupportedOperationException.)))
  (isDeleted [_ idx]
    (.isDeleted (KdTreeVectorPointAccess. (aget cells (BitUtil/unsignedBitShiftRight idx cell-shift)) k)
                (bit-and idx cell-mask)))
  (isInRange [this idx min-range max-range mask]
    (.isInRange (KdTreeVectorPointAccess. (aget cells (BitUtil/unsignedBitShiftRight idx cell-shift)) k)
                (bit-and idx cell-mask) min-range max-range mask)))

(defn- ->simple-grid-point-access [^SimpleGrid grid]
  (let [cell-shift (.cell-shift grid)
        cell-mask (dec (bit-shift-left 1 cell-shift))]
    (SimpleGridPointAccess. (.cells grid) cell-shift cell-mask (.k grid))))

(defn- next-power-of-two ^long [^long x]
  (bit-shift-left 1 (inc (- (dec Long/SIZE) (Long/numberOfLeadingZeros (dec x))))))

(defn ->simple-grid
  (^core2.temporal.grid.SimpleGrid [^BufferAllocator allocator ^long k points]
   (->simple-grid allocator k points {}))
  (^core2.temporal.grid.SimpleGrid [^BufferAllocator allocator ^long k points {:keys [max-histogram-bins ^long cell-size]
                                                                               :or {max-histogram-bins 16
                                                                                    cell-size 1024}}]
   (let [total (count points)
         _ (assert (= 1 (Long/bitCount cell-size)))
         number-of-cells (Math/ceil (/ total cell-size))
         cells-per-dimension (next-power-of-two (Math/ceil (Math/pow number-of-cells (/ 1 k))))
         _ (assert (= 1 (Long/bitCount cells-per-dimension)))
         number-of-cells (Math/ceil (Math/pow cells-per-dimension k))
         axis-shift (Long/bitCount (dec cells-per-dimension))
         cell-shift (Long/bitCount (dec (bit-shift-left cell-size 4)))
         ^List histograms (vec (repeatedly k #(hist/->histogram max-histogram-bins)))]
     (doseq [p points]
       (let [p (kd/->longs p)]
         (dotimes [n k]
           (.update ^IHistogram (.get histograms n) (aget p n)))))
     (let [scales (object-array (for [^IHistogram h histograms]
                                  (long-array (.uniform h cells-per-dimension))))
           mins (long-array (for [^IHistogram h histograms]
                              (Math/floor (.getMin h))))
           maxs (long-array (for [^IHistogram h histograms]
                              (Math/ceil (.getMax h))))
           cells (object-array number-of-cells)
           grid (SimpleGrid. allocator scales mins maxs cells k axis-shift cell-shift total)
           ^Field point-field (kd/->point-field k)]
       (doseq [p points
               :let [p (kd/->longs p)
                     cell-idx (.cellIdx grid p)
                     ^FixedSizeListVector cell (or (aget cells cell-idx)
                                                   (doto (.createVector point-field allocator)
                                                     (->> (aset cells cell-idx))))]]
         (kd/write-point cell (KdTreeVectorPointAccess. cell k) p))
       (doseq [cell cells
               :when cell]
         (kd/build-breadth-first-tree-in-place (VectorSchemaRoot/of (into-array [cell]))))
       grid))))
