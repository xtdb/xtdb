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
           [java.util.function LongConsumer LongFunction]
           java.util.stream.LongStream
           java.io.Closeable))

;; "Learning Multi-dimensional Indexes"
;; https://arxiv.org/pdf/1912.01668.pdf

;; TODO:

;; Sort cells by a dimension (last in paper) and leave it out from the
;; grid. Can reuse variant of old quick select.

;; Fix boxing inefficiencies in boundary generation and cartesian
;; product.

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

(defn- three-way-partition ^longs [^IKdTreePointAccess access ^long low ^long hi ^long axis]
  (let [pivot (.getCoordinate access (quot (+ low hi) 2) axis)]
    (loop [i (int low)
           j (int low)
           k (inc (int hi))]
      (if (< j k)
        (let [diff (Long/compare (.getCoordinate access j axis) pivot)]
          (cond
            (neg? diff)
            (do (.swapPoint access i j)
                (recur (inc i) (inc j) k))

            (pos? diff)
            (let [k (dec k)]
              (.swapPoint access j k)
              (recur i j k))

            :else
            (recur i (inc j) k)))
        (doto (long-array 2)
          (aset 0 i)
          (aset 1 (dec k)))))))

(defn- quick-sort [^IKdTreePointAccess access ^long low ^long hi ^long axis]
  (when (< low hi)
    (let [^longs left-right (three-way-partition access low hi axis)
          left (dec (aget left-right 0))
          right (inc (aget left-right 1))]
      (if (< (- left low) (- hi right))
        (do (quick-sort access low left axis)
            (recur access right hi axis))
        (do (quick-sort access right hi axis)
            (recur access low left axis))))))

(definterface ISimpleGrid
  (^int cellIdx [^longs point]))

(declare ->simple-grid-point-access)

(defn- binary-search-axis ^long [^IKdTreePointAccess access ^long n ^long axis ^long x]
  (let [n (int n)
        axis (int axis)]
    (loop [low (int 0)
           hi n]
      (if (and (<= low hi) (< low n))
        (let [idx (quot (+ low hi) 2)
              c (.getCoordinate access idx axis)
              diff (Long/compare x c)]
          (cond
            (zero? diff)
            idx

            (pos? diff)
            (recur (inc idx) hi)

            (neg? diff)
            (recur low (dec idx))))
        (- (inc low))))))

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
      (if (= n (dec k))
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
          k-minus-one (dec k)
          axis-cell-idxs (for [n (range k-minus-one)
                               :let [min-r (aget min-range n)
                                     max-r (aget max-range n)
                                     min-v (aget mins n)
                                     max-v (aget maxs n)]
                               :when (BitUtil/bitNot (or (< max-v min-r) (> min-v max-r)))
                               :let [^longs axis-scale (aget scales n)
                                     min-axis-idx (Arrays/binarySearch axis-scale min-r)
                                     min-axis-idx (if (neg? min-axis-idx)
                                                    (dec (- min-axis-idx))
                                                    (long min-axis-idx))
                                     max-axis-idx (Arrays/binarySearch axis-scale max-r)
                                     max-axis-idx (if (neg? max-axis-idx)
                                                    (dec (- max-axis-idx))
                                                    (long max-axis-idx))]
                               :let [r (range min-axis-idx (inc max-axis-idx))]]
                           (-> (mapv vector r (repeat true))
                               (assoc-in [0 1] false)
                               (assoc-in [(dec (count r)) 1] false)))
          cell-idxs (when (= k-minus-one (count axis-cell-idxs))
                      (for [cell-idxs+cell-in-ranges (cartesian-product axis-cell-idxs)
                            :let [cell-idxs (kd/->longs (map first cell-idxs+cell-in-ranges))
                                  cell-in-range? (every? true? (map second cell-idxs+cell-in-ranges))]]
                        (loop [n 0
                               idx 0]
                          (if (= n k-minus-one)
                            [idx cell-in-range?]
                            (let [axis-idx (aget cell-idxs n)]
                              (recur (inc n) (bit-or (bit-shift-left idx axis-shift) axis-idx)))))))
          axis-mask (kd/range-bitmask min-range max-range)
          partial-match-last-axis? (BitUtil/bitNot (BitUtil/isBitSet axis-mask k-minus-one))
          acc (LongStream/builder)]
      (doseq [[^long cell-idx cell-in-range?] cell-idxs
              :let [^FixedSizeListVector cell (aget cells cell-idx)]
              :when cell
              :let [access (KdTreeVectorPointAccess. cell k)
                    start-point-idx (bit-shift-left cell-idx cell-shift)
                    start-idx (if partial-match-last-axis?
                                0
                                (binary-search-axis access (.getValueCount cell) k-minus-one (aget min-range k-minus-one)))
                    start-idx (if (neg? start-idx)
                                (dec (- start-idx))
                                start-idx)
                    end-idx (if partial-match-last-axis?
                              (long (.getValueCount cell))
                              (binary-search-axis access (.getValueCount cell) k-minus-one (aget max-range k-minus-one)))
                    end-idx (if (neg? end-idx)
                              (- (- end-idx) 2)
                              end-idx)]]
        (if cell-in-range?
          (loop [idx start-idx]
            (when (<= idx end-idx)
              (.add acc (+ start-point-idx idx))
              (recur (inc idx))))
          (loop [idx start-idx]
            (when (<= idx end-idx)
              (when (.isInRange access idx min-range max-range axis-mask)
                (.add acc (+ start-point-idx idx)))
              (recur (inc idx))))))
      (.build acc)))
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
                                                                                    cell-size (* 4 1024)}}]
   (let [^long total (if (satisfies? kd/KdTree points)
                       (kd/kd-tree-size points)
                       (count points))
         _ (assert (= 1 (Long/bitCount cell-size)))
         number-of-cells (Math/ceil (/ total cell-size))
         k-minus-one (dec k)
         cells-per-dimension (next-power-of-two (Math/ceil (Math/pow number-of-cells (/ 1 k-minus-one))))
         _ (assert (= 1 (Long/bitCount cells-per-dimension)))
         number-of-cells (Math/ceil (Math/pow cells-per-dimension k-minus-one))
         axis-shift (Long/bitCount (dec cells-per-dimension))
         cell-shift (Long/bitCount (dec (bit-shift-left cell-size 12)))
         ^List histograms (vec (repeatedly k #(hist/->histogram max-histogram-bins)))
         update-histograms-fn (fn [^longs p]
                                (dotimes [n k]
                                  (.update ^IHistogram (.get histograms n) (aget p n))))]
     (if (satisfies? kd/KdTree points)
       (let [^IKdTreePointAccess access (kd/kd-tree-point-access points)]
         (.forEach ^LongStream (kd/kd-tree-points points)
                   (reify LongConsumer
                     (accept [_ x]
                       (update-histograms-fn (.getArrayPoint access x))))))
       (doseq [p points]
         (update-histograms-fn (kd/->longs p))))
     (let [scales (object-array (for [^IHistogram h histograms]
                                  (long-array (.uniform h cells-per-dimension))))
           mins (long-array (for [^IHistogram h histograms]
                              (Math/floor (.getMin h))))
           maxs (long-array (for [^IHistogram h histograms]
                              (Math/ceil (.getMax h))))
           cells (object-array number-of-cells)
           grid (SimpleGrid. allocator scales mins maxs cells k axis-shift cell-shift total)
           ^Field point-field (kd/->point-field k)
           write-point-fn (fn [^longs p]
                            (let [cell-idx (.cellIdx grid p)
                                  ^FixedSizeListVector cell (or (aget cells cell-idx)
                                                                (doto (.createVector point-field allocator)
                                                                  (->> (aset cells cell-idx))))]
                              (kd/write-point cell (KdTreeVectorPointAccess. cell k) p)))]
       (if (satisfies? kd/KdTree points)
         (let [^IKdTreePointAccess access (kd/kd-tree-point-access points)]
           (.forEach ^LongStream (kd/kd-tree-points points)
                     (reify LongConsumer
                       (accept [_ x]
                         (write-point-fn (.getArrayPoint access x))))))
         (doseq [p points]
           (write-point-fn (kd/->longs p))))
       (doseq [^FixedSizeListVector cell cells
               :when cell]
         (quick-sort (KdTreeVectorPointAccess. cell k) 0 (dec (.getValueCount cell)) k-minus-one))
       grid))))
