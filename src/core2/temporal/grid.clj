(ns core2.temporal.grid
  (:require [clojure.data.json :as json]
            [core2.util :as util]
            [core2.temporal.kd-tree :as kd]
            [core2.temporal.histogram :as hist])
  (:import [core2.temporal.kd_tree IKdTreePointAccess KdTreeVectorPointAccess]
           core2.temporal.histogram.IHistogram
           core2.BitUtil
           [org.apache.arrow.memory ArrowBuf BufferAllocator]
           org.apache.arrow.vector.complex.FixedSizeListVector
           [org.apache.arrow.vector VectorLoader VectorSchemaRoot]
           org.apache.arrow.vector.types.pojo.Schema
           org.apache.arrow.vector.ipc.ArrowFileWriter
           org.apache.arrow.compression.CommonsCompressionFactory
           [java.util ArrayList Arrays List]
           [java.util.function Function IntToLongFunction LongConsumer LongFunction]
           java.util.stream.LongStream
           [java.io BufferedInputStream BufferedOutputStream Closeable DataInputStream DataOutputStream]
           [java.nio.channels Channels FileChannel]
           java.nio.file.Path))

;; "Learning Multi-dimensional Indexes"
;; https://arxiv.org/pdf/1912.01668.pdf

;; TODO:

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

(defn- binary-search-leftmost ^long [^IntToLongFunction access-fn ^long n ^long idx ^long x]
  (loop [l 0
         r n
         m (max (min idx (dec n)) 0)]
    (if (< l r)
      (if (< (.applyAsLong access-fn m) x)
        (let [l (inc m)]
          (recur l r (BitUtil/unsignedBitShiftRight (+ l r) 1)))
        (let [r m]
          (recur l r (BitUtil/unsignedBitShiftRight (+ l r) 1))))
      l)))

(defn- binary-search-rightmost ^long [^IntToLongFunction access-fn ^long n ^long idx ^long x]
  (loop [l 0
         r n
         m (max (min idx (dec n)) 0)]
    (if (< l r)
      (if (> (.applyAsLong access-fn m) x)
        (let [r m]
          (recur l r (BitUtil/unsignedBitShiftRight (+ l r) 1)))
        (let [l (inc m)]
          (recur l r (BitUtil/unsignedBitShiftRight (+ l r) 1))))
      (dec r))))

(defn- ->cell-idx ^long [^objects scales ^longs point ^long k-minus-one ^long axis-shift]
  (loop [n 0
         idx 0]
    (if (= n k-minus-one)
      idx
      (let [axis-idx (Arrays/binarySearch ^longs (aget scales n) (aget point n))
            ^long axis-idx (if (neg? axis-idx)
                             (dec (- axis-idx))
                             axis-idx)]
        (recur (inc n) (bit-or (bit-shift-left idx axis-shift) axis-idx))))))

(declare ->simple-grid-point-access)

(deftype SimpleGrid [^ArrowBuf arrow-buf
                     ^objects scales
                     ^longs mins
                     ^longs maxs
                     ^objects cells
                     ^doubles k-minus-one-slope+base
                     ^int k
                     ^int axis-shift
                     ^int cell-shift
                     ^long total]
  kd/KdTree
  (kd-tree-insert [this allocator point]
    (throw (UnsupportedOperationException.)))
  (kd-tree-delete [this allocator point]
    (throw (UnsupportedOperationException.)))
  (kd-tree-range-search [this min-range max-range]
    (let [min-range (kd/->longs min-range)
          max-range (kd/->longs max-range)
          k-minus-one (dec k)
          axis-mask (kd/range-bitmask min-range max-range)
          axis-idxs+masks (for [^long n (range k-minus-one)
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
                                :let [mask (bit-and axis-mask (bit-shift-left 1 n))
                                      axis-idxs+masks (-> (LongStream/range min-axis-idx (inc max-axis-idx))
                                                          (.mapToObj (reify LongFunction
                                                                       (apply [_ x]
                                                                         (doto (long-array 2)
                                                                           (aset 0 x)))))
                                                          (.toArray))]]
                            (do (aset ^longs (aget axis-idxs+masks 0) 1 mask)
                                (aset ^longs (aget axis-idxs+masks (dec (alength axis-idxs+masks))) 1 mask)
                                axis-idxs+masks))
          cell-idx+masks (when (= k-minus-one (count axis-idxs+masks))
                           (for [axis-idxs+masks (cartesian-product axis-idxs+masks)]
                             (reduce
                              (fn [^longs acc ^longs axis-idx+mask]
                                (doto acc
                                  (aset 0 (bit-or (bit-shift-left (aget acc 0) axis-shift) (aget axis-idx+mask 0)))
                                  (aset 1 (bit-or (aget acc 1) (aget axis-idx+mask 1)))))
                              (long-array 2)
                              axis-idxs+masks)))
          partial-match-last-axis? (BitUtil/bitNot (BitUtil/isBitSet axis-mask k-minus-one))
          min-r (aget min-range k-minus-one)
          max-r (aget max-range k-minus-one)
          acc (LongStream/builder)]
      (doseq [^longs cell-idx+mask cell-idx+masks
              :let [cell-idx (aget cell-idx+mask 0)
                    cell-axis-mask (aget cell-idx+mask 1)
                    ^FixedSizeListVector cell (aget cells cell-idx)]
              :when cell
              :let [access (KdTreeVectorPointAccess. cell k)
                    access-fn (reify IntToLongFunction
                                (applyAsLong [_ idx]
                                  (.getCoordinate access idx k-minus-one)))
                    slope-idx (bit-shift-left cell-idx 1)
                    slope (aget k-minus-one-slope+base slope-idx)
                    base (aget k-minus-one-slope+base (inc slope-idx))
                    n (.getValueCount cell)
                    start-point-idx (bit-shift-left cell-idx cell-shift)
                    start-idx (if partial-match-last-axis?
                                0
                                (binary-search-leftmost access-fn n (+ (* slope min-r) base) min-r))
                    end-idx (if partial-match-last-axis?
                              (dec n)
                              (binary-search-rightmost access-fn n (+ (* slope max-r) base) max-r))]]
        (if (zero? cell-axis-mask)
          (loop [idx start-idx]
            (when (<= idx end-idx)
              (.add acc (+ start-point-idx idx))
              (recur (inc idx))))
          (loop [idx start-idx]
            (when (<= idx end-idx)
              (when (.isInRange access idx min-range max-range cell-axis-mask)
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
  (kd-tree-height [_] 1)
  (kd-tree-retain [this _] this)
  (kd-tree-point-access [this]
    (->simple-grid-point-access this))
  (kd-tree-size [_] total)
  (kd-tree-value-count [_] total)
  (kd-tree-dimensions [_] k)

  Closeable
  (close [_]
    (doseq [cell cells]
      (util/try-close cell))
    (util/try-close arrow-buf)))

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

(defn- ->grid-meta-json->simple-grid
  ^core2.temporal.grid.SimpleGrid [arrow-buf
                                   cells
                                   {:keys [scales
                                           mins
                                           maxs
                                           k-minus-one-slope+base
                                           k
                                           axis-shift
                                           cell-shift
                                           total]}]
  (SimpleGrid. arrow-buf
               (object-array (map long-array scales))
               (long-array mins)
               (long-array maxs)
               cells
               (double-array k-minus-one-slope+base)
               k
               axis-shift
               cell-shift
               total))

(def ^:private ^:const point-vec-idx 0)

(defn ->arrow-buf-grid ^core2.temporal.grid.SimpleGrid [^ArrowBuf arrow-buf]
  (let [footer (util/read-arrow-footer arrow-buf)
        schema (.getSchema footer)
        grid-meta (json/read-str (get (.getCustomMetadata schema) "grid-meta") :key-fn keyword)
        allocator (.getAllocator (.getReferenceManager arrow-buf))
        cells (object-array
               (for [block (.getRecordBatches footer)]
                 (with-open [arrow-record-batch (util/->arrow-record-batch-view block arrow-buf)
                             root (VectorSchemaRoot/create schema allocator)]
                   (.load (VectorLoader. root CommonsCompressionFactory/INSTANCE) arrow-record-batch)
                   (.getTo (doto (.getTransferPair (.getVector root point-vec-idx) allocator)
                             (.transfer))))))]
    (->grid-meta-json->simple-grid arrow-buf cells grid-meta)))

(defn ->mmap-grid ^core2.temporal.grid.SimpleGrid [^BufferAllocator allocator ^Path path]
  (let [nio-buffer (util/->mmap-path path)
        arrow-buf (util/->arrow-buf-view allocator nio-buffer)]
    (->arrow-buf-grid arrow-buf)))

(defn ->disk-grid
  (^core2.temporal.grid.SimpleGrid [^BufferAllocator allocator ^Path path points {:keys [^long max-histogram-bins ^long cell-size ^long k]
                                                                                  :or {max-histogram-bins 256
                                                                                       cell-size (* 8 1024)}}]
   (assert (number? k))
   (util/mkdirs (.getParent path))
   (let [^long total (if (satisfies? kd/KdTree points)
                       (kd/kd-tree-size points)
                       (count points))
         _ (assert (= 1 (Long/bitCount cell-size)))
         number-of-cells (Math/ceil (/ total cell-size))
         k-minus-one (dec k)
         cells-per-dimension (BitUtil/ceilPowerOfTwo (Math/ceil (Math/pow number-of-cells (/ 1 k-minus-one))))
         number-of-cells (Math/ceil (Math/pow cells-per-dimension k-minus-one))
         axis-shift (Long/bitCount (dec cells-per-dimension))
         histogram-bins (min max-histogram-bins (* 2 cells-per-dimension))
         ^List histograms (vec (repeatedly k #(hist/->histogram histogram-bins)))
         update-histograms-fn (fn [^longs p]
                                (dotimes [n k]
                                  (.update ^IHistogram (.get histograms n) (aget p n))))
         cells (object-array number-of-cells)
         cell-paths (object-array number-of-cells)]
     (if (satisfies? kd/KdTree points)
       (let [^IKdTreePointAccess access (kd/kd-tree-point-access points)]
         (.forEach ^LongStream (kd/kd-tree-points points)
                   (reify LongConsumer
                     (accept [_ x]
                       (update-histograms-fn (.getArrayPoint access x))))))
       (doseq [p points]
         (update-histograms-fn (kd/->longs p))))
     (try
       (let [scales (object-array (for [^IHistogram h histograms]
                                    (long-array (.uniform h cells-per-dimension))))
             mins (long-array (for [^IHistogram h histograms]
                                (Math/floor (.getMin h))))
             maxs (long-array (for [^IHistogram h histograms]
                                (Math/ceil (.getMax h))))
             k-minus-one-mins (long-array number-of-cells Long/MAX_VALUE)
             k-minus-one-maxs (long-array number-of-cells Long/MIN_VALUE)
             k-minus-one-slope+base (double-array (* 2 number-of-cells))
             write-point-fn (fn [^longs p]
                              (let [cell-idx (->cell-idx scales p k-minus-one axis-shift)
                                    ^DataOutputStream out (or (aget cells cell-idx)
                                                              (let [f (str "." (.getFileName path) (format "_cell_%016x.raw" cell-idx))
                                                                    cell-path (.resolveSibling path f)]
                                                                (aset cell-paths cell-idx cell-path)
                                                                (let [file-ch (util/->file-channel cell-path util/write-new-file-opts)]
                                                                  (try
                                                                    (doto (DataOutputStream. (BufferedOutputStream. (Channels/newOutputStream file-ch)))
                                                                      (->> (aset cells cell-idx)))
                                                                    (catch Exception e
                                                                      (util/try-close file-ch)
                                                                      (throw e))))))]
                                (dotimes [n k]
                                  (let [x (aget p n)]
                                    (when (= n k-minus-one)
                                      (aset k-minus-one-mins cell-idx (min x (aget k-minus-one-mins cell-idx)))
                                      (aset k-minus-one-maxs cell-idx (max x (aget k-minus-one-maxs cell-idx))))
                                    (.writeLong out x)))))]
         (if (satisfies? kd/KdTree points)
           (let [^IKdTreePointAccess access (kd/kd-tree-point-access points)]
             (.forEach ^LongStream (kd/kd-tree-points points)
                       (reify LongConsumer
                         (accept [_ x]
                           (write-point-fn (.getArrayPoint access x))))))
           (doseq [p points]
             (write-point-fn (kd/->longs p))))
         (dotimes [n number-of-cells]
           (util/try-close (aget cells n))
           (when-let [^Path cell-path (aget cell-paths n)]
             (let [min-r (aget k-minus-one-mins n)
                   max-r (aget k-minus-one-maxs n)
                   value-count (quot (util/path-size cell-path) k)
                   slope (double (/ value-count (- max-r min-r)))
                   base (- (* slope min-r))
                   slope-idx (* 2 n)]
               (aset k-minus-one-slope+base slope-idx slope)
               (aset k-minus-one-slope+base (inc slope-idx) base))))
         (let [max-cell-size (reduce max (for [^Path cell-path cell-paths
                                               :when cell-path]
                                           (quot (util/path-size cell-path) (* k Long/BYTES))))
               cell-shift (Long/bitCount (dec (BitUtil/ceilPowerOfTwo max-cell-size)))
               grid-meta (json/write-str {:scales scales
                                          :mins mins
                                          :maxs maxs
                                          :k-minus-one-slope+base k-minus-one-slope+base
                                          :k k
                                          :axis-shift axis-shift
                                          :cell-shift cell-shift
                                          :total total})
               schema (Schema. [(kd/->point-field k)] {"grid-meta" grid-meta})
               buf (long-array k)]
           (with-open [root (VectorSchemaRoot/create schema allocator)
                       ch (util/->file-channel path util/write-new-file-opts)
                       out (ArrowFileWriter. root nil ch)]
             (let [^IKdTreePointAccess out-access (kd/kd-tree-point-access root)
                   ^FixedSizeListVector point-vec (.getVector root point-vec-idx)]
               (dotimes [n number-of-cells]
                 (.clear root)
                 (when-let [^Path cell-path (aget cell-paths n)]
                   (with-open [in (DataInputStream. (BufferedInputStream. (Channels/newInputStream (util/->file-channel cell-path))))]
                     (let [value-count (quot (util/path-size cell-path) (* k Long/BYTES))]
                       (dotimes [_ value-count]
                         (dotimes [m k]
                           (aset buf m (.readLong in)))
                         (kd/write-point point-vec out-access buf))
                       (.setRowCount root value-count)))
                   (util/delete-file cell-path)
                   (quick-sort out-access 0 (dec (.getRowCount root)) k-minus-one))
                 (.writeBatch out))))))
       path
       (finally
         (doseq [cell cells]
           (util/try-close cell))
         (doseq [cell-path cell-paths
                 :when cell-path]
           (util/delete-file cell-path)))))))
