(ns core2.temporal.grid
  (:require [core2.temporal.kd-tree :as kd])
  (:import core2.temporal.kd_tree.IKdTreePointAccess
           core2.BitUtil
           [java.util ArrayList Arrays Collection Collections Comparator HashMap List Map]
           [java.util.function BiFunction Function LongPredicate]
           java.util.stream.LongStream
           java.nio.LongBuffer))

;; TODO: Move this to use histograms via
;; https://www.jmlr.org/papers/volume11/ben-haim10a/ben-haim10a.pdf to
;; build its scales (via one pass, see Appendix A), and don't
;; implement a real grid-file. Also, see
;; https://github.com/VividCortex/gohistogram
;; https://www.researchgate.net/publication/2611689_A_One-Pass_Space-Efficient_Algorithm_for_Finding_Quantiles

;; http://www.mathcs.emory.edu/~cheung/Courses/554/Syllabus/3-index/grid.html

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

(deftype GridFile [^int k ^int data-page-size ^List scales ^Map directory ^List data-pages]
  kd/KdTree
  (kd-tree-insert [this allocator point]
    (let [point (->longs point)
          cell-idx (long-array k)
          cell-key (long-array k)]
      (dotimes [n k]
        (let [x (aget point n)
              ^longs axis-scale (nth scales n)
              cell-axis-idx (Arrays/binarySearch axis-scale x)
              ^long cell-axis-idx (if (neg? cell-axis-idx)
                                    (dec (- cell-axis-idx))
                                    cell-axis-idx)]
          (aset cell-idx n cell-axis-idx)
          (aset cell-key n (aget axis-scale cell-axis-idx))))
      (let [cell-key (LongBuffer/wrap cell-key)
            ^long data-page-id (.computeIfAbsent directory cell-key (reify Function
                                                                      (apply [_ k]
                                                                        (.add data-pages (ArrayList.))
                                                                        (dec (.size data-pages)))))
            ^List data-page (.get data-pages data-page-id)]
        (.add data-page point)
        (when (> (.size data-page) data-page-size)
          (let [shared-entries (for [[k v :as kv] directory
                                     :when (= v data-page-id)]
                                 kv)]
            (if (> (count shared-entries) 1)
              (let [[^LongBuffer first-cell-key ^LongBuffer second-cell-key] (sort (keys shared-entries))
                    split-axis (->> (map = (.array first-cell-key) (.array second-cell-key))
                                    (take-while true?)
                                    (count))
                    new-min (.get second-cell-key split-axis)
                    [^List old-data-page ^List new-data-page] (->> (sort-by #(aget ^longs % split-axis) data-page)
                                                                   (split-with #(< (aget ^longs % split-axis) new-min)))]
                (.set data-pages data-page-id (ArrayList. old-data-page))
                (.add data-pages (ArrayList. new-data-page))
                (doseq [[^LongBuffer k] shared-entries]
                  (.put directory k (if (= new-min (.get k split-axis))
                                      (.size data-pages)
                                      data-page-id))))
              (let [^long split-axis (ffirst (sort-by (comp count second) (map-indexed vector scales)))
                    [^List old-data-page ^List new-data-page] (->> (sort-by #(aget ^longs % split-axis) data-page)
                                                                   (split-at (quot data-page-size 2)))
                    new-axis-idx (inc (aget cell-idx split-axis))
                    old-min (.get cell-key split-axis)
                    new-min (aget ^longs (first new-data-page) split-axis)
                    new-cell-key (LongBuffer/wrap (doto (long-array (.array cell-key))
                                                    (aset split-axis new-min)))]
                (.set data-pages data-page-id (ArrayList. old-data-page))
                (.add data-pages (ArrayList. new-data-page))
                (.put directory new-cell-key (.size data-pages))
                (doseq [[^LongBuffer cell-key-to-split existing-data-page] (for [[^LongBuffer k :as kv] directory
                                                                                 :when (= old-min (.get k split-axis))]
                                                                             kv)]
                  (.put directory (LongBuffer/wrap (doto (long-array (.array cell-key-to-split))
                                                     (aset split-axis new-min))) existing-data-page))
                (.set scales split-axis (long-array (doto (ArrayList. ^List (vec (.get scales split-axis)))
                                                      (.add new-axis-idx new-min))))))))
        this)))
  (kd-tree-delete [this allocator point]
    (let [point (->longs point)
          cell-key (long-array k)]
      (dotimes [n k]
        (let [x (aget point n)
              ^longs axis-scale (nth scales n)
              cell-axis-idx (Arrays/binarySearch axis-scale x)
              ^long cell-axis-idx (if (neg? cell-axis-idx)
                                    (dec (- cell-axis-idx))
                                    cell-axis-idx)]
          (aset cell-key n (aget axis-scale cell-axis-idx))))
      (let [cell-key (LongBuffer/wrap cell-key)
            ^long data-page-id (.computeIfAbsent directory cell-key (reify Function
                                                                      (apply [_ k]
                                                                        (.add data-pages (ArrayList.))
                                                                        (dec (.size data-pages)))))
            ^List data-page (.get data-pages data-page-id)]
        (doseq [idx (for [[idx p] (map-indexed vector data-page)
                          :when (= (LongBuffer/wrap p) cell-key)]
                      idx)]
          (.remove data-page idx))
        this)))
  (kd-tree-range-search [this min-range max-range]
    (let [min-range (->longs min-range)
          max-range (->longs max-range)
          axis-cell-keys (for [n (range k)
                               :let [x (aget min-range n)
                                     y (aget max-range n)
                                     ^longs axis-scale (nth scales n)
                                     splits (alength axis-scale)
                                     cell-axis-idx (Arrays/binarySearch axis-scale x)
                                     ^long cell-axis-idx (if (neg? cell-axis-idx)
                                                           (dec (- cell-axis-idx))
                                                           cell-axis-idx)]]
                           (take-while #(<= ^long % y) (drop (dec cell-axis-idx) axis-scale)))
          data-page-ids (for [cell-key (distinct (cartesian-product axis-cell-keys))
                              :when (= k (count cell-key))
                              :let [cell-key (LongBuffer/wrap (->longs cell-key))
                                    data-page-id (.get directory cell-key)]
                              :when data-page-id]
                          data-page-id)
          ^IKdTreePointAccess access (kd/kd-tree-point-access this)
          axis-mask (kd/range-bitmask min-range max-range)
          acc (LongStream/builder)]
      (loop [[data-page-id & data-page-ids] data-page-ids]
        (if-not data-page-id
          (.build acc)
          (let [data-page (.get data-pages data-page-id)
                data-page-offset (* data-page-size (long data-page-id))
                data-page-size (count data-page)]
            (dotimes [n data-page-size]
              (let [point (nth data-page n)]
                (when (.isInRange access k min-range max-range axis-mask)
                  (.add acc (+ data-page-offset n)))))
            (recur data-pages))))))
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
    (reduce + (map count data-pages)))
  (kd-tree-dimensions [_]
    k))

(deftype GridFilePointAccess [^GridFile gf ^int data-page-shift ^int data-page-mask]
  IKdTreePointAccess
  (getPoint [this idx]
    (vec (.getArrayPoint this idx)))
  (getArrayPoint [this idx]
    (nth (nth (.data-pages gf) (BitUtil/unsignedBitShiftRight idx data-page-shift))
         (bit-and idx data-page-mask)))
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
  (let [data-page-mask (dec (.data-page-size gf))]
    (GridFilePointAccess. gf (Long/bitCount data-page-mask) data-page-mask)))

(defn ->grid-file ^core2.temporal.grid.GridFile [allocator ^long k ^long data-page-size points]
  (assert (= 1 (Long/bitCount data-page-size)))
  (let [axis-splits (int-array k)
        scales (ArrayList. ^Collection (repeatedly k #(long-array 0)))
        directory (HashMap.)
        data-pages (ArrayList.)
        gf (GridFile. k data-page-size scales directory data-pages)]
    (reduce (fn [acc point]
              (kd/kd-tree-insert acc allocator point)) gf points)))

;; TODO: implement sum and uniform from paper. Arrays for faster updates.
;; https://github.com/soundcloud/spdt/blob/master/compute/src/main/scala/com.soundcloud.spdt/Histogram.scala

;; TODO: try p2 algorithm instead:
;; https://www.researchgate.net/publication/255672978_The_P_2_algorithm_for_dynamic_calculation_of_quantiles_and_histograms_without_storing_observations
;; https://bitbucket.org/scassidy/livestats/src/master/livestats/livestats.py
;; https://github.com/absmall/p2/blob/master/p2.cc

;; https://github.com/MaxHalford/streaming-cdf-benchmark
;; https://github.com/carsonfarmer/streamhist
;; https://github.com/bigmlcom/histogram

;; Also, TDigest:
;; https://github.com/henrygarner/t-digest

;; https://www.cs.rutgers.edu/~muthu/bquant.pdf
;; https://github.com/matttproud/python_quantile_estimation/blob/master/com/matttproud/quantile/__init__.py

(definterface IHistogram
  (^core2.temporal.grid.IHistogram update [^double x])
  (^double quantile [^double q])
  (^double cdf [^double x])
  (^double sum [^double x])
  (^doubles uniform [^int number-of-buckets])
  (^double getMin [])
  (^double getMax [])
  (^long getTotal [])
  (^String histogramString []))

(definterface IBin
  (^double getValue [])
  (^long getCount [])
  (^void increment []))

(deftype Bin [^double value ^:unsynchronized-mutable ^long count]
  IBin
  (getValue [_] value)

  (getCount [_] count)

  (increment [this]
    (set! (.count this) (inc count)))

  (equals [_ other]
    (= value (.value ^Bin other)))

  (hashCode [_]
    (Double/hashCode value))

  Comparable
  (compareTo [_ other]
    (Double/compare value (.value ^Bin other))))

(deftype Histogram [^int max-bins
                    ^:unsynchronized-mutable ^long total
                    ^:unsynchronized-mutable ^double min-v
                    ^:unsynchronized-mutable ^double max-v
                    ^List bins]
  IHistogram
  (update [this x]
    (set! (.total this) (inc total))
    (set! (.min-v this) (min x min-v))
    (set! (.max-v this) (max x max-v))

    (let [new-bin (Bin. x 1)
          idx (Collections/binarySearch bins new-bin)]
      (if (neg? idx)
        (.add bins (dec (- idx)) new-bin)
        (.increment ^IBin (.get bins idx))))

    (while (> (.size bins) max-bins)
      (let [^long min-idx (loop [n 0
                                 min-idx 0
                                 delta Double/MAX_VALUE]
                            (if (= n (dec (.size bins)))
                              min-idx
                              (let [new-delta (- (.getValue ^IBin (.get bins (inc n)))
                                                 (.getValue ^IBin (.get bins n)))]
                                (if (< new-delta delta)
                                  (recur (inc n) n new-delta)
                                  (recur (inc n) min-idx delta)))))
            ^IBin kv1 (.get bins min-idx)
            ^IBin kv2 (.get bins (inc min-idx))
            k1 (.getValue kv1)
            v1 (.getCount kv1)
            k2 (.getValue kv2)
            v2 (.getCount kv2)
            new-v (+ v1 v2)
            new-k (/ (+ (* k1 v1) (* k2 v2)) new-v)
            new-bin (Bin. new-k new-v)]
        (doto bins
          (.remove (inc min-idx))
          (.set min-idx new-bin))))

    this)

  (quantile [this q]
    (loop [[^IBin bin & bins] bins
           count (* q total)]
      (if-not bin
        max-v
        (let [v (.getCount bin)]
          (if (and (pos? count) v)
            (recur bins (- count v))
            (.getValue bin))))))

  (cdf [this x]
    (loop [[^IBin bin & bins] bins
           count 0]
      (if-not bin
        (double (/ count total))
        (if (<= (.getValue bin) x)
          (recur bins (+ count (.getCount bin)))
          (double (/ count total))))))

  (sum [this x]
    (let [last-idx (dec (.size bins))]
      (cond
        (< x (.getValue ^IBin (.get bins 0))) 0
        (>= x (.getValue ^IBin (.get bins last-idx))) total
        :else
        (let [probe-bin (Bin. x 0)
              idx (Collections/binarySearch bins probe-bin)
              ^long idx (if (neg? idx)
                          (dec (- idx))
                          idx)]
          (if (> idx last-idx)
            total
            (let [^IBin kv1 (.get bins idx)
                  ^IBin kv2 (if (= idx last-idx)
                              (Bin. 0 0)
                              (.get bins (inc idx)))
                  k1 (.getValue kv1)
                  v1 (.getCount kv1)
                  k2 (.getValue kv2)
                  v2 (.getCount kv2)

                  vx (+ v1 (* (/ (+ v2 v1) (- k2 k1))
                              (- x k1)))
                  s (* (/ (+ v1 vx) 2)
                       (/ (- x k1) (- k2 k1)))
                  ^double s (loop [n 0
                                   s s]
                              (if (< n idx)
                                (recur (inc n) (+ s (.getCount ^IBin (.get bins n))))
                                s))]
              (+ s (/ v1 2.0))))))))

  (uniform [this number-of-buckets]
    (let [last-idx (dec (.size bins))
          number-of-buckets number-of-buckets]
      (double-array
       (for [^long x (range 1 number-of-buckets)
             :let [s (* (double (/ x number-of-buckets)) total)
                   ^long idx (loop [[^IBin bin & bins] bins
                                    idx 0]
                               (if-not bin
                                 idx
                                 (if (< (.sum this (.getValue bin)) s)
                                   (recur bins (inc idx))
                                   idx)))
                   ^IBin kv1 (.get bins idx)
                   ^IBin kv2 (if (= idx last-idx)
                               (Bin. 0 0)
                               (.get bins (inc idx)))
                   k1 (.getValue kv1)
                   v1 (.getCount kv1)
                   k2 (.getValue kv2)
                   v2 (.getCount kv2)
                   d (- s (.sum this k1))
                   a (- v2 v1)
                   b (* 2.0 v1)
                   c (- (* 2.0 d))
                   ;; NOTE: unsure if this NaN handling is correct?
                   z (if (zero? a)
                       (- (/ c b))
                       (/ (+ (- b) (Math/sqrt (Math/abs (- (* b b) (* 4.0 a c)))))
                          (* 2.0 a)))]]
         (+ k1 (* (- k2 k1) z))))))

  (getMin [this]
    min-v)

  (getMax [this]
    max-v)

  (getTotal [this]
    total)

  (histogramString [this]
    (str "total: " total " min: " min-v " max: " max-v "\n"
         (apply str (for [^IBin b bins
                          :let [k (.getValue b)
                                v (.getCount b)]]
                      (str (format "%10.4f"  k) "\t" (apply str (repeat (* 200 (double (/ v total))) "*")) "\n"))))))

(defn ->histogram ^core2.temporal.grid.Histogram [^long max-bins]
  (Histogram. max-bins 0 Double/MAX_VALUE Double/MIN_VALUE (ArrayList. (inc max-bins))))

(definterface ISimpleGrid
  (^int cellIdx [^longs point]))

(declare ->simple-grid-point-access)

(deftype SimpleGrid [^List scales ^longs mins ^longs maxs ^List cells ^int k ^int axis-shift ^int cell-shift ^long total]
  ISimpleGrid
  (cellIdx [_ point]
    (loop [n 0
           idx 0]
      (if (= n k)
        idx
        (let [axis-idx (Arrays/binarySearch ^longs (.get scales n) (aget point n))
              ^long axis-idx (if (neg? axis-idx)
                               (dec (- axis-idx))
                               axis-idx)]
          (recur (inc n) (bit-or (bit-shift-left idx axis-shift) axis-idx))))))

  kd/KdTree
  (kd-tree-insert [this allocator point]
    (.add ^List (.get cells (.cellIdx this point)) (->longs point))
    this)
  (kd-tree-delete [this allocator point]
    (let [point (->longs point)
          ^List cell (.get cells (.cellIdx this point))
          it (.iterator cell)]
      (while (.hasNext it)
        (when (Arrays/equals ^longs (.next it) point)
          (.remove it))))
    this)
  (kd-tree-range-search [this min-range max-range]
    (let [min-range (->longs min-range)
          max-range (->longs max-range)
          axis-cell-idxs (for [n (range k)
                               :let [min-r (aget min-range n)
                                     max-r (aget max-range n)
                                     min-v (aget mins n)
                                     max-v (aget maxs n)]
                               :when (BitUtil/bitNot (or (< max-v min-r) (> min-v max-r)))
                               :let [^longs axis-scale (.get scales n)
                                     min-axis-idx (Arrays/binarySearch axis-scale min-r)
                                     min-axis-idx (if (neg? min-axis-idx)
                                                    (dec (- min-axis-idx))
                                                    min-axis-idx)
                                     max-axis-idx (Arrays/binarySearch axis-scale max-r)
                                     max-axis-idx (if (neg? max-axis-idx)
                                                    (- max-axis-idx)
                                                    (inc max-axis-idx))]
                               :when (not= min-axis-idx max-axis-idx)]
                           (range min-axis-idx max-axis-idx))
          cell-idxs (when (= k (count axis-cell-idxs))
                      (for [cell-idxs (cartesian-product axis-cell-idxs)
                            :let [cell-idxs (->longs cell-idxs)]]
                        (loop [n 0
                               idx 0]
                          (if (= n k)
                            idx
                            (let [axis-idx (aget cell-idxs n)]
                              (recur (inc n) (bit-or (bit-shift-left idx axis-shift) axis-idx)))))))
          ^IKdTreePointAccess access (kd/kd-tree-point-access this)
          axis-mask (kd/range-bitmask min-range max-range)
          acc (LongStream/builder)]
      (loop [[cell-idx & cell-idxs] cell-idxs]
        (if-not cell-idx
          (.build acc)
          (let [^long cell-idx cell-idx
                ^List cell (.get cells cell-idx)
                start-point-idx (bit-shift-left cell-idx cell-shift)]
            (dotimes [n (.size cell)]
              (let [idx (+ start-point-idx n)]
                (when (.isInRange access idx min-range max-range axis-mask)
                  (.add acc idx))))
            (recur cell-idxs))))))
  (kd-tree-points [this]
    (reduce
     (fn [^LongStream acc ^LongStream x]
       (LongStream/concat acc x))
     (LongStream/empty)
     (for [^List cell cells
           :when (BitUtil/bitNot (.isEmpty cell))
           :let [start-point-idx (bit-shift-left (.cellIdx this (.get cell 0)) cell-shift)]]
       (LongStream/range start-point-idx (+ start-point-idx (.size cell))))))
  (kd-tree-depth [_] 0)
  (kd-tree-retain [this _] this)
  (kd-tree-point-access [this]
    (->simple-grid-point-access this))
  (kd-tree-size [_] total)
  (kd-tree-value-count [_] total)
  (kd-tree-dimensions [_] k))

(deftype SimpleGridPointAccess [^SimpleGrid grid ^int cell-shift ^int cell-mask]
  IKdTreePointAccess
  (getPoint [this idx]
    (ArrayList. ^List (seq (.getArrayPoint this idx))))
  (getArrayPoint [this idx]
    (.get ^List (.get ^List (.cells grid) (BitUtil/unsignedBitShiftRight idx cell-shift))
          (bit-and idx cell-mask)))
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
          k (.k grid)]
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

(defn- ->simple-grid-point-access [^SimpleGrid grid]
  (let [cell-shift (.cell-shift grid)
        cell-mask (dec (bit-shift-left 1 cell-shift))]
    (SimpleGridPointAccess. grid cell-shift cell-mask)))

(defn ->simple-grid
  ([^long k points]
   (->simple-grid k points {}))
  ([^long k points {:keys [max-histogram-bins ^long cell-size]
                    :or {max-histogram-bins 16
                         cell-size 1024}}]
   (let [total (count points)
         _ (assert (= 1 (Long/bitCount cell-size)))
         number-of-cells (Math/ceil (/ total cell-size))
         cells-per-dimension (Long/highestOneBit (Math/ceil (Math/pow number-of-cells (/ 1 k))))
         _ (assert (= 1 (Long/bitCount cells-per-dimension)))
         number-of-cells (Math/ceil (Math/pow cells-per-dimension k))
         axis-shift (Long/bitCount (dec cells-per-dimension))
         cell-shift (* 2 (Long/bitCount (dec cell-size)))
         ^List histograms (vec (repeatedly k #(->histogram max-histogram-bins)))]
     (doseq [p points]
       (let [p (->longs p)]
         (dotimes [n k]
           (.update ^IHistogram (.get histograms n) (aget p n)))))
     (let [scales (ArrayList. ^List (vec (for [^IHistogram h histograms]
                                           (long-array (.uniform h cells-per-dimension)))))
           mins (long-array (for [^IHistogram h histograms]
                              (Math/floor (.getMin h))))
           maxs (long-array (for [^IHistogram h histograms]
                              (Math/ceil (.getMax h))))
           cells (ArrayList. ^List (repeatedly number-of-cells #(ArrayList.)))
           grid (SimpleGrid. scales mins maxs cells k axis-shift cell-shift total)]
       (doseq [p points
               :let [p (->longs p)
                     cell-idx (.cellIdx grid p)]]
         (.add ^List (.get cells cell-idx) p))
       grid))))
