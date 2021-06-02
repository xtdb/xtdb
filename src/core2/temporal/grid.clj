(ns core2.temporal.grid
  (:require [core2.temporal.kd-tree :as kd])
  (:import core2.temporal.kd_tree.IKdTreePointAccess
           core2.BitUtil
           [java.util ArrayList Arrays Collection HashMap List Map Map$Entry NavigableMap TreeMap]
           [java.util.function BiFunction Function]
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
              ^long cell-axis-idx (if (pos? cell-axis-idx)
                                    cell-axis-idx
                                    (dec (- cell-axis-idx)))]
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
              ^long cell-axis-idx (if (pos? cell-axis-idx)
                                    cell-axis-idx
                                    (dec (- cell-axis-idx)))]
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
                                     ^long cell-axis-idx (if (pos? cell-axis-idx)
                                                           cell-axis-idx
                                                           (dec (- cell-axis-idx)))]]
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

;; TODO: implement sum and uniform from paper.
;; try p2 algorithm instead:
;; https://www.researchgate.net/publication/255672978_The_P_2_algorithm_for_dynamic_calculation_of_quantiles_and_histograms_without_storing_observations
;; https://bitbucket.org/scassidy/livestats/src/master/livestats/livestats.py
;; https://github.com/absmall/p2/blob/master/p2.cc

(definterface IHistogram
  (^core2.temporal.grid.IHistogram update [^double x])
  (^double quantile [^double q])
  (^double cdf [^double x])
  (^double getMin [])
  (^double getMax [])
  (^long getTotal [])
  (^String histogramString []))

(deftype Histogram [^int max-bins
                    ^:unsynchronized-mutable ^long total
                    ^:unsynchronized-mutable ^double min-v
                    ^:unsynchronized-mutable ^double max-v
                    ^NavigableMap bins]
  IHistogram
  (update [this x]
    (set! (.total this) (inc total))
    (set! (.min-v this) (min x min-v))
    (set! (.max-v this) (max x max-v))
    (when-not (.computeIfPresent bins x (reify BiFunction
                                          (apply [_ k v]
                                            (inc (long v)))))
      (.put bins x 1)
      (while (> (.size bins) max-bins)
        (let [kv1+kv2 (->> (partition 2 1 bins)
                           (apply min-key (fn [[^Map$Entry kv1 ^Map$Entry kv2]]
                                            (- ^double (.getKey kv2) ^double (.getKey kv1)))))
              ^Map$Entry kv1 (first kv1+kv2)
              ^double k1 (.getKey kv1)
              ^long v1 (.getValue kv1)
              ^Map$Entry kv2 (second kv1+kv2)
              ^double k2 (.getKey kv2)
              ^long v2 (.getValue kv2)
              new-v (+ v1 v2)
              new-k (/ (+ (* k1 v1) (* k2 v2)) new-v)]
          (doto bins
            (.remove k1)
            (.remove k2)
            (.put new-k new-v)))))
    this)

  (quantile [this q]
    (loop [[[k ^long v] & bins] bins
           count (* q total)]
      (if (and (pos? count) v)
        (recur bins (- count v))
        (or k -1))))

  (cdf [this x]
    (/ (long (reduce + (map val (.headMap bins x true)))) total))

  (getMin [this]
    min-v)

  (getMax [this]
    max-v)

  (getTotal [this]
    total)

  (histogramString [this]
    (str "total: " total " min: " min-v " max: " max-v "\n"
         (apply str (for [[k ^long v] bins]
                      (str (format "%10.4f"  k) "\t" (apply str (repeat (* 20 max-bins (double (/ v total))) "*")) "\n"))))))

(defn ->histogram ^core2.temporal.grid.Histogram [^long max-bins]
  (Histogram. max-bins 0 Double/MAX_VALUE Double/MIN_VALUE (TreeMap.)))
