(ns core2.temporal.histogram
  (:import [java.util ArrayList Collections List]))

;; "A Streaming Parallel Decision Tree Algorithm"
;; https://www.jmlr.org/papers/volume11/ben-haim10a/ben-haim10a.pdf

;; NOTES, checkout:
;; http://engineering.nyu.edu/~suel/papers/pxp.pdf
;; https://www.researchgate.net/publication/2459804_Approximating_Multi-Dimensional_Aggregate_Range_Queries_Over_Real_Attributes
;; https://ashraf.aboulnaga.me/pubs/sigmod99sthist.pdf

;; I think adapting the current histogram to store points in the bins
;; and figure out an efficient way to calculate nearest points during
;; updates might be enough, and then use the normal sum/uniform across
;; a single axis at at time on the result.

;; We might need a way which can accept many empty buckets and
;; maintains a mapping of sparse cell-idxs to dense block-idxs.

;; Also, check out the Tsunami paper again.

(set! *unchecked-math* :warn-on-boxed)

(definterface IHistogram
  (^core2.temporal.histogram.IHistogram update [^double x])
  (^double sum [^double x])
  (^doubles uniform [^int number-of-buckets])
  (^double getMin [])
  (^double getMax [])
  (^long getTotal [])
  (^java.util.List getBins [])
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
  (update [this p]
    (set! (.total this) (inc total))
    (set! (.min-v this) (min p min-v))
    (set! (.max-v this) (max p max-v))

    (let [new-bin (Bin. p 1)
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
            ^IBin bin-i (.get bins min-idx)
            ^IBin bin-i+1 (.get bins (inc min-idx))
            qi (.getValue bin-i)
            ki (.getCount bin-i)
            qi+1 (.getValue bin-i+1)
            ki+1 (.getCount bin-i+1)
            new-k (+ ki ki+1)
            new-q (/ (+ (* qi ki) (* qi+1 ki+1)) new-k)
            new-bin (Bin. new-q new-k)]
        (doto bins
          (.remove (inc min-idx))
          (.set min-idx new-bin))))

    this)

  (sum [this b]
    (let [last-idx (dec (.size bins))]
      (cond
        (< b (.getValue ^IBin (.get bins 0))) 0
        (>= b (.getValue ^IBin (.get bins last-idx))) total
        :else
        (let [probe-bin (Bin. b 0)
              idx (Collections/binarySearch bins probe-bin)
              ^long idx (if (neg? idx)
                          (- (- idx) 2)
                          idx)]
          (cond
            (neg? idx)
            0.0

            (> idx last-idx)
            total

            :else
            (let [^IBin bin-i (.get bins idx)
                  ^IBin bin-i+1 (if (= idx last-idx)
                                  (Bin. (inc max-v) 0)
                                  (.get bins (inc idx)))
                  pi (.getValue bin-i)
                  mi (.getCount bin-i)
                  pi+1 (.getValue bin-i+1)
                  mi+1 (.getCount bin-i+1)

                  mb (+ mi (* (/ (- mi+1 mi) (- pi+1 pi))
                              (- b pi)))
                  s (* (/ (+ mi mb) 2.0)
                       (/ (- b pi) (- pi+1 pi)))]
              (loop [n 0
                     s s]
                (if (< n idx)
                  (recur (inc n) (+ s (.getCount ^IBin (.get bins n))))
                  (+ s (/ mi 2.0))))))))))

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
                                   (max 0 (dec idx)))))
                   ^IBin bin-i (.get bins idx)
                   ^IBin bin-i+1 (if (= idx last-idx)
                                   (Bin. (inc max-v) 0)
                                   (.get bins (inc idx)))
                   pi (.getValue bin-i)
                   mi (.getCount bin-i)
                   pi+1 (.getValue bin-i+1)
                   mi+1 (.getCount bin-i+1)
                   d (- s (.sum this pi))
                   a (- mi+1 mi)
                   b (* 2.0 mi)
                   c (- (* 2.0 d))
                   ;; NOTE: unsure if this NaN handling is correct?
                   z (if (zero? a)
                       (- (/ c b))
                       (/ (+ (- b) (Math/sqrt (Math/abs (- (* b b) (* 4.0 a c)))))
                          (* 2.0 a)))]]
         (+ pi (* (- pi+1 pi) z))))))

  (getMin [this]
    min-v)

  (getMax [this]
    max-v)

  (getTotal [this]
    total)

  (getBins [this]
    bins)

  (histogramString [this]
    (str "total: " total " min: " min-v " max: " max-v "\n"
         (apply str (for [^IBin b bins
                          :let [k (.getValue b)
                                v (.getCount b)]]
                      (str (format "%10.4f"  k) "\t" (apply str (repeat (* 200 (double (/ v total))) "*")) "\n"))))))

(defn ->histogram ^core2.temporal.histogram.Histogram [^long max-bins]
  (Histogram. max-bins 0 Double/MAX_VALUE Double/MIN_VALUE (ArrayList. (inc max-bins))))
