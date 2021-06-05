(ns core2.temporal.histogram
  (:import [java.util ArrayList Collections List]))

;; "A Streaming Parallel Decision Tree Algorithm"
;; https://www.jmlr.org/papers/volume11/ben-haim10a/ben-haim10a.pdf

(set! *unchecked-math* :warn-on-boxed)

(definterface IHistogram
  (^core2.temporal.histogram.IHistogram update [^double x])
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

(defn ->histogram ^core2.temporal.histogram.Histogram [^long max-bins]
  (Histogram. max-bins 0 Double/MAX_VALUE Double/MIN_VALUE (ArrayList. (inc max-bins))))
