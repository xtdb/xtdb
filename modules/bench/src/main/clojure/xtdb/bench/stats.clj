(ns xtdb.bench.stats)

(defn round-down [x]
  (long (Math/floor (double x))))

(defn percentile
  "Calculate percentile from sorted values.
   Uses nearest-rank method: returns value at index floor(p * n)."
  [sorted-vals p]
  (let [n (count sorted-vals)
        idx (-> (* p (dec n)) Math/floor long (max 0) (min (dec n)))]
    (nth sorted-vals idx)))

(defn distribution-stats
  "Compute summary statistics from a sequence of numeric values.
   Returns map with :count, :total, :min, :max, :mean, and percentiles (:p50, :p90, :p95, :p99)."
  [values]
  (when (seq values)
    (let [sorted (vec (sort values))
          n (count sorted)
          total (reduce + 0 sorted)
          mean (/ total (double n))]
      {:count n
       :total total
       :min (first sorted)
       :max (peek sorted)
       :mean mean
       :p50 (percentile sorted 0.50)
       :p90 (percentile sorted 0.90)
       :p95 (percentile sorted 0.95)
       :p99 (percentile sorted 0.99)})))

(defn timing-stats
  "Calculate statistics from a collection of timing values (in nanoseconds).
   Returns map with count, total-ms, min-ms, max-ms, mean-ms, and percentiles."
  [timings-ns]
  (when-let [stats (distribution-stats timings-ns)]
    (let [->ms #(/ % 1e6)]
      {:count (:count stats)
       :total-ms (->ms (:total stats))
       :min-ms (->ms (:min stats))
       :max-ms (->ms (:max stats))
       :mean-ms (->ms (:mean stats))
       :p50-ms (->ms (:p50 stats))
       :p90-ms (->ms (:p90 stats))
       :p95-ms (->ms (:p95 stats))
       :p99-ms (->ms (:p99 stats))})))

(defn draw-lognormal
  "Draw a log-normal random variable using the underlying normal distribution
  parameters `mu` (mean) and `sigma` (standard deviation)."
  ^double [^java.util.Random random mu sigma]
  (Math/exp (+ mu (* sigma (.nextGaussian random)))))

(defn draw-poisson
  "Knuth's algorithm for small λ; Normal approximation for large λ.
   Returns a non-negative long."
  ^long [^java.util.Random random ^double lambda]
  (cond
    (<= lambda 0.0) 0
    (<= lambda 50.0)
    (let [L (Math/exp (- lambda))]
      (loop [k 0, p 1.0]
        (let [p (* p (.nextDouble random))]
          (if (> p L)
            (recur (inc k) p)
            k))))
    :else
    (max 0 (long (Math/round (+ lambda (* (Math/sqrt lambda)
                                          (.nextGaussian random))))))))

(defn draw-lognormal-poisson
  "Draw a Poisson count whose rate λ is sampled from a log-normal distribution
  with the given mean and median.

  For a log-normal distribution, median = exp(mu) and mean = exp(mu + σ²/2).
  Rearranging gives us the standard deviation σ = sqrt(2 * log(mean / median)).

  Takes an optional scale parameter to fit desired distribution"
  ([random mean median]
   (draw-lognormal-poisson random mean median 1.0))
  ([random mean median scale]
   (when-not (pos? median)
     (throw (IllegalArgumentException. "median must be positive for log-normal sampling")))
   (let [;; Calculate sigma from mean and median
         sigma (Math/sqrt (* 2.0 (Math/log (/ (double mean) (double median)))))
         ;; Calculate mu for the log-normal distribution s.t. E[λ]≈mean
         mu (- (Math/log (max mean 1e-9)) (/ (* sigma sigma) 2.0))
         lambda (draw-lognormal random mu sigma)]
     (draw-poisson random (* scale lambda)))))
