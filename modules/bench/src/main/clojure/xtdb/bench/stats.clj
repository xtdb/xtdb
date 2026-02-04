(ns xtdb.bench.stats)

(defn round-down [x]
  (long (Math/floor (double x))))

(defn distribution-stats
  "Compute summary statistics from a sequence of numeric counts."
  [counts]
  (let [counts (vec (map long counts))
        n (count counts)
        sorted (vec (sort counts))
        mean (if (pos? n) (/ (reduce + 0 sorted) (double n)) 0.0)
        mn (if (pos? n) (first sorted) 0)
        mx (if (pos? n) (peek sorted) 0)
        idx (fn [p]
              (cond
                (zero? n) 0
                :else (-> (Math/floor (* p (dec n))) long (max 0) (min (dec n)))))
        median (if (pos? n) (sorted (idx 0.5)) 0)
        p90 (if (pos? n) (sorted (idx 0.90)) 0)
        p99 (if (pos? n) (sorted (idx 0.99)) 0)]
    {:mean mean :median median :p90 p90 :p99 p99 :min mn :max mx}))

(defn percentile
  "Calculate percentile from sorted timings"
  [sorted-timings p]
  (let [n (count sorted-timings)
        idx (min (dec n) (int (* p n)))]
    (nth sorted-timings idx)))

(defn timing-stats
  "Calculate statistics from a collection of timing values (in nanoseconds).
   Returns map with count, total-ms, min-ms, max-ms, mean-ms, and percentiles."
  [timings-ns]
  (when (seq timings-ns)
    (let [sorted (vec (sort timings-ns))
          n (count sorted)
          total-ns (reduce + sorted)
          mean-ns (/ total-ns n)
          ->ms #(/ % 1e6)]
      {:count n
       :total-ms (->ms total-ns)
       :min-ms (->ms (first sorted))
       :max-ms (->ms (last sorted))
       :mean-ms (->ms mean-ns)
       :p50-ms (->ms (percentile sorted 0.50))
       :p90-ms (->ms (percentile sorted 0.90))
       :p95-ms (->ms (percentile sorted 0.95))
       :p99-ms (->ms (percentile sorted 0.99))})))

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
