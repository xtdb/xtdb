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
