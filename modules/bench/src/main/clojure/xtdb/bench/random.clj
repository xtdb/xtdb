(ns xtdb.bench.random
  (:import [java.time Instant LocalDate LocalTime ZoneOffset]
           (java.util UUID)))

(defn next-int
  ([^java.util.Random random]
   (next-int random Integer/MAX_VALUE))
  ([^java.util.Random random bound]
   (.nextInt random (min (max 1 bound) Integer/MAX_VALUE))))

(defn next-long
  ([^java.util.Random random]
   (next-long random Long/MAX_VALUE))
  ([^java.util.Random random bound]
   (.nextLong random (min (max 1 bound) Long/MAX_VALUE))))

(defn uniform-nth
  [^java.util.Random random coll]
  (let [c (count coll)]
    (when (pos? c)
      (nth coll (next-int random c)))))

(defn chance?
  [^java.util.Random random ^double p]
  (< (.nextDouble random) p))

(defn weighted-nth
  "Draw a single element from xs with non-negative weights.
   Returns nil if xs empty. If all weights are zero, falls back to uniform."
  [^java.util.Random rng weights xs]
  (let [ws (mapv #(double (max 0.0 %)) weights)
        xs (vec xs)
        n  (count xs)]
    (when (pos? n)
      (let [s (reduce + 0.0 ws)]
        (if (pos? s)
          (let [target (* (.nextDouble rng) s)]
            (loop [i 0 acc 0.0]
              (let [acc' (+ acc (ws i))]
                (if (or (>= acc' target) (= i (dec n)))
                  (xs i)
                  (recur (inc i) acc')))))
          ;; uniform fallback
          (xs (next-int rng n)))))))

(defn next-uuid
  "Generate a version-4 UUID using the supplied RNG, matching java.util.UUID/randomUUID idioms."
  [^java.util.Random random]
  (let [msb (.nextLong random)
        lsb (.nextLong random)
        msb (-> msb
                (bit-and (bit-not (bit-shift-left 0xF 12)))
                (bit-or  (bit-shift-left 0x4 12)))
        lsb (-> lsb
                (bit-and (bit-not (bit-shift-left 0x3 62)))
                (bit-or  (bit-shift-left 0x2 62)))]
    (UUID. msb lsb)))

(def ^:private ^String charset "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

(defn next-string
  ^String [^java.util.Random random ^long n]
  (let [sb (StringBuilder. n)]
    (loop [i 0]
      (if (< i n)
        (do (.append sb (.charAt charset (next-int random (.length charset))))
            (recur (unchecked-inc i)))
        (.toString sb)))))

(defn next-instant
  "Return a random instant between the beginning of `start-year` (inclusive)
  and the beginning of `(inc end-year)` (exclusive).

  Defaults to covering the last 10 calendar years."
  ([^java.util.Random random]
   (let [zone ZoneOffset/UTC
         end-year (.getYear (LocalDate/now zone))
         start-year (- end-year 10)]
     (next-instant random start-year end-year)))
  ([^java.util.Random random ^long start-year ^long end-year]
   (let [[^long start-year ^long end-year]
         (if (> start-year end-year)
           [end-year start-year]
           [start-year end-year])
         zone ZoneOffset/UTC
         ^Instant start (-> (LocalDate/of start-year 1 1)
                            (.atStartOfDay zone)
                            (.toInstant))
         ^Instant end   (-> (LocalDate/of (inc end-year) 1 1)
                            (.atStartOfDay zone)
                            (.toInstant))
         start-ms (.toEpochMilli start)
         end-ms (.toEpochMilli end)
         span (max 0 (- end-ms start-ms))
         offset (if (pos? span)
                  (long (Math/floor (* (.nextDouble random) (double span))))
                  0)]
     (Instant/ofEpochMilli (+ start-ms offset)))))

(defn next-local-time [^java.util.Random random]
  (LocalTime/of (next-int random 24) (next-int random 60) (next-int random 60)))

(defn shuffle
  "Shuffle a collection using the provided RNG (Fisher-Yates)."
  [^java.util.Random random coll]
  (let [arr (object-array coll)
        n (alength arr)]
    (loop [i (dec n)]
      (when (pos? i)
        (let [j (next-int random (inc i))
              tmp (aget arr i)]
          (aset arr i (aget arr j))
          (aset arr j tmp)
          (recur (dec i)))))
    (vec arr)))
