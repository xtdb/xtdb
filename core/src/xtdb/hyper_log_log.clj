(ns xtdb.hyper-log-log
  (:require [xtdb.memory :as mem])
  (:import [org.agrona DirectBuffer MutableDirectBuffer]
           java.nio.ByteOrder))

;; http://dimacs.rutgers.edu/~graham/pubs/papers/cacm-sketch.pdf
;; http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf

(def ^{:tag 'int} default-buffer-size (* Integer/BYTES 1024))

(defn add ^org.agrona.MutableDirectBuffer [^MutableDirectBuffer hll v]
  (let [m (quot (.capacity hll) Integer/BYTES)
        b (Integer/numberOfTrailingZeros m)
        x (mix-collection-hash (clojure.lang.Util/hasheq v) 0)
        j (bit-and (bit-shift-right x (- Integer/SIZE b)) (dec m))
        w (bit-and x (dec (bit-shift-left 1 (- Integer/SIZE b))))]
    (doto hll
      (.putInt (* j Integer/BYTES)
               (max (.getInt hll (* j Integer/BYTES) ByteOrder/BIG_ENDIAN)
                    (- (inc (Integer/numberOfLeadingZeros w)) b))
               ByteOrder/BIG_ENDIAN))))

(defn estimate ^double [^DirectBuffer hll]
  (let [m (quot (.capacity hll) Integer/BYTES)
        z (/ 1.0 (double (loop [n 0
                                acc 0.0]
                           (if (< n (.capacity hll))
                             (recur (+ n Integer/BYTES)
                                    (+ acc (Math/pow 2.0 (- (.getInt hll n ByteOrder/BIG_ENDIAN)))))
                             acc))))
        am (/ 0.7213 (inc (/ 1.079 m)))
        e (* am (Math/pow m 2.0) z)]
    (cond
      (<= e (* (double (/ 5 2)) m))
      (let [v (long (loop [n 0
                           acc 0]
                      (if (< n (.capacity hll))
                        (recur (+ n Integer/BYTES)
                               (+ acc (if (zero? (.getInt hll n ByteOrder/BIG_ENDIAN))
                                        1
                                        0)))
                        acc)))]
        (if (zero? v)
          e
          (* m (Math/log (/ m v)))))
      (> e (* (double (/ 1 30)) (Integer/toUnsignedLong -1)))
      (* (Math/pow -2.0 32)
         (Math/log (- 1 (/ e (Integer/toUnsignedLong -1)))))
      :else
      e)))

(defn combine ^DirectBuffer [^DirectBuffer hll-a ^DirectBuffer hll-b]
  (assert (= (.capacity hll-a) (.capacity hll-b)))
  (loop [n 0
         result ^MutableDirectBuffer (mem/allocate-unpooled-buffer (.capacity hll-a))]
    (if (= n (.capacity result))
      result
      (recur (+ n Integer/BYTES)
             (doto result
               (.putInt n (max (.getInt hll-a n ByteOrder/BIG_ENDIAN)
                               (.getInt hll-b n ByteOrder/BIG_ENDIAN)) ByteOrder/BIG_ENDIAN))))))

(defn estimate-union ^double [^DirectBuffer hll-a ^DirectBuffer hll-b]
  (estimate (combine hll-a hll-b)))

(defn estimate-intersection ^double [^DirectBuffer hll-a ^DirectBuffer hll-b]
  (- (+ (estimate hll-a)
        (estimate hll-b))
     (estimate-union hll-a hll-b)))

(defn estimate-difference ^double [^DirectBuffer hll-a ^DirectBuffer hll-b]
  (- (estimate hll-a)
     (estimate-intersection hll-a hll-b)))
