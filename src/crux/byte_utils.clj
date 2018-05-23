(ns crux.byte-utils
  (:import [java.math BigInteger]
           [java.nio ByteBuffer]
           [java.net URI]
           [java.security MessageDigest]
           [java.util Arrays Comparator UUID]))

(set! *unchecked-math* :warn-on-boxed)

(defn hash-keyword [k]
  (hash (str (namespace k) (name k))))

(defn long->bytes [l]
  (-> (ByteBuffer/allocate 8)
      (.putLong l)
      (.array)))

(defn bytes->long ^long [data]
  (-> (ByteBuffer/allocate 8)
      (.put data 0 8)
      (.flip)
      (.getLong)))

(defn md5 ^bytes [^bytes bytes]
  (.digest (MessageDigest/getInstance "MD5") bytes))

(defn sha1 ^bytes [^bytes bytes]
  (.digest (MessageDigest/getInstance "SHA-1") bytes))

(def ^"[Ljava.lang.Object;" ^:private
  byte->hex (object-array (for [n (range 256)]
                            (char-array (format "%02x" n)))))

(defn bytes->hex ^String [^bytes bytes]
  (let [len (alength bytes)
        acc (char-array (bit-shift-left len 1))]
    (loop [idx 0]
      (if (= idx len)
        (String. acc)
        (let [cs ^chars (aget byte->hex (Byte/toUnsignedInt (aget bytes idx)))
              acc-idx (bit-shift-left idx 1)]
          (doto acc
            (aset acc-idx (aget cs 0))
            (aset (unchecked-inc-int acc-idx) (aget cs 1)))
          (recur (unchecked-inc-int idx)))))))

(defn hex->bytes ^bytes [^String hex]
  (let [len (count hex)
        acc (byte-array (bit-shift-right len 1))]
    (loop [idx 0]
      (if (= idx len)
        acc
        (let [b (unchecked-byte (bit-or (bit-shift-left (Character/digit (.charAt hex idx) 16) 4)
                                        (Character/digit (.charAt hex (unchecked-inc-int idx)) 16)))]
          (aset acc (bit-shift-right idx 1) b)
          (recur (unchecked-add-int idx 2)))))))

(defn byte-buffer->bytes ^bytes [^ByteBuffer b]
  (if (.hasArray b)
    (.array b)
    (doto (byte-array (.remaining b))
      (->> (.get b)))))

(defn compare-bytes
  (^long [^bytes a ^bytes b]
   (compare-bytes a b Integer/MAX_VALUE))
  (^long [^bytes a ^bytes b max-length]
   (let [a-length (int (alength a))
         b-length (int (alength b))
         max-length (int max-length)]
     (loop [idx (int 0)]
       (cond
         (= idx max-length)
         0

         (or (= idx a-length)
             (= idx b-length))
         (- a-length b-length)

         :else
         (let [diff (unchecked-subtract-int (Byte/toUnsignedInt (aget a idx))
                                            (Byte/toUnsignedInt (aget b idx)))]
           (if (zero? diff)
             (recur (unchecked-inc-int idx))
             diff)))))))

(def ^Comparator bytes-comparator
  (reify Comparator
    (compare [_ a b]
      (compare-bytes a b))))

(defn bytes=?
  ([^bytes k1 ^bytes k2]
   (bytes=? k1 (alength k1) k2))
  ([^bytes k1 array-length ^bytes k2]
   (zero? (compare-bytes k1 k2 array-length))))

;; (def ^:private ^sun.misc.Unsafe
;;   the-unsafe
;;   (let [f (doto (.getDeclaredField sun.misc.Unsafe "theUnsafe")
;;             (.setAccessible true))]
;;     (.get ^java.lang.reflect.Field f nil)))

;; (def ^:private ^:const byte-array-base-offset
;;   ^{:tag 'long}
;;   (.arrayBaseOffset the-unsafe (class (byte-array 0))))

;; (def ^:private ^:const
;;   ^{:tag 'boolean}
;;   little-endian? (= java.nio.ByteOrder/LITTLE_ENDIAN
;;                     (java.nio.ByteOrder/nativeOrder)))

;; Inspired by
;; https://github.com/google/guava/blob/master/guava/src/com/google/common/primitives/UnsignedBytes.java#L365

;; NOTE: is not faster nor used at the moment.

#_(defn compare-bytes-unsafe ^long [^bytes a ^bytes b ^long max-length]
  (let [a-length (int (alength a))
        b-length (int (alength b))
        max-length (int (max a-length b-length max-length))
        the-unsafe the-unsafe]
    (loop [idx (int 0)]
      (cond
        (= idx max-length)
        0

        (or (= idx a-length)
            (= idx b-length))
        (unchecked-subtract-int a-length b-length)

        :else
        (let [array-idx (long (unchecked-add byte-array-base-offset idx))
              al (.getLong the-unsafe a array-idx)
              bl (.getLong the-unsafe b array-idx)]

          (if-not (= al bl)
            (if little-endian?
              (Long/compareUnsigned (Long/reverseBytes al)
                                    (Long/reverseBytes bl))
              (Long/compareUnsigned al bl))

            (let [next-idx (unchecked-add-int idx Long/BYTES)]
              (if (> next-idx max-length)
                (loop [idx idx]
                  (cond
                    (= idx max-length)
                    0

                    (or (= idx a-length)
                        (= idx b-length))
                    (unchecked-subtract-int a-length b-length)

                    :else
                    (let [diff (unchecked-subtract-int (Byte/toUnsignedInt (aget a idx))
                                                       (Byte/toUnsignedInt (aget b idx)))]
                      (if (zero? diff)
                        (recur (unchecked-inc-int idx))
                        diff))))
                (recur next-idx)))))))))
