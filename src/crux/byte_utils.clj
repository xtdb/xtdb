(ns crux.byte-utils
  (:import [java.lang.reflect Field]
           [java.math BigInteger]
           [java.nio Buffer ByteBuffer ByteOrder]
           [java.net URI]
           [java.security MessageDigest]
           [java.util Arrays Comparator UUID]
           [sun.misc Unsafe]))

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
      ^ByteBuffer (.flip)
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

(defn byte-array-slice
  [^bytes bs index length]
  (let [dst (byte-array length)]
    (System/arraycopy bs index dst 0 length)
    dst))

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

(def ^:private ^Unsafe
  the-unsafe
  (let [f (doto (.getDeclaredField Unsafe "theUnsafe")
            (.setAccessible true))]
    (.get ^Field f nil)))

(def ^:private ^:const
  ^{:tag 'boolean}
  little-endian? (= ByteOrder/LITTLE_ENDIAN (ByteOrder/nativeOrder)))

(defn compare-bytes-unsafe ^long [^bytes a ^bytes b ^long max-length]
  (let [a-length (int (alength a))
        b-length (int (alength b))
        max-length (int max-length)
        the-unsafe the-unsafe
        max-stride-length (min a-length b-length max-length)]
    (loop [idx (int 0)]
      (cond
        (= idx max-length)
        0

        (or (= idx a-length)
            (= idx b-length))
        (unchecked-subtract-int a-length b-length)

        :else
        (let [next-idx (unchecked-add-int idx Long/BYTES)]
          (if (> next-idx max-stride-length)
            (loop [idx idx]
              (cond
                (= idx max-length)
                0

                (or (= idx a-length)
                    (= idx b-length))
                (unchecked-subtract-int a-length b-length)

                :else
                (let [array-idx (long (unchecked-add Unsafe/ARRAY_BYTE_BASE_OFFSET idx))
                      diff (unchecked-subtract-int (Byte/toUnsignedInt (.getByte the-unsafe a array-idx))
                                                   (Byte/toUnsignedInt (.getByte the-unsafe b array-idx)))]
                  (if (zero? diff)
                    (recur (unchecked-inc-int idx))
                    diff))))
            (let [array-idx (long (unchecked-add Unsafe/ARRAY_BYTE_BASE_OFFSET idx))
                  al (.getLong the-unsafe a array-idx)
                  bl (.getLong the-unsafe b array-idx)]
              (if-not (= al bl)
                (if little-endian?
                  (Long/compareUnsigned (Long/reverseBytes al)
                                        (Long/reverseBytes bl))
                  (Long/compareUnsigned al bl))
                (recur next-idx)))))))))

(def ^Comparator bytes-comparator
  (reify Comparator
    (compare [_ a b]
      (compare-bytes-unsafe a b Integer/MAX_VALUE))))

(defn bytes=?
  ([^bytes k1 ^bytes k2]
   (bytes=? k1 (alength k1) k2))
  ([^bytes k1 array-length ^bytes k2]
   (zero? (compare-bytes-unsafe k1 k2 array-length))))
