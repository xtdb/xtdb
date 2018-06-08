(ns crux.byte-utils
  (:import [java.lang.reflect Field]
           [java.math BigInteger]
           [java.nio Buffer ByteBuffer ByteOrder]
           [java.net URI]
           [java.security MessageDigest]
           [java.util Arrays Comparator UUID]
           [crux ByteUtils]))

(set! *unchecked-math* :warn-on-boxed)

(defn long->bytes [l]
  (-> (ByteBuffer/allocate 8)
      (.putLong l)
      (.array)))

(defn bytes->long ^long [data]
  (-> (ByteBuffer/allocate 8)
      (.put data 0 8)
      ^ByteBuffer (.flip)
      (.getLong)))

(defn sha1 ^bytes [^bytes bytes]
  (.digest (MessageDigest/getInstance "SHA-1") bytes))

(def ^"[Ljava.lang.Object;" ^:private
  byte->hex (object-array (for [n (range 256)]
                            (char-array (format "%02x" n)))))

(defn bytes->hex ^String [^bytes bytes]
  (ByteUtils/bytesToHex bytes))

(defn hex->bytes ^bytes [^String hex]
  (ByteUtils/hexToBytes hex))

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
  ByteUtils/UNSIGNED_BYTES_COMPARATOR)

(defn bytes=?
  ([^bytes k1 ^bytes k2]
   (bytes=? k1 (alength k1) k2))
  ([^bytes k1 array-length ^bytes k2]
   (zero? (ByteUtils/compareBytes k1 k2 array-length))))
