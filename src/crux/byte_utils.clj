(ns crux.byte-utils
  (:import [java.math BigInteger]
           [java.nio ByteBuffer ByteOrder]
           [java.net URI]
           [java.security MessageDigest]
           [java.util Comparator UUID]))

(defn hash-keyword [k]
  (hash (str (namespace k) (name k))))

(defn long->bytes [l]
  (-> (ByteBuffer/allocate 8)
      (.order (ByteOrder/nativeOrder))
      (.putLong l)
      (.array)))

(defn bytes->long [data]
  (-> (ByteBuffer/allocate 8)
      (.order (ByteOrder/nativeOrder))
      (.put data 0 8)
      (.flip)
      (.getLong)))

(defn uuid->bytes [^UUID uuid]
  (-> (ByteBuffer/allocate 16)
      (.order (ByteOrder/nativeOrder))
      (.putLong (.getMostSignificantBits uuid))
      (.putLong (.getLeastSignificantBits uuid))
      (.array)))

(def ^java.security.MessageDigest md5-algo (MessageDigest/getInstance "MD5"))

(defn md5 [#^bytes bytes]
  (.digest md5-algo bytes))

(defn to-byte-array [v]
  (cond (bytes? v)
        v

        (string? v)
        (.getBytes ^String v)

        (keyword? v)
        (to-byte-array (name v))

        (or (instance? Integer v)
            (instance? Long v)
            (instance? Short v)
            (instance? Byte v))
        (long->bytes (long v))

        (or (instance? Double v)
            (instance? Float v))
        (long->bytes (Double/doubleToLongBits (double v)))

        (boolean? v)
        (long->bytes (if v 1 0))

        (inst? v)
        (long->bytes (inst-ms v))

        (instance? BigInteger v)
        (.toByteArray ^BigInteger v)

        (instance? BigDecimal v)
        (.getBytes (str v))

        (instance? UUID v)
        (uuid->bytes v)

        (instance? URI v)
        (.getBytes (str v))

        :else
        (.getBytes (pr-str v))))

(defn byte-buffer->bytes ^bytes [^ByteBuffer b]
  (doto (byte-array (.remaining b))
    (->> (.get b))))

(defn compare-bytes [^bytes a ^bytes b max-length]
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
            diff))))))

(def ^Comparator bytes-comparator
  (reify Comparator
    (compare [_ a b]
      (compare-bytes a b Integer/MAX_VALUE))))

(defn bytes=?
  ([#^bytes k1 #^bytes k2]
   (bytes=? k1 (alength k1) k2))
  ([#^bytes k1 array-length #^bytes k2]
   (zero? (compare-bytes k1 k2 array-length))))
