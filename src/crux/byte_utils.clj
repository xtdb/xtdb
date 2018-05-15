(ns crux.byte-utils
  (:import [java.math BigInteger]
           [java.nio ByteBuffer]
           [java.net URI]
           [java.security MessageDigest]
           [java.util Comparator UUID]))

(defn hash-keyword [k]
  (hash (str (namespace k) (name k))))

(defn long->bytes [l]
  (-> (ByteBuffer/allocate 8)
      (.putLong l)
      (.array)))

(defn bytes->long [data]
  (-> (ByteBuffer/allocate 8)
      (.put data 0 8)
      (.flip)
      (.getLong)))

(def ^MessageDigest md5-algo (MessageDigest/getInstance "MD5"))

(defn md5 [^bytes bytes]
  (.digest md5-algo bytes))

(defn byte-buffer->bytes ^bytes [^ByteBuffer b]
  (doto (byte-array (.remaining b))
    (->> (.get b))))

(defn compare-bytes
  ([^bytes a ^bytes b]
   (compare-bytes a b Integer/MAX_VALUE))
  ([^bytes a ^bytes b max-length]
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
