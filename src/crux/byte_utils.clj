(ns crux.byte-utils
  (:import [java.math BigInteger]
           [java.security MessageDigest]
           [java.util Comparator]))

(defn hash-keyword [k]
  (hash (str (namespace k) (name k))))

(defn long->bytes [l]
  (->(-> (java.nio.ByteBuffer/allocate 8)
         (.order (java.nio.ByteOrder/nativeOrder)))
     (.putLong l)
     (.array)))

(defn bytes->long [data]
  (.getLong (doto (-> (java.nio.ByteBuffer/allocate 8)
                      (.order (java.nio.ByteOrder/nativeOrder)))
              (.put data 0 8)
              (.flip))))

(def ^java.security.MessageDigest md5-algo (MessageDigest/getInstance "MD5"))

(defn md5 [#^bytes bytes]
  (.digest md5-algo bytes))

(defn to-byte-array [v]
  (cond (string? v)
        (.getBytes ^String v)

        (keyword? v)
        (to-byte-array (name v))

        (number? v)
        (long->bytes v)))

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
