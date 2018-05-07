(ns crux.byte-utils
  (:require [byte-streams :as bs])
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
  (bs/to-byte-array (cond (keyword? v)
                          (name v)

                          (number? v)
                          (long->bytes v)

                          :else
                          v)))

(defn bytes-subset? [#^bytes ba1 #^bytes ba2]
  (let [l (alength ba1)
        l2 (alength ba2)]
    (if (> l2 l)
      false
      (loop [n 0]
        (if (not= (aget ba1 n) (aget ba2 n))
          false
          (if (< (inc n) l2)
            (recur (inc n))
            true))))))

(defn compare-bytes [a b max-length]
  (let [a-length (count a)
        b-length (count b)]
    (loop [idx (int 0)]
      (cond
        (= idx max-length)
        0

        (or (= idx a-length)
            (= idx b-length))
        (- a-length b-length)

        :else
        (let [diff (Byte/compareUnsigned (aget ^bytes a idx)
                                         (aget ^bytes b idx))]
          (if (zero? diff)
            (recur (unchecked-inc-int idx))
            diff))))))

(def ^Comparator bytes-comparator
  (reify Comparator
    (compare [_ a b]
      (compare-bytes a b Integer/MAX_VALUE))))
