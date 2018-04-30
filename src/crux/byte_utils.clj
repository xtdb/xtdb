(ns crux.byte-utils
  (:require [byte-streams :as bs])
  (:import java.math.BigInteger
           java.security.MessageDigest))

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
