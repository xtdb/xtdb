(ns crux.byte-utils
  (:import [java.nio ByteBuffer ByteOrder]
           java.security.MessageDigest
           [java.util Arrays Comparator]
           org.agrona.concurrent.UnsafeBuffer
           crux.ByteUtils)
  (:require [crux.memory :as mem]))

(set! *unchecked-math* :warn-on-boxed)

(defn long->bytes ^bytes [^long l]
  (let [ub (UnsafeBuffer. (byte-array Long/BYTES))]
    (.putLong ub 0 l ByteOrder/BIG_ENDIAN)
    (.byteArray ub)))

(defn bytes->long ^long [^bytes data]
  (let [ub (UnsafeBuffer. data)]
    (.getLong ub 0 ByteOrder/BIG_ENDIAN)))

(defn sha1 ^bytes [^bytes bytes]
  (.digest (MessageDigest/getInstance "SHA-1") bytes))

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
   (ByteUtils/compareBytes a b))
  (^long [^bytes a ^bytes b max-length]
   (ByteUtils/compareBytes a b max-length)))

(def ^java.util.Comparator bytes-comparator
  ByteUtils/UNSIGNED_BYTES_COMPARATOR)

(defn bytes=?
  ([^bytes a ^bytes b]
   (ByteUtils/equalBytes a b))
  ([^bytes a ^bytes b ^long max-length]
   (ByteUtils/equalBytes a b max-length)))

(defn inc-unsigned-bytes!
  ([^bytes bs]
   (inc-unsigned-bytes! bs (alength bs)))
  ([^bytes bs ^long prefix-length]
   (some-> (UnsafeBuffer. bs 0 prefix-length)
           (mem/inc-unsigned-buffer!)
           (.byteArray))))
