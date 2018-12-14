(ns crux.byte-utils
  (:import [java.nio ByteBuffer ByteOrder]
           java.security.MessageDigest
           [java.util Arrays Comparator]
           org.agrona.concurrent.UnsafeBuffer
           crux.ByteUtils))

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

(def ^Comparator bytes-comparator
  ByteUtils/UNSIGNED_BYTES_COMPARATOR)

(defn bytes=?
  ([^bytes k1 ^bytes k2]
   (ByteUtils/equalBytes k1 k2))
  ([^bytes k1 ^bytes k2 ^long max-length]
   (ByteUtils/equalBytes k1 k2 max-length)))

(defn inc-unsigned-bytes!
  ([^bytes bs]
   (inc-unsigned-bytes! bs (alength bs)))
  ([^bytes bs ^long prefix-length]
   (loop [idx (dec (int prefix-length))]
     (when-not (neg? idx)
       (let [b (aget bs idx)]
         (if (= (byte 0xff) b)
           (do (aset bs idx (byte 0))
               (recur (dec idx)))
           (doto bs
             (aset idx (byte (inc b))))))))))
