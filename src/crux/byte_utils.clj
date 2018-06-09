(ns crux.byte-utils
  (:import [java.nio ByteBuffer]
           [java.security MessageDigest]
           [java.util Comparator]
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
   (zero? (ByteUtils/compareBytes k1 k2 (alength k1))))
  ([^bytes k1 array-length ^bytes k2]
   (zero? (ByteUtils/compareBytes k1 k2 array-length))))
