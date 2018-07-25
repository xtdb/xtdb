(ns crux.byte-utils
  (:import [java.nio ByteBuffer]
           [java.security MessageDigest]
           [java.util Arrays Comparator]
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
   (ByteUtils/equalBytes k1 k2))
  ([^bytes k1 ^bytes k2 ^long max-length]
   (ByteUtils/equalBytes k1 k2 max-length)))

(defn inc-unsigned-bytes
  ([^bytes bs]
   (inc-unsigned-bytes bs (alength bs)))
  ([^bytes bs ^long prefix-length]
   (let [bs (Arrays/copyOf bs (alength bs))]
     (loop [idx (dec prefix-length)]
       (when-not (neg? idx)
         (let [b (aget bs idx)]
           (if (= (unchecked-byte 0xff) b)
             (do (aset bs idx (byte 0))
                 (recur (dec idx)))
             (doto bs
               (aset idx (unchecked-byte (inc b)))))))))))
