(ns crux.doc.index
  (:require [crux.byte-utils :as bu]
            [taoensso.nippy :as nippy])
  (:import [java.nio ByteBuffer]
           [java.security MessageDigest]
           [java.util Arrays Date UUID]
           [clojure.lang Keyword IPersistentMap]))

(set! *unchecked-math* :warn-on-boxed)

;; Indexes

(def ^:const ^:private content-hash->doc-index-id 0)
(def ^:const ^:private attribute+value+content-hash-index-id 1)

(def ^:const ^:private content-hash+entity-index-id 2)
(def ^:const ^:private entity+bt+tt+tx-id->content-hash-index-id 3)

(def ^:const ^:private meta-key->value-index-id 4)

(def empty-byte-array (byte-array 0))
(def ^:const ^:private id-hash-algorithm "SHA-1")
(def ^:const id-size (.getDigestLength (MessageDigest/getInstance id-hash-algorithm)))

(defprotocol ValueToBytes
  (value->bytes ^bytes [this]))

;; Adapted from https://github.com/ndimiduk/orderly
(extend-protocol ValueToBytes
  (class (byte-array 0))
  (value->bytes [this]
    this)

  Long
  (value->bytes [this]
    (bu/long->bytes (bit-xor ^long this Long/MIN_VALUE)))

  Double
  (value->bytes [this]
    (let [l (Double/doubleToLongBits this)
          l (inc (bit-xor l (bit-or (bit-shift-right l (dec Long/SIZE)) Long/MIN_VALUE)))]
      (bu/long->bytes l)))

  Date
  (value->bytes [this]
    (value->bytes (.getTime this)))

  String
  (value->bytes [this]
    (let [empty-mark (byte 0)
          terminate-mark (byte 1)
          offset (byte 2)]
      (if (empty? this)
        (byte-array [empty-mark])
        (let [bs (.getBytes this "UTF-8")
              buffer (ByteBuffer/allocate (inc (alength bs)))]
          (doseq [^byte b bs]
            (.put buffer (byte (+ offset b))))
          (-> buffer
              (.put terminate-mark)
              (.array))))))

  Object
  (value->bytes [this]
    (bu/sha1 (nippy/fast-freeze this))))

(defprotocol IdToBytes
  (id->bytes ^bytes [this]))

(def ^:private hex-id-pattern
  (re-pattern (format "\\p{XDigit}{%d}" (* 2 id-size))))

(extend-protocol IdToBytes
  (class (byte-array 0))
  (id->bytes [this]
    this)

  ByteBuffer
  (id->bytes [this]
    (.array this))

  Keyword
  (id->bytes [this]
    (bu/sha1 (.getBytes (str this))))

  UUID
  (id->bytes [this]
    (bu/sha1 (.getBytes (str this))))

  String
  (id->bytes [this]
    (if (re-find hex-id-pattern this)
      (bu/hex->bytes this)
      (throw (IllegalArgumentException. (format "Not a %s hex string: %s" id-hash-algorithm this)))))

  IPersistentMap
  (id->bytes [this]
    (value->bytes this))

  Object
  (id->bytes [this]
    (throw (UnsupportedOperationException.))))

(deftype Id [^bytes bytes]
  IdToBytes
  (id->bytes [this]
    (id->bytes bytes))

  Object
  (toString [this]
    (bu/bytes->hex bytes))

  (equals [this that]
    (and (instance? Id that)
         (Arrays/equals bytes ^bytes (.bytes ^Id that))))

  (hashCode [this]
    (Arrays/hashCode bytes))

  Comparable
  (compareTo [this that]
    (bu/compare-bytes bytes (.bytes ^Id that))))

(defn ^Id new-id [id]
  (->Id (id->bytes id)))

(defn encode-doc-key ^bytes [^bytes content-hash]
  (assert (= id-size (alength content-hash)))
  (-> (ByteBuffer/allocate (+ Short/BYTES id-size))
      (.putShort content-hash->doc-index-id)
      (.put content-hash)
      (.array)))

(defn encode-doc-prefix-key ^bytes []
  (-> (ByteBuffer/allocate (+ Short/BYTES))
      (.putShort content-hash->doc-index-id)
      (.array)))

(defn decode-doc-key ^bytes [^bytes doc-key]
  (assert (= (+ Short/BYTES id-size) (alength doc-key)))
  (let [buffer (ByteBuffer/wrap doc-key)]
    (assert (= content-hash->doc-index-id (.getShort buffer)))
    (doto (byte-array id-size)
      (->> (.get buffer)))))

(defn encode-attribute+value+content-hash-key ^bytes [k v ^bytes content-hash]
  (assert (or (= id-size (alength content-hash))
              (zero? (alength content-hash))))
  (let [v (value->bytes v)]
    (-> (ByteBuffer/allocate (+ Short/BYTES id-size (alength v) (alength content-hash)))
        (.putShort attribute+value+content-hash-index-id)
        (.put (id->bytes k))
        (.put v)
        (.put content-hash)
        (.array))))

(defn encode-attribute+value-prefix-key ^bytes [k v]
  (encode-attribute+value+content-hash-key k v empty-byte-array))

(defn decode-attribute+value+content-hash-key->content-hash ^bytes [^bytes key]
  (assert (<= (+ Short/BYTES id-size id-size) (alength key)))
  (let [buffer (ByteBuffer/wrap key)]
    (assert (= attribute+value+content-hash-index-id (.getShort buffer)))
    (.position buffer (- (alength key) id-size))
    (doto (byte-array id-size)
      (->> (.get buffer)))))

(defn encode-content-hash+entity-key ^bytes [^bytes content-hash ^bytes eid]
  (assert (= id-size (alength content-hash)))
  (assert (or (= id-size (alength eid))
              (zero? (alength eid))))
  (-> (ByteBuffer/allocate (+ Short/BYTES id-size (alength eid)))
      (.putShort content-hash+entity-index-id)
      (.put content-hash)
      (.put eid)
      (.array)))

(defn encode-content-hash-prefix-key ^bytes [^bytes content-hash]
  (encode-content-hash+entity-key content-hash empty-byte-array))

(defn decode-content-hash+entity-key->entity ^bytes [^bytes key]
  (assert (= (+ Short/BYTES id-size id-size) (alength key)))
  (let [buffer (ByteBuffer/wrap key)]
    (assert (= content-hash+entity-index-id (.getShort buffer)))
    (.position buffer (+ Short/BYTES id-size))
    (doto (byte-array id-size)
      (->> (.get buffer)))))

(defn encode-meta-key ^bytes [k]
  (-> (ByteBuffer/allocate (+ Short/BYTES id-size))
      (.putShort meta-key->value-index-id)
      (.put (id->bytes k))
      (.array)))

(defn- date->reverse-time-ms ^long [^Date date]
  (bit-xor (bit-not (.getTime date)) Long/MIN_VALUE))

(defn- ^Date reverse-time-ms->date [^long reverse-time-ms]
  (Date. (bit-xor (bit-not reverse-time-ms) Long/MIN_VALUE)))

(defn encode-entity+bt+tt+tx-id-key ^bytes [^bytes eid ^Date business-time ^Date transact-time ^Long tx-id]
  (assert (= id-size (alength eid)))
  (cond-> (ByteBuffer/allocate (cond-> (+ Short/BYTES id-size Long/BYTES Long/BYTES)
                                 tx-id (+ Long/BYTES)))
    true (-> (.putShort entity+bt+tt+tx-id->content-hash-index-id)
             (.put eid)
             (.putLong (date->reverse-time-ms business-time))
             (.putLong (date->reverse-time-ms transact-time)))
    tx-id (.putLong tx-id)
    true (.array)))

(defn encode-entity+bt+tt-prefix-key
  (^bytes []
   (-> (ByteBuffer/allocate Short/BYTES)
       (.putShort entity+bt+tt+tx-id->content-hash-index-id)
       (.array)))
  (^bytes [^bytes eid]
   (-> (ByteBuffer/allocate (+ Short/BYTES id-size))
       (.putShort entity+bt+tt+tx-id->content-hash-index-id)
       (.put eid)
       (.array)))
  (^bytes [^bytes eid ^Date business-time ^Date transact-time]
   (encode-entity+bt+tt+tx-id-key eid business-time transact-time nil)))

(defn decode-entity+bt+tt+tx-id-key [^bytes key]
  (assert (= (+ Short/BYTES id-size Long/BYTES Long/BYTES Long/BYTES)) (alength key))
  (let [buffer (ByteBuffer/wrap key)]
    (assert (= entity+bt+tt+tx-id->content-hash-index-id (.getShort buffer)))
    {:eid (doto (byte-array id-size)
            (->> (.get buffer)))
     :bt (reverse-time-ms->date (.getLong buffer))
     :tt (reverse-time-ms->date (.getLong buffer))
     :tx-id (.getLong buffer)}))
