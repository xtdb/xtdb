(ns crux.index
  (:require [crux.byte-utils :as bu]
            [taoensso.nippy :as nippy])
  (:import [java.nio ByteBuffer]
           [java.security MessageDigest]
           [java.util Arrays Date UUID]
           [java.net URI]
           [clojure.lang IHashEq IPersistentMap Keyword]))

(set! *unchecked-math* :warn-on-boxed)

;; Indexes

(def ^:const ^:private content-hash->doc-index-id 0)

(def ^:const ^:private attribute+value+entity+content-hash-index-id 1)
(def ^:const ^:private attribute+entity+value+content-hash-index-id 2)

(def ^:const ^:private entity+bt+tt+tx-id->content-hash-index-id 3)

(def ^:const ^:private meta-key->value-index-id 4)

(def ^:const ^:private id-hash-algorithm "SHA-1")
(def ^:const id-size (.getDigestLength (MessageDigest/getInstance id-hash-algorithm)))

(defn id-function ^bytes [^bytes bytes]
  (.digest (MessageDigest/getInstance id-hash-algorithm) bytes))

(def empty-byte-array (byte-array 0))
(def nil-id-bytes (byte-array id-size))

(def ^:const ^:private max-string-index-length 128)

(defprotocol IdToBytes
  (id->bytes ^bytes [this]))

(defprotocol ValueToBytes
  (value->bytes ^bytes [this]))

;; Adapted from https://github.com/ndimiduk/orderly
(extend-protocol ValueToBytes
  (class (byte-array 0))
  (value->bytes [this]
    (if (empty? this)
      this
      (id-function this)))

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
    (let [terminate-mark (byte 1)
          offset (byte 2)]
      (let [s (if (< max-string-index-length (count this))
                (subs this 0 max-string-index-length)
                this)
            bs (.getBytes s "UTF-8")
            buffer (ByteBuffer/allocate (inc (alength bs)))]
        (doseq [^byte b bs]
          (.put buffer (unchecked-byte (+ offset b))))
        (-> buffer
            (.put terminate-mark)
            (.array)))))

  nil
  (value->bytes [this]
    nil-id-bytes)

  Object
  (value->bytes [this]
    (if (satisfies? IdToBytes this)
      (id->bytes this)
      (value->bytes (nippy/fast-freeze this)))))

(def ^:private hex-id-pattern
  (re-pattern (format "\\p{XDigit}{%d}" (* 2 id-size))))

(extend-protocol IdToBytes
  (class (byte-array 0))
  (id->bytes [this]
    this)

  ByteBuffer
  (id->bytes [this]
    (bu/byte-buffer->bytes this))

  Keyword
  (id->bytes [this]
    (id-function (.getBytes (subs (str this) 1))))

  UUID
  (id->bytes [this]
    (id-function (.getBytes (str this))))

  URI
  (id->bytes [this]
    (id-function (.getBytes (str (.normalize this)))))

  String
  (id->bytes [this]
    (if (re-find hex-id-pattern this)
      (bu/hex->bytes this)
      (throw (IllegalArgumentException. (format "Not a %s hex string: %s" id-hash-algorithm this)))))

  IPersistentMap
  (id->bytes [this]
    (value->bytes (nippy/fast-freeze this)))

  nil
  (id->bytes [this]
    nil-id-bytes))

(deftype Id [^bytes bytes ^:unsynchronized-mutable ^int hash-code]
  IdToBytes
  (id->bytes [this]
    (id->bytes bytes))

  Object
  (toString [this]
    (bu/bytes->hex bytes))

  (equals [this that]
    (or (identical? this that)
        (and (satisfies? IdToBytes that)
             (bu/bytes=? bytes (id->bytes that)))))

  (hashCode [this]
    (when (zero? hash-code)
      (set! hash-code (Arrays/hashCode bytes)))
    hash-code)

  IHashEq
  (hasheq [this]
    (.hashCode this))

  Comparable
  (compareTo [this that]
    (if (identical? this that)
      0
      (bu/compare-bytes bytes (id->bytes that)))))

(defn ^Id new-id [id]
  (->Id (id->bytes id) 0))

(nippy/extend-freeze
 Id
 :crux.index/id
 [x data-output]
 (.writeUTF data-output (str x)))

(nippy/extend-thaw
 :crux.index/id
 [data-input]
 (new-id (.readUTF data-input)))

(defn encode-doc-key ^bytes [content-hash]
  (-> (ByteBuffer/allocate (+ Short/BYTES id-size))
      (.putShort content-hash->doc-index-id)
      (.put (id->bytes content-hash))
      (.array)))

(defn encode-doc-prefix-key ^bytes []
  (-> (ByteBuffer/allocate (+ Short/BYTES))
      (.putShort content-hash->doc-index-id)
      (.array)))

(defn decode-doc-key ^bytes [^bytes doc-key]
  (assert (= (+ Short/BYTES id-size) (alength doc-key)))
  (let [buffer (ByteBuffer/wrap doc-key)]
    (assert (= content-hash->doc-index-id (.getShort buffer)))
    (new-id (doto (byte-array id-size)
              (->> (.get buffer))))))

(defn encode-attribute+value+entity+content-hash-key ^bytes [attr v entity content-hash]
  (let [content-hash (id->bytes content-hash)
        v (value->bytes v)]
    (-> (ByteBuffer/allocate (+ Short/BYTES id-size (alength v) id-size (alength content-hash)))
        (.putShort attribute+value+entity+content-hash-index-id)
        (.put (id->bytes attr))
        (.put v)
        (.put (id->bytes entity))
        (.put content-hash)
        (.array))))

(defn encode-attribute+value-entity-prefix-key ^bytes [attr v]
  (let [v (value->bytes v)]
    (-> (ByteBuffer/allocate (+ Short/BYTES id-size (alength v)))
        (.putShort attribute+value+entity+content-hash-index-id)
        (.put (id->bytes attr))
        (.put v)
        (.array))))

(defn ^Id decode-attribute+value+entity+content-hash-key->value+entity+content-hash [^bytes k]
  (assert (<= (+ Short/BYTES id-size id-size id-size) (alength k)))
  (let [buffer (ByteBuffer/wrap k)]
    (assert (= attribute+value+entity+content-hash-index-id (.getShort buffer)))
    (.position buffer (+ Short/BYTES id-size))
    [(doto (byte-array (- (.remaining buffer) id-size id-size))
       (->> (.get buffer)))
     (new-id (doto (byte-array id-size)
               (->> (.get buffer))))
     (new-id (doto (byte-array id-size)
               (->> (.get buffer))))]))

(defn encode-attribute+entity+value+content-hash-key ^bytes [attr entity v content-hash]
  (let [content-hash (id->bytes content-hash)
        v (value->bytes v)]
    (-> (ByteBuffer/allocate (+ Short/BYTES id-size (alength v) id-size (alength content-hash)))
        (.putShort attribute+entity+value+content-hash-index-id)
        (.put (id->bytes attr))
        (.put (id->bytes entity))
        (.put v)
        (.put content-hash)
        (.array))))

(defn encode-attribute+entity-value-prefix-key ^bytes [attr ^bytes entity]
  (-> (ByteBuffer/allocate (+ Short/BYTES id-size (alength entity)))
      (.putShort attribute+entity+value+content-hash-index-id)
      (.put (id->bytes attr))
      (.put entity)
      (.array)))

(defn ^Id decode-attribute+entity+value+content-hash-key->entity+value+content-hash [^bytes k]
  (assert (<= (+ Short/BYTES id-size id-size) (alength k)))
  (let [buffer (ByteBuffer/wrap k)]
    (assert (= attribute+entity+value+content-hash-index-id (.getShort buffer)))
    (.position buffer (+ Short/BYTES id-size))
    [(new-id (doto (byte-array id-size)
               (->> (.get buffer))))
     (doto (byte-array (- (.remaining buffer) id-size))
       (->> (.get buffer)))
     (new-id (doto (byte-array id-size)
               (->> (.get buffer))))]))

(defn encode-meta-key ^bytes [k]
  (-> (ByteBuffer/allocate (+ Short/BYTES id-size))
      (.putShort meta-key->value-index-id)
      (.put (id->bytes k))
      (.array)))

(defn- date->reverse-time-ms ^long [^Date date]
  (bit-xor (bit-not (.getTime date)) Long/MIN_VALUE))

(defn- ^Date reverse-time-ms->date [^long reverse-time-ms]
  (Date. (bit-xor (bit-not reverse-time-ms) Long/MIN_VALUE)))

(defn encode-entity+bt+tt+tx-id-key ^bytes [eid ^Date business-time ^Date transact-time ^Long tx-id]
  (cond-> (ByteBuffer/allocate (cond-> (+ Short/BYTES id-size Long/BYTES Long/BYTES)
                                 tx-id (+ Long/BYTES)))
    true (-> (.putShort entity+bt+tt+tx-id->content-hash-index-id)
             (.put (id->bytes eid))
             (.putLong (date->reverse-time-ms business-time))
             (.putLong (date->reverse-time-ms transact-time)))
    tx-id (.putLong tx-id)
    true (.array)))

(defn encode-entity+bt+tt-prefix-key
  (^bytes []
   (-> (ByteBuffer/allocate Short/BYTES)
       (.putShort entity+bt+tt+tx-id->content-hash-index-id)
       (.array)))
  (^bytes [eid]
   (-> (ByteBuffer/allocate (+ Short/BYTES id-size))
       (.putShort entity+bt+tt+tx-id->content-hash-index-id)
       (.put (id->bytes eid))
       (.array)))
  (^bytes [eid business-time transact-time]
   (encode-entity+bt+tt+tx-id-key eid business-time transact-time nil)))

(defrecord EntityTx [eid bt tt tx-id content-hash]
  IdToBytes
  (id->bytes [this]
    (id->bytes eid)))

;; TODO: Not sure why these are needed, external sorting thaws
;; incompatible records without it.
(nippy/extend-freeze
 EntityTx
 :crux.index/entity-tx
 [x data-output]
 (nippy/-freeze-without-meta! (into {} x) data-output))

(nippy/extend-thaw
 :crux.index/entity-tx
 [data-input]
 (map->EntityTx (nippy/thaw-from-in! data-input)))

(defn ^crux.index.EntityTx decode-entity+bt+tt+tx-id-key [^bytes key]
  (assert (= (+ Short/BYTES id-size Long/BYTES Long/BYTES Long/BYTES) (alength key)))
  (let [buffer (ByteBuffer/wrap key)]
    (assert (= entity+bt+tt+tx-id->content-hash-index-id (.getShort buffer)))
    (->EntityTx (new-id (doto (byte-array id-size)
                          (->> (.get buffer))))
                (reverse-time-ms->date (.getLong buffer))
                (reverse-time-ms->date (.getLong buffer))
                (.getLong buffer)
                nil)))
