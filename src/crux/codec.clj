(ns crux.codec
  (:require [crux.byte-utils :as bu]
            [taoensso.nippy :as nippy])
  (:import [clojure.lang IHashEq IPersistentMap Keyword]
           [java.io Closeable Writer]
           java.net.URI
           java.nio.ByteBuffer
           java.security.MessageDigest
           [java.util Arrays Date UUID]))

(set! *unchecked-math* :warn-on-boxed)

;; Indexes

(def ^{:tag 'long} index-id-size Byte/BYTES)

(def ^:const ^:private content-hash->doc-index-id 0)

(def ^:const ^:private attribute+value+entity+content-hash-index-id 1)
(def ^:const ^:private attribute+entity+value+content-hash-index-id 2)

(def ^:const ^:private entity+bt+tt+tx-id->content-hash-index-id 3)

(def ^:const ^:private meta-key->value-index-id 4)

(def ^:private ^{:tag 'long} value-type-id-size Byte/BYTES)

(def ^:const ^:private id-hash-algorithm "SHA-1")
(def ^:const id-size (+ (.getDigestLength (MessageDigest/getInstance id-hash-algorithm))
                        value-type-id-size))

(def empty-byte-array (byte-array 0))

(def ^:const ^:private max-string-index-length 128)

(defprotocol IdToBytes
  (id->bytes ^bytes [this]))

(defprotocol ValueToBytes
  (value->bytes ^bytes [this]))

(def ^:private id-value-type-id 0)
(def ^:private long-value-type-id 1)
(def ^:private double-value-type-id 2)
(def ^:private date-value-type-id 3)
(def ^:private string-value-type-id 4)
(def ^:private bytes-value-type-id 5)
(def ^:private object-value-type-id 6)

(def nil-id-bytes (doto (byte-array id-size)
                    (aset 0 (byte id-value-type-id))))

(defn- prepend-value-type-id [^bytes bs type-id]
  (let [bs-with-type-id (doto (byte-array (+ (alength bs) value-type-id-size))
                          (aset 0 (byte type-id)))]
    (System/arraycopy bs 0 bs-with-type-id value-type-id-size (alength bs))
    bs-with-type-id))

(defn id-function ^bytes [^bytes bytes]
  (-> (.digest (MessageDigest/getInstance id-hash-algorithm) bytes)
      (prepend-value-type-id id-value-type-id)))

;; Adapted from https://github.com/ndimiduk/orderly
(extend-protocol ValueToBytes
  (class (byte-array 0))
  (value->bytes [this]
    (throw (UnsupportedOperationException. "Byte arrays as values is not supported.")))

  Byte
  (value->bytes [this]
    (value->bytes (long this)))

  Short
  (value->bytes [this]
    (value->bytes (long this)))

  Integer
  (value->bytes [this]
    (value->bytes (long this)))

  Long
  (value->bytes [this]
    (-> (ByteBuffer/allocate (+ Long/BYTES value-type-id-size))
        (.put (byte long-value-type-id))
        (.putLong (bit-xor ^long this Long/MIN_VALUE))
        (.array)))

  Float
  (value->bytes [this]
    (value->bytes (double this)))

  Double
  (value->bytes [this]
    (let [l (Double/doubleToLongBits this)
          l (inc (bit-xor l (bit-or (bit-shift-right l (dec Long/SIZE)) Long/MIN_VALUE)))]
      (-> (ByteBuffer/allocate (+ Long/BYTES value-type-id-size))
          (.put (byte double-value-type-id))
          (.putLong l)
          (.array))))

  Date
  (value->bytes [this]
    (doto (value->bytes (.getTime this))
      (aset 0 (byte date-value-type-id))))

  Character
  (value->bytes [this]
    (value->bytes (str this)))

  String
  (value->bytes [this]
    (if (< max-string-index-length (count this))
      (doto (id-function (nippy/fast-freeze this))
        (aset 0 (byte object-value-type-id)))
      (let [terminate-mark (byte 1)
            offset (byte 2)]
        (let [s this
              bs (.getBytes s "UTF-8")
              buffer (ByteBuffer/allocate (+ (inc (alength bs)) value-type-id-size))]
          (.put buffer (byte string-value-type-id))
          (doseq [^byte b bs]
            (.put buffer (byte (+ offset b))))
          (-> buffer
              (.put terminate-mark)
              (.array))))))

  nil
  (value->bytes [this]
    nil-id-bytes)

  Object
  (value->bytes [this]
    (if (satisfies? IdToBytes this)
      (id->bytes this)
      (doto (id-function (nippy/fast-freeze this))
        (aset 0 (byte object-value-type-id))))))

(defn value-bytes-type-id ^bytes [^bytes bs]
  (Arrays/copyOfRange bs 0 value-type-id-size))

(def ^:private hex-id-pattern
  (re-pattern (format "\\p{XDigit}{%d}" (* 2 (dec id-size)))))

(defn hex-id? [s]
  (re-find hex-id-pattern s))

(defn- maybe-uuid-str [s]
  (try
    (UUID/fromString s)
    (catch IllegalArgumentException _)))

(defn- maybe-keyword-str [s]
  (when-let [[_ n] (re-find #"\:(.+)" s)]
    (keyword n)))

(extend-protocol IdToBytes
  (class (byte-array 0))
  (id->bytes [this]
    (if (= id-size (alength ^bytes this))
      this
      (throw (IllegalArgumentException.
              (str "Not an id byte array: " (bu/bytes->hex this))))))

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
    (if (hex-id? this)
      (prepend-value-type-id (bu/hex->bytes this) id-value-type-id)
      (if-let [id (or (maybe-uuid-str this)
                      (maybe-keyword-str this))]
        (id->bytes id)
        (throw (IllegalArgumentException. (format "Not a %s hex, keyword or an UUID string: %s" id-hash-algorithm this))))))

  IPersistentMap
  (id->bytes [this]
    (id-function (nippy/fast-freeze this)))

  nil
  (id->bytes [this]
    nil-id-bytes))

(deftype Id [^bytes bytes ^:unsynchronized-mutable ^int hash-code]
  IdToBytes
  (id->bytes [this]
    bytes)

  Object
  (toString [this]
    (bu/bytes->hex (Arrays/copyOfRange bytes value-type-id-size id-size)))

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

(defmethod print-method Id [id ^Writer w]
  (.write w "#crux/id ")
  (print-method (str id) w))

(defn new-id ^crux.codec.Id [id]
  (let [bs (id->bytes id)]
    (assert (= id-size (alength bs)))
    (->Id bs 0)))

(defn valid-id? [x]
  (try
    (id->bytes x)
    true
    (catch IllegalArgumentException _
      false)))

(nippy/extend-freeze
 Id
 :crux.codec/id
 [x data-output]
 (.write data-output (id->bytes x)))

(nippy/extend-thaw
 :crux.codec/id
 [data-input]
 (new-id (doto (byte-array id-size)
           (->> (.readFully data-input)))))

(defn encode-doc-key ^bytes [^bytes content-hash]
  (assert (= id-size (alength content-hash)))
  (-> (ByteBuffer/allocate (+ index-id-size id-size))
      (.put (byte content-hash->doc-index-id))
      (.put content-hash)
      (.array)))

(defn decode-doc-key ^bytes [^bytes doc-key]
  (assert (= (+ index-id-size id-size) (alength doc-key)))
  (let [buffer (ByteBuffer/wrap doc-key)]
    (assert (= content-hash->doc-index-id (.get buffer)))
    (new-id (doto (byte-array id-size)
              (->> (.get buffer))))))

(defn encode-attribute+value+entity+content-hash-key
  (^bytes [^bytes attr]
   (encode-attribute+value+entity+content-hash-key attr empty-byte-array))
  (^bytes [^bytes attr ^bytes v]
   (encode-attribute+value+entity+content-hash-key attr v empty-byte-array))
  (^bytes [^bytes attr ^bytes v ^bytes entity]
   (encode-attribute+value+entity+content-hash-key attr v entity empty-byte-array))
  (^bytes [^bytes attr ^bytes v ^bytes entity ^bytes content-hash]
   (assert (= id-size (alength attr)))
   (assert (or (= id-size (alength entity))
               (zero? (alength entity))))
   (assert (or (= id-size (alength content-hash))
               (zero? (alength content-hash))))
   (-> (ByteBuffer/allocate (+ index-id-size id-size (alength v) (alength entity) (alength content-hash)))
       (.put (byte attribute+value+entity+content-hash-index-id))
       (.put attr)
       (.put v)
       (.put entity)
       (.put content-hash)
       (.array))))

(defn decode-attribute+value+entity+content-hash-key->value+entity+content-hash ^crux.codec.Id [^bytes k]
  (assert (<= (+ index-id-size id-size id-size id-size) (alength k)))
  (let [buffer (ByteBuffer/wrap k)]
    (assert (= attribute+value+entity+content-hash-index-id (.get buffer)))
    (.position buffer (+ index-id-size id-size))
    [(doto (byte-array (- (.remaining buffer) id-size id-size))
       (->> (.get buffer)))
     (new-id (doto (byte-array id-size)
               (->> (.get buffer))))
     (new-id (doto (byte-array id-size)
               (->> (.get buffer))))]))

(defn encode-attribute+entity+value+content-hash-key
  (^bytes [^bytes attr]
   (encode-attribute+entity+value+content-hash-key attr empty-byte-array))
  (^bytes [^bytes attr ^bytes entity]
   (encode-attribute+entity+value+content-hash-key attr entity empty-byte-array))
  (^bytes [^bytes attr ^bytes entity ^bytes v]
   (encode-attribute+entity+value+content-hash-key attr entity v empty-byte-array))
  (^bytes [^bytes attr ^bytes entity ^bytes v ^bytes content-hash]
   (assert (= id-size (alength attr)))
   (assert (or (= id-size (alength entity))
               (zero? (alength entity))))
   (assert (or (= id-size (alength content-hash))
               (zero? (alength content-hash))))
   (-> (ByteBuffer/allocate (+ index-id-size id-size (alength entity) (alength v) (alength content-hash)))
       (.put (byte attribute+entity+value+content-hash-index-id))
       (.put attr)
       (.put entity)
       (.put v)
       (.put content-hash)
       (.array))))

(defn decode-attribute+entity+value+content-hash-key->entity+value+content-hash ^crux.codec.Id [^bytes k]
  (assert (<= (+ index-id-size id-size id-size) (alength k)))
  (let [buffer (ByteBuffer/wrap k)]
    (assert (= attribute+entity+value+content-hash-index-id (.get buffer)))
    (.position buffer (+ index-id-size id-size))
    [(new-id (doto (byte-array id-size)
               (->> (.get buffer))))
     (doto (byte-array (- (.remaining buffer) id-size))
       (->> (.get buffer)))
     (new-id (doto (byte-array id-size)
               (->> (.get buffer))))]))

(defn encode-meta-key ^bytes [^bytes k]
  (-> (ByteBuffer/allocate (+ index-id-size id-size))
      (.put (byte meta-key->value-index-id))
      (.put k)
      (.array)))

(defn- date->reverse-time-ms ^long [^Date date]
  (bit-xor (bit-not (.getTime date)) Long/MIN_VALUE))

(defn- ^Date reverse-time-ms->date [^long reverse-time-ms]
  (Date. (bit-xor (bit-not reverse-time-ms) Long/MIN_VALUE)))

(defn encode-entity+bt+tt+tx-id-key
  (^bytes []
   (-> (ByteBuffer/allocate index-id-size)
       (.put (byte entity+bt+tt+tx-id->content-hash-index-id))
       (.array)))
  (^bytes [^bytes entity]
   (assert (= id-size (alength entity)))
   (-> (ByteBuffer/allocate (+ index-id-size id-size))
       (.put (byte entity+bt+tt+tx-id->content-hash-index-id))
       (.put entity)
       (.array)))
  (^bytes [entity business-time transact-time]
   (encode-entity+bt+tt+tx-id-key entity business-time transact-time nil))
  (^bytes [^bytes entity ^Date business-time ^Date transact-time ^Long tx-id]
   (assert (= id-size (alength entity)))
   (cond-> (ByteBuffer/allocate (cond-> (+ index-id-size id-size Long/BYTES Long/BYTES)
                                  tx-id (+ Long/BYTES)))
     true (-> (.put (byte entity+bt+tt+tx-id->content-hash-index-id))
              (.put entity)
              (.putLong (date->reverse-time-ms business-time))
              (.putLong (date->reverse-time-ms transact-time)))
     tx-id (.putLong tx-id)
     true (.array))))

(defrecord EntityTx [eid bt tt tx-id content-hash]
  IdToBytes
  (id->bytes [this]
    (id->bytes eid)))

;; TODO: Not sure why these are needed, external sorting thaws
;; incompatible records without it.
(nippy/extend-freeze
 EntityTx
 :crux.codec/entity-tx
 [x data-output]
 (nippy/-freeze-without-meta! (into {} x) data-output))

(nippy/extend-thaw
 :crux.codec/entity-tx
 [data-input]
 (map->EntityTx (nippy/thaw-from-in! data-input)))

(defn decode-entity+bt+tt+tx-id-key ^crux.codec.EntityTx [^bytes key]
  (assert (= (+ index-id-size id-size Long/BYTES Long/BYTES Long/BYTES) (alength key)))
  (let [buffer (ByteBuffer/wrap key)]
    (assert (= entity+bt+tt+tx-id->content-hash-index-id (.get buffer)))
    (->EntityTx (new-id (doto (byte-array id-size)
                          (->> (.get buffer))))
                (reverse-time-ms->date (.getLong buffer))
                (reverse-time-ms->date (.getLong buffer))
                (.getLong buffer)
                nil)))

(defn entity-tx->edn [{:keys [eid bt tt tx-id content-hash] :as entity-tx}]
  (when entity-tx
    {:crux.db/id (str eid)
     :crux.db/content-hash (str content-hash)
     :crux.db/business-time bt
     :crux.tx/tx-id tx-id
     :crux.tx/tx-time tt}))
