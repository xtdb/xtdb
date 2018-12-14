(ns crux.codec
  (:require [crux.byte-utils :as bu]
            [taoensso.nippy :as nippy])
  (:import [clojure.lang IHashEq IPersistentMap Keyword]
           [java.io Closeable Writer]
           java.net.URI
           [java.nio ByteOrder ByteBuffer]
           java.security.MessageDigest
           [java.util Arrays Date UUID]
           org.agrona.concurrent.UnsafeBuffer))

(set! *unchecked-math* :warn-on-boxed)

;; Indexes

(def ^:const index-id-size Byte/BYTES)

(def ^:const ^:private content-hash->doc-index-id 0)

(def ^:const ^:private attribute+value+entity+content-hash-index-id 1)
(def ^:const ^:private attribute+entity+value+content-hash-index-id 2)

(def ^:const ^:private entity+bt+tt+tx-id->content-hash-index-id 3)

(def ^:const ^:private meta-key->value-index-id 4)

(def ^:const ^:private tx-id->tx-index-id 5)

(def ^:const ^:private value-type-id-size Byte/BYTES)

(def ^:const ^:private id-hash-algorithm "SHA-1")
(def ^:const id-size (+ (.getDigestLength (MessageDigest/getInstance id-hash-algorithm))
                        value-type-id-size))
(def ^:private ^MessageDigest id-digest-prototype (MessageDigest/getInstance id-hash-algorithm))

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

(defn- prepend-value-type-id ^bytes [^bytes bs ^long type-id]
  (let [ub (UnsafeBuffer. (byte-array (+ (alength bs) value-type-id-size)))]
    (.putByte ub 0 type-id)
    (.putBytes ub 1 bs)
    (.byteArray ub)))

(defn id-function ^bytes [^bytes bytes]
  (let [md (try
             (.clone id-digest-prototype)
             (catch CloneNotSupportedException e
               (MessageDigest/getInstance id-hash-algorithm)))]
    (-> (.digest ^MessageDigest md bytes)
        (prepend-value-type-id id-value-type-id))))

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
    (let [ub (UnsafeBuffer. (byte-array (+ Long/BYTES value-type-id-size)))]
      (.putByte ub 0 long-value-type-id)
      (.putLong ub value-type-id-size (bit-xor ^long this Long/MIN_VALUE) ByteOrder/BIG_ENDIAN)
      (.byteArray ub)))

  Float
  (value->bytes [this]
    (value->bytes (double this)))

  Double
  (value->bytes [this]
    (let [l (Double/doubleToLongBits this)
          l (inc (bit-xor l (bit-or (bit-shift-right l (dec Long/SIZE)) Long/MIN_VALUE)))
          ub (UnsafeBuffer. (byte-array (+ Long/BYTES value-type-id-size)))]
      (.putByte ub 0 double-value-type-id)
      (.putLong ub value-type-id-size l)
      (.byteArray ub)))

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
            terminate-mark-size Byte/BYTES
            offset (byte 2)
            ub-in (UnsafeBuffer. (.getBytes this "UTF-8"))
            length (.capacity ub-in)
            ub-out (UnsafeBuffer. (byte-array (+ value-type-id-size length terminate-mark-size)))]
        (.putByte ub-out 0 string-value-type-id)
        (loop [idx 0]
          (if (= idx length)
            (do (.putByte ub-out (inc idx) terminate-mark)
                (.byteArray ub-out))
            (let [b (.getByte ub-in idx)]
              (.putByte ub-out (inc idx) (byte (+ offset b)))
              (recur (inc idx))))))))

  nil
  (value->bytes [this]
    (id->bytes this))

  Keyword
  (value->bytes [this]
    (id->bytes this))

  UUID
  (value->bytes [this]
    (id->bytes this))

  URI
  (value->bytes [this]
    (id->bytes this))

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

(deftype Id [^bytes bytes ^long offset ^:unsynchronized-mutable ^int hash-code]
  IdToBytes
  (id->bytes [this]
    (if (= id-size (alength bytes))
      bytes
      (Arrays/copyOfRange bytes offset (+ offset id-size))))

  Object
  (toString [this]
    (bu/bytes->hex
     (Arrays/copyOfRange bytes (+ value-type-id-size offset) (+ offset id-size))))

  (equals [this that]
    (or (identical? this that)
        (and (satisfies? IdToBytes that)
             (bu/bytes=? (id->bytes this) (id->bytes that)))))

  (hashCode [this]
    (when (zero? hash-code)
      (set! hash-code (Arrays/hashCode (id->bytes this))))
    hash-code)

  IHashEq
  (hasheq [this]
    (.hashCode this))

  Comparable
  (compareTo [this that]
    (if (identical? this that)
      0
      (bu/compare-bytes (id->bytes this) (id->bytes that)))))

(defmethod print-method Id [id ^Writer w]
  (.write w "#crux/id ")
  (print-method (str id) w))

(defn new-id ^crux.codec.Id [id]
  (if (instance? Id id)
    id
    (let [bs (id->bytes id)]
      (assert (= id-size (alength bs)))
      (Id. bs 0 0))))

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
 (Id. (doto (byte-array id-size)
        (->> (.readFully data-input)))
      0
      0))

(defn encode-doc-key ^bytes [^bytes content-hash]
  (assert (= id-size (alength content-hash)))
  (let [ub (UnsafeBuffer. (byte-array (+ index-id-size id-size)))]
    (.putByte ub 0 content-hash->doc-index-id)
    (.putBytes ub index-id-size content-hash)
    (.byteArray ub)))

(defn decode-doc-key ^crux.codec.Id [^bytes k]
  (assert (= (+ index-id-size id-size) (alength k)))
  (let [ub (UnsafeBuffer. k)
        index-id (.getByte ub 0)]
    (assert (= content-hash->doc-index-id index-id))
    (Id. k index-id-size 0)))

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
   (let [ub (UnsafeBuffer. (byte-array (+ index-id-size id-size (alength v) (alength entity) (alength content-hash))))]
     (.putByte ub 0 attribute+value+entity+content-hash-index-id)
     (.putBytes ub index-id-size attr)
     (.putBytes ub (+ index-id-size id-size) v)
     (.putBytes ub (+ index-id-size id-size (alength v)) entity)
     (.putBytes ub (+ index-id-size id-size (alength v) (alength entity)) content-hash)
     (.byteArray ub))))

(defrecord EntityValueContentHash [eid value content-hash])

(defn decode-attribute+value+entity+content-hash-key->value+entity+content-hash
  ^crux.codec.EntityValueContentHash [^bytes k]
  (assert (<= (+ index-id-size id-size id-size id-size) (alength k)))
  (let [ub (UnsafeBuffer. k)
        index-id (.getByte ub 0)]
    (assert (= attribute+value+entity+content-hash-index-id index-id))
    (let [value-size (- (alength k) id-size id-size id-size index-id-size)
          value (doto (byte-array value-size)
                  (->> (.getBytes ub (+ index-id-size id-size))))
          entity (Id. k (+ index-id-size id-size value-size) 0)
          content-hash (Id. k (+ index-id-size id-size value-size id-size) 0)]
      (->EntityValueContentHash entity value content-hash))))

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
   (let [ub (UnsafeBuffer. (byte-array (+ index-id-size id-size (alength entity) (alength v) (alength content-hash))))]
     (.putByte ub 0 attribute+entity+value+content-hash-index-id)
     (.putBytes ub index-id-size attr)
     (.putBytes ub (+ index-id-size id-size) entity)
     (.putBytes ub (+ index-id-size id-size (alength entity)) v)
     (.putBytes ub (+ index-id-size id-size (alength entity) (alength v)) content-hash)
     (.byteArray ub))))

(defn decode-attribute+entity+value+content-hash-key->entity+value+content-hash
  ^crux.codec.EntityValueContentHash [^bytes k]
  (assert (<= (+ index-id-size id-size id-size) (alength k)))
  (let [ub (UnsafeBuffer. k)
        index-id (.getByte ub 0)]
    (assert (= attribute+entity+value+content-hash-index-id index-id))
    (let [value-size (- (alength k) id-size id-size id-size index-id-size)
          entity (Id. k (+ index-id-size id-size) 0)
          value (doto (byte-array value-size)
                  (->> (.getBytes ub (+ index-id-size id-size id-size))))
          content-hash (Id. k (+ index-id-size id-size id-size value-size) 0)]
      (->EntityValueContentHash entity value content-hash))))

(defn encode-meta-key ^bytes [^bytes k]
  (let [ub (UnsafeBuffer. (byte-array (+ index-id-size id-size)))]
    (.putByte ub 0 meta-key->value-index-id)
    (.putBytes ub index-id-size k)
    (.byteArray ub)))

(defn- date->reverse-time-ms ^long [^Date date]
  (bit-xor (bit-not (.getTime date)) Long/MIN_VALUE))

(defn- reverse-time-ms->date ^java.util.Date [^long reverse-time-ms]
  (Date. (bit-xor (bit-not reverse-time-ms) Long/MIN_VALUE)))

(defn encode-entity+bt+tt+tx-id-key
  (^bytes []
   (let [ub (UnsafeBuffer. (byte-array index-id-size))]
     (.putByte ub 0 entity+bt+tt+tx-id->content-hash-index-id)
     (.byteArray ub)))
  (^bytes [^bytes entity]
   (assert (= id-size (alength entity)))
   (let [ub (UnsafeBuffer. (byte-array (+ index-id-size id-size)))]
     (.putByte ub 0 entity+bt+tt+tx-id->content-hash-index-id)
     (.putBytes ub index-id-size entity)
     (.byteArray ub)))
  (^bytes [entity business-time transact-time]
   (encode-entity+bt+tt+tx-id-key entity business-time transact-time nil))
  (^bytes [^bytes entity ^Date business-time ^Date transact-time ^Long tx-id]
   (assert (= id-size (alength entity)))
   (let [ub (UnsafeBuffer. (byte-array (cond-> (+ index-id-size id-size Long/BYTES Long/BYTES)
                                         tx-id (+ Long/BYTES))))]
     (.putByte ub 0 entity+bt+tt+tx-id->content-hash-index-id)
     (.putBytes ub index-id-size entity)
     (.putLong ub (+ index-id-size id-size) (date->reverse-time-ms business-time) ByteOrder/BIG_ENDIAN)
     (.putLong ub (+ index-id-size id-size Long/BYTES) (date->reverse-time-ms transact-time) ByteOrder/BIG_ENDIAN)
     (when tx-id
       (.putLong ub (+ index-id-size id-size Long/BYTES Long/BYTES) tx-id ByteOrder/BIG_ENDIAN))
     (.byteArray ub))))

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

(defn decode-entity+bt+tt+tx-id-key ^crux.codec.EntityTx [^bytes k]
  (assert (= (+ index-id-size id-size Long/BYTES Long/BYTES Long/BYTES) (alength k)))
  (let [ub (UnsafeBuffer. k)
        index-id (.getByte ub 0)]
    (assert (= entity+bt+tt+tx-id->content-hash-index-id index-id))
    (let [entity (Id. k index-id-size 0)
          business-time (reverse-time-ms->date (.getLong ub (+ index-id-size id-size) ByteOrder/BIG_ENDIAN))
          transact-time (reverse-time-ms->date (.getLong ub (+ index-id-size id-size Long/BYTES) ByteOrder/BIG_ENDIAN))
          tx-id (.getLong ub (+ index-id-size id-size Long/BYTES Long/BYTES) ByteOrder/BIG_ENDIAN)]
      (->EntityTx entity business-time transact-time tx-id nil))))

(defn entity-tx->edn [{:keys [eid bt tt tx-id content-hash] :as entity-tx}]
  (when entity-tx
    {:crux.db/id (str eid)
     :crux.db/content-hash (str content-hash)
     :crux.db/business-time bt
     :crux.tx/tx-id tx-id
     :crux.tx/tx-time tt}))

(defn encode-tx-log-key
  (^bytes []
   (byte-array [tx-id->tx-index-id]))
  (^bytes [^long tx-id ^Date tx-time]
   (let [ub (UnsafeBuffer. (byte-array (+ index-id-size Long/BYTES Long/BYTES)))]
     (.putByte ub 0 tx-id->tx-index-id)
     (.putLong ub index-id-size tx-id ByteOrder/BIG_ENDIAN)
     (.putLong ub (+ index-id-size Long/BYTES) (.getTime tx-time) ByteOrder/BIG_ENDIAN)
     (.byteArray ub))))

(defn decode-tx-log-key [^bytes k]
  (assert (= (+ index-id-size Long/BYTES Long/BYTES) (alength k)))
  (let [ub (UnsafeBuffer. k)
        index-id (.getByte ub 0)]
    (assert (= tx-id->tx-index-id index-id))
    {:crux.tx/tx-id (.getLong ub index-id-size ByteOrder/BIG_ENDIAN)
     :crux.tx/tx-time (Date. (.getLong ub (+ index-id-size Long/BYTES) ByteOrder/BIG_ENDIAN))}))
