(ns crux.codec
  (:require [crux.byte-utils :as bu]
            [crux.hash :as hash]
            [crux.memory :as mem]
            [taoensso.nippy :as nippy])
  (:import [clojure.lang IHashEq IPersistentMap Keyword]
           [java.io Closeable Writer]
           java.net.URI
           [java.nio ByteOrder ByteBuffer]
           [java.util Arrays Date UUID]
           [org.agrona DirectBuffer ExpandableDirectByteBuffer MutableDirectBuffer]
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

(def ^:const id-size (+ hash/id-hash-size value-type-id-size))

(def empty-buffer (mem/allocate-buffer 0))

(def ^:const ^:private max-string-index-length 128)

(defprotocol IdToBuffer
  (id->buffer ^org.agrona.MutableDirectBuffer [this ^MutableDirectBuffer to]))

(defprotocol ValueToBuffer
  (value->buffer ^org.agrona.MutableDirectBuffer [this ^MutableDirectBuffer to]))

(def ^:private id-value-type-id 0)
(def ^:private long-value-type-id 1)
(def ^:private double-value-type-id 2)
(def ^:private date-value-type-id 3)
(def ^:private string-value-type-id 4)
(def ^:private bytes-value-type-id 5)
(def ^:private object-value-type-id 6)

(def nil-id-bytes (doto (byte-array id-size)
                    (aset 0 (byte id-value-type-id))))
(def nil-id-buffer (mem/->off-heap nil-id-bytes))

(defn- prepend-value-type-id ^bytes [^bytes bs ^long type-id]
  (let [ub (UnsafeBuffer. (byte-array (+ (alength bs) value-type-id-size)))]
    (.putByte ub 0 type-id)
    (.putBytes ub 1 bs)
    (.byteArray ub)))

(defn id-function ^bytes [^bytes bytes]
  (-> (hash/id-hash bytes)
      (prepend-value-type-id id-value-type-id)))

;; Adapted from https://github.com/ndimiduk/orderly
(extend-protocol ValueToBuffer
  (class (byte-array 0))
  (value->buffer [this to]
    (throw (UnsupportedOperationException. "Byte arrays as values is not supported.")))

  Byte
  (value->buffer [this to]
    (value->buffer (long this) to))

  Short
  (value->buffer [this to]
    (value->buffer (long this) to))

  Integer
  (value->buffer [this to]
    (value->buffer (long this) to))

  Long
  (value->buffer [this ^MutableDirectBuffer to]
    (UnsafeBuffer.
     (doto to
       (.putByte 0 long-value-type-id)
       (.putLong value-type-id-size (bit-xor ^long this Long/MIN_VALUE) ByteOrder/BIG_ENDIAN))
     0
     (+ value-type-id-size Long/BYTES)))

  Float
  (value->buffer [this to]
    (value->buffer (double this) to))

  Double
  (value->buffer [this ^MutableDirectBuffer to]
    (let [l (Double/doubleToLongBits this)
          l (inc (bit-xor l (bit-or (bit-shift-right l (dec Long/SIZE)) Long/MIN_VALUE)))]
      (UnsafeBuffer.
       (doto to
         (.putByte 0 double-value-type-id)
         (.putLong value-type-id-size l))
       0
       (+ value-type-id-size Long/BYTES))))

  Date
  (value->buffer [this ^MutableDirectBuffer to]
    (doto (value->buffer (.getTime this) to)
      (.putByte 0 (byte date-value-type-id))))

  Character
  (value->buffer [this to]
    (value->buffer (str this) to))

  String
  (value->buffer [this ^MutableDirectBuffer to]
    (if (< max-string-index-length (count this))
      (UnsafeBuffer.
       (doto to
         (.putBytes 0 (doto (id-function (nippy/fast-freeze this))
                        (aset 0 (byte object-value-type-id)))))
       0
       id-size)
      (let [terminate-mark (byte 1)
            terminate-mark-size Byte/BYTES
            offset (byte 2)
            ub-in (UnsafeBuffer. (.getBytes this "UTF-8"))
            length (.capacity ub-in)]
        (.putByte to 0 string-value-type-id)
        (loop [idx 0]
          (if (= idx length)
            (do (.putByte to (inc idx) terminate-mark)
                (UnsafeBuffer. to 0 (+ length value-type-id-size terminate-mark-size)))
            (let [b (.getByte ub-in idx)]
              (.putByte to (inc idx) (byte (+ offset b)))
              (recur (inc idx))))))))

  nil
  (value->buffer [this to]
    (id->buffer this to))

  Keyword
  (value->buffer [this to]
    (id->buffer this to))

  UUID
  (value->buffer [this to]
    (id->buffer this to))

  URI
  (value->buffer [this to]
    (id->buffer this to))

  Object
  (value->buffer [this ^MutableDirectBuffer to]
    (if (satisfies? IdToBuffer this)
      (id->buffer this to)
      (UnsafeBuffer.
       (doto to
         (.putBytes 0 (doto (id-function (nippy/fast-freeze this))
                        (aset 0 (byte object-value-type-id)))))
       0
       id-size))))

(defn ->value-buffer ^org.agrona.DirectBuffer [x]
  (value->buffer x (ExpandableDirectByteBuffer.)))

(defn value-buffer-type-id ^org.agrona.DirectBuffer [^DirectBuffer buffer]
  (mem/copy-buffer buffer value-type-id-size))

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

(declare ->id-buffer)

(extend-protocol IdToBuffer
  (class (byte-array 0))
  (id->buffer [this ^MutableDirectBuffer to]
    (if (= id-size (alength ^bytes this))
      (UnsafeBuffer.
       (doto to
         (.putBytes 0 this))
       0
       id-size)
      (throw (IllegalArgumentException.
              (str "Not an id byte array: " (bu/bytes->hex this))))))

  ByteBuffer
  (id->buffer [this ^MutableDirectBuffer to]
    (UnsafeBuffer. (doto to
                     (.putBytes 0 this)) 0 id-size))

  Keyword
  (id->buffer [this to]
    (id->buffer (id-function (.getBytes (subs (str this) 1))) to))

  UUID
  (id->buffer [this to]
    (id->buffer (id-function (.getBytes (str this))) to))

  URI
  (id->buffer [this to]
    (id->buffer (id-function (.getBytes (str (.normalize this)))) to))

  String
  (id->buffer [this to]
    (if (hex-id? this)
      (id->buffer (prepend-value-type-id (bu/hex->bytes this) id-value-type-id) to)
      (if-let [id (or (maybe-uuid-str this)
                      (maybe-keyword-str this))]
        (id->buffer id to)
        (throw (IllegalArgumentException. (format "Not a %s hex, keyword or an UUID string: %s" hash/id-hash-algorithm this))))))

  IPersistentMap
  (id->buffer [this to]
    (id->buffer (id-function (nippy/fast-freeze this)) to))

  nil
  (id->buffer [this to]
    (id->buffer nil-id-bytes to)))

(deftype Id [^DirectBuffer buffer ^:unsynchronized-mutable ^int hash-code]
  IdToBuffer
  (id->buffer [this to]
    (UnsafeBuffer.
     (doto ^MutableDirectBuffer to
       (.putBytes 0 buffer 0 (.capacity buffer)))
     0
     id-size))

  Object
  (toString [this]
    (bu/bytes->hex
     (let [bytes (byte-array (- id-size value-type-id-size))]
       (.getBytes buffer value-type-id-size bytes)
       bytes)))

  (equals [this that]
    (or (identical? this that)
        (and (satisfies? IdToBuffer that)
             (mem/buffers=? (.buffer this) (->id-buffer that)))))

  (hashCode [this]
    (when (zero? hash-code)
      (set! hash-code (.hashCode buffer)))
    hash-code)

  IHashEq
  (hasheq [this]
    (.hashCode this))

  Comparable
  (compareTo [this that]
    (if (identical? this that)
      0
      (mem/compare-buffers (->id-buffer this) (->id-buffer that)))))

(def ^:private ^crux.codec.Id nil-id
  (Id. nil-id-buffer (.hashCode ^DirectBuffer nil-id-buffer)))

(defn ->id-buffer ^org.agrona.DirectBuffer [x]
  (cond
    (instance? Id x)
    (.buffer ^Id x)

    (nil? x)
    nil-id-buffer

    (instance? DirectBuffer x)
    x

    :else
    (id->buffer x (ExpandableDirectByteBuffer.))))

(defn safe-id ^crux.codec.Id [^Id id]
  (Id. (mem/copy-buffer (.buffer id)) 0))

(defmethod print-method Id [id ^Writer w]
  (.write w "#crux/id ")
  (print-method (str id) w))

(defn new-id ^crux.codec.Id [id]
  (cond
    (instance? Id id)
    id

    (instance? DirectBuffer id)
    (do (assert (= id-size (mem/capacity id)))
        (Id. id 0))

    (nil? id)
    nil-id

    :else
    (let [bs (->id-buffer id)]
      (assert (= id-size (mem/capacity bs)))
      (Id. (UnsafeBuffer. bs) 0))))

(defn valid-id? [x]
  (try
    (->id-buffer x)
    true
    (catch IllegalArgumentException _
      false)))

(nippy/extend-freeze
 Id
 :crux.codec/id
 [x data-output]
 (.write data-output (mem/->on-heap (->id-buffer x))))

(nippy/extend-thaw
 :crux.codec/id
 [data-input]
 (Id. (mem/->off-heap (doto (byte-array id-size)
                        (->> (.readFully data-input))))
      0))

(defn encode-doc-key-to
  (^MutableDirectBuffer [^MutableDirectBuffer b content-hash]
   (encode-doc-key-to b 0 content-hash))
  (^MutableDirectBuffer [^MutableDirectBuffer b ^long offset content-hash]
   (assert (= id-size (mem/capacity content-hash)))
   (UnsafeBuffer.
    (doto b
      (.putByte offset content-hash->doc-index-id)
      (.putBytes (+ offset index-id-size) (mem/as-buffer content-hash) 0 (mem/capacity content-hash)))
    offset
    (+ index-id-size id-size))))

(defn encode-doc-key ^org.agrona.DirectBuffer [content-hash]
  (encode-doc-key-to (mem/allocate-buffer (+ index-id-size id-size)) content-hash))

(defn decode-doc-key-from
  (^crux.codec.Id [^MutableDirectBuffer k]
   (decode-doc-key-from k 0))
  (^crux.codec.Id [^MutableDirectBuffer k ^long offset]
   (assert (= (+ index-id-size id-size) (.capacity k)))
   (let [index-id (.getByte k offset)]
     (assert (= content-hash->doc-index-id index-id))
     (Id. (UnsafeBuffer. k (+ index-id-size offset) id-size) 0))))

(defn encode-attribute+value+entity+content-hash-key-to
  (^org.agrona.MutableDirectBuffer
   [^MutableDirectBuffer b ^DirectBuffer attr ^DirectBuffer v ^DirectBuffer entity ^DirectBuffer content-hash]
   (encode-attribute+value+entity+content-hash-key-to b 0 attr v entity content-hash))
  (^org.agrona.MutableDirectBuffer
   [^MutableDirectBuffer b offset ^DirectBuffer attr ^DirectBuffer v ^DirectBuffer entity ^DirectBuffer content-hash]
   (assert (= id-size (.capacity attr)))
   (assert (or (= id-size (.capacity entity))
               (zero? (.capacity entity))))
   (assert (or (= id-size (.capacity content-hash))
               (zero? (.capacity content-hash))))
   (let [offset (long offset)]
     (UnsafeBuffer.
      (doto b
        (.putByte offset attribute+value+entity+content-hash-index-id)
        (.putBytes (+ offset index-id-size) attr 0 id-size)
        (.putBytes (+ offset index-id-size id-size) v 0 (.capacity v))
        (.putBytes (+ offset index-id-size id-size (.capacity v)) entity 0 (.capacity entity))
        (.putBytes (+ offset index-id-size id-size (.capacity v) (.capacity entity)) content-hash 0 (.capacity content-hash)))
      offset
      (+ index-id-size id-size (.capacity v) (.capacity entity) (.capacity content-hash))))))

(defn encode-attribute+value+entity+content-hash-key
  (^org.agrona.DirectBuffer [attr]
   (encode-attribute+value+entity+content-hash-key attr empty-buffer))
  (^org.agrona.DirectBuffer [attr v]
   (encode-attribute+value+entity+content-hash-key attr v empty-buffer))
  (^org.agrona.DirectBuffer [attr v entity]
   (encode-attribute+value+entity+content-hash-key attr v entity empty-buffer))
  (^org.agrona.DirectBuffer [attr v entity content-hash]
   (encode-attribute+value+entity+content-hash-key-to
    (mem/allocate-buffer (+ index-id-size id-size (mem/capacity v) (mem/capacity entity) (mem/capacity content-hash)))
    attr v entity content-hash)))

(defrecord EntityValueContentHash [eid value content-hash])

(defn decode-attribute+value+entity+content-hash-key->value+entity+content-hash-from
  (^crux.codec.EntityValueContentHash [^DirectBuffer k]
   (decode-attribute+value+entity+content-hash-key->value+entity+content-hash-from k 0 (.capacity k)))
  (^crux.codec.EntityValueContentHash [^DirectBuffer k ^long offset ^long length]
   (assert (<= (+ index-id-size id-size id-size id-size) length))
   (let [index-id (.getByte k offset)]
     (assert (= attribute+value+entity+content-hash-index-id index-id))
     (let [value-size (- length id-size id-size id-size index-id-size)
           value (UnsafeBuffer. k (+ offset index-id-size id-size) value-size)
           entity (Id. (UnsafeBuffer. k (+ offset index-id-size id-size value-size) id-size) 0)
           content-hash (Id. (UnsafeBuffer. k (+ offset index-id-size id-size value-size id-size) id-size) 0)]
       (->EntityValueContentHash entity value content-hash)))))

(defn encode-attribute+entity+value+content-hash-key-to
  (^org.agrona.MutableDirectBuffer [^MutableDirectBuffer b ^DirectBuffer attr ^DirectBuffer entity ^DirectBuffer v ^DirectBuffer content-hash]
   (encode-attribute+entity+value+content-hash-key-to b 0 attr entity v content-hash))
  (^org.agrona.MutableDirectBuffer [^MutableDirectBuffer b offset ^DirectBuffer attr ^DirectBuffer entity ^DirectBuffer v ^DirectBuffer content-hash]
   (assert (= id-size (.capacity attr)))
   (assert (or (= id-size (.capacity entity))
               (zero? (.capacity entity))))
   (assert (or (= id-size (.capacity content-hash))
               (zero? (.capacity content-hash))))
   (let [offset (long offset)]
     (UnsafeBuffer.
      (doto b
        (.putByte offset attribute+entity+value+content-hash-index-id)
        (.putBytes (+ offset index-id-size) attr 0 id-size)
        (.putBytes (+ offset index-id-size id-size) entity 0 (.capacity entity))
        (.putBytes (+ offset index-id-size id-size (.capacity entity)) v 0 (.capacity v))
        (.putBytes (+ offset index-id-size id-size (.capacity entity) (.capacity v)) content-hash 0 (.capacity content-hash)))
      offset
      (+ index-id-size id-size (.capacity entity) (.capacity v) (.capacity content-hash))))))

(defn encode-attribute+entity+value+content-hash-key
  (^org.agrona.DirectBuffer [attr]
   (encode-attribute+entity+value+content-hash-key attr empty-buffer))
  (^org.agrona.DirectBuffer [attr entity]
   (encode-attribute+entity+value+content-hash-key attr entity empty-buffer))
  (^org.agrona.DirectBuffer [attr entity v]
   (encode-attribute+entity+value+content-hash-key attr entity v empty-buffer))
  (^org.agrona.DirectBuffer [attr entity v content-hash]
   (encode-attribute+entity+value+content-hash-key-to
    (mem/allocate-buffer (+ index-id-size id-size (mem/capacity entity) (mem/capacity v) (mem/capacity content-hash)))
    attr entity v content-hash)))

(defn decode-attribute+entity+value+content-hash-key->entity+value+content-hash-from
  (^crux.codec.EntityValueContentHash [^DirectBuffer k]
   (decode-attribute+entity+value+content-hash-key->entity+value+content-hash-from k 0 (.capacity k)))
  (^crux.codec.EntityValueContentHash [^DirectBuffer k ^long offset ^long length]
   (assert (<= (+ index-id-size id-size id-size) length))
   (let [index-id (.getByte k offset)]
     (assert (= attribute+entity+value+content-hash-index-id index-id))
     (let [value-size (- length id-size id-size id-size index-id-size)
           entity (Id. (UnsafeBuffer. k (+ offset index-id-size id-size) id-size) 0)
           value (UnsafeBuffer. k (+ offset index-id-size id-size id-size) value-size)
           content-hash (Id. (UnsafeBuffer. k (+ offset index-id-size id-size id-size value-size) id-size) 0)]
       (->EntityValueContentHash entity value content-hash)))))

(defn encode-meta-key-to
  (^MutableDirectBuffer [^MutableDirectBuffer b ^DirectBuffer k]
   (encode-meta-key-to b 0 k))
  (^MutableDirectBuffer [^MutableDirectBuffer b ^long offset ^DirectBuffer k]
   (assert (= id-size (.capacity k)))
   (UnsafeBuffer.
    (doto b
      (.putByte offset meta-key->value-index-id)
      (.putBytes (+ offset index-id-size) k 0 (.capacity k)))
    offset
    (+ index-id-size id-size))))

(defn encode-meta-key ^org.agrona.DirectBuffer [k]
  (encode-meta-key-to (mem/allocate-buffer (+ index-id-size id-size)) k))

(defn- date->reverse-time-ms ^long [^Date date]
  (bit-xor (bit-not (.getTime date)) Long/MIN_VALUE))

(defn- reverse-time-ms->date ^java.util.Date [^long reverse-time-ms]
  (Date. (bit-xor (bit-not reverse-time-ms) Long/MIN_VALUE)))

(defn- maybe-long-size ^long [x]
  (if x
    Long/BYTES
    0))

(defn encode-entity+bt+tt+tx-id-key-to
  (^org.agrona.MutableDirectBuffer [^MutableDirectBuffer b ^DirectBuffer entity ^Date business-time ^Date transact-time ^Long tx-id]
   (encode-entity+bt+tt+tx-id-key-to b 0 entity business-time transact-time tx-id))
  (^org.agrona.MutableDirectBuffer [^MutableDirectBuffer b offset ^DirectBuffer entity ^Date business-time ^Date transact-time ^Long tx-id]
   (assert (or (= id-size (.capacity entity))
               (zero? (.capacity entity))))
   (let [offset (long offset)]
     (.putByte b offset entity+bt+tt+tx-id->content-hash-index-id)
     (.putBytes b (+ offset index-id-size) entity 0 (.capacity entity))
     (when business-time
       (.putLong b (+ offset index-id-size id-size) (date->reverse-time-ms business-time) ByteOrder/BIG_ENDIAN))
     (when transact-time
       (.putLong b (+ offset index-id-size id-size Long/BYTES) (date->reverse-time-ms transact-time) ByteOrder/BIG_ENDIAN))
     (when tx-id
       (.putLong b (+ offset index-id-size id-size Long/BYTES Long/BYTES) tx-id ByteOrder/BIG_ENDIAN))
     (->> (+ offset index-id-size (.capacity entity)
             (maybe-long-size business-time) (maybe-long-size transact-time) (maybe-long-size tx-id))
          (UnsafeBuffer. b offset)))))

(defn encode-entity+bt+tt+tx-id-key
  (^org.agrona.DirectBuffer []
   (encode-entity+bt+tt+tx-id-key empty-buffer nil nil nil))
  (^org.agrona.DirectBuffer [entity]
   (encode-entity+bt+tt+tx-id-key entity nil nil nil))
  (^org.agrona.DirectBuffer [entity business-time transact-time]
   (encode-entity+bt+tt+tx-id-key entity business-time transact-time nil))
  (^org.agrona.DirectBuffer [entity ^Date business-time ^Date transact-time ^Long tx-id]
   (encode-entity+bt+tt+tx-id-key-to
    (mem/allocate-buffer (cond-> (+ index-id-size (mem/capacity entity))
                           business-time (+ Long/BYTES)
                           transact-time (+ Long/BYTES)
                           tx-id (+ Long/BYTES)))
    entity
    business-time
    transact-time
    tx-id)))

(defrecord EntityTx [^Id eid ^Date bt ^Date tt ^long tx-id ^Id content-hash]
  IdToBuffer
  (id->buffer [this to]
    (id->buffer eid to)))

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

(defn decode-entity+bt+tt+tx-id-key-from
  (^crux.codec.EntityTx [^DirectBuffer k]
   (decode-entity+bt+tt+tx-id-key-from k 0))
  (^crux.codec.EntityTx [^DirectBuffer k ^long offset]
   (assert (= (+ index-id-size id-size Long/BYTES Long/BYTES Long/BYTES) (.capacity k)))
   (let [index-id (.getByte k offset)]
     (assert (= entity+bt+tt+tx-id->content-hash-index-id index-id))
     (let [entity (Id. (UnsafeBuffer. k (+ offset index-id-size) id-size) 0)
           business-time (reverse-time-ms->date (.getLong k (+ offset index-id-size id-size) ByteOrder/BIG_ENDIAN))
           transact-time (reverse-time-ms->date (.getLong k (+ offset index-id-size id-size Long/BYTES) ByteOrder/BIG_ENDIAN))
           tx-id (.getLong k (+ offset index-id-size id-size Long/BYTES Long/BYTES) ByteOrder/BIG_ENDIAN)]
       (->EntityTx entity business-time transact-time tx-id nil)))))

(defn entity-tx->edn [{:keys [eid bt tt tx-id content-hash] :as entity-tx}]
  (when entity-tx
    {:crux.db/id (str eid)
     :crux.db/content-hash (str content-hash)
     :crux.db/business-time bt
     :crux.tx/tx-id tx-id
     :crux.tx/tx-time tt}))

(defn encode-tx-log-key-to
  (^org.agrona.MutableDirectBuffer [^MutableDirectBuffer b ^Long tx-id ^Date tx-time]
   (encode-tx-log-key-to b 0 tx-id tx-time))
  (^org.agrona.MutableDirectBuffer [^MutableDirectBuffer b ^long offset ^Long tx-id ^Date tx-time]
   (.putByte b offset tx-id->tx-index-id)
   (when tx-id
     (.putLong b (+ offset index-id-size) tx-id ByteOrder/BIG_ENDIAN))
   (when tx-time
     (.putLong b (+ offset index-id-size Long/BYTES) (.getTime tx-time) ByteOrder/BIG_ENDIAN))
   (UnsafeBuffer. b offset (+ index-id-size (maybe-long-size tx-id) (maybe-long-size tx-time)))))

(defn encode-tx-log-key
  (^org.agrona.DirectBuffer []
   (encode-tx-log-key nil nil))
  (^org.agrona.DirectBuffer [^Long tx-id ^Date tx-time]
   (encode-tx-log-key-to
    (mem/allocate-buffer (+ index-id-size (maybe-long-size tx-id) (maybe-long-size tx-time)))
    tx-id
    tx-time)))

(defn decode-tx-log-key-from
  ([^DirectBuffer k]
   (decode-tx-log-key-from k 0))
  ([^DirectBuffer k ^long offset]
   (assert (= (+ index-id-size Long/BYTES Long/BYTES) (.capacity k)))
   (let [index-id (.getByte k offset)]
     (assert (= tx-id->tx-index-id index-id))
     {:crux.tx/tx-id (.getLong k (+ offset index-id-size) ByteOrder/BIG_ENDIAN)
      :crux.tx/tx-time (Date. (.getLong k (+ offset index-id-size Long/BYTES) ByteOrder/BIG_ENDIAN))})))
