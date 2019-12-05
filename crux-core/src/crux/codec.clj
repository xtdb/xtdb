(ns crux.codec
  (:require [clojure.edn :as edn]
            [crux.hash :as hash]
            [crux.memory :as mem]
            [crux.morton :as morton]
            [taoensso.nippy :as nippy]
            [crux.io :as cio])
  (:import [clojure.lang IHashEq IPersistentMap Keyword]
           [java.io Closeable Writer]
           [java.net MalformedURLException URI URL]
           [java.nio ByteOrder ByteBuffer]
           [java.util Arrays Date UUID]
           [org.agrona DirectBuffer ExpandableDirectByteBuffer MutableDirectBuffer]
           org.agrona.concurrent.UnsafeBuffer))

(set! *unchecked-math* :warn-on-boxed)

;; Indexes

;; NOTE: Must be updated when existing indexes change structure.
(def ^:const index-version 5)
(def ^:const index-version-size Long/BYTES)

(def ^:const index-id-size Byte/BYTES)

; index for actual document store
(def ^:const ^:private content-hash->doc-index-id 0)


; two main indexes for querying
(def ^:const ^:private attribute+value+entity+content-hash-index-id 1)
(def ^:const ^:private attribute+entity+content-hash+value-index-id 2)

; how they work
(comment
  (api/submit-tx syst [:crux.tx/put {:crux.db/id :ids/ivan :name "ivan"}])

  ; [roughly speaking] for queries by attr name and value
  ; in attribute+value+entity+content-hash-index-id
  ; [:name "ivan" :ids/ivan "ivan-content-hash"]

  ; [roughly speaking] for entity queries by id
  ; in attribute+entity+content-hash+value-index-id
  ; [:name :ids/ivan "ivan-content-hash" "ivan"]

  (api/q db {:find ?e
             :where
             [?e :name "ivan"]}))

; main bitemp index [reverse]
(def ^:const ^:private entity+vt+tt+tx-id->content-hash-index-id 3)

; for crux own needs
(def ^:const ^:private meta-key->value-index-id 4)

; Repurpose old id from internal tx-log used for testing for failed tx
; ids.
(def ^:const ^:private failed-tx-id-index-id 5)

; to allow crux upgrades. rebuild indexes from kafka on backward incompatible
(def ^:const ^:private index-version-index-id 6)

; second bitemp index [also reverse]
; z combines vt and tt
; used when a lookup by the first index fails
(def ^:const ^:private entity+z+tx-id->content-hash-index-id 7)


(def ^:const ^:private value-type-id-size Byte/BYTES)

(def ^:const id-size (+ hash/id-hash-size value-type-id-size))

(def empty-buffer (mem/allocate-unpooled-buffer 0))

(def ^:const ^:private max-string-index-length 128)

(defprotocol IdOrBuffer
  (new-id ^crux.codec.Id [id])
  (->id-buffer ^org.agrona.DirectBuffer [this]))

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
(def nil-id-buffer
  (memoize
   (fn []
     (mem/->off-heap nil-id-bytes (mem/allocate-unpooled-buffer (count nil-id-bytes))))))

(defn id-function ^org.agrona.MutableDirectBuffer [^MutableDirectBuffer to bs]
  (.putByte to 0 (byte id-value-type-id))
  (hash/id-hash (mem/slice-buffer to value-type-id-size hash/id-hash-size) (mem/as-buffer bs))
  (mem/limit-buffer to id-size))

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
    (mem/limit-buffer
     (doto to
       (.putByte 0 long-value-type-id)
       (.putLong value-type-id-size (bit-xor ^long this Long/MIN_VALUE) ByteOrder/BIG_ENDIAN))
     (+ value-type-id-size Long/BYTES)))

  Float
  (value->buffer [this to]
    (value->buffer (double this) to))

  Double
  (value->buffer [this ^MutableDirectBuffer to]
    (let [l (Double/doubleToLongBits this)
          l (inc (bit-xor l (bit-or (bit-shift-right l (dec Long/SIZE)) Long/MIN_VALUE)))]
      (mem/limit-buffer
       (doto to
         (.putByte 0 double-value-type-id)
         (.putLong value-type-id-size l ByteOrder/BIG_ENDIAN))
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
      (doto (id-function to (nippy/fast-freeze this))
        (.putByte 0 (byte object-value-type-id)))
      (let [terminate-mark (byte 1)
            terminate-mark-size Byte/BYTES
            offset (byte 2)
            ub-in (mem/on-heap-buffer (.getBytes this "UTF-8"))
            length (.capacity ub-in)]
        (.putByte to 0 string-value-type-id)
        (loop [idx 0]
          (if (= idx length)
            (do (.putByte to (inc idx) terminate-mark)
                (mem/limit-buffer to (+ length value-type-id-size terminate-mark-size)))
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

  URL
  (value->buffer [this to]
    (id->buffer this to))

  Object
  (value->buffer [this ^MutableDirectBuffer to]
    (if (satisfies? IdToBuffer this)
      (id->buffer this to)
      (doto (id-function to (nippy/fast-freeze this))
        (.putByte 0 (byte object-value-type-id))))))

(defn ->value-buffer ^org.agrona.DirectBuffer [x]
  (value->buffer x (ExpandableDirectByteBuffer. 32)))

(defn value-buffer-type-id ^org.agrona.DirectBuffer [^DirectBuffer buffer]
  (mem/limit-buffer buffer value-type-id-size))

(def ^:private hex-id-pattern
  (re-pattern (format "\\p{XDigit}{%d}" (* 2 (dec id-size)))))

(defn hex-id? [s]
  (re-find hex-id-pattern s))

(defn- maybe-uuid-str [s]
  (try
    (UUID/fromString s)
    (catch IllegalArgumentException _)))

(defn- maybe-keyword-str [s]
  (when-let [[_ n] (re-find #"^\:(.+)" s)]
    (keyword n)))

(defn- maybe-url-str [s]
  (try
    (URL. s)
    (catch MalformedURLException _)))

(extend-protocol IdToBuffer
  (class (byte-array 0))
  (id->buffer [this ^MutableDirectBuffer to]
    (if (= id-size (alength ^bytes this))
      (mem/limit-buffer
       (doto to
         (.putBytes 0 this))
       id-size)
      (throw (IllegalArgumentException.
              (str "Not an id byte array: " (mem/buffer->hex (mem/as-buffer this)))))))

  ByteBuffer
  (id->buffer [this ^MutableDirectBuffer to]
    (mem/limit-buffer (doto to
                        (.putBytes 0 this)) id-size))

  DirectBuffer
  (id->buffer [this ^MutableDirectBuffer to]
    (mem/limit-buffer (doto to
                        (.putBytes 0 this 0 id-size)) id-size))

  Keyword
  (id->buffer [this to]
    (id-function to (.getBytes (subs (str this) 1))))

  UUID
  (id->buffer [this to]
    (id-function to (.getBytes (str this))))

  URI
  (id->buffer [this to]
    (id-function to (.getBytes (str (.normalize this)))))

  URL
  (id->buffer [this to]
    (id-function to (.getBytes (.toExternalForm this))))

  String
  (id->buffer [this to]
    (if (hex-id? this)
      (let [^MutableDirectBuffer to (mem/limit-buffer to id-size)]
        (.putByte to 0 id-value-type-id)
        (mem/hex->buffer this (mem/slice-buffer to value-type-id-size hash/id-hash-size))
        to)
      (if-let [id (or (maybe-uuid-str this)
                      (maybe-keyword-str this)
                      (maybe-url-str this))]
        (id->buffer id to)
        (throw (IllegalArgumentException. (format "Not a %s hex, keyword, URL or an UUID string: %s" hash/id-hash-algorithm this))))))

  IPersistentMap
  (id->buffer [this to]
    (id-function to (nippy/fast-freeze this)))

  nil
  (id->buffer [this to]
    (id->buffer (nil-id-buffer) to)))

(deftype Id [^org.agrona.DirectBuffer buffer ^:unsynchronized-mutable ^int hash-code]
  IdToBuffer
  (id->buffer [this to]
    (id->buffer buffer to))

  Object
  (toString [this]
    (mem/buffer->hex (mem/slice-buffer buffer value-type-id-size hash/id-hash-size)))

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

(defmethod print-method Id [id ^Writer w]
  (.write w "#crux/id ")
  (.write w (cio/pr-edn-str (str id))))

(defmethod print-dup Id [id ^Writer w]
  (.write w "#crux/id ")
  (.write w (cio/pr-edn-str (str id))))

(extend-protocol IdOrBuffer
  Id
  (->id-buffer [this]
    (.buffer this))

  (new-id [this]
    this)

  DirectBuffer
  (->id-buffer [this]
    this)

  (new-id [this]
    (do (assert (= id-size (.capacity this)) (mem/buffer->hex this))
        (Id. this 0)))

  nil
  (->id-buffer [this]
    (nil-id-buffer))

  (new-id [this]
    (Id. (nil-id-buffer) (.hashCode ^DirectBuffer (nil-id-buffer))))

  Object
  (->id-buffer [this]
    (id->buffer this (mem/allocate-buffer id-size)))

  (new-id [this]
    (let [bs (->id-buffer this)]
      (assert (= id-size (.capacity bs)) (mem/buffer->hex bs))
      (Id. (UnsafeBuffer. bs) 0))))

(defn safe-id ^crux.codec.Id [^Id id]
  (when id
    (Id. (mem/copy-to-unpooled-buffer (.buffer id)) 0)))

(deftype EDNId [hex original-id]
  IdOrBuffer
  (->id-buffer [this]
    (->id-buffer (new-id hex)))

  (new-id [this]
    (new-id hex))

  IdToBuffer
  (id->buffer [this to]
    (id->buffer (new-id hex) to))

  Object
  (toString [this]
    hex)

  (equals [this that]
    (.equals (new-id hex) that))

  (hashCode [this]
    (.hashCode (new-id hex)))

  IHashEq
  (hasheq [this]
    (.hashCode this))

  Comparable
  (compareTo [this that]
    (.compareTo (new-id hex) that)))

(defn id-edn-reader ^crux.codec.EDNId [id]
  (->EDNId (str (new-id id)) id))

(defn read-edn-string-with-readers [s]
  (edn/read-string {:readers {'crux/id id-edn-reader}} s))

(defn edn-id->original-id [^EDNId id]
  (str (or (.original-id id) (.hex id))))

(defmethod print-method EDNId [^EDNId id ^Writer w]
  (.write w "#crux/id ")
  (.write w (cio/pr-edn-str (edn-id->original-id id))))

(defmethod print-dup EDNId [^EDNId id ^Writer w]
  (.write w "#crux/id ")
  (.write w (cio/pr-edn-str (edn-id->original-id id))))

(nippy/extend-freeze
 EDNId
 :crux.codec/edn-id
 [^EDNId x data-output]
 (nippy/freeze-to-out! data-output (or (.original-id x) (.hex x))))

(nippy/extend-thaw
 :crux.codec/edn-id
 [data-input]
 (id-edn-reader (nippy/thaw-from-in! data-input)))

(defn valid-id? [x]
  (try
    (= id-size (.capacity (->id-buffer x)))
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

(defn encode-doc-key-to ^org.agrona.MutableDirectBuffer [^MutableDirectBuffer b ^DirectBuffer content-hash]
  (assert (= id-size (.capacity content-hash)) (mem/buffer->hex content-hash))
  (let [^MutableDirectBuffer b (or b (mem/allocate-buffer (+ index-id-size id-size)))]
    (mem/limit-buffer
     (doto b
       (.putByte 0 content-hash->doc-index-id)
       (.putBytes index-id-size (mem/as-buffer content-hash) 0 (.capacity content-hash)))
     (+ index-id-size id-size))))

(defn decode-doc-key-from ^crux.codec.Id [^MutableDirectBuffer k]
  (assert (= (+ index-id-size id-size) (.capacity k)) (mem/buffer->hex k))
  (let [index-id (.getByte k 0)]
    (assert (= content-hash->doc-index-id index-id))
    (Id. (mem/slice-buffer k index-id-size id-size) 0)))

(defn encode-attribute+value+entity+content-hash-key-to
  (^org.agrona.MutableDirectBuffer[b attr]
   (encode-attribute+value+entity+content-hash-key-to b attr empty-buffer empty-buffer empty-buffer))
  (^org.agrona.MutableDirectBuffer[b attr v]
   (encode-attribute+value+entity+content-hash-key-to b attr v empty-buffer empty-buffer))
  (^org.agrona.MutableDirectBuffer[b attr v entity]
   (encode-attribute+value+entity+content-hash-key-to b attr v entity empty-buffer))
  (^org.agrona.MutableDirectBuffer
   [^MutableDirectBuffer b ^DirectBuffer attr ^DirectBuffer v ^DirectBuffer entity ^DirectBuffer content-hash]
   (assert (= id-size (.capacity attr)) (mem/buffer->hex attr))
   (assert (or (= id-size (.capacity entity))
               (zero? (.capacity entity))) (mem/buffer->hex entity))
   (assert (or (= id-size (.capacity content-hash))
               (zero? (.capacity content-hash))) (mem/buffer->hex content-hash))
   (let [^MutableDirectBuffer b (or b (mem/allocate-buffer (+ index-id-size id-size (.capacity v) (.capacity entity) (.capacity content-hash))))]
     (mem/limit-buffer
      (doto b
        (.putByte 0 attribute+value+entity+content-hash-index-id)
        (.putBytes index-id-size attr 0 id-size)
        (.putBytes (+ index-id-size id-size) v 0 (.capacity v))
        (.putBytes (+ index-id-size id-size (.capacity v)) entity 0 (.capacity entity))
        (.putBytes (+ index-id-size id-size (.capacity v) (.capacity entity)) content-hash 0 (.capacity content-hash)))
      (+ index-id-size id-size (.capacity v) (.capacity entity) (.capacity content-hash))))))

(defrecord EntityValueContentHash [eid value content-hash])

(defn decode-attribute+value+entity+content-hash-key->value+entity+content-hash-from
  ^crux.codec.EntityValueContentHash [^DirectBuffer k]
  (let [length (long (.capacity k))]
    (assert (<= (+ index-id-size id-size id-size id-size) length) (mem/buffer->hex k))
    (let [index-id (.getByte k 0)]
      (assert (= attribute+value+entity+content-hash-index-id index-id))
      (let [value-size (- length id-size id-size id-size index-id-size)
            value (mem/slice-buffer k (+ index-id-size id-size) value-size)
            entity (Id. (mem/slice-buffer k (+ index-id-size id-size value-size) id-size) 0)
            content-hash (Id. (mem/slice-buffer k (+ index-id-size id-size value-size id-size) id-size) 0)]
        (->EntityValueContentHash entity value content-hash)))))

;; TODO: Swap v and content-hash, so it has the same order as AVE,
;; this allows skipping straight to the right version in crux.index,
;; otherwise one has to scan each change of a field.
(defn encode-attribute+entity+content-hash+value-key-to
  (^org.agrona.MutableDirectBuffer [b attr]
   ( encode-attribute+entity+content-hash+value-key-to b attr empty-buffer empty-buffer empty-buffer))
  (^org.agrona.MutableDirectBuffer [b attr entity]
   ( encode-attribute+entity+content-hash+value-key-to b attr entity empty-buffer empty-buffer))
  (^org.agrona.MutableDirectBuffer [b attr entity content-hash]
   ( encode-attribute+entity+content-hash+value-key-to b attr entity content-hash empty-buffer))
  (^org.agrona.MutableDirectBuffer [^MutableDirectBuffer b ^DirectBuffer attr ^DirectBuffer entity ^DirectBuffer content-hash ^DirectBuffer v]
   (assert (= id-size (.capacity attr)) (mem/buffer->hex attr))
   (assert (or (= id-size (.capacity content-hash))
               (zero? (.capacity content-hash))) (mem/buffer->hex content-hash))
   (assert (or (= id-size (.capacity entity))
               (zero? (.capacity entity))) (mem/buffer->hex entity))
   (let [^MutableDirectBuffer b (or b (mem/allocate-buffer (+ index-id-size id-size (.capacity entity) (.capacity content-hash) (.capacity v))))]
     (mem/limit-buffer
      (doto b
        (.putByte 0 attribute+entity+content-hash+value-index-id)
        (.putBytes index-id-size attr 0 id-size)
        (.putBytes (+ index-id-size id-size) entity 0 (.capacity entity))
        (.putBytes (+ index-id-size id-size (.capacity entity)) content-hash 0 (.capacity content-hash))
        (.putBytes (+ index-id-size id-size (.capacity entity) (.capacity content-hash)) v 0 (.capacity v)))
      (+ index-id-size id-size (.capacity entity) (.capacity content-hash) (.capacity v))))))

(defn decode-attribute+entity+content-hash+value-key->entity+value+content-hash-from
  ^crux.codec.EntityValueContentHash [^DirectBuffer k]
  (let [length (long (.capacity k))]
    (assert (<= (+ index-id-size id-size id-size) length) (mem/buffer->hex k))
    (let [index-id (.getByte k 0)]
      (assert (= attribute+entity+content-hash+value-index-id index-id))
      (let [value-size (- length id-size id-size id-size index-id-size)
            entity (Id. (mem/slice-buffer k (+ index-id-size id-size) id-size) 0)
            content-hash (Id. (mem/slice-buffer k (+ index-id-size id-size id-size) id-size) 0)
            value (mem/slice-buffer k (+ index-id-size id-size id-size id-size) value-size)]
        (->EntityValueContentHash entity value content-hash)))))

(defn encode-meta-key-to ^org.agrona.MutableDirectBuffer [^MutableDirectBuffer b ^DirectBuffer k]
  (assert (= id-size (.capacity k)) (mem/buffer->hex k))
  (let [^MutableDirectBuffer b (or b (mem/allocate-buffer (+ index-id-size id-size)))]
    (mem/limit-buffer
     (doto b
       (.putByte 0 meta-key->value-index-id)
       (.putBytes index-id-size k 0 (.capacity k)))
     (+ index-id-size id-size))))

(defn- descending-long ^long [^long l]
  (bit-xor (bit-not l) Long/MIN_VALUE))

(defn date->reverse-time-ms ^long [^Date date]
  (descending-long (.getTime date)))

(defn reverse-time-ms->date ^java.util.Date [^long reverse-time-ms]
  (Date. (descending-long reverse-time-ms)))

(defn- maybe-long-size ^long [x]
  (if x
    Long/BYTES
    0))

(defn encode-entity+vt+tt+tx-id-key-to
  (^org.agrona.MutableDirectBuffer [^MutableDirectBuffer b]
   (encode-entity+vt+tt+tx-id-key-to b empty-buffer nil nil nil))
  (^org.agrona.MutableDirectBuffer [^MutableDirectBuffer b entity]
   (encode-entity+vt+tt+tx-id-key-to b entity nil nil nil))
  (^org.agrona.MutableDirectBuffer [^MutableDirectBuffer b entity valid-time]
   (encode-entity+vt+tt+tx-id-key-to b entity valid-time nil nil))
  (^org.agrona.MutableDirectBuffer [^MutableDirectBuffer b ^DirectBuffer entity ^Date valid-time ^Date transact-time ^Long tx-id]
   (assert (or (= id-size (.capacity entity))
               (zero? (.capacity entity))) (mem/buffer->hex entity))
   (let [^MutableDirectBuffer b (or b (mem/allocate-buffer (cond-> (+ index-id-size (.capacity entity))
                                                             valid-time (+ Long/BYTES)
                                                             transact-time (+ Long/BYTES)
                                                             tx-id (+ Long/BYTES))))]
     (.putByte b 0 entity+vt+tt+tx-id->content-hash-index-id)
     (.putBytes b index-id-size entity 0 (.capacity entity))
     (when valid-time
       (.putLong b (+ index-id-size id-size) (date->reverse-time-ms valid-time) ByteOrder/BIG_ENDIAN))
     (when transact-time
       (.putLong b (+ index-id-size id-size Long/BYTES) (date->reverse-time-ms transact-time) ByteOrder/BIG_ENDIAN))
     (when tx-id
       (.putLong b (+ index-id-size id-size Long/BYTES Long/BYTES) (descending-long tx-id) ByteOrder/BIG_ENDIAN))
     (->> (+ index-id-size (.capacity entity)
             (maybe-long-size valid-time) (maybe-long-size transact-time) (maybe-long-size tx-id))
          (mem/limit-buffer b)))))

(defrecord EntityTx [^Id eid ^Date vt ^Date tt ^long tx-id ^Id content-hash]
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

(defn decode-entity+vt+tt+tx-id-key-from ^crux.codec.EntityTx [^DirectBuffer k]
  (assert (= (+ index-id-size id-size Long/BYTES Long/BYTES Long/BYTES) (.capacity k)) (mem/buffer->hex k))
  (let [index-id (.getByte k 0)]
    (assert (= entity+vt+tt+tx-id->content-hash-index-id index-id))
    (let [entity (Id. (mem/slice-buffer k index-id-size id-size) 0)
          valid-time (reverse-time-ms->date (.getLong k (+ index-id-size id-size) ByteOrder/BIG_ENDIAN))
          transact-time (reverse-time-ms->date (.getLong k (+ index-id-size id-size Long/BYTES) ByteOrder/BIG_ENDIAN))
          tx-id (descending-long (.getLong k (+ index-id-size id-size Long/BYTES Long/BYTES) ByteOrder/BIG_ENDIAN))]
      (->EntityTx entity valid-time transact-time tx-id nil))))

(defn encode-entity-tx-z-number [valid-time transaction-time]
  (morton/longs->morton-number (date->reverse-time-ms valid-time)
                               (date->reverse-time-ms transaction-time)))

(defn encode-entity+z+tx-id-key-to
  (^org.agrona.MutableDirectBuffer [^MutableDirectBuffer b]
   (encode-entity+z+tx-id-key-to b empty-buffer nil))
  (^org.agrona.MutableDirectBuffer [^MutableDirectBuffer b entity]
   (encode-entity+z+tx-id-key-to b entity nil nil))
  (^org.agrona.MutableDirectBuffer [^MutableDirectBuffer b entity z]
   (encode-entity+z+tx-id-key-to b entity z nil))
  (^org.agrona.MutableDirectBuffer [^MutableDirectBuffer b ^DirectBuffer entity z ^Long tx-id]
   (assert (or (= id-size (.capacity entity))
               (zero? (.capacity entity))) (mem/buffer->hex entity))
   (let [^MutableDirectBuffer b (or b (mem/allocate-buffer (cond-> (+ index-id-size (.capacity entity))
                                                             z (+ (* 2 Long/BYTES))
                                                             tx-id (+ Long/BYTES))))
         [upper-morton lower-morton] (when z
                                       (morton/morton-number->interleaved-longs z))]
     (.putByte b 0 entity+z+tx-id->content-hash-index-id)
     (.putBytes b index-id-size entity 0 (.capacity entity))
     (when z
       (.putLong b (+ index-id-size id-size) upper-morton ByteOrder/BIG_ENDIAN)
       (.putLong b (+ index-id-size id-size Long/BYTES) lower-morton ByteOrder/BIG_ENDIAN))
     (when tx-id
       (.putLong b (+ index-id-size id-size Long/BYTES Long/BYTES) (descending-long tx-id) ByteOrder/BIG_ENDIAN))
     (->> (+ index-id-size (.capacity entity) (if z (* 2 Long/BYTES) 0) (maybe-long-size tx-id))
          (mem/limit-buffer b)))))

(defn decode-entity+z+tx-id-key-as-z-number-from [^DirectBuffer k]
  (assert (= (+ index-id-size id-size Long/BYTES Long/BYTES Long/BYTES) (.capacity k)) (mem/buffer->hex k))
  (let [index-id (.getByte k 0)]
    (assert (= entity+z+tx-id->content-hash-index-id index-id))
    (morton/interleaved-longs->morton-number
     (.getLong k (+ index-id-size id-size) ByteOrder/BIG_ENDIAN)
     (.getLong k (+ index-id-size id-size Long/BYTES) ByteOrder/BIG_ENDIAN))))

(defn decode-entity+z+tx-id-key-from ^crux.codec.EntityTx [^DirectBuffer k]
  (assert (= (+ index-id-size id-size Long/BYTES Long/BYTES Long/BYTES) (.capacity k)) (mem/buffer->hex k))
  (let [index-id (.getByte k 0)]
    (assert (= entity+z+tx-id->content-hash-index-id index-id))
    (let [entity (Id. (mem/slice-buffer k index-id-size id-size) 0)
          [valid-time transaction-time] (morton/morton-number->longs (decode-entity+z+tx-id-key-as-z-number-from k))
          tx-id (descending-long (.getLong k (+ index-id-size id-size Long/BYTES Long/BYTES) ByteOrder/BIG_ENDIAN))]
      (->EntityTx entity (reverse-time-ms->date valid-time) (reverse-time-ms->date transaction-time) tx-id nil))))

(defn entity-tx->edn [^EntityTx entity-tx]
  (when entity-tx
    {:crux.db/id (str (.eid entity-tx))
     :crux.db/content-hash (str (.content-hash entity-tx))
     :crux.db/valid-time (.vt entity-tx)
     :crux.tx/tx-time (.tt entity-tx)
     :crux.tx/tx-id (.tx-id entity-tx)}))

(defn encode-failed-tx-id-key-to
  (^org.agrona.MutableDirectBuffer [^MutableDirectBuffer b]
   (encode-failed-tx-id-key-to b nil))
  (^org.agrona.MutableDirectBuffer [^MutableDirectBuffer b tx-id]
   (let [^MutableDirectBuffer b (or b (mem/allocate-buffer (+ index-id-size (maybe-long-size tx-id))))]
     (.putByte b 0 failed-tx-id-index-id)
     (when tx-id
       (.putLong b index-id-size (descending-long tx-id) ByteOrder/BIG_ENDIAN))
     (mem/limit-buffer b (+ index-id-size (maybe-long-size tx-id))))))

(defn decode-failed-tx-id-key-from [^DirectBuffer k]
  (assert (= (+ index-id-size Long/BYTES) (.capacity k)) (mem/buffer->hex k))
  (let [index-id (.getByte k 0)]
    (assert (= failed-tx-id-index-id index-id))
    (descending-long (.getLong k index-id-size ByteOrder/BIG_ENDIAN))))

(defn encode-index-version-key-to ^org.agrona.MutableDirectBuffer [^MutableDirectBuffer b]
  (let [^MutableDirectBuffer b (or b (mem/allocate-buffer index-id-size))]
    (.putByte b 0 index-version-index-id)
    (mem/limit-buffer b index-id-size)))

(defn encode-index-version-value-to ^org.agrona.MutableDirectBuffer [^MutableDirectBuffer b ^long version]
  (let [^MutableDirectBuffer b (or b (mem/allocate-buffer index-version-size))]
    (doto b
      (.putLong 0 version ByteOrder/BIG_ENDIAN))
    (mem/limit-buffer b index-version-size)))

(defn decode-index-version-value-from ^long [^MutableDirectBuffer b]
  (.getLong b 0 ByteOrder/BIG_ENDIAN))
