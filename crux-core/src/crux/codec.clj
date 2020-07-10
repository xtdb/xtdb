(ns ^:no-doc crux.codec
  #:clojure.tools.namespace.repl{:load false, :unload false} ; because of the deftypes in here
  (:require [clojure.edn :as edn]
            [crux.hash :as hash]
            [crux.memory :as mem]
            [crux.morton :as morton]
            [taoensso.nippy :as nippy]
            [crux.io :as cio]
            [clojure.walk :as walk])
  (:import [clojure.lang IHashEq Keyword APersistentMap APersistentSet]
           [java.io Closeable Writer]
           [java.net MalformedURLException URI URL]
           [java.nio ByteOrder ByteBuffer]
           java.nio.charset.StandardCharsets
           [java.util Arrays Date Map UUID Set]
           [org.agrona DirectBuffer ExpandableDirectByteBuffer MutableDirectBuffer]
           org.agrona.concurrent.UnsafeBuffer))

(set! *unchecked-math* :warn-on-boxed)

;; Indexes

;; NOTE: Must be updated when existing indexes change structure.
(def ^:const index-version 11)
(def ^:const index-version-size Long/BYTES)

(def ^:const index-id-size Byte/BYTES)

;; if you absolutely have to shuffle index numbers around on a version bump, don't move these four.
(def ^:const index-version-index-id 6)  ; to allow crux upgrades.
                                        ; rebuild indexes from kafka on backward incompatible
(def ^:const content-hash->doc-index-id 0) ; index for object store
(def ^:const tx-events-index-id 8) ; used in standalone TxLog
(def ^:const meta-key->value-index-id 4) ; for crux own needs

(def ^:const ave-index-id 1)
(def ^:const ecav-index-id 2)
(def ^:const hash-cache-index-id 3)

(def ^:const failed-tx-id-index-id 5)

(def ^:const entity+vt+tt+tx-id->content-hash-index-id 7) ; main bitemp index [reverse]

;; second bitemp index [also reverse]
;; z combines vt and tt
;; used when a lookup by the first index fails
(def ^:const entity+z+tx-id->content-hash-index-id 9)

;; prefix indexes
(def ^:const av-index-id 10)
(def ^:const ae-index-id 11)

(def ^:const value-type-id-size Byte/BYTES)

(def ^:const id-size (+ hash/id-hash-size value-type-id-size))

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
(def ^:private boolean-value-type-id 7)
(def ^:private char-value-type-id 8)

(def nil-id-bytes (doto (byte-array id-size)
                    (aset 0 (byte id-value-type-id))))
(def nil-id-buffer
  (mem/->off-heap nil-id-bytes (mem/allocate-unpooled-buffer (count nil-id-bytes))))

(def ^:dynamic ^:private *sort-unordered-colls* false)

(defn- sorted-kv-seq [kvs]
  (let [kvs (sort-by key kvs)]
    (reify
      clojure.lang.Counted
      (count [_]
        (count kvs))

      clojure.lang.IKVReduce
      (kvreduce [_ f init]
        (loop [[[k v] & more-kvs :as kvs] kvs
               res init]
          (if (or (empty? kvs) (reduced? res))
            res
            (recur more-kvs (f res k v))))))))

(extend-protocol nippy/IFreezable1
  Set
  (-freeze-without-meta! [this out]
    (if *sort-unordered-colls*
      (#'nippy/write-counted-coll out @#'nippy/id-sorted-set (sort this))
      (#'nippy/write-set out this)))

  APersistentSet
  (-freeze-without-meta! [this out]
    (if *sort-unordered-colls*
      (#'nippy/write-counted-coll out @#'nippy/id-sorted-set (sort this))
      (#'nippy/write-set out this)))

  Map
  (-freeze-without-meta! [this out]
    (if *sort-unordered-colls*
      (#'nippy/write-kvs out @#'nippy/id-sorted-map (sorted-kv-seq this))
      (#'nippy/write-map out this)))

  APersistentMap
  (-freeze-without-meta! [this out]
    (if *sort-unordered-colls*
      (#'nippy/write-kvs out @#'nippy/id-sorted-map (sorted-kv-seq this))
      (#'nippy/write-map out this))))

(defn id-function ^org.agrona.MutableDirectBuffer [^MutableDirectBuffer to bs]
  (.putByte to 0 (byte id-value-type-id))
  (hash/id-hash (mem/slice-buffer to value-type-id-size hash/id-hash-size) (mem/as-buffer bs))
  (mem/limit-buffer to id-size))

;; Adapted from https://github.com/ndimiduk/orderly
(extend-protocol ValueToBuffer
  (class (byte-array 0))
  (value->buffer [this to]
    (throw (UnsupportedOperationException. "Byte arrays as values is not supported.")))

  Boolean
  (value->buffer [this ^MutableDirectBuffer to]
    (mem/limit-buffer
     (doto to
       (.putByte 0 boolean-value-type-id)
       (.putByte value-type-id-size ^byte (if this
                                            1
                                            0)))
     (+ value-type-id-size Byte/BYTES)))

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
  (value->buffer [this ^MutableDirectBuffer to]
    (mem/limit-buffer
     (doto to
       (.putByte 0 char-value-type-id)
       (.putChar value-type-id-size this ByteOrder/BIG_ENDIAN))
     (+ value-type-id-size Character/BYTES)))

  String
  (value->buffer [this ^MutableDirectBuffer to]
    (if (< max-string-index-length (count this))
      (doto (id-function to (nippy/fast-freeze this))
        (.putByte 0 (byte object-value-type-id)))
      (let [terminate-mark (byte 1)
            terminate-mark-size Byte/BYTES
            offset (byte 2)
            ub-in (mem/on-heap-buffer (.getBytes this StandardCharsets/UTF_8))
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

  Map
  (value->buffer [this to]
    (id->buffer this to))

  DirectBuffer
  (value->buffer [this to]
    (id->buffer this to))

  ByteBuffer
  (value->buffer [this to]
    (id->buffer this to))

  Object
  (value->buffer [this ^MutableDirectBuffer to]
    (if (satisfies? IdToBuffer this)
      (id->buffer this to)
      (doto (id-function to (binding [*sort-unordered-colls* true]
                              (nippy/fast-freeze this)))
        (.putByte 0 (byte object-value-type-id))))))

(defn ->value-buffer ^org.agrona.DirectBuffer [x]
  (value->buffer x (ExpandableDirectByteBuffer. 32)))

(defn value-buffer-type-id ^org.agrona.DirectBuffer [^DirectBuffer buffer]
  (mem/limit-buffer buffer value-type-id-size))

(defn- decode-long ^long [^DirectBuffer buffer]
  (bit-xor (.getLong buffer value-type-id-size  ByteOrder/BIG_ENDIAN) Long/MIN_VALUE))

(defn- decode-double ^double [^DirectBuffer buffer]
  (let [l (dec (.getLong buffer value-type-id-size  ByteOrder/BIG_ENDIAN))
        l (bit-xor l (bit-or (bit-shift-right (bit-not l) (dec Long/SIZE)) Long/MIN_VALUE))]
    (Double/longBitsToDouble l)))

(defn- decode-string ^String [^DirectBuffer buffer]
  (let [terminate-mark-size Byte/BYTES
        offset (byte 2)
        length (- (.capacity buffer) terminate-mark-size)
        bs (byte-array (- length value-type-id-size))]
    (loop [idx value-type-id-size]
      (if (= idx length)
        (String. bs StandardCharsets/UTF_8)
        (let [b (.getByte buffer idx)]
          (aset bs (dec idx) (unchecked-byte (- b offset)))
          (recur (inc idx)))))))

(defn- decode-boolean ^Boolean [^DirectBuffer buffer]
  (= 1 (.getByte buffer value-type-id-size)))

(defn- decode-char ^Character [^DirectBuffer buffer]
  (.getChar buffer value-type-id-size ByteOrder/BIG_ENDIAN))

(defn can-decode-value-buffer? [^DirectBuffer buffer]
  (when (and buffer (pos? (.capacity buffer)))
    (case (.getByte buffer 0)
      (1 2 3 4 7 8) true
      0 (= buffer nil-id-buffer)
      false)))

(defn decode-value-buffer [^DirectBuffer buffer]
  (let [type-id (.getByte buffer 0)]
    (case type-id
      1 (decode-long buffer) ;; long-value-type-id
      2 (decode-double buffer) ;; double-value-type-id
      3 (Date. (decode-long buffer)) ;; date-value-type-id
      4 (decode-string buffer) ;; string-value-type-id
      7 (decode-boolean buffer) ;; boolean-value-type-id
      8 (decode-char buffer) ;; char-value-type-id
      0 (if (mem/buffers=? buffer nil-id-buffer)  ;; id-value-type-id
          nil
          (throw (IllegalArgumentException. (str "Unknown type id: " type-id))))
      (throw (IllegalArgumentException. (str "Unknown type id: " type-id))))))

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

(defn- maybe-map-str [s]
  (try
    (let [edn (edn/read-string s)]
      (when (map? edn) edn))
    (catch Exception _)))

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
                      (maybe-url-str this)
                      (maybe-map-str this))]
        (id->buffer id to)
        (throw (IllegalArgumentException. (format "Not a %s hex, keyword, EDN map, URL or an UUID string: %s" hash/id-hash-algorithm this))))))

  Map
  (id->buffer [this to]
    (id-function to (binding [*sort-unordered-colls* true]
                      (nippy/fast-freeze this))))

  nil
  (id->buffer [this to]
    (id->buffer nil-id-buffer to)))

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
    nil-id-buffer)

  (new-id [this]
    (Id. nil-id-buffer (.hashCode ^DirectBuffer nil-id-buffer)))

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

(defn descending-long ^long [^long l]
  (bit-xor (bit-not l) Long/MIN_VALUE))

(defn date->reverse-time-ms ^long [^Date date]
  (descending-long (.getTime date)))

(defn reverse-time-ms->date ^java.util.Date [^long reverse-time-ms]
  (Date. (descending-long reverse-time-ms)))

(defn maybe-long-size ^long [x]
  (if x
    Long/BYTES
    0))

(defrecord EntityTx [^Id eid ^Date vt ^Date tt ^long tx-id ^Id content-hash]
  IdToBuffer
  (id->buffer [this to]
    (id->buffer eid to)))

(defn entity-tx->edn [^EntityTx entity-tx]
  (when entity-tx
    {:crux.db/id (.eid entity-tx)
     :crux.db/content-hash (.content-hash entity-tx)
     :crux.db/valid-time (.vt entity-tx)
     :crux.tx/tx-time (.tt entity-tx)
     :crux.tx/tx-id (.tx-id entity-tx)}))

(defn multiple-values? [v]
  (or (vector? v) (set? v)))

(defn vectorize-value [v]
  (cond-> v
    (not (multiple-values? v))
    (vector)))

(defn evicted-doc?
  [{:crux.db/keys [id evicted?] :as doc}]
  (boolean (or (= :crux.db/evicted id) evicted?)))

(defn keep-non-evicted-doc [doc]
  (when-not (evicted-doc? doc)
    doc))
