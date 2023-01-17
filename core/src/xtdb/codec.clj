(ns ^:no-doc xtdb.codec
  #:clojure.tools.namespace.repl{:load false, :unload false} ; because of the deftypes in here
  (:require [clojure.edn :as edn]
            [xtdb.error :as err]
            [xtdb.hash :as hash]
            [xtdb.query-state :as cqs]
            [xtdb.io :as xio]
            [xtdb.memory :as mem]
            [juxt.clojars-mirrors.nippy.v3v1v1.taoensso.nippy :as nippy]
            [clojure.set :as set])
  (:import [clojure.lang APersistentMap APersistentSet BigInt IHashEq Keyword]
           xtdb.codec.MathCodec
           java.io.Writer
           java.math.BigDecimal
           [java.net MalformedURLException URI URL]
           [java.nio ByteBuffer ByteOrder]
           java.nio.charset.StandardCharsets
           [java.time LocalDate LocalTime LocalDateTime Instant Duration ZonedDateTime]
           [java.util Base64 Date Map Set UUID]
           java.util.function.Supplier
           [org.agrona DirectBuffer ExpandableDirectByteBuffer MutableDirectBuffer]
           org.agrona.concurrent.UnsafeBuffer))

(set! *unchecked-math* :warn-on-boxed)

;; Indexes

;; NOTE: Must be updated when existing indexes change structure.
(def index-version 22)
(def ^:const index-version-size Long/BYTES)

(def ^:const index-id-size Byte/BYTES)

;; if you absolutely have to shuffle index numbers around on a version bump, don't move these four.
(def ^:const index-version-index-id 6)  ; to allow XTDB upgrades.
                                        ; rebuild indexes from kafka on backward incompatible
(def ^:const content-hash->doc-index-id 0) ; index for object store
(def ^:const tx-events-index-id 8) ; used in standalone TxLog
(def ^:const meta-key->value-index-id 4) ; for XTDB's own needs

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

(def ^:const tx-time-mapping-id 12)

(def ^:const stats-index-id 13)

(def ^:const value-type-id-size Byte/BYTES)

(def ^:const id-size (+ hash/id-hash-size value-type-id-size))

;; LMDB #MDB_MAXKEYSIZE 511 (>= 511 (+ value-type-id-size (* 2 (+ value-type-id-size 224)) id-size id-size))
(def ^:const ^:private max-value-index-length 224)

(def ^:const ^:private string-terminate-mark-size Byte/BYTES)
(def ^:const ^:private string-terminate-mark 1)

(defprotocol IdOrBuffer
  (new-id ^xtdb.codec.Id [id])
  (->id-buffer ^org.agrona.DirectBuffer [this]))

(defprotocol IdToBuffer
  (id->buffer ^org.agrona.MutableDirectBuffer [this ^MutableDirectBuffer to]))

(defprotocol ValueToBuffer
  (value->buffer ^org.agrona.MutableDirectBuffer [this ^MutableDirectBuffer to]))

(def ^:private id-value-type-id 0)
(def ^:private object-value-type-id 1)
(def ^:private clob-value-type-id 2)
(def ^:private nil-value-type-id 3)
(def ^:private boolean-value-type-id 4)
(def ^:private long-value-type-id 5)
(def ^:private double-value-type-id 6)
(def ^:private date-value-type-id 7)
(def ^:private string-value-type-id 8)
(def ^:private char-value-type-id 9)
(def ^:private nippy-value-type-id 10)
(def ^:private bigdec-value-type-id 11)
(def ^:private bigint-value-type-id 12)
(def ^:private biginteger-value-type-id 13)
(def ^:private localdate-value-type-id 14)
(def ^:private localtime-value-type-id 15)
(def ^:private localdatetime-value-type-id 16)
(def ^:private instant-value-type-id 17)
(def ^:private duration-value-type-id 18)

(def nil-id-bytes
  (doto (byte-array id-size)
    (aset 0 (byte id-value-type-id))))

(def nil-id-buffer
  (mem/->off-heap nil-id-bytes (mem/allocate-unpooled-buffer (count nil-id-bytes))))

(defn id-function ^org.agrona.MutableDirectBuffer [^MutableDirectBuffer to bs]
  (.putByte to 0 (byte id-value-type-id))
  (hash/id-hash (mem/slice-buffer to value-type-id-size hash/id-hash-size) (mem/as-buffer bs))
  (mem/limit-buffer to id-size))

(def ^:dynamic ^:private *consistent-ids?* false)
(def ^:dynamic ^:private *consistent-values?* false)

(defn- sort-set [coll]
  (try
    (sort coll)
    (catch ClassCastException _
      (->> coll
           (sort-by (fn [el]
                      (doto (mem/allocate-buffer id-size)
                        (id-function (nippy/fast-freeze el))))
                    mem/buffer-comparator)))))

(defn- freeze-set [out coll]
  ;; have to maintain id-sorted-set here because it's on tx-logs,
  ;; but we can't thaw this because nippy thaws it straight to PersistentTreeSet
  ;; which doesn't allow incomparable keys
  (cond
    *consistent-ids?* (#'nippy/write-coll out @#'nippy/id-sorted-set (sort-set coll))
    *consistent-values?* (#'nippy/write-set out (sort-set coll))
    :else (#'nippy/write-set out coll)))

(defn- ->kv-reduce [kvs]
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
          (recur more-kvs (f res k v)))))))

(defn- sort-map [m]
  (->kv-reduce (try
                 (sort-by key m)
                 (catch ClassCastException _
                   (->> m
                        (sort-by (fn [[k _]]
                                   (doto (mem/allocate-buffer id-size)
                                     (id-function (nippy/fast-freeze k))))
                                 mem/buffer-comparator))))))

(defn- freeze-map [out m]
  (try
    (cond
      ;; have to maintain id-sorted-map here because it's on tx-logs,
      ;; but we can't thaw this because nippy thaws it straight to PersistentTreeMap
      ;; which doesn't allow incomparable keys
      *consistent-ids?* (#'nippy/write-kvs out @#'nippy/id-sorted-map (sort-map m))
      *consistent-values?* (#'nippy/write-map out (sort-map m))
      :else (#'nippy/write-map out (->kv-reduce m)))
    (catch Exception e
      (prn *consistent-ids?* *consistent-values?* m)
      (throw e))))

(extend-protocol nippy/IFreezable1
  Set
  (-freeze-without-meta! [this out]
    (freeze-set out this))

  APersistentSet
  (-freeze-without-meta! [this out]
    (freeze-set out this))

  Map
  (-freeze-without-meta! [this out]
    (freeze-map out this))

  APersistentMap
  (-freeze-without-meta! [this out]
    (freeze-map out this)))

(defn- biginteger->buffer [^BigInteger x type-id ^MutableDirectBuffer to]
  (let [signum (.signum x)]
    (doto to
      (.putByte 0 type-id)
      (.putByte value-type-id-size (inc (.signum x))))

    (if (zero? signum)
      (-> to (mem/limit-buffer (+ value-type-id-size Byte/BYTES)))

      (let [s (str (.abs x))
            bcd-len (MathCodec/putBinaryCodedDecimal signum
                                                     s
                                                     to
                                                     (+ value-type-id-size Byte/BYTES Integer/BYTES))]
        (-> (doto to
              (.putInt (+ value-type-id-size Byte/BYTES)
                       (-> (* signum (count s)) (bit-xor Integer/MIN_VALUE))
                       ByteOrder/BIG_ENDIAN))
            (mem/limit-buffer (+ value-type-id-size Byte/BYTES Integer/BYTES bcd-len)))))))

;; Adapted from https://github.com/ndimiduk/orderly
(extend-protocol ValueToBuffer
  Boolean
  (value->buffer [this ^MutableDirectBuffer to]
    (-> (doto to
          (.putByte 0 boolean-value-type-id)
          (.putByte value-type-id-size ^byte (if this
                                               1
                                               0)))
        (mem/limit-buffer (+ value-type-id-size Byte/BYTES))))

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
    (-> (doto to
          (.putByte 0 long-value-type-id)
          (.putLong value-type-id-size (bit-xor ^long this Long/MIN_VALUE) ByteOrder/BIG_ENDIAN))
        (mem/limit-buffer (+ value-type-id-size Long/BYTES))))

  Float
  (value->buffer [this to]
    (value->buffer (double this) to))

  Double
  (value->buffer [this ^MutableDirectBuffer to]
    (let [l (Double/doubleToLongBits this)
          l (inc (bit-xor l (bit-or (bit-shift-right l (dec Long/SIZE)) Long/MIN_VALUE)))]
      (-> (doto to
            (.putByte 0 double-value-type-id)
            (.putLong value-type-id-size l ByteOrder/BIG_ENDIAN))
          (mem/limit-buffer (+ value-type-id-size Double/BYTES)))))

  Date
  (value->buffer [this ^MutableDirectBuffer to]
    (doto (value->buffer (.getTime this) to)
      (.putByte 0 (byte date-value-type-id))))

  Character
  (value->buffer [this ^MutableDirectBuffer to]
    (-> (doto to
          (.putByte 0 char-value-type-id)
          (.putChar value-type-id-size this ByteOrder/BIG_ENDIAN))
        (mem/limit-buffer (+ value-type-id-size Character/BYTES))))

  String
  (value->buffer [this ^MutableDirectBuffer to]
    (let [bs (.getBytes this StandardCharsets/UTF_8)]
      (if (< max-value-index-length (alength bs))
        (doto (id->buffer this to)
          (.putByte 0 clob-value-type-id))
        (let [offset (byte 2)
              ub-in (mem/on-heap-buffer bs)
              length (.capacity ub-in)]
          (.putByte to 0 string-value-type-id)
          (loop [idx 0]
            (if (= idx length)
              (do (.putByte to (inc idx) string-terminate-mark)
                  (mem/limit-buffer to (+ length value-type-id-size string-terminate-mark-size)))
              (let [b (.getByte ub-in idx)]
                (.putByte to (inc idx) (unchecked-byte (+ offset b)))
                (recur (inc idx)))))))))

  BigDecimal
  (value->buffer [this ^MutableDirectBuffer to]
    (let [this (.stripTrailingZeros this)
          signum (.signum this)]

      (doto to
        (.putByte 0 bigdec-value-type-id)
        (.putByte value-type-id-size (inc (.signum this))))

      (if (zero? signum)
        (-> to (mem/limit-buffer (+ value-type-id-size Byte/BYTES)))

        (let [bcd-len (MathCodec/putBinaryCodedDecimal signum
                                                       (str (.abs (.unscaledValue this)))
                                                       to
                                                       (+ value-type-id-size Byte/BYTES Integer/BYTES))]
          (-> (doto to
                (.putInt (+ value-type-id-size Byte/BYTES)
                         (MathCodec/encodeExponent this)
                         ByteOrder/BIG_ENDIAN))
              (mem/limit-buffer (+ value-type-id-size Byte/BYTES Integer/BYTES bcd-len)))))))

  BigInteger
  (value->buffer [this ^MutableDirectBuffer to]
    (biginteger->buffer this biginteger-value-type-id to))

  BigInt
  (value->buffer [this ^MutableDirectBuffer to]
    (biginteger->buffer (biginteger this) bigint-value-type-id to))

  LocalDate
  (value->buffer [this ^MutableDirectBuffer to]
    (-> (doto to
          (.putByte 0 localdate-value-type-id)
          (.putInt value-type-id-size (-> (.getYear this) (bit-xor Integer/MIN_VALUE)) ByteOrder/BIG_ENDIAN)
          (.putByte (+ value-type-id-size Integer/BYTES) (.getMonthValue this))
          (.putByte (+ value-type-id-size Integer/BYTES Byte/BYTES) (.getDayOfMonth this)))
        (mem/limit-buffer (+ value-type-id-size Integer/BYTES Byte/BYTES Byte/BYTES))))

  LocalTime
  (value->buffer [this ^MutableDirectBuffer to]
    (-> (doto to
          (.putByte 0 localtime-value-type-id)
          (.putLong value-type-id-size (.toNanoOfDay this) ByteOrder/BIG_ENDIAN))
        (mem/limit-buffer (+ value-type-id-size Long/BYTES))))

  LocalDateTime
  (value->buffer [this ^MutableDirectBuffer to]
    (-> (doto to
          (.putByte 0 localdatetime-value-type-id)
          (.putInt value-type-id-size (-> (.getYear this) (bit-xor Integer/MIN_VALUE)) ByteOrder/BIG_ENDIAN)
          (.putByte (+ value-type-id-size Integer/BYTES) (.getMonthValue this))
          (.putByte (+ value-type-id-size Integer/BYTES Byte/BYTES) (.getDayOfMonth this))
          (.putLong (+ value-type-id-size Integer/BYTES Byte/BYTES Byte/BYTES)
                    (.toNanoOfDay (.toLocalTime this))
                    ByteOrder/BIG_ENDIAN))
        (mem/limit-buffer (+ value-type-id-size Integer/BYTES Byte/BYTES Byte/BYTES Long/BYTES))))

  Instant
  (value->buffer [this ^MutableDirectBuffer to]
    (-> (doto to
          (.putByte 0 instant-value-type-id)
          (.putLong value-type-id-size (-> (.getEpochSecond this) (bit-xor Long/MIN_VALUE)) ByteOrder/BIG_ENDIAN)
          (.putInt (+ value-type-id-size Long/BYTES) (.getNano this) ByteOrder/BIG_ENDIAN))
        (mem/limit-buffer (+ value-type-id-size Long/BYTES Integer/BYTES))))

  Duration
  (value->buffer [this ^MutableDirectBuffer to]
    (-> (doto to
          (.putByte 0 duration-value-type-id)
          (.putLong value-type-id-size (-> (.getSeconds this) (bit-xor Long/MIN_VALUE)) ByteOrder/BIG_ENDIAN)
          (.putInt (+ value-type-id-size Long/BYTES) (.getNano this) ByteOrder/BIG_ENDIAN))
        (mem/limit-buffer (+ value-type-id-size Long/BYTES Integer/BYTES))))

  Class
  (value->buffer [this ^MutableDirectBuffer to]
    (doto (id-function to (mem/->nippy-buffer this))
      (.putByte 0 object-value-type-id)))

  Object
  (value->buffer [this ^MutableDirectBuffer to]
    (let [^DirectBuffer nippy-buffer (if (coll? this)
                                       (binding [*consistent-values?* true]
                                         (mem/->nippy-buffer this))
                                       (mem/->nippy-buffer this))]
      (if (or (< max-value-index-length (.capacity nippy-buffer))
              (not (or (nippy/freezable? this)
                       (uri? this))))
        (doto (id-function to nippy-buffer)
          (.putByte 0 object-value-type-id))
        (-> (doto to
              (.putByte 0 nippy-value-type-id)
              (.putBytes value-type-id-size nippy-buffer 0 (.capacity nippy-buffer)))
            (mem/limit-buffer (+ value-type-id-size (.capacity nippy-buffer)))))))

  nil
  (value->buffer [this ^MutableDirectBuffer to]
    (-> (doto to
          (.putByte 0 nil-value-type-id))
        (mem/limit-buffer value-type-id-size))))

(def ^:private ^ThreadLocal value-buffer-tl
  (ThreadLocal/withInitial
   (reify Supplier
     (get [_]
       (ExpandableDirectByteBuffer. 32)))))

(defn ->value-buffer ^org.agrona.DirectBuffer [x]
  (mem/copy-to-unpooled-buffer (value->buffer x (.get value-buffer-tl))))

(defn value-buffer-type-id ^org.agrona.DirectBuffer [^DirectBuffer buffer]
  (mem/limit-buffer buffer value-type-id-size))

(defn crux->xt [doc]
  (set/rename-keys doc {:crux.db/id :xt/id}))

(defn xt->crux [doc]
  (set/rename-keys doc {:xt/id :crux.db/id}))

(defn hash-doc [doc]
  (-> doc (xt->crux) (new-id)))

(defn- decode-long ^long [^DirectBuffer buffer]
  (bit-xor (.getLong buffer value-type-id-size  ByteOrder/BIG_ENDIAN) Long/MIN_VALUE))

(defn- decode-double ^double [^DirectBuffer buffer]
  (let [l (dec (.getLong buffer value-type-id-size  ByteOrder/BIG_ENDIAN))
        l (bit-xor l (bit-or (bit-shift-right (bit-not l) (dec Long/SIZE)) Long/MIN_VALUE))]
    (Double/longBitsToDouble l)))

(defn- decode-string ^String [^DirectBuffer buffer]
  (let [offset (byte 2)
        length (- (.capacity buffer) string-terminate-mark-size)
        bs (byte-array (- length value-type-id-size))]
    (loop [idx value-type-id-size]
      (if (= idx length)
        (do (assert (= string-terminate-mark (.getByte buffer idx)) "String not terminated.")
            (String. bs StandardCharsets/UTF_8))
        (let [b (.getByte buffer idx)]
          (aset bs (dec idx) (unchecked-byte (- b offset)))
          (recur (inc idx)))))))

(defn- decode-boolean ^Boolean [^DirectBuffer buffer]
  (= 1 (.getByte buffer value-type-id-size)))

(defn- decode-char ^Character [^DirectBuffer buffer]
  (.getChar buffer value-type-id-size ByteOrder/BIG_ENDIAN))

(defn- decode-nippy [^DirectBuffer buffer]
  (mem/<-nippy-buffer (mem/slice-buffer buffer value-type-id-size (- (.capacity buffer) value-type-id-size))))

(defn- decode-bigdec [^DirectBuffer buffer]
  (let [signum (dec (.getByte buffer value-type-id-size))]
    (if (zero? signum)
      0M

      (let [prefix-len (+ value-type-id-size Byte/BYTES Integer/BYTES)
            decoded-bcd (MathCodec/getBinaryCodedDecimal
                         (-> buffer
                             (mem/slice-buffer prefix-len (- (.capacity buffer) prefix-len)))
                         signum)]
        (MathCodec/decodeBigDecimal signum
                                    (.getInt buffer (+ value-type-id-size Byte/BYTES) ByteOrder/BIG_ENDIAN)
                                    decoded-bcd)))))

(defn- decode-biginteger ^BigInteger [^DirectBuffer buffer]
  (let [signum (dec (.getByte buffer value-type-id-size))]
    (if (zero? signum)
      BigInteger/ZERO

      (let [prefix-len (+ value-type-id-size Byte/BYTES Integer/BYTES)]
        (cond-> (BigInteger. (MathCodec/getBinaryCodedDecimal
                              (-> buffer
                                  (mem/slice-buffer prefix-len (- (.capacity buffer) prefix-len)))
                              signum))
          (neg? signum) (.negate))))))

(defn- decode-localdate [^DirectBuffer buffer]
  (LocalDate/of (-> (.getInt buffer value-type-id-size ByteOrder/BIG_ENDIAN) (bit-xor Integer/MIN_VALUE))
                (.getByte buffer (+ value-type-id-size Integer/BYTES))
                (.getByte buffer (+ value-type-id-size Integer/BYTES Byte/BYTES))))

(defn- decode-localtime [^DirectBuffer buffer]
  (LocalTime/ofNanoOfDay (.getLong buffer value-type-id-size ByteOrder/BIG_ENDIAN)))

(defn- decode-localdatetime [^DirectBuffer buffer]
  (LocalDateTime/of
   (LocalDate/of (-> (.getInt buffer value-type-id-size ByteOrder/BIG_ENDIAN) (bit-xor Integer/MIN_VALUE))
                 (.getByte buffer (+ value-type-id-size Integer/BYTES))
                 (.getByte buffer (+ value-type-id-size Integer/BYTES Byte/BYTES)))
   (LocalTime/ofNanoOfDay (.getLong buffer (+ value-type-id-size Integer/BYTES Byte/BYTES Byte/BYTES) ByteOrder/BIG_ENDIAN))))

(defn- decode-instant [^DirectBuffer buffer]
  (Instant/ofEpochSecond (-> (.getLong buffer value-type-id-size ByteOrder/BIG_ENDIAN) (bit-xor Long/MIN_VALUE))
                         (.getInt buffer (+ value-type-id-size Long/BYTES) ByteOrder/BIG_ENDIAN)))

(defn- decode-duration [^DirectBuffer buffer]
  (Duration/ofSeconds (-> (.getLong buffer value-type-id-size ByteOrder/BIG_ENDIAN) (bit-xor Long/MIN_VALUE))
                      (.getInt buffer (+ value-type-id-size Long/BYTES) ByteOrder/BIG_ENDIAN)))

(defn can-decode-value-buffer? [^DirectBuffer buffer]
  (when (and buffer (pos? (.capacity buffer)))
    (case (.getByte buffer 0)
      (0 1 2) false
      true)))

(defn decode-value-buffer [^DirectBuffer buffer]
  (let [type-id (.getByte buffer 0)]
    (case type-id
      3 nil ; nil-value-type-id
      4 (decode-boolean buffer) ; boolean-value-type-id
      5 (decode-long buffer) ; long-value-type-id
      6 (decode-double buffer) ; double-value-type-id
      7 (Date. (decode-long buffer)) ; date-value-type-id
      8 (decode-string buffer) ; string-value-type-id
      9 (decode-char buffer) ; char-value-type-id
      10 (decode-nippy buffer) ; nippy-value-type-id
      11 (decode-bigdec buffer) ; bigdec-value-type-id
      12 (bigint (decode-biginteger buffer)) ; bigint-value-type-id
      13 (decode-biginteger buffer) ; biginteger-value-type-id
      14 (decode-localdate buffer) ; localdate-value-type-id
      15 (decode-localtime buffer) ; localtime-value-type-id
      16 (decode-localdatetime buffer) ; localdatetime-value-type-id
      17 (decode-instant buffer) ; instant-value-type-id
      18 (decode-duration buffer) ; duration-value-type-id
      (throw (err/illegal-arg :unknown-type-id
                              {::err/message (str "Unknown type id: " type-id)})))))

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
    (if (and (= id-size (alength ^bytes this))
             (= id-value-type-id (aget ^bytes this 0)))
      (-> (doto to
            (.putBytes 0 this))
          (mem/limit-buffer id-size))
      (throw (err/illegal-arg :not-an-id-byte-array
                              {::err/message (str "Not an id byte array: " (mem/buffer->hex (mem/as-buffer this)))}))))

  ByteBuffer
  (id->buffer [this ^MutableDirectBuffer to]
    (-> (doto to
          (.putBytes 0 this))
        (mem/limit-buffer id-size)))

  DirectBuffer
  (id->buffer [this ^MutableDirectBuffer to]
    (-> (doto to
          (.putBytes 0 this 0 id-size))
        (mem/limit-buffer id-size)))

  Keyword
  (id->buffer [this to]
    (id-function to (.getBytes (subs (str this) 1) StandardCharsets/UTF_8)))

  UUID
  (id->buffer [this to]
    (id-function to (.getBytes (str this) StandardCharsets/UTF_8)))

  URI
  (id->buffer [this to]
    (id-function to (.getBytes (str (.normalize this)) StandardCharsets/UTF_8)))

  URL
  (id->buffer [this to]
    (id-function to (.getBytes (.toExternalForm this) StandardCharsets/UTF_8)))

  String
  (id->buffer [this to]
    (id-function to (nippy/fast-freeze this)))

  Boolean
  (id->buffer [this to]
    (id-function to (nippy/fast-freeze this)))

  Byte
  (id->buffer [this to]
    (id->buffer (long this) to))

  Short
  (id->buffer [this to]
    (id->buffer (long this) to))

  Integer
  (id->buffer [this to]
    (id->buffer (long this) to))

  Float
  (id->buffer [this to]
    (id->buffer (double this) to))

  Number
  (id->buffer [this to]
    (id-function to (nippy/fast-freeze this)))

  Date
  (id->buffer [this to]
    (id-function to (nippy/fast-freeze this)))

  Character
  (id->buffer [this to]
    (id-function to (nippy/fast-freeze this)))

  Map
  (id->buffer [this to]
    (id-function to (binding [*consistent-ids?* true]
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
  (.write w "#xtdb/id ")
  (.write w (xio/pr-edn-str (str id))))

(defmethod print-dup Id [id ^Writer w]
  (.write w "#xtdb/id ")
  (.write w (xio/pr-edn-str (str id))))

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

(defn safe-id ^xtdb.codec.Id [^Id id]
  (when id
    (Id. (mem/copy-to-unpooled-buffer (.buffer id)) 0)))

(defn hex->id-buffer
  ([hex] (hex->id-buffer hex (mem/allocate-buffer id-size)))
  ([hex to]
   (.putByte ^MutableDirectBuffer to 0 id-value-type-id)
   (mem/hex->buffer hex (mem/slice-buffer to value-type-id-size hash/id-hash-size))
   (mem/limit-buffer to id-size)))

(deftype EDNId [hex original-id]
  IdToBuffer
  (id->buffer [this to]
    (hex->id-buffer hex to))

  Object
  (toString [this]
    hex)

  (equals [this that]
    (.equals (new-id this) (new-id that)))

  (hashCode [this]
    (.hashCode (new-id this)))

  IHashEq
  (hasheq [this]
    (.hashCode this))

  Comparable
  (compareTo [this that]
    (.compareTo (new-id this) (new-id that))))

(defn id-edn-reader ^xtdb.codec.EDNId [id]
  (->EDNId (if (string? id)
             (if (hex-id? id)
               id
               (str (new-id (or (maybe-uuid-str id)
                                (maybe-keyword-str id)
                                (maybe-url-str id)
                                (maybe-map-str id)
                                (throw (err/illegal-arg :unsupported-string-ids
                                                        {::err/message "EDN reader doesn't support string IDs"}))))))
             (new-id id))
           id))

(defn base64-reader ^bytes [s]
  (.decode (Base64/getDecoder) (str s)))

(defn base64-writer ^String [^bytes bytes]
  (.encodeToString (Base64/getEncoder) bytes))

(defn duration-reader [s] (Duration/parse s))
(defn instant-reader [s] (Instant/parse s))
(defn zdt-reader [s] (ZonedDateTime/parse s))
(defn local-date-reader [s] (LocalDate/parse s))
(defn local-time-reader [s] (LocalTime/parse s))
(defn local-date-time-reader [s] (LocalDateTime/parse s))

(when (or (System/getenv "XTDB_ENABLE_JAVA_TIME_PRINT_METHODS")
          (System/getProperty "xtdb.enable-java-time-print-methods"))
  (defmethod print-dup Duration [^Duration d, ^Writer w]
    (.write w (format "#xtdb/duration \"%s\"" d)))

  (defmethod print-dup Instant [^Instant i, ^Writer w]
    (.write w (format "#xtdb/instant \"%s\"" i)))

  (defmethod print-dup ZonedDateTime [^ZonedDateTime zdt, ^Writer w]
    (.write w (format "#xtdb/zdt \"%s\"" zdt)))

  (defmethod print-dup LocalDate [^LocalDateTime ld, ^Writer w]
    (.write w (format "#xtdb/local-date \"%s\"" ld)))

  (defmethod print-dup LocalTime [^LocalTime lt, ^Writer w]
    (.write w (format "#xtdb/local-time \"%s\"" lt)))

  (defmethod print-dup LocalDateTime [^LocalDateTime ldt, ^Writer w]
    (.write w (format "#xtdb/local-date-time \"%s\"" ldt)))

  (defmethod print-method Duration [d w] (print-dup d w))
  (defmethod print-method Instant [i w] (print-dup i w))
  (defmethod print-method ZonedDateTime [zdt w] (print-dup zdt w))
  (defmethod print-method LocalDate [ld w] (print-dup ld w))
  (defmethod print-method LocalTime [lt w] (print-dup lt w))
  (defmethod print-method LocalDateTime [ldt w] (print-dup ldt w)))

(defn read-edn-string-with-readers [s]
  (edn/read-string {:readers {'crux/id id-edn-reader
                              'xtdb/id id-edn-reader
                              'crux/base64 base64-reader
                              'xtdb/base64 base64-reader

                              'xtdb/duration duration-reader
                              'xtdb/instant instant-reader
                              'xtdb/zdt zdt-reader
                              'xtdb/local-date local-date-reader
                              'xtdb/local-time local-time-reader
                              'xtdb/local-date-time local-date-time-reader

                              'crux/query-state cqs/->QueryState
                              'xtdb/query-state cqs/->QueryState
                              'crux/query-error cqs/->QueryError
                              'xtdb/query-error cqs/->QueryError}}
                   s))

(when (or (Boolean/parseBoolean (System/getenv "XTDB_ENABLE_BASE64_PRINT_METHOD"))
          (Boolean/parseBoolean (System/getProperty "xtdb.enable-base64-print-method")))
      (defmethod print-dup (class (byte-array 0)) [^bytes b ^Writer w]
        (.write w "#xtdb/base64 ")
        (.write w (xio/pr-edn-str (base64-writer b))))

      (defmethod print-method (class (byte-array 0)) [^bytes b ^Writer w]
        (print-dup b w)))

(defn edn-id->original-id [^EDNId id]
  (str (or (.original-id id) (.hex id))))

(defmethod print-dup EDNId [^EDNId id ^Writer w]
  (.write w "#xtdb/id ")
  (.write w (xio/pr-edn-str (edn-id->original-id id))))

(defmethod print-method EDNId [^EDNId id ^Writer w]
  (print-dup id w))

(nippy/extend-freeze
 EDNId
 :crux.codec/edn-id
 [^EDNId x data-output]
 (nippy/freeze-to-out! data-output (or (.original-id x) (.hex x))))

(nippy/extend-thaw
 :crux.codec/edn-id
 [data-input]
 (id-edn-reader (nippy/thaw-from-in! data-input)))

(defn id-buffer? [^DirectBuffer buffer]
  (and (= id-size (.capacity buffer))
       (= id-value-type-id (.getByte buffer 0))))

(defn valid-id? [x]
  (try
    (id-buffer? (->id-buffer x))
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
    {:xt/id (.eid entity-tx)
     :xtdb.api/content-hash (.content-hash entity-tx)
     :xtdb.api/valid-time (.vt entity-tx)
     :xtdb.api/tx-time (.tt entity-tx)
     :xtdb.api/tx-id (.tx-id entity-tx)}))

(defn multiple-values? [v]
  (or (vector? v) (set? v)))

(defn vectorize-value [v]
  (cond-> v
    (not (multiple-values? v))
    (vector)))

(defn evicted-doc? [doc]
  (boolean (:xtdb.api/evicted? doc)))

(defn keep-non-evicted-doc [doc]
  (when-not (evicted-doc? doc)
    doc))
