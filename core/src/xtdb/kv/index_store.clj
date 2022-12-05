(ns ^:no-doc xtdb.kv.index-store
  (:require [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            [xtdb.api :as xt]
            [xtdb.cache :as cache]
            [xtdb.cache.nop :as nop-cache]
            [xtdb.codec :as c]
            [xtdb.db :as db]
            [xtdb.error :as err]
            [xtdb.hyper-log-log :as hll]
            [xtdb.io :as xio]
            [xtdb.kv :as kv]
            [xtdb.memory :as mem]
            [xtdb.morton :as morton]
            [xtdb.status :as status]
            [xtdb.kv.mutable-kv :as mut-kv]
            [xtdb.fork :as fork]
            [xtdb.system :as sys])
  (:import clojure.lang.MapEntry
           java.io.Closeable
           java.nio.ByteOrder
           [java.util ArrayList Date HashMap List Map NavigableSet TreeSet]
           [java.util.concurrent Semaphore TimeUnit]
           java.util.concurrent.atomic.AtomicBoolean
           [java.util.function BiFunction Supplier Function BiConsumer]
           [org.agrona DirectBuffer ExpandableDirectByteBuffer MutableDirectBuffer]
           xtdb.api.IndexVersionOutOfSyncException
           [xtdb.codec EntityTx Id]
           (org.agrona.collections MutableLong)))

(set! *unchecked-math* :warn-on-boxed)

(def ^:private ^ThreadLocal seek-buffer-tl
  (ThreadLocal/withInitial
   (reify Supplier
     (get [_]
       (ExpandableDirectByteBuffer.)))))

;; NOTE: A buffer returned from an kv/KvIterator can only be assumed
;; to be valid until the next call on the same iterator. In practice
;; this limitation is only for RocksJNRKv.
;; TODO: It would be nice to make this explicit somehow.

(defrecord PrefixKvIterator [i ^DirectBuffer prefix]
  kv/KvIterator
  (seek [_ k]
    (when-let [k (kv/seek i k)]
      (when (mem/buffers=? k prefix (.capacity prefix))
        k)))

  (prev [_]
    (when-let [k (kv/prev i)]
      (when (mem/buffers=? k prefix (.capacity prefix))
        k)))

  (next [_]
    (when-let [k (kv/next i)]
      (when (mem/buffers=? k prefix (.capacity prefix))
        k)))

  (value [_]
    (kv/value i))

  Closeable
  (close [_]
    (.close ^Closeable i)))

(defn- new-prefix-kv-iterator ^java.io.Closeable [i prefix]
  (->PrefixKvIterator i prefix))

(defn- all-keys-in-prefix
  ([i ^DirectBuffer prefix] (all-keys-in-prefix i prefix (.capacity prefix) {}))
  ([i seek-k prefix-length] (all-keys-in-prefix i seek-k prefix-length {}))
  ([i ^DirectBuffer seek-k, prefix-length {:keys [entries? reverse?]}]
   (lazy-seq
    (letfn [(step [k]
              (lazy-seq
               (when (and k (mem/buffers=? seek-k k prefix-length))
                 (cons (if entries?
                         (MapEntry/create (mem/copy-to-unpooled-buffer k)
                                          (mem/copy-to-unpooled-buffer (kv/value i)))
                         (mem/copy-to-unpooled-buffer k))
                       (step (if reverse? (kv/prev i) (kv/next i)))))))]
      (step
       (if reverse?
         (if (kv/seek i (-> seek-k (mem/copy-buffer) (mem/inc-unsigned-buffer!)))
           (kv/prev i)
           (letfn [(step [k]
                     (when k
                       (let [next-k (kv/next i)]
                         (if (and next-k (mem/buffers=? seek-k next-k prefix-length))
                           (step next-k)
                           (kv/seek i k)))))]
             (step (kv/seek i seek-k))))
         (kv/seek i seek-k)))))))

(defn- buffer-or-value-buffer ^org.agrona.DirectBuffer [v]
  (cond
    (instance? DirectBuffer v)
    v

    (some? v)
    (c/->value-buffer v)

    :else
    mem/empty-buffer))

(defn ^EntityTx enrich-entity-tx [entity-tx ^DirectBuffer content-hash]
  (assoc entity-tx :content-hash (when (pos? (.capacity content-hash))
                                   (c/safe-id (c/new-id content-hash)))))

(defn safe-entity-tx ^xtdb.codec.EntityTx [entity-tx]
  (-> entity-tx
      (update :eid c/safe-id)
      (update :content-hash c/safe-id)))

(defrecord Quad [attr eid content-hash value])

(defn- key-suffix [^DirectBuffer k ^long prefix-size]
  (mem/slice-buffer k prefix-size (- (.capacity k) prefix-size)))

;;;; Content indices

(defn- encode-av-key-to
  (^org.agrona.MutableDirectBuffer[b attr]
   (encode-av-key-to b attr mem/empty-buffer))
  (^org.agrona.MutableDirectBuffer
   [^MutableDirectBuffer b ^DirectBuffer attr ^DirectBuffer v]
   (assert (= c/id-size (.capacity attr)) (mem/buffer->hex attr))
   (let [^MutableDirectBuffer b (or b (mem/allocate-buffer (+ c/index-id-size c/id-size (.capacity v))))]
     (mem/limit-buffer
      (doto b
        (.putByte 0 c/av-index-id)
        (.putBytes c/index-id-size attr 0 c/id-size)
        (.putBytes (+ c/index-id-size c/id-size) v 0 (.capacity v)))
      (+ c/index-id-size c/id-size (.capacity v))))))

(defn- encode-ave-key-to
  (^org.agrona.MutableDirectBuffer[b attr]
   (encode-ave-key-to b attr mem/empty-buffer mem/empty-buffer))
  (^org.agrona.MutableDirectBuffer[b attr v]
   (encode-ave-key-to b attr v mem/empty-buffer))
  (^org.agrona.MutableDirectBuffer
   [^MutableDirectBuffer b ^DirectBuffer attr ^DirectBuffer v ^DirectBuffer entity]
   (assert (= c/id-size (.capacity attr)) (mem/buffer->hex attr))
   (let [^MutableDirectBuffer b (or b (mem/allocate-buffer (+ c/index-id-size c/id-size (.capacity v) (.capacity entity))))]
     (mem/limit-buffer
      (doto b
        (.putByte 0 c/ave-index-id)
        (.putBytes c/index-id-size attr 0 c/id-size)
        (.putBytes (+ c/index-id-size c/id-size) v 0 (.capacity v))
        (.putBytes (+ c/index-id-size c/id-size (.capacity v)) entity 0 (.capacity entity)))
      (+ c/index-id-size c/id-size (.capacity v) (.capacity entity))))))

(defn- decode-ave-key->e-from [^DirectBuffer k, ^long v-size]
  (let [av-size (+ c/index-id-size c/id-size v-size)]
    (mem/slice-buffer k av-size (- (.capacity k) av-size))))

(defn- encode-ae-key-to
  (^org.agrona.MutableDirectBuffer [b]
   (encode-ae-key-to b mem/empty-buffer mem/empty-buffer))
  (^org.agrona.MutableDirectBuffer [b attr]
   (encode-ae-key-to b attr mem/empty-buffer))
  (^org.agrona.MutableDirectBuffer [b ^DirectBuffer attr ^DirectBuffer entity]
   (assert (or (zero? (.capacity attr)) (= c/id-size (.capacity attr)))
           (mem/buffer->hex attr))
   (let [^MutableDirectBuffer b (or b (mem/allocate-buffer (+ c/index-id-size (.capacity attr) (.capacity entity))))]
     (-> (doto b
           (.putByte 0 c/ae-index-id)
           (.putBytes c/index-id-size attr 0 (.capacity attr))
           (.putBytes (+ c/index-id-size (.capacity attr)) entity 0 (.capacity entity)))
         (mem/limit-buffer (+ c/index-id-size (.capacity attr) (.capacity entity)))))))

(defn- encode-ecav-key-to
  (^org.agrona.MutableDirectBuffer [b entity]
   (encode-ecav-key-to b entity mem/empty-buffer mem/empty-buffer mem/empty-buffer))
  (^org.agrona.MutableDirectBuffer [b entity content-hash]
   (encode-ecav-key-to b entity content-hash mem/empty-buffer mem/empty-buffer))
  (^org.agrona.MutableDirectBuffer [b entity content-hash attr]
   (encode-ecav-key-to b entity content-hash attr mem/empty-buffer))
  (^org.agrona.MutableDirectBuffer [^MutableDirectBuffer b ^DirectBuffer entity ^DirectBuffer content-hash ^DirectBuffer attr ^DirectBuffer v]
   (assert (or (zero? (.capacity attr)) (= c/id-size (.capacity attr)))
           (mem/buffer->hex attr))
   (assert (or (zero? (.capacity content-hash)) (= c/id-size (.capacity content-hash)))
           (mem/buffer->hex content-hash))
   (let [^MutableDirectBuffer b (or b (mem/allocate-buffer (+ c/index-id-size (.capacity entity) (.capacity content-hash) (.capacity attr) (.capacity v))))]
     (-> (doto b
           (.putByte 0 c/ecav-index-id)
           (.putBytes c/index-id-size entity 0 (.capacity entity))
           (.putBytes (+ c/index-id-size (.capacity entity)) content-hash 0 (.capacity content-hash))
           (.putBytes (+ c/index-id-size (.capacity entity) (.capacity content-hash)) attr 0 (.capacity attr))
           (.putBytes (+ c/index-id-size (.capacity entity) (.capacity content-hash) (.capacity attr)) v 0 (.capacity v)))
         (mem/limit-buffer (+ c/index-id-size (.capacity entity) (.capacity content-hash) (.capacity attr) (.capacity v)))))))

(defn- decode-ecav-key-from ^xtdb.kv.index_store.Quad [^DirectBuffer k ^long eid-size]
  (let [length (long (.capacity k))]
    (assert (<= (+ c/index-id-size eid-size c/id-size c/id-size) length) (mem/buffer->hex k))
    (let [index-id (.getByte k 0)]
      (assert (= c/ecav-index-id index-id))
      (let [entity (mem/slice-buffer k c/index-id-size eid-size)
            content-hash (Id. (mem/slice-buffer k (+ c/index-id-size eid-size) c/id-size) 0)
            attr (Id. (mem/slice-buffer k (+ c/index-id-size eid-size c/id-size) c/id-size) 0)
            value (key-suffix k (+ c/index-id-size eid-size c/id-size c/id-size))]
        (->Quad attr entity content-hash value)))))

(defn- decode-ecav-value-from ^ints [^DirectBuffer v]
  (let [v-size (.capacity v)]
    (when-not (zero? v-size)
      (let [len (/ v-size Integer/BYTES)
            out (int-array len)]
        (dotimes [idx len]
          (aset out idx (.getInt v (* idx Integer/BYTES))))
        out))))

(defrecord ECAVEntry [a v idx])

(defn- encode-hash-cache-key-to
  (^org.agrona.MutableDirectBuffer [b value]
   (encode-hash-cache-key-to b value mem/empty-buffer))
  (^org.agrona.MutableDirectBuffer [^MutableDirectBuffer b ^DirectBuffer value ^DirectBuffer entity]
   (let [^MutableDirectBuffer b (or b (mem/allocate-buffer (+ c/index-id-size (.capacity value) (.capacity entity))))]
     (-> (doto b
           (.putByte 0 c/hash-cache-index-id)
           (.putBytes c/index-id-size value 0 (.capacity value))
           (.putBytes (+ c/index-id-size (.capacity value)) entity 0 (.capacity entity)))
         (mem/limit-buffer (+ c/index-id-size (.capacity value) (.capacity entity)))))))

;;;; Bitemp indices

(defn- encode-bitemp-key-to
  (^org.agrona.MutableDirectBuffer [^MutableDirectBuffer b]
   (encode-bitemp-key-to b mem/empty-buffer nil nil nil))
  (^org.agrona.MutableDirectBuffer [^MutableDirectBuffer b entity]
   (encode-bitemp-key-to b entity nil nil nil))
  (^org.agrona.MutableDirectBuffer [^MutableDirectBuffer b entity valid-time]
   (encode-bitemp-key-to b entity valid-time nil nil))
  (^org.agrona.MutableDirectBuffer [^MutableDirectBuffer b ^DirectBuffer entity ^Date valid-time ^Long tx-id ^Date tx-time]
   (assert (or (= c/id-size (.capacity entity))
               (zero? (.capacity entity))) (mem/buffer->hex entity))
   (let [^MutableDirectBuffer b (or b (mem/allocate-buffer (cond-> (+ c/index-id-size (.capacity entity))
                                                             valid-time (+ Long/BYTES)
                                                             tx-id (+ Long/BYTES)
                                                             tx-time (+ Long/BYTES))))]
     (-> b
         (doto (.putByte 0 c/entity+vt+tt+tx-id->content-hash-index-id))
         (doto (.putBytes c/index-id-size entity 0 (.capacity entity)))
         (doto (cond-> valid-time (.putLong (+ c/index-id-size c/id-size)
                                            (c/date->reverse-time-ms valid-time)
                                            ByteOrder/BIG_ENDIAN)))
         (doto (cond-> tx-id (.putLong (+ c/index-id-size c/id-size Long/BYTES)
                                       (c/descending-long tx-id)
                                       ByteOrder/BIG_ENDIAN)))
         (doto (cond-> tx-time (.putLong (+ c/index-id-size c/id-size Long/BYTES Long/BYTES)
                                         (c/date->reverse-time-ms tx-time)
                                         ByteOrder/BIG_ENDIAN)))
         (mem/limit-buffer (+ c/index-id-size
                              (.capacity entity)
                              (c/maybe-long-size valid-time)
                              (c/maybe-long-size tx-time)
                              (c/maybe-long-size tx-id)))))))

(defn- decode-bitemp-key-from ^xtdb.codec.EntityTx [^DirectBuffer k]
  (assert (= (+ c/index-id-size c/id-size Long/BYTES Long/BYTES Long/BYTES) (.capacity k)) (mem/buffer->hex k))
  (let [index-id (.getByte k 0)]
    (assert (= c/entity+vt+tt+tx-id->content-hash-index-id index-id))
    (let [entity (Id. (mem/slice-buffer k c/index-id-size c/id-size) 0)
          valid-time (c/reverse-time-ms->date (.getLong k (+ c/index-id-size c/id-size) ByteOrder/BIG_ENDIAN))
          tx-id (c/descending-long (.getLong k (+ c/index-id-size c/id-size Long/BYTES) ByteOrder/BIG_ENDIAN))
          tx-time (c/reverse-time-ms->date (.getLong k (+ c/index-id-size c/id-size Long/BYTES Long/BYTES) ByteOrder/BIG_ENDIAN))]
      (c/->EntityTx entity valid-time tx-time tx-id nil))))

(defn- decode-bitemp-key-as-tx-id-from ^java.util.Date [^DirectBuffer k]
  (c/descending-long (.getLong k (+ c/index-id-size c/id-size Long/BYTES) ByteOrder/BIG_ENDIAN)))

(defn- encode-entity-tx-z-number [valid-time tx-id]
  (morton/longs->morton-number (c/date->reverse-time-ms valid-time)
                               (c/descending-long tx-id)))

(defn- encode-bitemp-z-key-to
  (^org.agrona.MutableDirectBuffer [^MutableDirectBuffer b]
   (encode-bitemp-z-key-to b mem/empty-buffer nil))
  (^org.agrona.MutableDirectBuffer [^MutableDirectBuffer b entity]
   (encode-bitemp-z-key-to b entity nil nil))
  (^org.agrona.MutableDirectBuffer [^MutableDirectBuffer b entity z]
   (encode-bitemp-z-key-to b entity z nil))
  (^org.agrona.MutableDirectBuffer [^MutableDirectBuffer b ^DirectBuffer entity z ^Long tx-time]
   (assert (or (= c/id-size (.capacity entity))
               (zero? (.capacity entity))) (mem/buffer->hex entity))
   (let [^MutableDirectBuffer b (or b (mem/allocate-buffer (cond-> (+ c/index-id-size (.capacity entity))
                                                             z (+ (* 2 Long/BYTES))
                                                             tx-time (+ Long/BYTES))))
         [upper-morton lower-morton] (when z
                                       (morton/morton-number->interleaved-longs z))]
     (.putByte b 0 c/entity+z+tx-id->content-hash-index-id)
     (.putBytes b c/index-id-size entity 0 (.capacity entity))
     (when z
       (.putLong b (+ c/index-id-size c/id-size) upper-morton ByteOrder/BIG_ENDIAN)
       (.putLong b (+ c/index-id-size c/id-size Long/BYTES) lower-morton ByteOrder/BIG_ENDIAN))
     (when tx-time
       (.putLong b (+ c/index-id-size c/id-size Long/BYTES Long/BYTES) (c/date->reverse-time-ms tx-time) ByteOrder/BIG_ENDIAN))
     (->> (+ c/index-id-size (.capacity entity) (if z (* 2 Long/BYTES) 0) (c/maybe-long-size tx-time))
          (mem/limit-buffer b)))))

(defn- decode-bitemp-z-key-as-z-number-from [^DirectBuffer k]
  (assert (= (+ c/index-id-size c/id-size Long/BYTES Long/BYTES Long/BYTES) (.capacity k)) (mem/buffer->hex k))
  (let [index-id (.getByte k 0)]
    (assert (= c/entity+z+tx-id->content-hash-index-id index-id))
    (morton/interleaved-longs->morton-number
     (.getLong k (+ c/index-id-size c/id-size) ByteOrder/BIG_ENDIAN)
     (.getLong k (+ c/index-id-size c/id-size Long/BYTES) ByteOrder/BIG_ENDIAN))))

(defn- decode-bitemp-z-key-from ^xtdb.codec.EntityTx [^DirectBuffer k]
  (assert (= (+ c/index-id-size c/id-size Long/BYTES Long/BYTES Long/BYTES) (.capacity k)) (mem/buffer->hex k))
  (let [index-id (.getByte k 0)]
    (assert (= c/entity+z+tx-id->content-hash-index-id index-id))
    (let [entity (Id. (mem/slice-buffer k c/index-id-size c/id-size) 0)
          [valid-time tx-id] (morton/morton-number->longs (decode-bitemp-z-key-as-z-number-from k))
          tx-time (c/reverse-time-ms->date (.getLong k (+ c/index-id-size c/id-size Long/BYTES Long/BYTES) ByteOrder/BIG_ENDIAN))]
      (c/->EntityTx entity (c/reverse-time-ms->date valid-time) tx-time (c/descending-long tx-id) nil))))

(defn- etx->kvs [^EntityTx etx]
  (let [eid (c/->id-buffer (.eid etx))
        z (encode-entity-tx-z-number (.vt etx) (.tx-id etx))]
    [(MapEntry/create (encode-bitemp-key-to nil eid (.vt etx) (.tx-id etx) (.tt etx))
                      (c/->id-buffer (.content-hash etx)))
     (MapEntry/create (encode-bitemp-z-key-to nil eid z (.tt etx))
                      (c/->id-buffer (.content-hash etx)))]))

;; Index Version

(defn- encode-index-version-key-to ^org.agrona.MutableDirectBuffer [^MutableDirectBuffer b]
  (let [^MutableDirectBuffer b (or b (mem/allocate-buffer c/index-id-size))]
    (.putByte b 0 c/index-version-index-id)
    (mem/limit-buffer b c/index-id-size)))

(defn- encode-index-version-value-to ^org.agrona.MutableDirectBuffer [^MutableDirectBuffer b ^long version]
  (let [^MutableDirectBuffer b (or b (mem/allocate-buffer c/index-version-size))]
    (doto b
      (.putLong 0 version ByteOrder/BIG_ENDIAN))
    (mem/limit-buffer b c/index-version-size)))

(defn- decode-index-version-value-from ^long [^MutableDirectBuffer b]
  (.getLong b 0 ByteOrder/BIG_ENDIAN))

(defn- current-index-version [kv]
  (with-open [snapshot (kv/new-snapshot kv)]
    (some->> (kv/get-value snapshot (encode-index-version-key-to (.get seek-buffer-tl)))
             (decode-index-version-value-from))))

(defn- check-and-store-index-version [{:keys [kv-store skip-index-version-bump]}]
  (let [index-version (current-index-version kv-store)]
    (or (when (and index-version (not= c/index-version index-version))
          (let [[skip-from skip-to] skip-index-version-bump]
            (when-not (and (= skip-from index-version)
                           (= skip-to c/index-version))
              (throw (IndexVersionOutOfSyncException.
                      (str "Index version on disk: " index-version " does not match index version of code: " c/index-version))))))

        (doto kv-store
          (kv/store [[(encode-index-version-key-to nil)
                      (encode-index-version-value-to nil c/index-version)]])
          (kv/fsync)))))

;; Meta

(defn- encode-meta-key-to ^org.agrona.MutableDirectBuffer [^MutableDirectBuffer b ^DirectBuffer k]
  (assert (= c/id-size (.capacity k)) (mem/buffer->hex k))
  (let [^MutableDirectBuffer b (or b (mem/allocate-buffer (+ c/index-id-size c/id-size)))]
    (mem/limit-buffer
     (doto b
       (.putByte 0 c/meta-key->value-index-id)
       (.putBytes c/index-id-size k 0 (.capacity k)))
     (+ c/index-id-size c/id-size))))

(defn meta-kv [k v]
  [(encode-meta-key-to nil (c/->id-buffer k))
   (mem/->nippy-buffer v)])

(defn store-meta [kv k v]
  (kv/store kv [(meta-kv k v)]))

(defn- read-meta-snapshot
  ([snapshot k] (read-meta-snapshot snapshot k nil))
  ([snapshot k not-found]
   (if-let [v (kv/get-value snapshot (encode-meta-key-to (.get seek-buffer-tl) (c/->id-buffer k)))]
     (mem/<-nippy-buffer v)
     not-found)))

(defn read-meta
  ([kv k] (read-meta kv k nil))
  ([kv k not-found]
   (with-open [snapshot (kv/new-snapshot kv)]
     (read-meta-snapshot snapshot k not-found))))

;;;; Failed tx-id

(defn- encode-failed-tx-id-key-to
  (^org.agrona.MutableDirectBuffer [^MutableDirectBuffer b]
   (encode-failed-tx-id-key-to b nil))
  (^org.agrona.MutableDirectBuffer [^MutableDirectBuffer b tx-id]
   (let [^MutableDirectBuffer b (or b (mem/allocate-buffer (+ c/index-id-size (c/maybe-long-size tx-id))))]
     (.putByte b 0 c/failed-tx-id-index-id)
     (when tx-id
       (.putLong b c/index-id-size (c/descending-long tx-id) ByteOrder/BIG_ENDIAN))
     (mem/limit-buffer b (+ c/index-id-size (c/maybe-long-size tx-id))))))

;;;; tx-id/tx-time mappings

(def tx-time-mapping-prefix
  (doto ^MutableDirectBuffer (mem/allocate-unpooled-buffer c/index-id-size)
    (.putByte 0 c/tx-time-mapping-id)))

(defn- encode-tx-time-mapping-key-to [to tx-time tx-id]
  (let [size (+ c/index-id-size (c/maybe-long-size tx-time) (c/maybe-long-size tx-id))
        ^MutableDirectBuffer to (or to (mem/allocate-buffer size))]
    (assert (>= (.capacity to) size))
    (-> to
        (doto (.putByte 0 c/tx-time-mapping-id))
        (cond-> tx-time (doto (.putLong c/index-id-size (c/date->reverse-time-ms tx-time) ByteOrder/BIG_ENDIAN)))
        (cond-> tx-id (doto (.putLong (+ c/index-id-size Long/BYTES) (c/descending-long tx-id) ByteOrder/BIG_ENDIAN)))
        (mem/limit-buffer size))))

(defn- decode-tx-time-mapping-key-from [^DirectBuffer k]
  (assert (= c/tx-time-mapping-id (.getByte k 0)))
  {::xt/tx-time (c/reverse-time-ms->date (.getLong k c/index-id-size ByteOrder/BIG_ENDIAN))
   ::xt/tx-id (c/descending-long (.getLong k (+ c/index-id-size Long/BYTES) ByteOrder/BIG_ENDIAN))})

;;;; stats

(defn- encode-stats-key-to
  (^org.agrona.MutableDirectBuffer [b]
   (encode-stats-key-to b mem/empty-buffer))
  (^org.agrona.MutableDirectBuffer [b ^DirectBuffer attr]
   (let [^MutableDirectBuffer b (or b (mem/allocate-buffer (+ c/index-id-size (.capacity attr))))]
     (-> (doto b
           (.putByte 0 c/stats-index-id)
           (.putBytes c/index-id-size attr 0 (.capacity attr)))
         (mem/limit-buffer (+ c/index-id-size (.capacity attr)))))))

(defn- decode-stats-key->attr-from ^org.agrona.DirectBuffer [^DirectBuffer k]
  (assert (= c/stats-index-id (.getByte k 0)))
  (mem/slice-buffer k c/index-id-size))

(defn- new-stats-value ^org.agrona.MutableDirectBuffer []
  (doto ^MutableDirectBuffer (mem/allocate-buffer (+ Long/BYTES Long/BYTES hll/default-buffer-size hll/default-buffer-size))
    (.putByte 0 c/stats-index-id)
    (.putLong c/index-id-size 0)
    (.putLong (+ c/index-id-size Long/BYTES) 0)))

(defn- decode-stats-value->doc-count-from ^long [^DirectBuffer b]
  (.getLong b c/index-id-size))

(defn- inc-stats-value-doc-count ^org.agrona.MutableDirectBuffer [^MutableDirectBuffer b]
  (doto b
    (.putLong c/index-id-size (inc (decode-stats-value->doc-count-from b)))))

(defn- decode-stats-value->doc-value-count-from ^long [^DirectBuffer b]
  (.getLong b (+ c/index-id-size Long/BYTES)))

(defn- inc-stats-value-doc-value-count ^org.agrona.MutableDirectBuffer [^MutableDirectBuffer b]
  (doto b
    (.putLong (+ c/index-id-size Long/BYTES)
              (inc (decode-stats-value->doc-value-count-from b)))))

(defn decode-stats-value->eid-hll-buffer-from ^org.agrona.MutableDirectBuffer [^MutableDirectBuffer b]
  (let [hll-size (quot (- (.capacity b) Long/BYTES Long/BYTES) 2)]
    (mem/slice-buffer b (+ Long/BYTES Long/BYTES) hll-size)))

(defn decode-stats-value->value-hll-buffer-from ^org.agrona.MutableDirectBuffer [^MutableDirectBuffer b]
  (let [hll-size (quot (- (.capacity b) Long/BYTES Long/BYTES) 2)]
    (mem/slice-buffer b (+ Long/BYTES Long/BYTES hll-size) hll-size)))

(defn- stats-kvs [stats-kvs-cache persistent-kv-store docs]
  (let [persistent-kv-snapshot (atom nil)
        get-persistent-value (fn [k-buf]
                               (when (not @persistent-kv-snapshot)
                                 (reset! persistent-kv-snapshot (kv/new-snapshot persistent-kv-store)))
                               (kv/get-value @persistent-kv-snapshot k-buf))]
    (try
      ;; TODO, put this into a cache, see #1509
      (let [attr-key-bufs (reduce (fn [acc doc]
                                    (reduce (fn [acc k]
                                              (if (get acc k)
                                                acc
                                                (assoc acc k (encode-stats-key-to nil (c/->value-buffer k)))))
                                            acc
                                            (keys doc)))
                                  {}
                                  docs)]
        (->> docs
             (reduce (fn [acc doc]
                       (let [e (:crux.db/id doc)]
                         (reduce-kv (fn [acc k v]
                                      (let [k-buf (get attr-key-bufs k)
                                            stats-buf (or (get acc k-buf)
                                                          (cache/compute-if-absent stats-kvs-cache k-buf mem/copy-to-unpooled-buffer
                                                                                   (fn [_k-buf]
                                                                                     (or (some-> (get-persistent-value k-buf) mem/copy-buffer)
                                                                                         (new-stats-value)))))]
                                        (doto stats-buf
                                          (inc-stats-value-doc-count)
                                          (-> decode-stats-value->eid-hll-buffer-from (hll/add e)))

                                        (if (or (vector? v) (set? v))
                                          (doseq [v v]
                                            (doto stats-buf
                                              (inc-stats-value-doc-value-count)
                                              (-> decode-stats-value->value-hll-buffer-from (hll/add v))))
                                          (doto stats-buf
                                            (inc-stats-value-doc-value-count)
                                            (-> decode-stats-value->value-hll-buffer-from (hll/add v))))
                                        (assoc! acc k-buf stats-buf)))
                                    acc
                                    doc)))
                     (transient {}))
             persistent!))
      (finally
        (when @persistent-kv-snapshot
          (.close ^Closeable @persistent-kv-snapshot))))))

;;;; Entity as-of

(defn- find-first-entity-tx-within-range [i min max eid]
  (let [prefix-size (+ c/index-id-size c/id-size)
        seek-k (encode-bitemp-z-key-to (.get seek-buffer-tl)
                                       eid
                                       min)]
    (loop [k (kv/seek i seek-k)]
      (when (and k (mem/buffers=? seek-k k prefix-size))
        (let [z (decode-bitemp-z-key-as-z-number-from k)]
          (if (morton/morton-number-within-range? min max z)
            (let [entity-tx (safe-entity-tx (decode-bitemp-z-key-from k))
                  v (kv/value i)]
              (if-not (mem/buffers=? c/nil-id-buffer v)
                [(c/->id-buffer (.eid entity-tx))
                 (enrich-entity-tx entity-tx v)
                 z]
                [::deleted-entity entity-tx z]))
            (let [[_litmax bigmin] (morton/morton-range-search min max z)]
              (when-not (neg? (.compareTo ^Comparable bigmin z))
                (recur (kv/seek i (encode-bitemp-z-key-to (.get seek-buffer-tl)
                                                          eid
                                                          bigmin)))))))))))

(defn- find-entity-tx-within-range-with-highest-valid-time [i min max eid prev-candidate]
  (if-let [[_ _entity-tx z :as candidate] (find-first-entity-tx-within-range i min max eid)]
    (let [[^long x ^long y] (morton/morton-number->longs z)
          min-x (long (first (morton/morton-number->longs min)))
          max-x (dec x)]
      (if (and (not (pos? (Long/compareUnsigned min-x max-x)))
               (not= y -1))
        (let [min (morton/longs->morton-number
                   min-x
                   (unchecked-inc y))
              max (morton/longs->morton-number
                   max-x
                   -1)]
          (recur i min max eid candidate))
        candidate))
    prev-candidate))

;;;; History

(defn- ->entity-tx [[k v]]
  (-> (decode-bitemp-key-from k)
      (enrich-entity-tx v)))

(defn- entity-history-seq-ascending
  ([i eid] ([i eid] (entity-history-seq-ascending i eid {})))
  ([i eid {:keys [with-corrections? start-valid-time start-tx end-valid-time end-tx]}]
   (let [{start-tx-id ::xt/tx-id, start-tx-time ::xt/tx-time} start-tx
         {end-tx-id ::xt/tx-id, end-tx-time ::xt/tx-time} end-tx
         seek-k (encode-bitemp-key-to nil (c/->id-buffer eid) start-valid-time)]
     (-> (all-keys-in-prefix i seek-k (+ c/index-id-size c/id-size)
                             {:reverse? true, :entries? true})
         (->> (map ->entity-tx))
         (cond->> end-valid-time (take-while (fn [^EntityTx entity-tx]
                                               (neg? (compare (.vt entity-tx) end-valid-time))))

                  start-tx-id (remove (fn [^EntityTx entity-tx]
                                        (< ^long (.tx-id entity-tx) ^long start-tx-id)))
                  start-tx-time (remove (fn [^EntityTx entity-tx]
                                          (neg? (compare (.tt entity-tx) start-tx-time))))

                  end-tx-id (filter (fn [^EntityTx entity-tx]
                                      (< ^long (.tx-id entity-tx) ^long end-tx-id)))
                  end-tx-time (filter (fn [^EntityTx entity-tx]
                                        (neg? (compare (.tt entity-tx) end-tx-time)))))

         (cond-> (not with-corrections?) (->> (partition-by :vt)
                                              (map last)))))))

(defn- entity-history-seq-descending
  ([i eid] (entity-history-seq-descending i eid {}))
  ([i eid {:keys [with-corrections? start-valid-time start-tx end-valid-time end-tx]}]
   (let [{start-tx-id ::xt/tx-id, start-tx-time ::xt/tx-time} start-tx
         {end-tx-id ::xt/tx-id, end-tx-time ::xt/tx-time} end-tx
         seek-k (encode-bitemp-key-to nil (c/->id-buffer eid) start-valid-time)]
     (-> (all-keys-in-prefix i seek-k (+ c/index-id-size c/id-size)
                             {:entries? true})
         (->> (map ->entity-tx))
         (cond->> end-valid-time (take-while (fn [^EntityTx entity-tx]
                                               (pos? (compare (.vt entity-tx) end-valid-time))))

                  start-tx-id (remove (fn [^EntityTx entity-tx]
                                        (> ^long (.tx-id entity-tx) ^long start-tx-id)))
                  start-tx-time (remove (fn [^EntityTx entity-tx]
                                          (pos? (compare (.tt entity-tx) start-tx-time))))

                  end-tx-id (filter (fn [^EntityTx entity-tx]
                                      (> ^long (.tx-id entity-tx) ^long end-tx-id)))
                  end-tx-time (filter (fn [^EntityTx entity-tx]
                                        (pos? (compare (.tt entity-tx) end-tx-time)))))

         (cond-> (not with-corrections?) (->> (partition-by :vt)
                                              (map first)))))))

;;;; IndexSnapshot

(declare new-kv-index-snapshot)

(defn- advance-iterator-to-hash-cache-value [i value-buffer]
  (let [hash-cache-prefix-key (encode-hash-cache-key-to (.get seek-buffer-tl) value-buffer)
        found-k (kv/seek i hash-cache-prefix-key)]
    (and found-k
         (mem/buffers=? found-k hash-cache-prefix-key (.capacity hash-cache-prefix-key)))))

(defn- canonical-buffer-lookup ^org.agrona.DirectBuffer [canonical-buffer-cache ^DirectBuffer buffer]
  (cache/compute-if-absent canonical-buffer-cache
                           buffer
                           mem/copy-to-unpooled-buffer
                           identity))

(defn- cav-cache-lookup ^NavigableSet [cav-cache canonical-buffer-cache cache-i ^DirectBuffer eid-value-buffer
                                                 ^DirectBuffer content-hash-buffer ^DirectBuffer attr-buffer]
  (cache/compute-if-absent cav-cache
                           (MapEntry/create content-hash-buffer attr-buffer)
                           (fn [_]
                             (MapEntry/create (canonical-buffer-lookup canonical-buffer-cache content-hash-buffer)
                                              (canonical-buffer-lookup canonical-buffer-cache attr-buffer)))
                           (fn [_]
                             (let [vs (TreeSet. mem/buffer-comparator)
                                   prefix (encode-ecav-key-to nil
                                                              eid-value-buffer
                                                              content-hash-buffer
                                                              attr-buffer)
                                   i (new-prefix-kv-iterator cache-i prefix)]
                               (loop [k (kv/seek i prefix)]
                                 (when k
                                   (let [v (key-suffix k (.capacity prefix))
                                         v (canonical-buffer-lookup canonical-buffer-cache v)]
                                     (.add vs v)
                                     (recur (kv/next i)))))
                               vs))))

(defn- step-fn [i k-fn seek-k]
  ((fn step [^DirectBuffer k]
     (when k
       (if-let [k (k-fn k)]
         (cons k (lazy-seq (step (kv/next i))))
         (recur (kv/next i)))))
   (kv/seek i seek-k)))

(defn- latest-completed-tx-i [i]
  (some-> (kv/seek i tx-time-mapping-prefix)
          decode-tx-time-mapping-key-from))

(defn latest-completed-tx [kv-store]
  (with-open [snapshot (kv/new-snapshot kv-store)
              i (-> (kv/new-iterator snapshot)
                    (new-prefix-kv-iterator tx-time-mapping-prefix))]
    (latest-completed-tx-i i)))

(defprotocol PThreadManager
  (snapshot-opened [_ snapshot])
  (snapshot-closed [_ snapshot]))

(deftype ThreadManager [^Map snapshot-threads
                        ^:volatile-mutable ^Semaphore closing-semaphore]
  PThreadManager
  (snapshot-opened [this snapshot]
    (locking this
      (when closing-semaphore
        (throw (IllegalStateException. "closing")))

      (.put snapshot-threads snapshot (Thread/currentThread))))

  (snapshot-closed [this snapshot]
    (locking this
      (.remove snapshot-threads snapshot)
      (when closing-semaphore
        (.release closing-semaphore))))

  Closeable
  (close [this]
    (let [open-thread-count (locking this
                              (when-not closing-semaphore
                                (set! (.closing-semaphore this) (Semaphore. 0))

                                (let [open-threads (vals snapshot-threads)]
                                  (doseq [^Thread thread (set open-threads)]
                                    (.interrupt thread))
                                  (count open-threads))))]

      (when-not (.tryAcquire closing-semaphore open-thread-count 60 TimeUnit/SECONDS)
        (log/warn "Failed to shut down index-store after 60s due to outstanding snapshots"
                  (pr-str snapshot-threads))))))

(defrecord KvIndexSnapshot [snapshot
                            close-snapshot?
                            level-1-iterator-delay
                            level-2-iterator-delay
                            entity-as-of-iterator-delay
                            entity-as-of-prefix-optimised-iterator-delay
                            decode-value-iterator-delay
                            cache-iterator-delay
                            nested-index-snapshot-state
                            thread-mgr
                            cav-cache
                            canonical-buffer-cache
                            ^AtomicBoolean closed?]
  Closeable
  (close [_]
    (when (.compareAndSet closed? false true)
      (doseq [nested-index-snapshot @nested-index-snapshot-state]
        (xio/try-close nested-index-snapshot))
      (doseq [i [level-1-iterator-delay level-2-iterator-delay entity-as-of-iterator-delay entity-as-of-prefix-optimised-iterator-delay decode-value-iterator-delay cache-iterator-delay]
              :when (realized? i)]
        (xio/try-close @i))
      (when close-snapshot?
        (xio/try-close snapshot)
        (snapshot-closed thread-mgr snapshot))))

  db/IndexSnapshot
  (av [_ a min-v]
    (let [attr-buffer (c/->id-buffer a)
          prefix (encode-av-key-to nil attr-buffer)
          i (new-prefix-kv-iterator @level-1-iterator-delay prefix)]
      (some->> (encode-av-key-to (.get seek-buffer-tl)
                                 attr-buffer
                                 (buffer-or-value-buffer min-v))
               (step-fn i #(key-suffix % (.capacity prefix))))))

  (ave [_ a v min-e entity-resolver-fn]
    (let [attr-buffer (c/->id-buffer a)
          value-buffer (buffer-or-value-buffer v)
          prefix (encode-ave-key-to nil attr-buffer value-buffer)
          i (new-prefix-kv-iterator @level-2-iterator-delay prefix)]
      (some->> (encode-ave-key-to (.get seek-buffer-tl)
                                  attr-buffer
                                  value-buffer
                                  (buffer-or-value-buffer min-e))
               (step-fn i #(let [eid-value-buffer (key-suffix % (.capacity prefix))]
                             (when-let [content-hash-buffer (entity-resolver-fn eid-value-buffer)]
                               (when-let [vs (cav-cache-lookup cav-cache canonical-buffer-cache @cache-iterator-delay
                                                               eid-value-buffer content-hash-buffer attr-buffer)]
                                 (when (.contains vs value-buffer)
                                   eid-value-buffer))))))))

  (ae [_ a min-e]
    (let [attr-buffer (c/->id-buffer a)
          prefix (encode-ae-key-to nil attr-buffer)
          i (new-prefix-kv-iterator @level-1-iterator-delay prefix)]
      (some->> (encode-ae-key-to (.get seek-buffer-tl)
                                 attr-buffer
                                 (buffer-or-value-buffer min-e))
               (step-fn i #(key-suffix % (.capacity prefix))))))

  (aev [_ a e min-v entity-resolver-fn]
    (let [attr-buffer (c/->id-buffer a)
          eid-value-buffer (buffer-or-value-buffer e)]
      (when-let [content-hash-buffer (entity-resolver-fn eid-value-buffer)]
        (when-let [vs (cav-cache-lookup cav-cache canonical-buffer-cache @cache-iterator-delay
                                        eid-value-buffer content-hash-buffer attr-buffer)]
          (.tailSet vs (buffer-or-value-buffer min-v))))))

  (entity [this eid content-hash]
    (let [eid-value-buffer (if (instance? Id eid)
                             (c/->value-buffer (db/decode-value this (.buffer ^Id eid)))
                             (buffer-or-value-buffer eid))
          eid-size (.capacity eid-value-buffer)
          ecav-k-prefix (encode-ecav-key-to nil
                                            eid-value-buffer
                                            (c/->id-buffer content-hash))]

      (when-let [ecav-kvs (seq (all-keys-in-prefix @level-1-iterator-delay
                                                   ecav-k-prefix (.capacity ecav-k-prefix)
                                                   {:entries? true}))]
        (->> (for [[k idxs] ecav-kvs
                   :let [^Quad quad (decode-ecav-key-from k eid-size)
                         a (db/decode-value this (.buffer ^Id (.attr quad)))
                         v (db/decode-value this (.value quad))]
                   idx (or (decode-ecav-value-from idxs) [nil])]
               (->ECAVEntry a v idx))
             (group-by #(.a ^ECAVEntry %))
             (into {}
                   (map (fn [[a entries]]
                          (let [^ECAVEntry first-entry (first entries)
                                first-idx (.idx first-entry)
                                v (cond
                                    (nil? first-idx) (:v first-entry)
                                    (= -1 first-idx) (into #{} (map :v) entries)
                                    :else (->> entries
                                               (sort-by #(.idx ^ECAVEntry %))
                                               (mapv #(.v ^ECAVEntry %))))]
                            (MapEntry/create a v)))))))))

  (entity-as-of-resolver [this eid valid-time tx-id]
    (assert tx-id)
    (let [i @entity-as-of-prefix-optimised-iterator-delay
          prefix-size (+ c/index-id-size c/id-size)
          eid (if (instance? DirectBuffer eid)
                (if (c/id-buffer? eid)
                  eid
                  (db/decode-value this eid))
                eid)
          eid-buffer (c/->id-buffer eid)
          seek-k (encode-bitemp-key-to (.get seek-buffer-tl)
                                       eid-buffer
                                       valid-time
                                       tx-id
                                       nil)]
      (loop [k (kv/seek i seek-k)]
        (when (and k (mem/buffers=? seek-k k prefix-size))
          (if (<= (compare (decode-bitemp-key-as-tx-id-from k) tx-id) 0)
            (let [v (kv/value i)]
              (when-not (mem/buffers=? c/nil-id-buffer v)
                v))
            (if morton/*use-space-filling-curve-index?*
              (let [seek-z (encode-entity-tx-z-number valid-time tx-id)]
                (when-let [[k v] (find-entity-tx-within-range-with-highest-valid-time i seek-z morton/z-max-mask eid-buffer nil)]
                  (when-not (= ::deleted-entity k)
                    (c/->id-buffer (.content-hash ^EntityTx v)))))
              (recur (kv/next i))))))))

  (entity-as-of [_ eid valid-time tx-id]
    (assert tx-id)
    (let [i @entity-as-of-prefix-optimised-iterator-delay
          prefix-size (+ c/index-id-size c/id-size)
          eid-buffer (c/->id-buffer eid)
          seek-k (encode-bitemp-key-to (.get seek-buffer-tl)
                                       eid-buffer
                                       valid-time
                                       tx-id
                                       nil)]
      (loop [k (kv/seek i seek-k)]
        (when (and k (mem/buffers=? seek-k k prefix-size))
          (let [entity-tx (safe-entity-tx (decode-bitemp-key-from k))
                v (kv/value i)]
            (if (<= (compare (.tx-id entity-tx) tx-id) 0)
              (cond-> entity-tx
                (not (mem/buffers=? c/nil-id-buffer v)) (enrich-entity-tx v))
              (if morton/*use-space-filling-curve-index?*
                (let [seek-z (encode-entity-tx-z-number valid-time tx-id)]
                  (when-let [[_ v] (find-entity-tx-within-range-with-highest-valid-time i seek-z morton/z-max-mask eid-buffer nil)]
                    v))
                (recur (kv/next i)))))))))

  (entity-history [_ eid sort-order opts]
    (let [i @entity-as-of-iterator-delay
          entity-history-seq (case sort-order
                               :asc entity-history-seq-ascending
                               :desc entity-history-seq-descending)]
      (entity-history-seq i eid opts)))

  (resolve-tx [_ {::xt/keys [tx-time tx-id] :as tx}]
    (with-open [i (-> (kv/new-iterator snapshot)
                      (new-prefix-kv-iterator tx-time-mapping-prefix))]
      (let [latest-tx (latest-completed-tx-i i)]
        (cond
          (= tx latest-tx) tx

          tx-time (if (or (nil? latest-tx) (pos? (compare tx-time (::xt/tx-time latest-tx))))
                    (throw (err/node-out-of-sync {:requested tx, :available latest-tx}))

                    (let [found-tx (some-> (kv/seek i (encode-tx-time-mapping-key-to (.get seek-buffer-tl) tx-time tx-id))
                                           decode-tx-time-mapping-key-from)]
                      (if (and tx-id (not= tx-id (::xt/tx-id found-tx)))
                        (throw (err/illegal-arg :tx-id-mismatch
                                                {::err/message "Mismatching tx-id for tx-time"
                                                 :requested tx
                                                 :available found-tx}))

                        (do
                          (when (and found-tx (not= tx-time (::xt/tx-time found-tx)))
                            (let [next-tx (some-> (kv/prev i)
                                                  decode-tx-time-mapping-key-from)]
                              (when (= tx-time (::xt/tx-time next-tx))
                                (throw (err/illegal-arg :tx-id-mismatch
                                                        {::err/message "Mismatching tx-id for tx-time"
                                                         :requested tx
                                                         :available next-tx})))))

                          {::xt/tx-time tx-time
                           ::xt/tx-id (::xt/tx-id found-tx)}))))

          tx-id (if (or (nil? latest-tx) (> ^long tx-id ^long (::xt/tx-id latest-tx)))
                  (throw (err/node-out-of-sync {:requested tx, :available latest-tx}))
                  ;; TODO find corresponding tx-time?
                  tx)

          :else latest-tx))))

  (open-nested-index-snapshot [_]
    (let [nested-index-snapshot (new-kv-index-snapshot snapshot false nil cav-cache canonical-buffer-cache)]
      (swap! nested-index-snapshot-state conj nested-index-snapshot)
      nested-index-snapshot))

  db/ValueSerde
  (decode-value [_ value-buffer]
    (assert (some? value-buffer))
    (if (c/can-decode-value-buffer? value-buffer)
      (c/decode-value-buffer value-buffer)
      (let [i @decode-value-iterator-delay]
        (when (advance-iterator-to-hash-cache-value i value-buffer)
          (xio/with-nippy-thaw-all
            (some-> (kv/value i) (mem/<-nippy-buffer)))))))

  (encode-value [_ value] (c/->value-buffer value))

  db/AttributeStats
  (all-attrs [_]
    (with-open [i (kv/new-iterator snapshot)]
      (->> (for [stats-k (all-keys-in-prefix i (encode-stats-key-to nil))]
             (c/decode-value-buffer (decode-stats-key->attr-from stats-k)))
           (into #{}))))

  (doc-count [_ attr]
    (or (some-> (kv/get-value snapshot (encode-stats-key-to nil (c/->value-buffer attr)))
                decode-stats-value->doc-count-from)
        0))

  (doc-value-count [_ attr]
    (or (some-> (kv/get-value snapshot (encode-stats-key-to nil (c/->value-buffer attr)))
                decode-stats-value->doc-value-count-from)
        0))

  (value-cardinality [_ attr]
    (or (some-> (kv/get-value snapshot (encode-stats-key-to nil (c/->value-buffer attr)))
                decode-stats-value->value-hll-buffer-from
                hll/estimate)
        0.0))

  (eid-cardinality [_ attr]
    (or (some-> (kv/get-value snapshot (encode-stats-key-to nil (c/->value-buffer attr)))
                decode-stats-value->eid-hll-buffer-from
                hll/estimate)
        0.0))

  db/IndexMeta
  (-read-index-meta [_ k not-found]
    (read-meta-snapshot snapshot k not-found)))

(defn- new-kv-index-snapshot [snapshot close-snapshot? thread-mgr cav-cache canonical-buffer-cache]
  (when close-snapshot?
    (snapshot-opened thread-mgr snapshot))

  (->KvIndexSnapshot snapshot
                     close-snapshot?
                     (delay (kv/new-iterator snapshot))
                     (delay (kv/new-iterator snapshot))
                     (delay (kv/new-iterator snapshot))
                     (delay (kv/new-prefix-seek-optimised-iterator snapshot))
                     (delay (kv/new-iterator snapshot))
                     (delay (kv/new-iterator snapshot))
                     (atom [])
                     thread-mgr
                     cav-cache
                     canonical-buffer-cache
                     (AtomicBoolean.)))

;;;; IndexStore

(defn- val-idxs [v]
  (let [val->idxs (HashMap.)]
    (cond
      (vector? v) (dorun
                   (map-indexed (fn [idx el]
                                  (.compute val->idxs el
                                            (reify BiFunction
                                              (apply [_ _ acc]
                                                (let [^List acc (or acc (ArrayList.))]
                                                  (doto acc (.add idx)))))))
                                v))
      (set? v) (doseq [el v]
                 (.put val->idxs el [-1]))
      :else (.put val->idxs v nil))
    val->idxs))

(defn- encode-ecav-value [idxs]
  (if idxs
    (let [^MutableDirectBuffer buf (mem/allocate-buffer (* (count idxs) Integer/BYTES))]
      (dotimes [idx (count idxs)]
        (.putInt buf (* Integer/BYTES idx) (nth idxs idx)))
      buf)
    mem/empty-buffer))

(defn- buffer-tl ^ThreadLocal []
  (ThreadLocal/withInitial
   (reify Supplier
     (get [_]
       (ExpandableDirectByteBuffer. 32)))))

(def ^:private ^ThreadLocal content-buffer-tl (buffer-tl))
(def ^:private ^ThreadLocal eid-buffer-tl (buffer-tl))
(def ^:private ^ThreadLocal value-buffer-tl (buffer-tl))
(def ^:private ^ThreadLocal nippy-buffer-tl (buffer-tl))
(def ^:private ^ThreadLocal hash-key-buffer-tl (buffer-tl))
(def ^:private ^ThreadLocal content-hash-buffer-tl (buffer-tl))

(defrecord KvIndexStoreTx [kv-store kv-tx thread-mgr cav-cache canonical-buffer-cache]
  db/IndexStoreTx
  (index-tx [_ tx]
    (let [{::xt/keys [tx-id tx-time]} tx]
      (kv/put-kv kv-tx (encode-tx-time-mapping-key-to nil tx-time tx-id) mem/empty-buffer)))

  (index-docs [_ docs]
    (let [attr-bufs (HashMap.)
          attr-buf-compute (reify Function (apply [_ k] (c/->id-buffer k)))
          attr-buf-rf-kv (fn [^Map attr-buf k _] (.computeIfAbsent attr-buf k attr-buf-compute) attr-buf)
          attr-buf-rf-docs (fn [attr-buf doc] (reduce-kv attr-buf-rf-kv attr-buf doc))
          _ (reduce attr-buf-rf-docs attr-bufs (if (instance? Map docs) (.values ^Map docs) (vals docs)))

          bytes-indexed (MutableLong. 0)
          store (fn [^DirectBuffer k ^DirectBuffer v]
                  (.set bytes-indexed (+ (.get bytes-indexed) (.capacity k) (.capacity v)))
                  (kv/put-kv kv-tx k v))

          index-attribute-key
          (reify BiConsumer
            (accept [_ a a-buf]
              (store (encode-hash-cache-key-to (.get content-buffer-tl) a-buf) (mem/nippy->buffer a (.get nippy-buffer-tl)))))

          index-doc
          (reify BiConsumer
            (accept [_ content-hash doc]
              (let [id (:crux.db/id doc)
                    eid-value-buffer (c/value->buffer id (.get eid-buffer-tl))
                    content-hash (c/id->buffer content-hash (.get content-hash-buffer-tl))
                    index-positional-value
                    (fn [a-buffer v idxs]
                      (let [value-buffer (c/value->buffer v (.get value-buffer-tl))]
                        (when (pos? (.capacity value-buffer))
                          (store (encode-av-key-to (.get content-buffer-tl) a-buffer value-buffer) mem/empty-buffer)
                          (store (encode-ave-key-to (.get content-buffer-tl) a-buffer value-buffer eid-value-buffer) mem/empty-buffer)
                          (store (encode-ae-key-to (.get content-buffer-tl) a-buffer eid-value-buffer) mem/empty-buffer)
                          (store (encode-ecav-key-to (.get content-buffer-tl) eid-value-buffer content-hash a-buffer value-buffer) (encode-ecav-value idxs))
                          (when (not (c/can-decode-value-buffer? value-buffer))
                            (store (encode-hash-cache-key-to (.get content-buffer-tl) value-buffer eid-value-buffer) (mem/nippy->buffer v (.get nippy-buffer-tl)))))))

                    index-value
                    (reify BiConsumer
                      (accept [_ a v]
                        (let [a-buffer (.get attr-bufs a)]
                          (if (or (set? v) (vector? v))
                            ;; slow path for coll values, we can make this more efficient but not sure on distribution
                            ;; of complex vals
                            (doseq [[v idxs] (val-idxs v)]
                              (index-positional-value a-buffer v idxs))
                            (index-positional-value a-buffer v nil)))))]

                (store (encode-hash-cache-key-to (.get hash-key-buffer-tl) (c/id->buffer id (.get content-buffer-tl)) eid-value-buffer)
                       (mem/nippy->buffer id (.get nippy-buffer-tl)))

                (.forEach ^Map doc index-value))))]

      (.forEach attr-bufs index-attribute-key)

      (if (instance? Map docs)
        (.forEach ^Map docs index-doc)
        (run! (fn [[k v]] (.accept index-doc k v)) docs))

      {:bytes-indexed (.get bytes-indexed)
       :doc-ids (into #{} (map c/new-id (keys docs)))
       :av-count (transduce (map count) + 0 (vals docs))}))

  (unindex-eids [_ base-snapshot eids]
    (let [snapshot (kv/new-tx-snapshot kv-tx)
          pi (kv/new-iterator snapshot)
          ti (some-> base-snapshot kv/new-iterator)]
      (try
        (letfn [(merge-idxs [k]
                  (if ti
                    (fork/merge-seqs (all-keys-in-prefix pi k)
                                     (all-keys-in-prefix ti k))
                    (all-keys-in-prefix pi k)))]
          (let [bitemp-ks (->> (for [eid eids
                                     :let [eid-buf (c/->id-buffer eid)]
                                     k (concat (merge-idxs (encode-bitemp-key-to nil eid-buf))
                                               (merge-idxs (encode-bitemp-z-key-to nil eid-buf)))]
                                 k)
                               (into #{}))

                ecav-ks (->> (for [eid eids
                                   :let [eid-buf (c/->value-buffer eid)]
                                   ecav-key (merge-idxs (encode-ecav-key-to nil eid-buf))]
                               [eid eid-buf ecav-key (decode-ecav-key-from ecav-key (.capacity eid-buf))])
                             (vec))

                tombstones (->> (for [[eid _ _ ^Quad quad] ecav-ks]
                                  (MapEntry/create (.content-hash quad) eid))
                                (into {})
                                (into {} (map (fn [[ch eid]]
                                                (MapEntry/create ch
                                                                 {:crux.db/id (c/new-id eid)
                                                                  ::xt/evicted? true})))))
                content-ks (->> (for [[_ eid-buf ecav-key ^Quad quad] ecav-ks
                                      :let [attr-buf (c/->id-buffer (.attr quad))
                                            value-buf ^DirectBuffer (.value quad)
                                            sole-av? (empty? (->> (merge-idxs (encode-ave-key-to nil attr-buf value-buf))
                                                                  (remove (comp #(mem/buffers=? % eid-buf)
                                                                                #(decode-ave-key->e-from % (.capacity value-buf))))))]

                                      k (cond-> [ecav-key
                                                 (encode-ae-key-to nil attr-buf eid-buf)
                                                 (encode-ave-key-to nil attr-buf value-buf eid-buf)]
                                          sole-av? (conj (encode-av-key-to nil attr-buf value-buf))

                                          (c/can-decode-value-buffer? value-buf)
                                          (conj (encode-hash-cache-key-to nil value-buf eid-buf)))]
                                  k)
                                (into #{}))]

            (run! #(cache/evict cav-cache %) (keys tombstones))

            (doseq [k (concat bitemp-ks content-ks)]
              (kv/put-kv kv-tx k nil))

            {:tombstones tombstones}))
        (finally
          (doseq [^Closeable c [snapshot pi ti]]
            (when c (.close c)))))))

  (index-entity-txs [_ entity-txs]
    (doseq [kv (->> (mapcat etx->kvs entity-txs)
                    (sort-by key mem/buffer-comparator))]
      (apply kv/put-kv kv-tx kv)))

  (commit-index-tx [_]
    (kv/commit-kv-tx kv-tx))

  (abort-index-tx [_ tx docs]
    (when kv-tx
      (kv/abort-kv-tx kv-tx))
    (let [{::xt/keys [tx-id tx-time]} tx
          attr-bufs (->> (into #{} (mapcat keys) (vals docs))
                         (into {} (map (juxt identity c/->id-buffer))))]

      ;; we still put the ECAV KVs in so that we can keep track of what we need to evict later
      ;; the bitemp indices will ensure these are never returned in queries

      (kv/store kv-store
                (into [[(encode-failed-tx-id-key-to nil tx-id) mem/empty-buffer]
                       [(encode-tx-time-mapping-key-to nil tx-time tx-id) mem/empty-buffer]]
                      (for [[content-hash doc] docs
                            :let [id (:crux.db/id doc)
                                  eid-value-buffer (c/->value-buffer id)
                                  content-hash (c/->id-buffer content-hash)]

                            [a v] doc
                            :let [a (get attr-bufs a)]

                            [v idxs] (val-idxs v)
                            :let [value-buffer (c/->value-buffer v)]
                            :when (pos? (.capacity value-buffer))]
                        [(encode-ecav-key-to nil eid-value-buffer content-hash a value-buffer) (encode-ecav-value idxs)])))))

  db/IndexSnapshotFactory
  (open-index-snapshot [_]
    (new-kv-index-snapshot (kv/new-tx-snapshot kv-tx) true thread-mgr cav-cache canonical-buffer-cache)))

(defn- forked-index-tx [{:keys [thread-mgr kv-store] :as index-store}]
  (let [abort-index-tx (->KvIndexStoreTx kv-store nil thread-mgr (nop-cache/->nop-cache {}) (nop-cache/->nop-cache {}))
        transient-kv (mut-kv/->mutable-kv-store)
        transient-kv-tx (kv/begin-kv-tx transient-kv)
        transient-tx (->KvIndexStoreTx transient-kv transient-kv-tx thread-mgr (nop-cache/->nop-cache {}) (nop-cache/->nop-cache {}))]
    (fork/->ForkedKvIndexStoreTx index-store (atom #{}) transient-tx abort-index-tx)))

(defrecord KvIndexStore [kv-store thread-mgr cav-cache canonical-buffer-cache stats-kvs-cache]
  db/IndexStore
  (begin-index-tx [this]
    (if (satisfies? kv/KvStoreWithReadTransaction kv-store)
      (->KvIndexStoreTx kv-store (kv/begin-kv-tx kv-store) thread-mgr cav-cache canonical-buffer-cache)
      (forked-index-tx this)))

  (store-index-meta [_ k v]
    (store-meta kv-store k v))

  (index-stats [_ docs]
    (when (seq docs)
      (kv/store kv-store (stats-kvs stats-kvs-cache kv-store (vals docs)))))

  (tx-failed? [_ tx-id]
    (with-open [snapshot (kv/new-snapshot kv-store)]
      (some? (kv/get-value snapshot (encode-failed-tx-id-key-to nil tx-id)))))

  db/LatestCompletedTx
  (latest-completed-tx [_]
    (latest-completed-tx kv-store))

  db/IndexMeta
  (-read-index-meta [_ k not-found]
    (read-meta kv-store k not-found))

  db/IndexSnapshotFactory
  (open-index-snapshot [_]
    (new-kv-index-snapshot (kv/new-snapshot kv-store) true thread-mgr cav-cache canonical-buffer-cache))

  status/Status
  (status-map [this]
    {:xtdb.index/index-version (current-index-version kv-store)
     :xtdb.tx-log/consumer-state (db/read-index-meta this :xtdb.tx-log/consumer-state)})

  Closeable
  (close [_]
    (xio/try-close thread-mgr)))

(defn ->kv-index-store {::sys/deps {:kv-store 'xtdb.mem-kv/->kv-store
                                    :cav-cache 'xtdb.cache/->cache
                                    :canonical-buffer-cache 'xtdb.cache/->cache
                                    :stats-kvs-cache 'xtdb.cache/->cache}
                        ::sys/args {:skip-index-version-bump {:spec (s/tuple int? int?)
                                                              :doc "Skip an index version bump. For example, to skip from v10 to v11, specify [10 11]"}}}
  [{:keys [kv-store cav-cache canonical-buffer-cache stats-kvs-cache] :as opts}]
  (check-and-store-index-version opts)
  (->KvIndexStore kv-store (ThreadManager. (HashMap.) nil) cav-cache canonical-buffer-cache stats-kvs-cache))
