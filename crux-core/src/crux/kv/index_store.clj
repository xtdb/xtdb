(ns ^:no-doc crux.kv.index-store
  (:require [crux.cache :as cache]
            [crux.codec :as c]
            [crux.db :as db]
            [crux.io :as cio]
            [crux.kv :as kv]
            [crux.memory :as mem]
            [crux.status :as status]
            [crux.morton :as morton]
            [crux.system :as sys]
            [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            [clojure.java.io :as io])
  (:import (crux.codec Id EntityTx)
           crux.api.IndexVersionOutOfSyncException
           java.io.Closeable
           java.nio.ByteOrder
           [java.util Date HashMap Map NavigableSet TreeSet]
           [java.util.function Function Supplier]
           java.util.concurrent.atomic.AtomicBoolean
           (clojure.lang MapEntry)
           (org.agrona DirectBuffer MutableDirectBuffer ExpandableDirectByteBuffer)))

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
   (letfn [(step [k]
             (lazy-seq
              (when (and k (mem/buffers=? seek-k k prefix-length))
                (cons (if entries?
                        (MapEntry/create (mem/copy-to-unpooled-buffer k) (mem/copy-to-unpooled-buffer (kv/value i)))
                        (mem/copy-to-unpooled-buffer k))
                      (step (if reverse? (kv/prev i) (kv/next i)))))))]
     (step (if reverse?
             (when (kv/seek i (-> seek-k (mem/copy-buffer) (mem/inc-unsigned-buffer!)))
               (kv/prev i))
             (kv/seek i seek-k))))))

(defn- buffer-or-value-buffer [v]
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

(defn safe-entity-tx ^crux.codec.EntityTx [entity-tx]
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

(defn- decode-ecav-key-from ^crux.kv.index_store.Quad [^DirectBuffer k ^long eid-size]
  (let [length (long (.capacity k))]
    (assert (<= (+ c/index-id-size eid-size c/id-size c/id-size) length) (mem/buffer->hex k))
    (let [index-id (.getByte k 0)]
      (assert (= c/ecav-index-id index-id))
      (let [entity (mem/slice-buffer k c/index-id-size eid-size)
            content-hash (Id. (mem/slice-buffer k (+ c/index-id-size eid-size) c/id-size) 0)
            attr (Id. (mem/slice-buffer k (+ c/index-id-size eid-size c/id-size) c/id-size) 0)
            value (key-suffix k (+ c/index-id-size eid-size c/id-size c/id-size))]
        (->Quad attr entity content-hash value)))))

(defn- decode-ecav-key-as-v-from [^DirectBuffer k ^long eid-size]
  (key-suffix k (+ c/index-id-size eid-size c/id-size c/id-size)))

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

(defn- encode-entity+vt+tt+tx-id-key-to
  (^org.agrona.MutableDirectBuffer [^MutableDirectBuffer b]
   (encode-entity+vt+tt+tx-id-key-to b mem/empty-buffer nil nil nil))
  (^org.agrona.MutableDirectBuffer [^MutableDirectBuffer b entity]
   (encode-entity+vt+tt+tx-id-key-to b entity nil nil nil))
  (^org.agrona.MutableDirectBuffer [^MutableDirectBuffer b entity valid-time]
   (encode-entity+vt+tt+tx-id-key-to b entity valid-time nil nil))
  (^org.agrona.MutableDirectBuffer [^MutableDirectBuffer b ^DirectBuffer entity ^Date valid-time ^Date transact-time ^Long tx-id]
   (assert (or (= c/id-size (.capacity entity))
               (zero? (.capacity entity))) (mem/buffer->hex entity))
   (let [^MutableDirectBuffer b (or b (mem/allocate-buffer (cond-> (+ c/index-id-size (.capacity entity))
                                                             valid-time (+ Long/BYTES)
                                                             transact-time (+ Long/BYTES)
                                                             tx-id (+ Long/BYTES))))]
     (.putByte b 0 c/entity+vt+tt+tx-id->content-hash-index-id)
     (.putBytes b c/index-id-size entity 0 (.capacity entity))
     (when valid-time
       (.putLong b (+ c/index-id-size c/id-size) (c/date->reverse-time-ms valid-time) ByteOrder/BIG_ENDIAN))
     (when transact-time
       (.putLong b (+ c/index-id-size c/id-size Long/BYTES) (c/date->reverse-time-ms transact-time) ByteOrder/BIG_ENDIAN))
     (when tx-id
       (.putLong b (+ c/index-id-size c/id-size Long/BYTES Long/BYTES) (c/descending-long tx-id) ByteOrder/BIG_ENDIAN))
     (->> (+ c/index-id-size (.capacity entity)
             (c/maybe-long-size valid-time) (c/maybe-long-size transact-time) (c/maybe-long-size tx-id))
          (mem/limit-buffer b)))))

(defn- decode-entity+vt+tt+tx-id-key-from ^crux.codec.EntityTx [^DirectBuffer k]
  (assert (= (+ c/index-id-size c/id-size Long/BYTES Long/BYTES Long/BYTES) (.capacity k)) (mem/buffer->hex k))
  (let [index-id (.getByte k 0)]
    (assert (= c/entity+vt+tt+tx-id->content-hash-index-id index-id))
    (let [entity (Id. (mem/slice-buffer k c/index-id-size c/id-size) 0)
          valid-time (c/reverse-time-ms->date (.getLong k (+ c/index-id-size c/id-size) ByteOrder/BIG_ENDIAN))
          transact-time (c/reverse-time-ms->date (.getLong k (+ c/index-id-size c/id-size Long/BYTES) ByteOrder/BIG_ENDIAN))
          tx-id (c/descending-long (.getLong k (+ c/index-id-size c/id-size Long/BYTES Long/BYTES) ByteOrder/BIG_ENDIAN))]
      (c/->EntityTx entity valid-time transact-time tx-id nil))))

(defn- decode-entity+vt+tt+tx-id-key-as-tt-from ^java.util.Date [^DirectBuffer k]
  (c/reverse-time-ms->date (.getLong k (+ c/index-id-size c/id-size Long/BYTES) ByteOrder/BIG_ENDIAN)))

(defn- encode-entity-tx-z-number [valid-time transaction-time]
  (morton/longs->morton-number (c/date->reverse-time-ms valid-time)
                               (c/date->reverse-time-ms transaction-time)))

(defn- encode-entity+z+tx-id-key-to
  (^org.agrona.MutableDirectBuffer [^MutableDirectBuffer b]
   (encode-entity+z+tx-id-key-to b mem/empty-buffer nil))
  (^org.agrona.MutableDirectBuffer [^MutableDirectBuffer b entity]
   (encode-entity+z+tx-id-key-to b entity nil nil))
  (^org.agrona.MutableDirectBuffer [^MutableDirectBuffer b entity z]
   (encode-entity+z+tx-id-key-to b entity z nil))
  (^org.agrona.MutableDirectBuffer [^MutableDirectBuffer b ^DirectBuffer entity z ^Long tx-id]
   (assert (or (= c/id-size (.capacity entity))
               (zero? (.capacity entity))) (mem/buffer->hex entity))
   (let [^MutableDirectBuffer b (or b (mem/allocate-buffer (cond-> (+ c/index-id-size (.capacity entity))
                                                             z (+ (* 2 Long/BYTES))
                                                             tx-id (+ Long/BYTES))))
         [upper-morton lower-morton] (when z
                                       (morton/morton-number->interleaved-longs z))]
     (.putByte b 0 c/entity+z+tx-id->content-hash-index-id)
     (.putBytes b c/index-id-size entity 0 (.capacity entity))
     (when z
       (.putLong b (+ c/index-id-size c/id-size) upper-morton ByteOrder/BIG_ENDIAN)
       (.putLong b (+ c/index-id-size c/id-size Long/BYTES) lower-morton ByteOrder/BIG_ENDIAN))
     (when tx-id
       (.putLong b (+ c/index-id-size c/id-size Long/BYTES Long/BYTES) (c/descending-long tx-id) ByteOrder/BIG_ENDIAN))
     (->> (+ c/index-id-size (.capacity entity) (if z (* 2 Long/BYTES) 0) (c/maybe-long-size tx-id))
          (mem/limit-buffer b)))))

(defn- decode-entity+z+tx-id-key-as-z-number-from [^DirectBuffer k]
  (assert (= (+ c/index-id-size c/id-size Long/BYTES Long/BYTES Long/BYTES) (.capacity k)) (mem/buffer->hex k))
  (let [index-id (.getByte k 0)]
    (assert (= c/entity+z+tx-id->content-hash-index-id index-id))
    (morton/interleaved-longs->morton-number
     (.getLong k (+ c/index-id-size c/id-size) ByteOrder/BIG_ENDIAN)
     (.getLong k (+ c/index-id-size c/id-size Long/BYTES) ByteOrder/BIG_ENDIAN))))

(defn- decode-entity+z+tx-id-key-from ^crux.codec.EntityTx [^DirectBuffer k]
  (assert (= (+ c/index-id-size c/id-size Long/BYTES Long/BYTES Long/BYTES) (.capacity k)) (mem/buffer->hex k))
  (let [index-id (.getByte k 0)]
    (assert (= c/entity+z+tx-id->content-hash-index-id index-id))
    (let [entity (Id. (mem/slice-buffer k c/index-id-size c/id-size) 0)
          [valid-time transaction-time] (morton/morton-number->longs (decode-entity+z+tx-id-key-as-z-number-from k))
          tx-id (c/descending-long (.getLong k (+ c/index-id-size c/id-size Long/BYTES Long/BYTES) ByteOrder/BIG_ENDIAN))]
      (c/->EntityTx entity (c/reverse-time-ms->date valid-time) (c/reverse-time-ms->date transaction-time) tx-id nil))))

(defn- etx->kvs [^EntityTx etx]
  [[(encode-entity+vt+tt+tx-id-key-to nil
                                      (c/->id-buffer (.eid etx))
                                      (.vt etx)
                                      (.tt etx)
                                      (.tx-id etx))
    (c/->id-buffer (.content-hash etx))]
   [(encode-entity+z+tx-id-key-to nil
                                  (c/->id-buffer (.eid etx))
                                  (encode-entity-tx-z-number (.vt etx) (.tt etx))
                                  (.tx-id etx))
    (c/->id-buffer (.content-hash etx))]])

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

;;;; Entity as-of

(defn- find-first-entity-tx-within-range [i min max eid]
  (let [prefix-size (+ c/index-id-size c/id-size)
        seek-k (encode-entity+z+tx-id-key-to (.get seek-buffer-tl)
                                             eid
                                             min)]
    (loop [k (kv/seek i seek-k)]
      (when (and k (mem/buffers=? seek-k k prefix-size))
        (let [z (decode-entity+z+tx-id-key-as-z-number-from k)]
          (if (morton/morton-number-within-range? min max z)
            (let [entity-tx (safe-entity-tx (decode-entity+z+tx-id-key-from k))
                  v (kv/value i)]
              (if-not (mem/buffers=? c/nil-id-buffer v)
                [(c/->id-buffer (.eid entity-tx))
                 (enrich-entity-tx entity-tx v)
                 z]
                [::deleted-entity entity-tx z]))
            (let [[litmax bigmin] (morton/morton-range-search min max z)]
              (when-not (neg? (.compareTo ^Comparable bigmin z))
                (recur (kv/seek i (encode-entity+z+tx-id-key-to (.get seek-buffer-tl)
                                                                eid
                                                                bigmin)))))))))))

(defn- find-entity-tx-within-range-with-highest-valid-time [i min max eid prev-candidate]
  (if-let [[_ ^EntityTx entity-tx z :as candidate] (find-first-entity-tx-within-range i min max eid)]
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
  (-> (decode-entity+vt+tt+tx-id-key-from k)
      (enrich-entity-tx v)))

(defn- entity-history-seq-ascending
  ([i eid] ([i eid] (entity-history-seq-ascending i eid {})))
  ([i eid {{^Date start-vt :crux.db/valid-time, ^Date start-tt :crux.tx/tx-time} :start
           {^Date end-vt :crux.db/valid-time, ^Date end-tt :crux.tx/tx-time} :end
           :keys [with-corrections?]}]
   (let [seek-k (encode-entity+vt+tt+tx-id-key-to nil (c/->id-buffer eid) start-vt)]
     (-> (all-keys-in-prefix i seek-k (+ c/index-id-size c/id-size)
                             {:reverse? true, :entries? true})
         (->> (map ->entity-tx))
         (cond->> end-vt (take-while (fn [^EntityTx entity-tx]
                                       (neg? (compare (.vt entity-tx) end-vt))))
                  start-tt (remove (fn [^EntityTx entity-tx]
                                     (neg? (compare (.tt entity-tx) start-tt))))
                  end-tt (filter (fn [^EntityTx entity-tx]
                                   (neg? (compare (.tt entity-tx) end-tt)))))
         (cond-> (not with-corrections?) (->> (partition-by :vt)
                                              (map last)))))))

(defn- entity-history-seq-descending
  ([i eid] (entity-history-seq-descending i eid {}))
  ([i eid {{^Date start-vt :crux.db/valid-time, ^Date start-tt :crux.tx/tx-time} :start
           {^Date end-vt :crux.db/valid-time, ^Date end-tt :crux.tx/tx-time} :end
           :keys [with-corrections?]}]
   (let [seek-k (encode-entity+vt+tt+tx-id-key-to nil (c/->id-buffer eid) start-vt)]
     (-> (all-keys-in-prefix i seek-k (+ c/index-id-size c/id-size)
                             {:entries? true})
         (->> (map ->entity-tx))
         (cond->> end-vt (take-while (fn [^EntityTx entity-tx]
                                       (pos? (compare (.vt entity-tx) end-vt))))
                  start-tt (remove (fn [^EntityTx entity-tx]
                                     (pos? (compare (.tt entity-tx) start-tt))))
                  end-tt (filter (fn [^EntityTx entity-tx]
                                   (pos? (compare (.tt entity-tx) end-tt)))))
         (cond-> (not with-corrections?) (->> (partition-by :vt)
                                              (map first)))))))

;;;; IndexSnapshot

(declare new-kv-index-snapshot)

(defn- advance-iterator-to-hash-cache-value [i value-buffer]
  (let [hash-cache-prefix-key (encode-hash-cache-key-to (.get seek-buffer-tl) value-buffer)
        found-k (kv/seek i hash-cache-prefix-key)]
    (and found-k
         (mem/buffers=? found-k hash-cache-prefix-key (.capacity hash-cache-prefix-key)))))

(defn- value-buffer->id-buffer ^org.agrona.DirectBuffer [index-snapshot ^DirectBuffer value-buffer]
  (c/->id-buffer (db/decode-value index-snapshot value-buffer)))

(defn- canonical-buffer-lookup ^org.agrona.DirectBuffer [canonical-buffer-cache ^DirectBuffer buffer]
  (cache/compute-if-absent canonical-buffer-cache
                           buffer
                           mem/copy-to-unpooled-buffer
                           identity))

(defn- cav-cache-lookup ^java.util.NavigableSet [cav-cache canonical-buffer-cache cache-i ^DirectBuffer eid-value-buffer
                                                 ^DirectBuffer content-hash-buffer ^DirectBuffer attr-buffer]
  (cache/compute-if-absent cav-cache
                           [content-hash-buffer attr-buffer]
                           (fn [_]
                             [(canonical-buffer-lookup canonical-buffer-cache content-hash-buffer)
                              (canonical-buffer-lookup canonical-buffer-cache attr-buffer)])
                           (fn [_]
                             (let [eid-size (mem/capacity eid-value-buffer)
                                   vs (TreeSet. mem/buffer-comparator)
                                   prefix (encode-ecav-key-to nil
                                                              eid-value-buffer
                                                              content-hash-buffer
                                                              attr-buffer)
                                   i (new-prefix-kv-iterator cache-i prefix)]
                               (loop [k (kv/seek i prefix)]
                                 (when k
                                   (let [v (decode-ecav-key-as-v-from k eid-size)
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

(defrecord KvIndexSnapshot [snapshot
                            close-snapshot?
                            level-1-iterator-delay
                            level-2-iterator-delay
                            entity-as-of-iterator-delay
                            decode-value-iterator-delay
                            cache-iterator-delay
                            nested-index-snapshot-state
                            ^Map temp-hash-cache
                            value-cache
                            cav-cache
                            canonical-buffer-cache
                            ^AtomicBoolean closed?]
  Closeable
  (close [_]
    (when (.compareAndSet closed? false true)
      (doseq [nested-index-snapshot @nested-index-snapshot-state]
        (cio/try-close nested-index-snapshot))
      (doseq [i [level-1-iterator-delay level-2-iterator-delay entity-as-of-iterator-delay decode-value-iterator-delay cache-iterator-delay]
              :when (realized? i)]
        (cio/try-close @i))
      (when close-snapshot?
        (cio/try-close snapshot))))

  db/IndexSnapshot
  (av [this a min-v]
    (let [attr-buffer (c/->id-buffer a)
          prefix (encode-av-key-to nil attr-buffer)
          i (new-prefix-kv-iterator @level-1-iterator-delay prefix)]
      (some->> (encode-av-key-to (.get seek-buffer-tl)
                                 attr-buffer
                                 (buffer-or-value-buffer min-v))
               (step-fn i #(key-suffix % (.capacity prefix))))))

  (ave [this a v min-e entity-resolver-fn]
    (let [attr-buffer (c/->id-buffer a)
          value-buffer (buffer-or-value-buffer v)
          prefix (encode-ave-key-to nil attr-buffer value-buffer)
          i (new-prefix-kv-iterator @level-2-iterator-delay prefix)]
      (some->> (encode-ave-key-to (.get seek-buffer-tl)
                                  attr-buffer
                                  value-buffer
                                  (buffer-or-value-buffer min-e))
               (step-fn i #(let [eid-value-buffer (key-suffix % (.capacity prefix))
                                 eid-buffer (value-buffer->id-buffer this eid-value-buffer)]
                             (when-let [content-hash-buffer (entity-resolver-fn eid-buffer)]
                               (when-let [vs (cav-cache-lookup cav-cache canonical-buffer-cache @cache-iterator-delay
                                                               eid-value-buffer content-hash-buffer attr-buffer)]
                                 (when (.contains vs value-buffer)
                                   eid-value-buffer))))))))

  (ae [this a min-e]
    (let [attr-buffer (c/->id-buffer a)
          prefix (encode-ae-key-to nil attr-buffer)
          i (new-prefix-kv-iterator @level-1-iterator-delay prefix)]
      (some->> (encode-ae-key-to (.get seek-buffer-tl)
                                 attr-buffer
                                 (buffer-or-value-buffer min-e))
               (step-fn i #(key-suffix % (.capacity prefix))))))

  (aev [this a e min-v entity-resolver-fn]
    (let [attr-buffer (c/->id-buffer a)
          eid-value-buffer (buffer-or-value-buffer e)
          eid-buffer (value-buffer->id-buffer this eid-value-buffer)]
      (when-let [content-hash-buffer (entity-resolver-fn eid-buffer)]
        (when-let [vs (cav-cache-lookup cav-cache canonical-buffer-cache @cache-iterator-delay
                                        eid-value-buffer content-hash-buffer attr-buffer)]
          (.tailSet vs (buffer-or-value-buffer min-v))))))

  (entity-as-of-resolver [this eid valid-time transact-time]
    (let [i @entity-as-of-iterator-delay
          prefix-size (+ c/index-id-size c/id-size)
          eid-buffer (c/->id-buffer eid)
          seek-k (encode-entity+vt+tt+tx-id-key-to (.get seek-buffer-tl)
                                                   eid-buffer
                                                   valid-time
                                                   transact-time
                                                   nil)]
      (loop [k (kv/seek i seek-k)]
        (when (and k (mem/buffers=? seek-k k prefix-size))
          (if (<= (compare (decode-entity+vt+tt+tx-id-key-as-tt-from k) transact-time) 0)
            (let [v (kv/value i)]
              (when-not (mem/buffers=? c/nil-id-buffer v)
                v))
            (if morton/*use-space-filling-curve-index?*
              (let [seek-z (encode-entity-tx-z-number valid-time transact-time)]
                (when-let [[k v] (find-entity-tx-within-range-with-highest-valid-time i seek-z morton/z-max-mask eid-buffer nil)]
                  (when-not (= ::deleted-entity k)
                    (c/->id-buffer (.content-hash ^EntityTx v)))))
              (recur (kv/next i))))))))

  (entity-as-of [this eid valid-time transact-time]
    (let [i @entity-as-of-iterator-delay
          prefix-size (+ c/index-id-size c/id-size)
          eid-buffer (c/->id-buffer eid)
          seek-k (encode-entity+vt+tt+tx-id-key-to (.get seek-buffer-tl)
                                                   eid-buffer
                                                   valid-time
                                                   transact-time
                                                   nil)]
      (loop [k (kv/seek i seek-k)]
        (when (and k (mem/buffers=? seek-k k prefix-size))
          (let [entity-tx (safe-entity-tx (decode-entity+vt+tt+tx-id-key-from k))
                v (kv/value i)]
            (if (<= (compare (.tt entity-tx) transact-time) 0)
              (cond-> entity-tx
                (not (mem/buffers=? c/nil-id-buffer v)) (enrich-entity-tx v))
              (if morton/*use-space-filling-curve-index?*
                (let [seek-z (encode-entity-tx-z-number valid-time transact-time)]
                  (when-let [[_ v] (find-entity-tx-within-range-with-highest-valid-time i seek-z morton/z-max-mask eid-buffer nil)]
                    v))
                (recur (kv/next i)))))))))

  (entity-history [this eid sort-order opts]
    (let [i @entity-as-of-iterator-delay
          entity-history-seq (case sort-order
                               :asc entity-history-seq-ascending
                               :desc entity-history-seq-descending)]
      (entity-history-seq i eid opts)))

  (decode-value [this value-buffer]
    (assert (some? value-buffer))
    (if (c/can-decode-value-buffer? value-buffer)
      (c/decode-value-buffer value-buffer)
      (cache/compute-if-absent
       value-cache
       value-buffer
       #(canonical-buffer-lookup canonical-buffer-cache %)
       (fn [value-buffer]
         (or (.get temp-hash-cache value-buffer)
             (let [i @decode-value-iterator-delay]
               (when (advance-iterator-to-hash-cache-value i value-buffer)
                 (some-> (kv/value i) (mem/<-nippy-buffer)))))))))

  (encode-value [this value]
    (let [value-buffer (c/->value-buffer value)]
      (when (and (not (c/can-decode-value-buffer? value-buffer))
                 (not (advance-iterator-to-hash-cache-value @decode-value-iterator-delay value-buffer)))
        (.put temp-hash-cache (canonical-buffer-lookup canonical-buffer-cache value-buffer) value))
      value-buffer))

  (open-nested-index-snapshot [this]
    (let [nested-index-snapshot (new-kv-index-snapshot snapshot temp-hash-cache value-cache cav-cache canonical-buffer-cache false)]
      (swap! nested-index-snapshot-state conj nested-index-snapshot)
      nested-index-snapshot)))

;;;; IndexStore

(defn- ->content-idx-kvs [docs]
  (let [attr-bufs (->> (into #{} (mapcat keys) (vals docs))
                       (into {} (map (juxt identity c/->id-buffer))))]
    (->> (for [[content-hash doc] docs
               :let [id (:crux.db/id doc)
                     eid-value-buffer (c/->value-buffer id)
                     content-hash (c/->id-buffer content-hash)]
               [a v] doc
               :let [a (get attr-bufs a)]
               v (c/vectorize-value v)
               :let [value-buffer (c/->value-buffer v)]
               :when (pos? (.capacity value-buffer))]
           (cond-> [(MapEntry/create (encode-av-key-to nil a value-buffer) mem/empty-buffer)
                    (MapEntry/create (encode-ave-key-to nil a value-buffer eid-value-buffer) mem/empty-buffer)
                    (MapEntry/create (encode-ae-key-to nil a eid-value-buffer) mem/empty-buffer)
                    (MapEntry/create (encode-ecav-key-to nil eid-value-buffer content-hash a value-buffer) mem/empty-buffer)]
             (not (c/can-decode-value-buffer? value-buffer))
             (conj (MapEntry/create (encode-hash-cache-key-to nil value-buffer eid-value-buffer) (mem/->nippy-buffer v)))))
         (apply concat))))

(defn- new-kv-index-snapshot [snapshot temp-hash-cache value-cache cav-cache canonical-buffer-cache close-snapshot?]
  (->KvIndexSnapshot snapshot
                     close-snapshot?
                     (delay (kv/new-iterator snapshot))
                     (delay (kv/new-iterator snapshot))
                     (delay (kv/new-iterator snapshot))
                     (delay (kv/new-iterator snapshot))
                     (delay (kv/new-iterator snapshot))
                     (atom [])
                     temp-hash-cache
                     value-cache
                     cav-cache
                     canonical-buffer-cache
                     (AtomicBoolean.)))

(defn latest-completed-tx [kv-store]
  ;; TODO at next version bump, let's make the kw here unrelated to the (old) namespace name
  (read-meta kv-store :crux.kv-indexer/latest-completed-tx))

(defrecord KvIndexStore [kv-store value-cache cav-cache canonical-buffer-cache]
  db/IndexStore
  (index-docs [this docs]
    (let [crux-db-id (c/->id-buffer :crux.db/id)
          docs (with-open [snapshot (kv/new-snapshot kv-store)]
                 (->> docs
                      (into {} (remove (fn [[k doc]]
                                         (let [eid-value (c/->value-buffer (:crux.db/id doc))
                                               content-hash (c/->id-buffer k)]
                                           (kv/get-value snapshot (encode-ecav-key-to (.get seek-buffer-tl)
                                                                                      eid-value
                                                                                      content-hash
                                                                                      crux-db-id
                                                                                      eid-value))))))
                      not-empty))
          content-idx-kvs (->content-idx-kvs docs)]
      (some->> (seq content-idx-kvs) (kv/store kv-store))
      {:bytes-indexed (->> content-idx-kvs (transduce (comp (mapcat seq) (map mem/capacity)) +))
       :indexed-docs docs}))

  (unindex-eids [this eids]
    (let [{:keys [tombstones ks]} (with-open [snapshot (kv/new-snapshot kv-store)
                                              bitemp-i (kv/new-iterator snapshot)
                                              ecav-i (kv/new-iterator snapshot)
                                              av-i (kv/new-iterator snapshot)]
                                    (->> (for [eid eids
                                               :let [eid-value-buffer (c/->value-buffer eid)]
                                               ecav-key (all-keys-in-prefix ecav-i
                                                                            (encode-ecav-key-to nil eid-value-buffer))]
                                           [eid eid-value-buffer ecav-key])

                                         (reduce (fn [acc [eid ^DirectBuffer eid-value-buffer ecav-key]]
                                                   (let [quad ^Quad (decode-ecav-key-from ecav-key (.capacity eid-value-buffer))
                                                         attr-buffer (c/->id-buffer (.attr quad))
                                                         value-buffer (.value quad)
                                                         shared-av? (> (->> (all-keys-in-prefix av-i (encode-ave-key-to nil
                                                                                                                        attr-buffer
                                                                                                                        value-buffer))
                                                                            (take 2)
                                                                            count)
                                                                       1)]
                                                     (when-not (c/can-decode-value-buffer? value-buffer)
                                                       (cache/evict value-cache value-buffer))
                                                     (cache/evict cav-cache (.content-hash quad))
                                                     (cond-> acc
                                                       true (update :tombstones assoc (.content-hash quad) {:crux.db/id (c/new-id eid)
                                                                                                            :crux.db/evicted? true})
                                                       true (update :ks conj
                                                                    (encode-ae-key-to nil
                                                                                      attr-buffer
                                                                                      eid-value-buffer)
                                                                    (encode-ave-key-to nil
                                                                                       attr-buffer
                                                                                       value-buffer
                                                                                       eid-value-buffer)
                                                                    ecav-key)
                                                       (not shared-av?) (update :ks conj
                                                                                (encode-av-key-to nil
                                                                                                  attr-buffer
                                                                                                  value-buffer))
                                                       (not (c/can-decode-value-buffer? value-buffer))
                                                       (update :ks conj (encode-hash-cache-key-to nil value-buffer eid-value-buffer)))))
                                                 {:tombstones {}
                                                  :ks (into #{}
                                                            (mapcat (fn [eid]
                                                                      (let [eid-id-buffer (c/->id-buffer eid)]
                                                                        (into (set (all-keys-in-prefix bitemp-i (encode-entity+vt+tt+tx-id-key-to nil eid-id-buffer)))
                                                                              (set (all-keys-in-prefix bitemp-i (encode-entity+z+tx-id-key-to nil eid-id-buffer)))))))
                                                            eids)})))]

      (kv/delete kv-store ks)
      {:tombstones tombstones}))

  (mark-tx-as-failed [this {:crux.tx/keys [tx-id] :as tx}]
    (kv/store kv-store [(meta-kv :crux.kv-indexer/latest-completed-tx tx)
                        [(encode-failed-tx-id-key-to nil tx-id) mem/empty-buffer]]))

  (index-entity-txs [this tx entity-txs]
    (kv/store kv-store (->> (conj (mapcat etx->kvs entity-txs)
                                  (meta-kv :crux.kv-indexer/latest-completed-tx tx))
                            (into (sorted-map-by mem/buffer-comparator)))))

  (store-index-meta [_ k v]
    (store-meta kv-store k v))

  (read-index-meta [this k]
    (db/read-index-meta this k nil))

  (read-index-meta [this k not-found]
    (read-meta kv-store k not-found))

  (latest-completed-tx [this]
    (latest-completed-tx kv-store))

  (tx-failed? [this tx-id]
    (with-open [snapshot (kv/new-snapshot kv-store)]
      (some? (kv/get-value snapshot (encode-failed-tx-id-key-to nil tx-id)))))

  (open-index-snapshot [this]
    (new-kv-index-snapshot (kv/new-snapshot kv-store) (HashMap.) value-cache cav-cache canonical-buffer-cache true))

  status/Status
  (status-map [this]
    {:crux.index/index-version (current-index-version kv-store)
     :crux.doc-log/consumer-state (db/read-index-meta this :crux.doc-log/consumer-state)
     :crux.tx-log/consumer-state (db/read-index-meta this :crux.tx-log/consumer-state)}))

(defn ->kv-index-store {::sys/deps {:kv-store 'crux.mem-kv/->kv-store
                                    :value-cache 'crux.cache/->cache
                                    :cav-cache 'crux.cache/->cache
                                    :canonical-buffer-cache 'crux.cache.soft-values/->soft-values-cache}
                        ::sys/args {:skip-index-version-bump {:spec (s/tuple int? int?)
                                                              :doc "Skip an index version bump. For example, to skip from v10 to v11, specify [10 11]"}}}
  [{:keys [kv-store value-cache cav-cache canonical-buffer-cache] :as opts}]
  (check-and-store-index-version opts)
  (->KvIndexStore kv-store value-cache cav-cache canonical-buffer-cache))
