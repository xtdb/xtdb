(ns crux.moberg
  (:require [crux.codec :as c]
            [crux.memory :as mem]
            [crux.kv :as kv]
            [taoensso.nippy :as nippy])
  (:import [org.agrona DirectBuffer ExpandableDirectByteBuffer MutableDirectBuffer]
           org.agrona.io.DirectBufferInputStream
           java.util.function.Supplier
           [java.io DataInputStream DataOutput]
           java.nio.ByteOrder
           java.util.Date))

;; Based on
;; https://github.com/facebook/rocksdb/wiki/Implement-Queue-Service-Using-RocksDB

(set! *unchecked-math* :warn-on-boxed)

(def ^:private ^:const message-idx 1)
(def ^:private ^:const reverse-key-idx 2)

(def ^:private ^:const idx-id-size Byte/BYTES)

(def ^:private ^ThreadLocal topic-key-buffer-tl
  (ThreadLocal/withInitial
   (reify Supplier
     (get [_]
       (ExpandableDirectByteBuffer.)))))

(defn- topic-key ^org.agrona.DirectBuffer [topic message-id k]
  (let [^DirectBuffer k (if k
                          (c/->id-buffer k)
                          c/empty-buffer)
        topic (c/->id-buffer topic)]
    (mem/limit-buffer
     (doto ^MutableDirectBuffer (.get topic-key-buffer-tl)
       (.putByte 0 message-idx)
       (.putBytes idx-id-size topic 0 c/id-size)
       (.putLong (+ idx-id-size c/id-size) message-id ByteOrder/BIG_ENDIAN)
       (.putBytes (+ idx-id-size c/id-size Long/BYTES) k 0 (.capacity k)))
     (+ idx-id-size c/id-size Long/BYTES (.capacity k)))))

(def ^:private ^ThreadLocal reverse-key-key-buffer-tl
  (ThreadLocal/withInitial
   (reify Supplier
     (get [_]
       (ExpandableDirectByteBuffer.)))))

(defn- reverse-key-key ^org.agrona.DirectBuffer [topic k]
  (let [topic (c/->id-buffer topic)
        k (c/->id-buffer k)]
    (mem/limit-buffer
     (doto ^MutableDirectBuffer (.get reverse-key-key-buffer-tl)
       (.putByte 0 reverse-key-idx)
       (.putBytes idx-id-size topic 0 c/id-size)
       (.putBytes (+ idx-id-size c/id-size) k 0 (.capacity k)))
     (+ idx-id-size c/id-size c/id-size))))

(defn- nippy-thaw [^DirectBuffer buffer]
  (when (pos? (.capacity buffer))
    (nippy/thaw-from-in! (DataInputStream. (DirectBufferInputStream. buffer)))))

(deftype CompactKVsAndKey [kvs key])

(defn- compact-topic-kvs+compact-k ^crux.moberg.CompactKVsAndKey [kv topic k new-message-k]
  (if k
    (with-open [snapshot (kv/new-snapshot kv)
                i (kv/new-iterator snapshot)]
      (let [seek-k (reverse-key-key topic k)
            compact-k (when-let [found-k (kv/seek i seek-k)]
                        (when (mem/buffers=? found-k seek-k)
                          (kv/value i)))]
        (CompactKVsAndKey.
         (cond-> [[seek-k new-message-k]]
           compact-k (conj [compact-k c/empty-buffer]))
         compact-k)))
    (CompactKVsAndKey. [] nil)))

(def ^:private topic+date->count (atom {}))
(def ^:private ^:const seq-size 10)
(def ^:private ^:const max-seq-id (dec (bit-shift-left 1 seq-size)))

(deftype MessageId [^Date time ^long id])

(defn- next-message-id ^crux.moberg.MessageId [topic]
  (let [message-time (Date.)
        k [topic message-time]
        seq (long (get (swap! topic+date->count (fn [topic+date->count]
                                                  {k (inc (long (get topic+date->count k 0)))}))
                       k))]
    (if (> seq max-seq-id)
      (recur topic)
      (MessageId. message-time (bit-or (bit-shift-left (.getTime message-time) 10) seq)))))

(def ^:private ^ThreadLocal send-buffer-tl
  (ThreadLocal/withInitial
   (reify Supplier
     (get [_]
       (ExpandableDirectByteBuffer.)))))

(defn send-message
  ([kv topic v]
   (send-message kv topic nil v nil))
  ([kv topic k v]
   (send-message kv topic k v nil))
  ([kv topic k v headers]
   (let [id (next-message-id topic)
         message-time (.time id)
         message-id (.id id)
         message-k (topic-key topic message-id k)
         compact-kvs+k (compact-topic-kvs+compact-k kv topic k message-k)]
     (kv/store kv (concat
                   (.kvs compact-kvs+k)
                   [[message-k
                     (mem/with-buffer-out
                       (.get send-buffer-tl)
                       (fn [^DataOutput out]
                         (nippy/freeze-to-out! out (cond-> {:crux.moberg/body v
                                                            :crux.moberg/topic topic
                                                            :crux.moberg/message-id message-id
                                                            :crux.moberg/message-time message-time}
                                                     k (assoc :crux.moberg/key k)
                                                     headers (assoc :crux.moberg/headers headers))))
                       false)]]))
     (when-let [compact-k (.key compact-kvs+k)]
       (kv/delete kv [compact-k]))
     {:crux.moberg/message-id message-id
      :crux.moberg/message-time message-time})))

(defn next-message [i topic]
  (let [seek-k (topic-key topic 0 nil)]
    (when-let [k (kv/next i)]
      (when (mem/buffers=? k seek-k (+ idx-id-size c/id-size))
        (or (nippy-thaw (kv/value i))
            (recur i topic))))))

(defn seek-message
  ([i topic]
   (seek-message i topic nil))
  ([i topic message-id]
   (let [seek-k (topic-key topic (or message-id 0) nil)]
     (when-let [k (kv/seek i seek-k)]
       (when (mem/buffers=? k seek-k (+ idx-id-size c/id-size))
         (or (nippy-thaw (kv/value i))
             (next-message i topic)))))))

(defn end-message-id [kv topic]
  (with-open [snapshot (kv/new-snapshot kv)
              i (kv/new-iterator snapshot)]
    (let [seek-k (topic-key topic Long/MAX_VALUE nil)]
      (if (kv/seek i seek-k)
        (when-let [k ^DirectBuffer (kv/prev i)]
          (when (mem/buffers=? k seek-k (+ idx-id-size c/id-size))
            (inc (.getLong k (+ idx-id-size c/id-size) ByteOrder/BIG_ENDIAN))))
        (do (kv/store kv [[(topic-key topic Long/MAX_VALUE nil)
                           c/empty-buffer]])
            (end-message-id kv topic))))))
