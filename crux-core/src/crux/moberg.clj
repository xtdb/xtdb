(ns crux.moberg
  (:require [crux.codec :as c]
            [crux.memory :as mem]
            [crux.kv :as kv]
            [clojure.spec.alpha :as s]
            [crux.db :as db]
            [clojure.java.io :as io]
            [crux.tx :as tx]
            [crux.backup :as backup]
            [crux.api :as api]
            [taoensso.nippy :as nippy]
            [crux.tx.polling :as p]
            [clojure.tools.logging :as log]
            [crux.moberg.types]
            [crux.tx.consumer])
  (:import [org.agrona DirectBuffer ExpandableDirectByteBuffer MutableDirectBuffer]
           org.agrona.io.DirectBufferInputStream
           crux.api.NonMonotonicTimeException
           crux.tx.consumer.Message
           crux.moberg.types.CompactKVsAndKey
           crux.moberg.types.SentMessage
           java.util.function.Supplier
           [java.io Closeable DataInputStream DataOutput]
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

(defn- topic-key ^org.agrona.DirectBuffer [topic message-id]
  (let [topic (c/->id-buffer topic)]
    (mem/limit-buffer
     (doto ^MutableDirectBuffer (.get topic-key-buffer-tl)
       (.putByte 0 message-idx)
       (.putBytes idx-id-size topic 0 c/id-size)
       (.putLong (+ idx-id-size c/id-size) message-id ByteOrder/BIG_ENDIAN))
     (+ idx-id-size c/id-size Long/BYTES))))

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

(defn- compact-topic-kvs+compact-k ^crux.moberg.types.CompactKVsAndKey [snapshot topic k new-message-k]
  (let [seek-k (reverse-key-key topic k)
        compact-k (kv/get-value snapshot seek-k)]
    (CompactKVsAndKey.
     (cond-> [[seek-k new-message-k]]
       compact-k (conj [compact-k c/empty-buffer]))
     compact-k)))

(def ^:private topic+date->count (atom {}))
(def ^:private ^:const seq-size 10)
(def ^:private ^:const max-seq-id (dec (bit-shift-left 1 seq-size)))

(defn- same-topic? [a b]
  (mem/buffers=? a b (+ idx-id-size c/id-size)))

(defn- message-id->message-time ^java.util.Date [^long message-id]
  (Date. (bit-shift-right message-id seq-size)))

(defn- message-key->message-id ^long [^DirectBuffer k]
  (.getLong k (+ idx-id-size c/id-size) ByteOrder/BIG_ENDIAN))

(defn end-message-id-offset ^long [kv topic]
  (or (with-open [snapshot (kv/new-snapshot kv)
                  i (kv/new-iterator snapshot)]
        (let [seek-k (topic-key topic Long/MAX_VALUE)
              k (kv/seek i seek-k)]
          (if (and k (same-topic? seek-k k))
            (or (when-let [k ^DirectBuffer (kv/prev i)]
                  (when (same-topic? k seek-k)
                    (inc (message-key->message-id k))))
                1)
            (do (kv/store kv [[seek-k c/empty-buffer]])
                nil))))
      (recur kv topic)))

(defn- now ^java.util.Date []
  (Date.))

(def ^:const detect-clock-drift? (not (Boolean/parseBoolean (System/getenv "CRUX_NO_CLOCK_DRIFT_CHECK"))))

(defn- next-message-id ^crux.moberg.types.SentMessage [kv topic]
  (let [message-time (now)
        seq (long (get (swap! topic+date->count
                              (fn [topic+date->count]
                                {message-time (inc (long (get topic+date->count message-time 0)))}))
                       message-time))
        message-id (bit-or (bit-shift-left (.getTime message-time) seq-size) seq)
        end-message-id (when detect-clock-drift?
                         (end-message-id-offset kv topic))]
    (cond
      (and detect-clock-drift? (< message-id (long end-message-id)))
      (throw (NonMonotonicTimeException.
              (str "Clock has moved backwards in time, message id: " message-id
                   " was generated using " (pr-str message-time)
                   " lowest valid next id: " end-message-id
                   " was generated using " (pr-str (message-id->message-time end-message-id)))))

      (> seq max-seq-id)
      (recur kv topic)

      :else
      (SentMessage. message-time message-id topic))))

(defn message->edn [^Message m]
  (cond-> {::body (.body m)
           ::topic (.topic m)
           ::message-id (.message-id m)
           ::message-time (.message-time m)}
    (.key m) (assoc ::key (.key m))
    (.headers m) (assoc ::headers (.headers m))))

(defn- nippy-thaw-message [topic message-id ^DirectBuffer buffer]
  (when (pos? (.capacity buffer))
    (with-open [in (DataInputStream. (DirectBufferInputStream. buffer))]
      (let [body (nippy/thaw-from-in! in)
            k (when (pos? (.available in))
                (nippy/thaw-from-in! in))
            headers (when (pos? (.available in))
                      (nippy/thaw-from-in! in))]
        (Message. body topic message-id (message-id->message-time message-id) k headers)))))

(def ^:private ^ThreadLocal send-buffer-tl
  (ThreadLocal/withInitial
   (reify Supplier
     (get [_]
       (ExpandableDirectByteBuffer.)))))

(defn sent-message->edn [^SentMessage m]
  {::message-id (.id m)
   ::message-time (.time m)
   ::topic (.topic m)})

(defn send-message
  (^SentMessage [kv topic v]
   (send-message kv topic nil v nil))
  (^SentMessage [kv topic k v]
   (send-message kv topic k v nil))
  (^SentMessage [kv topic k v headers]
   (with-open [^Closeable snapshot (if k
                                     (kv/new-snapshot kv)
                                     (reify Closeable
                                       (close [_])))]
     (let [id (next-message-id kv topic)
           message-time (.time id)
           message-id (.id id)
           message-k (topic-key topic message-id)
           compact-kvs+k (when k
                           (compact-topic-kvs+compact-k snapshot topic k message-k))]
       (kv/store kv (concat
                     (some->> compact-kvs+k (.kvs))
                     [[message-k
                       (mem/with-buffer-out
                         (.get send-buffer-tl)
                         (fn [^DataOutput out]
                           (nippy/freeze-to-out! out v)
                           (when (or k headers)
                             (nippy/freeze-to-out! out k)
                             (nippy/freeze-to-out! out headers)))
                         false)]]))
       (when-let [compact-k (some-> compact-kvs+k (.key))]
         (kv/delete kv [compact-k]))
       id))))

(defn next-message ^Message [i topic]
  (let [seek-k (topic-key topic 0)]
    (when-let [k ^DirectBuffer (kv/next i)]
      (when (same-topic? k seek-k)
        (or (nippy-thaw-message topic (message-key->message-id k) (kv/value i))
            (recur i topic))))))

(defn seek-message
  (^crux.tx.consumer.Message [i topic]
   (seek-message i topic nil))
  (^crux.tx.consumer.Message [i topic message-id]
   (let [seek-k (topic-key topic (or message-id 0))]
     (when-let [k ^DirectBuffer (kv/seek i seek-k)]
       (when (same-topic? k seek-k)
         (or (nippy-thaw-message topic (message-key->message-id k) (kv/value i))
             (next-message i topic)))))))

(defrecord MobergTxLog [event-log-kv]
  db/TxLog
  (submit-doc [this content-hash doc]
    (send-message event-log-kv ::event-log content-hash doc {:crux.tx/sub-topic :docs}))

  (submit-tx [this tx-ops]
    (s/assert :crux.api/tx-ops tx-ops)
    (doseq [doc (tx/tx-ops->docs tx-ops)]
      (db/submit-doc this (str (c/new-id doc)) doc))
    (let [tx-events (tx/tx-ops->tx-events tx-ops)
          m (send-message event-log-kv ::event-log nil tx-events {:crux.tx/sub-topic :txs})]
      (delay {:crux.tx/tx-id (.id m)
              :crux.tx/tx-time (.time m)})))

  (new-tx-log-context [this]
    (kv/new-snapshot event-log-kv))

  (tx-log [this tx-log-context from-tx-id]
    (let [i (kv/new-iterator tx-log-context)]
      (when-let [m (seek-message i ::event-log from-tx-id)]
        (for [^Message m (->> (repeatedly #(next-message i ::event-log))
                              (take-while identity)
                              (cons m))
              :when (= :txs (get (.headers m) :crux.tx/sub-topic))]
          {:crux.tx.event/tx-events (.body m)
           :crux.tx/tx-id (.message-id m)
           :crux.tx/tx-time (.message-time m)}))))

  backup/INodeBackup
  (write-checkpoint [this {:keys [crux.backup/checkpoint-directory]}]
    (kv/backup event-log-kv (io/file checkpoint-directory "event-log-kv-store"))))

(defrecord MobergEventLogContext [^Closeable snapshot ^Closeable i]
  Closeable
  (close [_]
    (.close i)
    (.close snapshot)))

(defrecord MobergEventLogConsumer [event-log-kv batch-size]
  crux.tx.consumer/PolledEventLog

  (new-event-log-context [this]
    (let [snapshot (kv/new-snapshot event-log-kv)]
      (MobergEventLogContext. snapshot (kv/new-iterator snapshot))))

  (next-events [this context next-offset]
    (let [i (:i context)]
      (reify clojure.lang.IReduceInit
        (reduce [_ f init]
          (if-let [m (seek-message i ::event-log next-offset)]
            (do
              (log/debug "Consuming message:" (pr-str (message->edn m)))
              (loop [init' init m m n 1]
                (let [result (f init' m)]
                  (if (reduced? result)
                    @result
                    (if-let [next-m (and (< n (long batch-size))
                                         (next-message i ::event-log))]
                      (recur result next-m (inc n))
                      result)))))
            init)))))

  (end-offset [this]
    (end-message-id-offset event-log-kv ::event-log)))
