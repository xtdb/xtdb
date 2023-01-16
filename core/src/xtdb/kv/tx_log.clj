(ns xtdb.kv.tx-log
  (:require [clojure.tools.logging :as log]
            [xtdb.api :as xt]
            [xtdb.codec :as c]
            [xtdb.db :as db]
            [xtdb.io :as xio]
            [xtdb.kv :as kv]
            [xtdb.kv.index-store :as kvi]
            [xtdb.memory :as mem]
            [xtdb.system :as sys]
            [xtdb.tx.subscribe :as tx-sub])
  (:import java.io.Closeable
           java.nio.ByteOrder
           [java.util.concurrent ExecutorService TimeUnit]
           java.util.Date
           [org.agrona DirectBuffer MutableDirectBuffer]))

(defn encode-tx-event-key-to ^org.agrona.MutableDirectBuffer [^MutableDirectBuffer b, {::xt/keys [tx-id tx-time]}]
  (let [^MutableDirectBuffer b (or b (mem/allocate-buffer (+ c/index-id-size Long/BYTES Long/BYTES)))]
    (doto b
      (.putByte 0 c/tx-events-index-id)
      (.putLong c/index-id-size tx-id ByteOrder/BIG_ENDIAN)
      (.putLong (+ c/index-id-size Long/BYTES)
                (c/date->reverse-time-ms (or tx-time (Date.)))
                ByteOrder/BIG_ENDIAN))))

(defn tx-event-key? [^DirectBuffer k]
  (= c/tx-events-index-id (.getByte k 0)))

(defn decode-tx-event-key-from [^DirectBuffer k]
  (assert (= (+ c/index-id-size Long/BYTES Long/BYTES) (.capacity k)) (mem/buffer->hex k))
  (assert (tx-event-key? k))
  {::xt/tx-id (.getLong k c/index-id-size ByteOrder/BIG_ENDIAN)
   ::xt/tx-time (c/reverse-time-ms->date (.getLong k (+ c/index-id-size Long/BYTES) ByteOrder/BIG_ENDIAN))})

(defn- latest-submitted-tx [kv-store]
  (when-let [tx-id (kvi/read-meta kv-store :crux.kv-tx-log/latest-submitted-tx-id)]
    {::xt/tx-id tx-id}))

(defn- submit-tx [tx-events {:keys [^ExecutorService tx-submit-executor kv-store fsync? subscriber-handler]} opts]
  (if (.isShutdown tx-submit-executor)
    ::closed

    ;; this needs to remain `:crux.kv-tx-log/latest-submitted-tx-id` because we're a TxLog
    (let [tx-time (Date.)
          tx-id (inc (or (kvi/read-meta kv-store :crux.kv-tx-log/latest-submitted-tx-id) -1))
          next-tx {::xt/tx-id tx-id, ::xt/tx-time tx-time}]

      (kv/store kv-store [[(encode-tx-event-key-to nil next-tx)
                           (mem/->nippy-buffer {:xtdb.tx.event/tx-events tx-events
                                                ::xt/submit-tx-opts opts})]
                          (kvi/meta-kv :crux.kv-tx-log/latest-submitted-tx-id tx-id)])

      (when fsync?
        (kv/fsync kv-store))

      (tx-sub/notify-tx! subscriber-handler next-tx)

      {::xt/tx-id tx-id
       ::xt/tx-time (or (::xt/tx-time opts) tx-time)})))

(defn- txs-after [{:keys [kv-store]} after-tx-id {:keys [limit], :or {limit 100}}]
  (with-open [snapshot (kv/new-snapshot kv-store)
              iterator (kv/new-iterator snapshot)]
    (letfn [(tx-log [k]
              (lazy-seq
               (when (some-> k (tx-event-key?))
                 (cons (xio/conform-tx-log-entry (decode-tx-event-key-from k)
                                                 (mem/<-nippy-buffer (kv/value iterator)))
                       (tx-log (kv/next iterator))))))]
      (let [after-tx-id (or (some-> after-tx-id (+ 1)) 0)]
        (->> (tx-log (kv/seek iterator (encode-tx-event-key-to nil {::xt/tx-id after-tx-id})))
             (take limit)
             vec)))))

(defn- throw-if-closed [tx-submit-executor]
  (when (.isShutdown tx-submit-executor)
    (throw (IllegalStateException. "TxLog is closed."))))

(defrecord KvTxLog [^ExecutorService tx-submit-executor
                    kv-store fsync? subscriber-handler]
  db/TxLog
  (submit-tx [this tx-events]
    (db/submit-tx this tx-events {}))

  (submit-tx [this tx-events opts]
    (throw-if-closed tx-submit-executor)

    (let [!submitted-tx (.submit tx-submit-executor ^Callable #(submit-tx tx-events this opts))]
      (delay
        (let [submitted-tx @!submitted-tx]
          (when (= ::closed submitted-tx)
            (throw (IllegalStateException. "TxLog is closed.")))

          submitted-tx))))

  (latest-submitted-tx [_]
    (throw-if-closed tx-submit-executor)
    (latest-submitted-tx kv-store))

  (open-tx-log [this after-tx-id _]
    (throw-if-closed tx-submit-executor)
    (let [batch-size 100]
      (letfn [(tx-log [after-tx-id]
                (lazy-seq
                 (let [txs (txs-after this after-tx-id {:limit batch-size})]
                   (concat txs
                           (when (= batch-size (count txs))
                             (tx-log (::xt/tx-id (last txs))))))))]
        (xio/->cursor (fn []) (tx-log after-tx-id)))))

  (subscribe [this after-tx-id f]
    (throw-if-closed tx-submit-executor)
    (tx-sub/handle-notifying-subscriber subscriber-handler this after-tx-id f))

  Closeable
  (close [_]
    (try
      (.shutdownNow tx-submit-executor)
      (catch Exception e
        (log/warn e "Error shutting down tx-submit-executor")))

    (or (.awaitTermination tx-submit-executor 5 TimeUnit/SECONDS)
        (log/warn "waited 5s for tx-submit-executor to exit, no dice."))))

(defn ->tx-log {::sys/deps {:kv-store 'xtdb.mem-kv/->kv-store}}
  [{:keys [kv-store]}]
  (map->KvTxLog {:tx-submit-executor (xio/bounded-thread-pool 1 16 (xio/thread-factory "xtdb-standalone-submit-tx"))
                 :kv-store kv-store
                 :subscriber-handler (tx-sub/->notifying-subscriber-handler (latest-submitted-tx kv-store))}))
