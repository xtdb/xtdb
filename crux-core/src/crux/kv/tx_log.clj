(ns crux.kv.tx-log
  (:require [clojure.tools.logging :as log]
            [crux.codec :as c]
            [crux.db :as db]
            [crux.io :as cio]
            [crux.kv :as kv]
            [crux.kv.index-store :as kvi]
            [crux.memory :as mem]
            [crux.system :as sys]
            [crux.tx :as tx]
            [crux.tx.subscribe :as tx-sub])
  (:import java.io.Closeable
           java.nio.ByteOrder
           [java.util.concurrent ExecutorService LinkedBlockingQueue RejectedExecutionHandler ThreadPoolExecutor TimeUnit]
           java.util.Date
           [org.agrona DirectBuffer MutableDirectBuffer]))

(defn encode-tx-event-key-to ^org.agrona.MutableDirectBuffer [^MutableDirectBuffer b, {:xt/keys [tx-id tx-time]}]
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
  {:xt/tx-id (.getLong k c/index-id-size ByteOrder/BIG_ENDIAN)
   :xt/tx-time (c/reverse-time-ms->date (.getLong k (+ c/index-id-size Long/BYTES) ByteOrder/BIG_ENDIAN))})

(defn- latest-submitted-tx [kv-store]
  (when-let [tx-id (kvi/read-meta kv-store :crux.kv-tx-log/latest-submitted-tx-id)]
    {:xt/tx-id tx-id}))

(defn- submit-tx [tx-events {:keys [^ExecutorService tx-submit-executor kv-store fsync? subscriber-handler]}]
  (if (.isShutdown tx-submit-executor)
    ::closed

    ;; this needs to remain `:crux.kv-tx-log/latest-submitted-tx-id` because we're a TxLog
    (let [tx-time (Date.)
          tx-id (inc (or (kvi/read-meta kv-store :crux.kv-tx-log/latest-submitted-tx-id) -1))
          next-tx {:xt/tx-id tx-id, :xt/tx-time tx-time}]
      (kv/store kv-store [[(encode-tx-event-key-to nil next-tx)
                           (mem/->nippy-buffer tx-events)]
                          (kvi/meta-kv :crux.kv-tx-log/latest-submitted-tx-id tx-id)])

      (when fsync?
        (kv/fsync kv-store))

      (tx-sub/notify-tx! subscriber-handler next-tx)

      next-tx)))

(defn- txs-after [{:keys [kv-store]} after-tx-id {:keys [limit], :or {limit 100}}]
  (with-open [snapshot (kv/new-snapshot kv-store)
              iterator (kv/new-iterator snapshot)]
    (letfn [(tx-log [k]
              (lazy-seq
               (when (some-> k (tx-event-key?))
                 (cons (assoc (decode-tx-event-key-from k)
                              :crux.tx.event/tx-events (mem/<-nippy-buffer (kv/value iterator)))
                       (tx-log (kv/next iterator))))))]
      (let [after-tx-id (or (some-> after-tx-id (+ 1)) 0)]
        (->> (tx-log (kv/seek iterator (encode-tx-event-key-to nil {:xt/tx-id after-tx-id})))
             (take limit)
             vec)))))

(defrecord KvTxLog [^ExecutorService tx-submit-executor
                    kv-store fsync? subscriber-handler]
  db/TxLog
  (submit-tx [this tx-events]
    (when (.isShutdown tx-submit-executor)
      (throw (IllegalStateException. "TxLog is closed.")))

    (let [!submitted-tx (.submit tx-submit-executor ^Callable #(submit-tx tx-events this))]
      (delay
        (let [submitted-tx @!submitted-tx]
          (when (= ::closed submitted-tx)
            (throw (IllegalStateException. "TxLog is closed.")))

          submitted-tx))))

  (latest-submitted-tx [_]
    (latest-submitted-tx kv-store))

  (open-tx-log [this after-tx-id]
    (let [batch-size 100]
      (letfn [(tx-log [after-tx-id]
                (lazy-seq
                 (let [txs (txs-after this after-tx-id {:limit batch-size})]
                   (concat txs
                           (when (= batch-size (count txs))
                             (tx-log (:xt/tx-id (last txs))))))))]
        (cio/->cursor (fn []) (tx-log after-tx-id)))))

  (subscribe [this after-tx-id f]
    (tx-sub/handle-notifying-subscriber subscriber-handler this after-tx-id f))

  Closeable
  (close [_]
    (try
      (.shutdownNow tx-submit-executor)
      (catch Exception e
        (log/warn e "Error shutting down tx-submit-executor")))

    (or (.awaitTermination tx-submit-executor 5 TimeUnit/SECONDS)
        (log/warn "waited 5s for tx-submit-executor to exit, no dice."))))

(defn- bounded-solo-thread-pool [^long queue-size thread-factory]
  (let [queue (LinkedBlockingQueue. queue-size)]
    (ThreadPoolExecutor. 1 1
                         0 TimeUnit/MILLISECONDS
                         queue
                         thread-factory
                         (reify RejectedExecutionHandler
                           (rejectedExecution [_ runnable executor]
                             (.put queue runnable))))))

(defn ->tx-log {::sys/deps {:kv-store 'crux.mem-kv/->kv-store}}
  [{:keys [kv-store]}]
  (map->KvTxLog {:tx-submit-executor (bounded-solo-thread-pool 16 (cio/thread-factory "crux-standalone-submit-tx"))
                 :kv-store kv-store
                 :subscriber-handler (tx-sub/->notifying-subscriber-handler (latest-submitted-tx kv-store))}))
