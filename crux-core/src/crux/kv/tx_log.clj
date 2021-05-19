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
            [crux.tx.event :as txe]
            [clojure.java.io :as io])
  (:import java.io.Closeable
           java.nio.ByteOrder
           [java.util.concurrent ExecutorService LinkedBlockingQueue RejectedExecutionHandler ThreadPoolExecutor TimeUnit]
           java.util.Date
           [org.agrona DirectBuffer MutableDirectBuffer]))

(defn encode-tx-event-key-to ^org.agrona.MutableDirectBuffer [^MutableDirectBuffer b, {:crux.tx/keys [tx-id tx-time]}]
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
  {:crux.tx/tx-id (.getLong k c/index-id-size ByteOrder/BIG_ENDIAN)
   :crux.tx/tx-time (c/reverse-time-ms->date (.getLong k (+ c/index-id-size Long/BYTES) ByteOrder/BIG_ENDIAN))})

(defn- ingest-tx [tx-ingester tx tx-events]
  (let [in-flight-tx (db/begin-tx tx-ingester tx nil)]
    (if (db/index-tx-events in-flight-tx tx-events)
      (db/commit in-flight-tx)
      (db/abort in-flight-tx))))

(defn- submit-tx [tx-events
                  {:keys [^ExecutorService tx-submit-executor
                          ^ExecutorService tx-ingest-executor
                          kv-store tx-ingester fsync?]}]
  (if (.isShutdown tx-submit-executor)
    ::closed

    ;; this needs to remain `:crux.kv-tx-log/latest-submitted-tx-id` because we're a TxLog
    (let [tx-time (Date.)
          tx-id (inc (or (kvi/read-meta kv-store :crux.kv-tx-log/latest-submitted-tx-id) -1))
          next-tx {:crux.tx/tx-id tx-id, :crux.tx/tx-time tx-time}]
      (kv/store kv-store [[(encode-tx-event-key-to nil next-tx)
                           (mem/->nippy-buffer tx-events)]
                          (kvi/meta-kv :crux.kv-tx-log/latest-submitted-tx-id tx-id)])

      (when (and tx-ingest-executor tx-ingester)
        (.submit tx-ingest-executor
                 ^Runnable #(ingest-tx tx-ingester next-tx tx-events)))

      (when fsync?
        (kv/fsync kv-store))

      next-tx)))

(defrecord KvTxLog [^ExecutorService tx-submit-executor
                    ^ExecutorService tx-ingest-executor
                    kv-store tx-ingester fsync?]
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

  (latest-submitted-tx [this]
    (when-let [tx-id (kvi/read-meta kv-store :crux.kv-tx-log/latest-submitted-tx-id)]
      {::tx/tx-id tx-id}))

  (open-tx-log [this after-tx-id]
    (let [batch-size 100]
      (letfn [(tx-log [after-tx-id]
                (lazy-seq
                 (let [txs (with-open [snapshot (kv/new-snapshot kv-store)
                                       iterator (kv/new-iterator snapshot)]
                             (letfn [(tx-log [k]
                                       (lazy-seq
                                        (when (some-> k (tx-event-key?))
                                          (cons (assoc (decode-tx-event-key-from k)
                                                       :crux.tx.event/tx-events (mem/<-nippy-buffer (kv/value iterator)))
                                                (tx-log (kv/next iterator))))))]
                               (let [after-tx-id (or (some-> after-tx-id (+ 1)) 0)]
                                 (->> (tx-log (kv/seek iterator (encode-tx-event-key-to nil {::tx/tx-id after-tx-id})))
                                      (take batch-size)
                                      vec))))]
                   (concat txs
                           (when (= batch-size (count txs))
                             (tx-log (::tx/tx-id (last txs))))))))]
        (cio/->cursor (fn []) (tx-log after-tx-id)))))

  Closeable
  (close [_]
    (try
      (.shutdownNow tx-submit-executor)
      (catch Exception e
        (log/warn e "Error shutting down tx-submit-executor")))

    (when tx-ingest-executor
      (try
        (.shutdownNow tx-ingest-executor)
        (catch Exception e
          (log/warn e "Error shutting down tx-ingest-executor"))))

    (or (.awaitTermination tx-submit-executor 5 TimeUnit/SECONDS)
        (log/warn "waited 5s for tx-submit-executor to exit, no dice."))

    (when tx-ingest-executor
      (or (.awaitTermination tx-ingest-executor 5 TimeUnit/SECONDS)
          (log/warn "waited 5s for tx-ingest-executor to exit, no dice.")))))

(defn- bounded-solo-thread-pool [^long queue-size thread-factory]
  (let [queue (LinkedBlockingQueue. queue-size)]
    (ThreadPoolExecutor. 1 1
                         0 TimeUnit/MILLISECONDS
                         queue
                         thread-factory
                         (reify RejectedExecutionHandler
                           (rejectedExecution [_ runnable executor]
                             (.put queue runnable))))))

(defn ->ingest-only-tx-log {::sys/deps {:kv-store 'crux.mem-kv/->kv-store}}
  [{:keys [kv-store]}]
  (map->KvTxLog {:tx-submit-executor (bounded-solo-thread-pool 16 (cio/thread-factory "crux-standalone-submit-tx"))
                 :kv-store kv-store}))

(defn ->tx-log {::sys/deps {:kv-store 'crux.mem-kv/->kv-store
                            :tx-ingester :crux/tx-ingester
                            :index-store :crux/index-store}
                ::sys/args {:fsync? {:spec ::sys/boolean
                                     :required? true
                                     :default true}}}
  [{:keys [tx-ingester index-store kv-store fsync?]}]
  (let [^ExecutorService ingest-executor (bounded-solo-thread-pool 1024 (cio/thread-factory "crux-standalone-tx-ingest"))
        tx-log (->KvTxLog (bounded-solo-thread-pool 16 (cio/thread-factory "crux-standalone-submit-tx"))
                          ingest-executor
                          kv-store
                          tx-ingester
                          fsync?)
        latest-submitted-tx-id (::tx/tx-id (db/latest-submitted-tx tx-log))
        latest-completed-tx-id (::tx/tx-id (db/latest-completed-tx index-store))]
    (when (not= latest-submitted-tx-id latest-completed-tx-id)
      (.submit ingest-executor
               ^Runnable (fn []
                           (with-open [txs (db/open-tx-log tx-log latest-completed-tx-id)]
                             (doseq [tx (iterator-seq txs)
                                     :while (<= (::tx/tx-id tx) latest-submitted-tx-id)]
                               (ingest-tx tx-ingester
                                          (select-keys tx [::tx/tx-id ::tx/tx-time])
                                          (::txe/tx-events tx)))))))
    tx-log))

(defn -main [kv-store-config-file out-file]
  (with-open [sys (-> (sys/prep-system {:kv-store (read-string (slurp (io/file kv-store-config-file)))})
                      (sys/start-system))
              snapshot (kv/new-snapshot (:kv-store sys))
              iterator (kv/new-iterator snapshot)
              w (io/writer (io/file out-file))]
    (letfn [(tx-log [k]
              (lazy-seq
               (when (some-> k (tx-event-key?))
                 (cons (assoc (decode-tx-event-key-from k)
                              :crux.tx.event/tx-events (mem/<-nippy-buffer (kv/value iterator)))
                       (tx-log (kv/next iterator))))))]
      (doseq [e (tx-log (kv/seek iterator (encode-tx-event-key-to nil {::tx/tx-id 0})))]
        (.write w (prn-str e))))))

(comment
  (-main "/home/james/src/juxt/crux/crux-core/src/crux/kv/kv-tx-log.edn"
         "/tmp/rocks-tx-log.edn"))
