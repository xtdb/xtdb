(ns crux.standalone
  (:require [clojure.set :as set]
            [clojure.tools.logging :as log]
            [crux.codec :as c]
            [crux.db :as db]
            [crux.io :as cio]
            [crux.kv :as kv]
            [crux.kv-indexer :as kvi]
            [crux.memory :as mem]
            [crux.node :as n]
            [crux.topology :as topo]
            [crux.tx :as tx])
  (:import java.io.Closeable
           [java.util.concurrent ArrayBlockingQueue Executors ExecutorService ThreadPoolExecutor ThreadPoolExecutor$DiscardPolicy TimeUnit]
           java.nio.ByteOrder
           [org.agrona DirectBuffer MutableDirectBuffer]
           java.util.Date))

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

(defn- submit-tx [{:keys [!submitted-tx tx-events]}
                  {:keys [^ExecutorService tx-submit-executor, event-log-kv-store]}]
  (when (.isShutdown tx-submit-executor)
    (deliver !submitted-tx ::closed))

  (let [tx-time (Date.)
        tx-id (inc (or (kvi/read-meta event-log-kv-store ::latest-submitted-tx-id) -1))
        next-tx {:crux.tx/tx-id tx-id, :crux.tx/tx-time tx-time}]
    (kv/store event-log-kv-store [[(encode-tx-event-key-to nil next-tx)
                                   (mem/->nippy-buffer tx-events)]
                                  (kvi/meta-kv ::latest-submitted-tx-id tx-id)])

    (deliver !submitted-tx next-tx)))

(defrecord StandaloneTxLog [^ExecutorService tx-submit-executor, event-log-kv-store]
  db/TxLog
  (submit-tx [this tx-events]
    (when (.isShutdown tx-submit-executor)
      (throw (IllegalStateException. "TxLog is closed.")))

    (let [!submitted-tx (promise)]
      (.submit tx-submit-executor
               ^Runnable #(submit-tx {:!submitted-tx !submitted-tx
                                      :tx-events tx-events}
                                     this))
      (delay
       (let [submitted-tx @!submitted-tx]
         (when (= ::closed submitted-tx)
           (throw (IllegalStateException. "TxLog is closed.")))

         submitted-tx))))

  (latest-submitted-tx [this]
    (when-let [tx-id (kvi/read-meta event-log-kv-store ::latest-submitted-tx-id)]
      {::tx/tx-id tx-id}))

  (open-tx-log [this after-tx-id]
    (let [batch-fn (fn [after-tx-id]
                     (with-open [snapshot (kv/new-snapshot event-log-kv-store)
                                 iterator (kv/new-iterator snapshot)]
                       (letfn [(tx-log [k]
                                 (lazy-seq
                                  (when (some-> k (tx-event-key?))
                                    (cons (assoc (decode-tx-event-key-from k)
                                                 :crux.tx.event/tx-events (mem/<-nippy-buffer (kv/value iterator)))
                                          (tx-log (kv/next iterator))))))]

                         (when-let [k (kv/seek iterator (encode-tx-event-key-to nil {::tx/tx-id (or after-tx-id 0)}))]
                           (->> (tx-log (if after-tx-id (kv/next iterator) k))
                                (take 100)
                                (vec))))))]
      (cio/->cursor
       (fn [])
       (lazy-seq
        (when-let [batch (seq (batch-fn after-tx-id))]
          (concat batch (when-let [next-after-tx-id (:crux.tx/id (last batch))]
                          (batch-fn next-after-tx-id))))))))

  Closeable
  (close [_]
    (try
      (.shutdown tx-submit-executor)
      (catch Exception e
        (log/warn e "Error shutting down tx-submit-executor")))

    (or (.awaitTermination tx-submit-executor 5 TimeUnit/SECONDS)
        (log/warn "waited 5s for tx-submit-executor to exit, no dice."))))

(defn- ->tx-log [{:keys [::event-log]} _]
  (->StandaloneTxLog (Executors/newSingleThreadExecutor (cio/thread-factory "crux-standalone-tx-log"))
                     (:kv-store event-log)))

(defn- ->document-store [{{:keys [document-store]} ::event-log} _]
  document-store)

(def ^:private event-log-args
  {::event-log-kv-store {:doc "The KV store to use for the standalone event log"
                         :default 'crux.kv.memdb/kv
                         :crux.config/type :crux.topology/module}

   ::event-log-dir {:doc "The directory to persist the standalone event log to"
                    :required? false
                    :crux.config/type :crux.config/string}

   ::event-log-sync? {:doc "Sync the event-log backed KV store to disk after every write."
                      :default true
                      :crux.config/type :crux.config/boolean}

   ::event-log-document-store {:doc "The document store to use for the standalone event log"
                               :default 'crux.document-store/kv-document-store
                               :crux.config/type :crux.config/module}})

(defrecord EventLog [kv-store document-store]
  Closeable
  (close [_]
    (cio/try-close kv-store)
    (cio/try-close document-store)))

(defn ->event-log [deps {::keys [event-log-kv-store event-log-document-store] :as args}]
  (let [args (-> args
                 (set/rename-keys {::event-log-dir :crux.kv/db-dir
                                   ::event-log-sync? :crux.kv/sync?}))
        kv-store (topo/start-component event-log-kv-store {} args)
        document-store (topo/start-component event-log-document-store (assoc deps ::n/kv-store kv-store) args)]
    (->EventLog kv-store document-store)))

(def topology
  (merge n/base-topology
         {::event-log {:start-fn ->event-log
                       :args event-log-args}
          ::n/tx-log {:start-fn ->tx-log
                      :deps [::n/kv-store ::n/document-store ::event-log]}
          ::n/document-store {:start-fn ->document-store
                              :deps [::event-log]}}))
