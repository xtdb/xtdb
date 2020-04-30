(ns crux.standalone
  (:require [clojure.set :as set]
            [clojure.tools.logging :as log]
            [crux.codec :as c]
            [crux.db :as db]
            [crux.index :as idx]
            [crux.io :as cio]
            [crux.kv :as kv]
            [crux.node :as n]
            [crux.object-store :as os]
            [crux.topology :as topo]
            [crux.tx :as tx])
  (:import java.io.Closeable
           [java.util.concurrent ArrayBlockingQueue Executors ExecutorService ThreadPoolExecutor ThreadPoolExecutor$DiscardPolicy TimeUnit]
           java.util.Date))

(defn- submit-tx [{:keys [!submitted-tx tx-events]}
                  {:keys [^ExecutorService tx-submit-executor, event-log-kv-store]}]
  (when (.isShutdown tx-submit-executor)
    (deliver !submitted-tx ::closed))

  (let [tx-time (Date.)
        tx-id (inc (or (idx/read-meta event-log-kv-store ::latest-submitted-tx-id) -1))
        next-tx {:crux.tx/tx-id tx-id, :crux.tx/tx-time tx-time}]
    (kv/store event-log-kv-store [[(c/encode-tx-event-key-to nil next-tx)
                                   (os/->nippy-buffer tx-events)]
                                  (idx/meta-kv ::latest-submitted-tx-id tx-id)])

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
    (when-let [tx-id (idx/read-meta event-log-kv-store ::latest-submitted-tx-id)]
      {::tx/tx-id tx-id}))

  (open-tx-log [this after-tx-id]
    (let [snapshot (kv/new-snapshot event-log-kv-store)
          iterator (kv/new-iterator snapshot)]
      (letfn [(tx-log [k]
                (lazy-seq
                  (when (some-> k c/tx-event-key?)
                    (cons (assoc (c/decode-tx-event-key-from k)
                            :crux.tx.event/tx-events (os/<-nippy-buffer (kv/value iterator)))
                          (tx-log (kv/next iterator))))))]

        (let [k (kv/seek iterator (c/encode-tx-event-key-to nil {::tx/tx-id (or after-tx-id 0)}))]
          (->> (when k (tx-log (if after-tx-id (kv/next iterator) k)))
               (cio/->cursor (fn []
                               (cio/try-close iterator)
                               (cio/try-close snapshot))))))))

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

(defrecord StandaloneDocumentStore [event-log-kv-store event-log-object-store]
  db/DocumentStore
  (submit-docs [this id-and-docs]
    (db/put-objects event-log-object-store id-and-docs))

  (fetch-docs [this ids]
    (with-open [snapshot (kv/new-snapshot event-log-kv-store)]
      (db/get-objects event-log-object-store snapshot ids))))

(defn- ->document-store [{{:keys [kv-store object-store]} ::event-log} _]
  (->StandaloneDocumentStore kv-store object-store))

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

   ::event-log-object-store {:doc "The object store to use for the standalone event log"
                             :default 'crux.object-store/kv-object-store
                             :crux.config/type :crux.config/module}})

(defrecord EventLog [kv-store object-store]
  Closeable
  (close [_]
    (cio/try-close kv-store)
    (cio/try-close object-store)))

(defn ->event-log [deps {::keys [event-log-kv-store event-log-object-store] :as args}]
  (let [args (-> args
                 (set/rename-keys {::event-log-dir :crux.kv/db-dir
                                   ::event-log-sync? :crux.kv/sync?}))
        kv-store (topo/start-component event-log-kv-store {} args)
        object-store (topo/start-component event-log-object-store {::n/kv-store kv-store} args)]
    (->EventLog kv-store object-store)))

(def topology
  (merge n/base-topology
         {::event-log {:start-fn ->event-log
                       :args event-log-args}
          ::n/tx-log {:start-fn ->tx-log
                      :deps [::n/kv-store ::n/object-store ::n/document-store ::event-log]}
          ::n/document-store {:start-fn ->document-store
                              :deps [::event-log]}}))
