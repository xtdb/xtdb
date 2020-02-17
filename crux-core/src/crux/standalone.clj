(ns crux.standalone
  (:require [clojure.tools.logging :as log]
            [crux.db :as db]
            [crux.kv :as kv]
            [crux.object-store :as os]
            [crux.node :as n]
            [crux.tx :as tx]
            [crux.codec :as c]
            [crux.io :as cio]
            [crux.index :as idx])
  (:import (java.io Closeable)
           (java.util Date)
           (java.util.concurrent ArrayBlockingQueue
                                 ExecutorService Executors TimeUnit
                                 ThreadPoolExecutor ThreadPoolExecutor$DiscardPolicy)))

(defn- index-txs [{:keys [indexer kv-store object-store]}]
  (with-open [snapshot (kv/new-snapshot kv-store)
              iterator (kv/new-iterator snapshot)]
    (let [tx (db/read-index-meta indexer ::tx/latest-completed-tx)
          k (kv/seek iterator (c/encode-tx-event-key-to nil (or tx {::tx/tx-id 0, ::tx/tx-time (Date.)})))
          k (if tx (kv/next iterator) k)]
      (loop [k k]
        (when (c/tx-event-key? k)
          (let [tx-events (os/<-nippy-buffer (kv/value iterator))
                doc-hashes (->> tx-events (into #{} (mapcat tx/tx-event->doc-hashes)))]
            (when-let [missing-docs (not-empty (db/missing-docs indexer doc-hashes))]
              (db/index-docs indexer (->> (db/get-objects object-store snapshot missing-docs)
                                          (into {} (map (juxt (comp c/new-id key) val))))))

            (db/index-tx indexer (c/decode-tx-event-key-from k) tx-events))

          (when-not (Thread/interrupted)
            (recur (kv/next iterator))))))))

(defn- submit-tx [{:keys [!submitted-tx tx-events]}
                  {:keys [^ExecutorService tx-executor ^ExecutorService tx-indexer indexer kv-store] :as deps}]
  (when (.isShutdown tx-executor)
    (deliver !submitted-tx ::closed))

  (let [tx-time (Date.)
        tx-id (inc (or (db/read-index-meta indexer ::latest-submitted-tx-id) -1))
        next-tx {:crux.tx/tx-id tx-id, :crux.tx/tx-time tx-time}]
    (kv/store kv-store [[(c/encode-tx-event-key-to nil next-tx)
                         (os/->nippy-buffer tx-events)]
                        (idx/meta-kv ::latest-submitted-tx-id tx-id)])

    (deliver !submitted-tx next-tx)

    (.submit tx-indexer ^Runnable #(index-txs deps))))

(defrecord StandaloneTxLog [^ExecutorService tx-executor
                            ^ExecutorService tx-indexer
                            indexer kv-store object-store]
  db/TxLog
  (submit-tx [this tx-ops]
    (when (.isShutdown tx-executor)
      (throw (IllegalStateException. "TxLog is closed.")))

    (let [!submitted-tx (promise)]
      (.submit tx-executor ^Runnable #(submit-tx {:!submitted-tx !submitted-tx
                                                  :tx-events (mapv tx/tx-op->tx-event tx-ops)}
                                                 this))
      (delay
        (let [submitted-tx @!submitted-tx]
          (when (= ::closed submitted-tx)
            (throw (IllegalStateException. "TxLog is closed.")))

          submitted-tx))))

  (latest-submitted-tx [this]
    (when-let [tx-id (db/read-index-meta indexer ::latest-submitted-tx-id)]
      {::tx/tx-id tx-id}))

  Closeable
  (close [_]
    (try
      (.shutdown tx-executor)
      (catch Exception e
        (log/warn e "Error shutting down tx-executor")))

    (try
      (.shutdownNow tx-indexer)
      (catch Exception e
        (log/warn e "Error shutting down tx-indexer")))

    (or (.awaitTermination tx-executor 5 TimeUnit/SECONDS)
        (log/warn "waited 5s for tx-executor to exit, no dice."))

    (or (.awaitTermination tx-indexer 5 TimeUnit/SECONDS)
        (log/warn "waited 5s for tx-indexer to exit, no dice."))))

(defn- ->tx-log [{::n/keys [indexer kv-store object-store]}]
  ;; TODO replay log if we restart the node
  (->StandaloneTxLog (Executors/newSingleThreadExecutor (cio/thread-factory "standalone-tx-log"))
                     (ThreadPoolExecutor. 1 1
                                          100 TimeUnit/MILLISECONDS
                                          (ArrayBlockingQueue. 1)
                                          (cio/thread-factory "standalone-tx-indexer")
                                          (ThreadPoolExecutor$DiscardPolicy.))
                     indexer kv-store object-store))

(defrecord StandaloneDocumentStore [kv-store object-store]
  db/DocumentStore
  (submit-docs [this id-and-docs]
    (db/put-objects object-store id-and-docs))

  (fetch-docs [this ids]
    (with-open [snapshot (kv/new-snapshot kv-store)]
      (db/get-objects object-store snapshot ids))))

(defn- ->document-store [{::n/keys [kv-store object-store]}]
  (->StandaloneDocumentStore kv-store object-store))

(def topology
  (merge n/base-topology
         {:crux.node/tx-log {:start-fn (fn [deps args] (->tx-log deps))
                             :deps [::n/indexer ::n/kv-store ::n/object-store]}

          :crux.node/document-store {:start-fn (fn [deps args] (->document-store deps))
                                     :deps [::n/kv-store ::n/object-store]}}))
