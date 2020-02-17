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
  (:import (crux.api ITxLog)
           (java.io Closeable)
           (java.util Date)
           (java.util.concurrent ArrayBlockingQueue
                                 ExecutorService Executors TimeUnit
                                 ThreadPoolExecutor ThreadPoolExecutor$DiscardPolicy)))

(defn- open-tx-log ^crux.api.ITxLog [{:keys [indexer kv-store object-store]} from-tx-id]
  (let [snapshot (kv/new-snapshot kv-store)
        iterator (kv/new-iterator snapshot)]
    (letfn [(tx-log [k]
              (lazy-seq
                (when (some-> k c/tx-event-key?)
                  (cons (assoc (c/decode-tx-event-key-from k)
                          :crux.tx.event/tx-events (os/<-nippy-buffer (kv/value iterator)))
                        (tx-log (kv/next iterator))))))]

      (let [k (kv/seek iterator (c/encode-tx-event-key-to nil {::tx/tx-id (or from-tx-id 0)}))]
        (->> (when k (tx-log (if from-tx-id (kv/next iterator) k)))
             (db/->closeable-tx-log-iterator (fn []
                                               (cio/try-close iterator)
                                               (cio/try-close snapshot))))))))

(defn- index-txs [{:keys [indexer kv-store object-store] :as deps}]
  (with-open [tx-log (open-tx-log deps (::tx/tx-id (db/read-index-meta indexer ::tx/latest-completed-tx)))
              snapshot (kv/new-snapshot kv-store)]
    (->> (iterator-seq tx-log)
         (run! (fn [{:keys [crux.tx.event/tx-events] :as tx}]
                 (let [doc-hashes (->> tx-events (into #{} (mapcat tx/tx-event->doc-hashes)))]
                   (when-let [missing-docs (not-empty (db/missing-docs indexer doc-hashes))]
                     (db/index-docs indexer (->> (db/get-objects object-store snapshot missing-docs)
                                                 (into {} (map (juxt (comp c/new-id key) val))))))

                   (db/index-tx indexer (select-keys tx [::tx/tx-time ::tx/tx-id]) tx-events)

                   (when (Thread/interrupted)
                     (reduced nil))))))))

(defn- submit-tx [{:keys [!submitted-tx tx-events]}
                  {:keys [^ExecutorService tx-submit-executor ^ExecutorService tx-indexer-executor indexer kv-store] :as deps}]
  (when (.isShutdown tx-submit-executor)
    (deliver !submitted-tx ::closed))

  (let [tx-time (Date.)
        tx-id (inc (or (db/read-index-meta indexer ::latest-submitted-tx-id) -1))
        next-tx {:crux.tx/tx-id tx-id, :crux.tx/tx-time tx-time}]
    (kv/store kv-store [[(c/encode-tx-event-key-to nil next-tx)
                         (os/->nippy-buffer tx-events)]
                        (idx/meta-kv ::latest-submitted-tx-id tx-id)])

    (deliver !submitted-tx next-tx)

    (.submit tx-indexer-executor ^Runnable #(index-txs deps))))

(defrecord StandaloneTxLog [^ExecutorService tx-submit-executor
                            ^ExecutorService tx-indexer-executor
                            indexer kv-store object-store]
  db/TxLog
  (submit-tx [this tx-ops]
    (when (.isShutdown tx-submit-executor)
      (throw (IllegalStateException. "TxLog is closed.")))

    (let [!submitted-tx (promise)]
      (.submit tx-submit-executor
               ^Runnable #(submit-tx {:!submitted-tx !submitted-tx
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

  (open-tx-log [this from-tx-id]
    (open-tx-log this from-tx-id))

  Closeable
  (close [_]
    (try
      (.shutdown tx-submit-executor)
      (catch Exception e
        (log/warn e "Error shutting down tx-submit-executor")))

    (try
      (.shutdownNow tx-indexer-executor)
      (catch Exception e
        (log/warn e "Error shutting down tx-indexer-executor")))

    (or (.awaitTermination tx-submit-executor 5 TimeUnit/SECONDS)
        (log/warn "waited 5s for tx-submit-executor to exit, no dice."))

    (or (.awaitTermination tx-indexer-executor 5 TimeUnit/SECONDS)
        (log/warn "waited 5s for tx-indexer-executor to exit, no dice."))))

(defn- ->tx-log [{::n/keys [indexer kv-store object-store]}]
  (let [tx-submit-executor (Executors/newSingleThreadExecutor (cio/thread-factory "standalone-tx-log"))
        tx-indexer-executor (ThreadPoolExecutor. 1 1
                                                 100 TimeUnit/MILLISECONDS
                                                 (ArrayBlockingQueue. 1)
                                                 (cio/thread-factory "standalone-tx-indexer")
                                                 (ThreadPoolExecutor$DiscardPolicy.))
        tx-log (->StandaloneTxLog tx-submit-executor tx-indexer-executor indexer kv-store object-store)]

    ;; when we restart the standalone node, we want it to start indexing straightaway if it's behind
    (.submit tx-indexer-executor ^Runnable #(index-txs tx-log))

    tx-log))

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
