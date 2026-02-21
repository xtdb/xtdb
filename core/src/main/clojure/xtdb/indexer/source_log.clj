(ns xtdb.indexer.source-log
  (:require [integrant.core :as ig]
            [xtdb.util :as util])
  (:import [xtdb.api IndexerConfig]
           [xtdb.database Database$Mode DatabaseStorage ReplicaIndexer SourceIndexer]
           [xtdb.indexer SourceLogProcessor]))

;; Nested integrant sub-system that owns the source indexer's state, processor and subscription.
;; The source processor resolves txs using its own Indexer.ForDatabase,
;; then forwards resolved data to the replica processor for import.
;; Also owns block finishing: writes tries, table blocks and block files to storage.

(defn- source-system [{:keys [indexer-conf mode replica-log block-flush-duration db-state] :as opts}]
  (let [child-opts (-> (dissoc opts :indexer-conf :mode :replica-log :block-flush-duration :db-state)
                       (assoc :db-state db-state))]
    (-> {:xtdb.indexer/live-index (assoc child-opts :indexer-conf indexer-conf)
         :xtdb.indexer/crash-logger child-opts
         :xtdb.indexer/for-db child-opts
         ::source-processor (assoc child-opts
                                   :indexer-conf indexer-conf
                                   :mode mode
                                   :replica-indexer replica-log
                                   :block-flush-duration block-flush-duration)
         ::source-indexer child-opts}
        (doto ig/load-namespaces))))

(defmethod ig/expand-key ::source-processor [k opts]
  {k (into {:allocator (ig/ref :xtdb.db-catalog/allocator)
            :db-storage (ig/ref :xtdb.db-catalog/storage)
            :indexer (ig/ref :xtdb.indexer/for-db)
            :live-index (ig/ref :xtdb.indexer/live-index)}
           opts)})

(defmethod ig/init-key ::source-processor [_ {:keys [allocator ^DatabaseStorage db-storage db-state
                                                     indexer live-index
                                                     ^IndexerConfig indexer-conf
                                                     ^Database$Mode mode
                                                     ^ReplicaIndexer replica-indexer
                                                     block-flush-duration]}]
  (when-let [replica-proc (.getLogProcessorOrNull replica-indexer)]
    (let [src-proc (SourceLogProcessor. allocator db-storage db-state
                                        indexer live-index
                                        replica-proc (set (.getSkipTxs indexer-conf))
                                        (= mode Database$Mode/READ_ONLY)
                                        block-flush-duration)]
      {:src-proc src-proc
       :subscription (let [source-log (.getSourceLog db-storage)]
                       (.tailAll source-log src-proc (.getLatestProcessedOffset replica-proc)))})))

(defmethod ig/resolve-key ::source-processor [_ {:keys [src-proc]}]
  src-proc)

(defmethod ig/halt-key! ::source-processor [_ {:keys [subscription]}]
  (util/close subscription))

(defmethod ig/expand-key ::source-indexer [k opts]
  {k (into {:live-index (ig/ref :xtdb.indexer/live-index)} opts)})

(defmethod ig/init-key ::source-indexer [_ {:keys [db-state live-index]}]
  (SourceIndexer. db-state live-index))

(defmethod ig/expand-key :xtdb.indexer/source-log [k opts]
  {k (into {:allocator (ig/ref :xtdb.db-catalog/allocator)
            :buffer-pool (ig/ref :xtdb/buffer-pool)
            :db-storage (ig/ref :xtdb.db-catalog/storage)}
           opts)})

(defmethod ig/init-key :xtdb.indexer/source-log [_ opts]
  (-> (source-system opts) ig/expand ig/init))

(defmethod ig/resolve-key :xtdb.indexer/source-log [_ {::keys [source-indexer]}]
  source-indexer)

(defmethod ig/halt-key! :xtdb.indexer/source-log [_ sys]
  (ig/halt! sys))
