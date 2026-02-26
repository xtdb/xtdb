(ns xtdb.indexer.direct-log-processor
  (:require [integrant.core :as ig]
            [xtdb.util :as util])
  (:import [xtdb.api IndexerConfig]
           [xtdb.database Database$Mode DatabaseState DatabaseStorage]
           [xtdb.indexer DirectLogProcessor ReplicaLogProcessor SourceLogProcessor]))

;; Single integrant sub-system combining source and replica log processing.
;; Replaces the separate :xtdb.indexer/source-log and :xtdb.indexer/replica-log sub-systems.

(defn- log-processor-system [{:keys [indexer-conf mode tx-source-conf db-catalog db-state watchers block-flush-duration]
                              :as opts}]
  (let [child-opts (-> (dissoc opts :indexer-conf :mode :tx-source-conf :db-catalog :db-state :watchers
                               :block-flush-duration)
                       (assoc :storage (:db-storage opts)
                              :db-state db-state
                              :state db-state
                              :live-index (.getLiveIndex ^DatabaseState db-state)
                              :watchers watchers))]
    (-> {:xtdb.indexer/crash-logger child-opts
         :xtdb.indexer/for-db child-opts
         :xtdb.tx-source/for-db (assoc child-opts :tx-source-conf tx-source-conf)
         :xtdb.compactor/for-db (assoc child-opts :mode mode)
         :xtdb.log/processor (cond-> (assoc child-opts :indexer-conf indexer-conf :mode mode :tx-source-conf tx-source-conf)
                               db-catalog (assoc :db-catalog db-catalog))
         ::source-processor (assoc child-opts
                                   :indexer-conf indexer-conf
                                   :mode mode
                                   :block-flush-duration block-flush-duration)}
        (doto ig/load-namespaces))))

(defmethod ig/expand-key ::source-processor [k opts]
  {k (into {:allocator (ig/ref :xtdb.db-catalog/allocator)
            :db-storage (ig/ref :xtdb.db-catalog/storage)
            :indexer (ig/ref :xtdb.indexer/for-db)
            :replica-processor (ig/ref :xtdb.log/processor)}
           opts)})

(defmethod ig/init-key ::source-processor [_ {:keys [allocator ^DatabaseStorage db-storage db-state
                                                     indexer live-index
                                                     ^IndexerConfig indexer-conf
                                                     ^Database$Mode mode
                                                     ^ReplicaLogProcessor replica-processor
                                                     block-flush-duration]}]
  (when replica-processor
    (let [src-proc (SourceLogProcessor. allocator db-storage db-state
                                        indexer live-index
                                        replica-processor (set (.getSkipTxs indexer-conf))
                                        (= mode Database$Mode/READ_ONLY)
                                        block-flush-duration)]
      {:src-proc src-proc
       :subscription (let [source-log (.getSourceLog db-storage)]
                       (.tailAll source-log src-proc (.getLatestProcessedOffset replica-processor)))})))

(defmethod ig/resolve-key ::source-processor [_ {:keys [src-proc]}]
  src-proc)

(defmethod ig/halt-key! ::source-processor [_ {:keys [subscription]}]
  (util/close subscription))

(defmethod ig/expand-key :xtdb.indexer/direct-log-processor [k opts]
  {k (into {:allocator (ig/ref :xtdb.db-catalog/allocator)
            :buffer-pool (ig/ref :xtdb/buffer-pool)
            :db-storage (ig/ref :xtdb.db-catalog/storage)}
           opts)})

(defmethod ig/init-key :xtdb.indexer/direct-log-processor [_ opts]
  (-> (log-processor-system opts) ig/expand ig/init))

(defmethod ig/resolve-key :xtdb.indexer/direct-log-processor [_ sys]
  (when-let [replica-proc (:xtdb.log/processor sys)]
    (DirectLogProcessor. replica-proc
                         (:xtdb.compactor/for-db sys)
                         (:xtdb.tx-source/for-db sys))))

(defmethod ig/halt-key! :xtdb.indexer/direct-log-processor [_ sys]
  (ig/halt! sys))
