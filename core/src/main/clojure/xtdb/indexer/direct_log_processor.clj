(ns xtdb.indexer.direct-log-processor
  (:require [integrant.core :as ig]
            [xtdb.util :as util])
  (:import [xtdb.api IndexerConfig]
           [xtdb.database Database$Mode DatabaseState DatabaseStorage]
           [xtdb.indexer ReplicaLogProcessor SourceLogProcessor]
           [xtdb.util MsgIdUtil]))

;; Single integrant sub-system combining source and replica log processing.
;; Replaces the separate :xtdb.indexer/source-log and :xtdb.indexer/replica-log sub-systems.

(defn- log-processor-system [{:keys [indexer-conf mode tx-source-conf db-catalog db-state watchers
                                     block-flush-duration compactor-for-db tx-source-for-db]
                              :as opts}]
  (let [child-opts (-> (dissoc opts :indexer-conf :mode :tx-source-conf :db-catalog :db-state :watchers
                               :block-flush-duration :compactor-for-db :tx-source-for-db)
                       (assoc :storage (:db-storage opts)
                              :db-state db-state
                              :state db-state
                              :live-index (.getLiveIndex ^DatabaseState db-state)
                              :watchers watchers))]
    (-> {:xtdb.indexer/crash-logger child-opts
         :xtdb.indexer/for-db child-opts
         :xtdb.log/processor (cond-> (assoc child-opts
                                            :indexer-conf indexer-conf :mode mode :tx-source-conf tx-source-conf
                                            :compactor compactor-for-db
                                            :tx-source tx-source-for-db)
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
       :subscription (let [source-log (.getSourceLog db-storage)
                           epoch (.getEpoch source-log)
                           latest-processed-msg-id (.getLatestProcessedMsgId (.getBlockCatalog db-state))
                           latest-offset (if latest-processed-msg-id
                                           (if (= (MsgIdUtil/msgIdToEpoch latest-processed-msg-id) epoch)
                                             (MsgIdUtil/msgIdToOffset latest-processed-msg-id)
                                             -1)
                                           -1)]
                       (.tailAll source-log src-proc latest-offset))})))

(defmethod ig/halt-key! ::source-processor [_ {:keys [subscription]}]
  (util/close subscription))

(defmethod ig/expand-key :xtdb.indexer/direct-log-processor [k opts]
  {k (into {:allocator (ig/ref :xtdb.db-catalog/allocator)
            :buffer-pool (ig/ref :xtdb/buffer-pool)
            :db-storage (ig/ref :xtdb.db-catalog/storage)
            :db-state (ig/ref :xtdb.db-catalog/state)
            :watchers (ig/ref :xtdb.db-catalog/watchers)
            :compactor-for-db (ig/ref :xtdb.compactor/for-db)
            :tx-source-for-db (ig/ref :xtdb.tx-source/for-db)}
           opts)})

(defmethod ig/init-key :xtdb.indexer/direct-log-processor [_ opts]
  (-> (log-processor-system opts) ig/expand ig/init))

(defmethod ig/halt-key! :xtdb.indexer/direct-log-processor [_ sys]
  (ig/halt! sys))
