(ns xtdb.log.processor
  (:require [integrant.core :as ig]
            [xtdb.util :as util])
  (:import [xtdb.api IndexerConfig]
           [xtdb.api.log Log]
           [xtdb.database Database$Mode DatabaseState DatabaseStorage]
           [xtdb.indexer BlockFinisher FollowerLogProcessor LeaderLogProcessor LogProcessor LogProcessor$LeaderSystem LogProcessor$ProcessorFactory SourceLogProcessor TransitionLogProcessor]
           [xtdb.util MsgIdUtil]))

(defn- open-source-processor [{:keys [allocator ^DatabaseStorage db-storage ^DatabaseState db-state
                                      indexer-for-db compactor-for-db watchers db-catalog
                                      ^IndexerConfig indexer-conf block-flush-duration]
                               {:keys [meter-registry]} :base}
                              read-only?]
  (SourceLogProcessor. allocator meter-registry
                       db-storage db-state
                       indexer-for-db (.getLiveIndex db-state)
                       watchers compactor-for-db (set (.getSkipTxs indexer-conf))
                       read-only?
                       db-catalog
                       block-flush-duration))

(defn- subscribe-source-log [^DatabaseStorage db-storage ^DatabaseState db-state ^SourceLogProcessor src-proc]
  (let [source-log (.getSourceLog db-storage)
        latest-processed-msg-id (or (.getLatestProcessedMsgId (.getBlockCatalog db-state)) -1)]
    (Log/tailAll source-log latest-processed-msg-id src-proc)))

(defmethod ig/expand-key :xtdb.log.processor/source [k opts]
  {k (into {:allocator (ig/ref :xtdb.db-catalog/allocator)
            :buffer-pool (ig/ref :xtdb/buffer-pool)
            :db-storage (ig/ref :xtdb.db-catalog/storage)
            :db-state (ig/ref :xtdb.db-catalog/state)
            :watchers (ig/ref :xtdb.db-catalog/watchers)
            :compactor-for-db (ig/ref :xtdb.compactor/for-db)
            :indexer-for-db (ig/ref :xtdb.indexer/for-db)}
           opts)})

(defmethod ig/init-key :xtdb.log.processor/source [_ {:keys [^IndexerConfig indexer-conf ^Database$Mode mode] :as opts}]
  (when (.getEnabled indexer-conf)
    (let [read-only? (= mode Database$Mode/READ_ONLY)
          src-proc (open-source-processor opts read-only?)
          subscription (subscribe-source-log (:db-storage opts) (:db-state opts) src-proc)]
      {:source-processor src-proc, :subscription subscription})))

(defmethod ig/halt-key! :xtdb.log.processor/source [_ {:keys [subscription source-processor]}]
  (util/close subscription)
  (util/close source-processor))

(defmethod ig/expand-key :xtdb.log.processor/block-finisher [k opts]
  {k (into {:db-storage (ig/ref :xtdb.db-catalog/storage)
            :db-state (ig/ref :xtdb.db-catalog/state)
            :compactor-for-db (ig/ref :xtdb.compactor/for-db)}
           opts)})

(defmethod ig/init-key :xtdb.log.processor/block-finisher [_ {:keys [^DatabaseStorage db-storage ^DatabaseState db-state
                                                                     compactor-for-db db-catalog]}]
  (BlockFinisher. db-storage db-state compactor-for-db db-catalog))

(defmethod ig/expand-key :xtdb.log/processor [k opts]
  {k (into {:allocator (ig/ref :xtdb.db-catalog/allocator)
            :buffer-pool (ig/ref :xtdb/buffer-pool)
            :db-storage (ig/ref :xtdb.db-catalog/storage)
            :db-state (ig/ref :xtdb.db-catalog/state)
            :source-watchers (ig/ref :xtdb.db-catalog/watchers)
            :compactor-for-db (ig/ref :xtdb.compactor/for-db)
            :indexer-for-db (ig/ref :xtdb.indexer/for-db)
            :block-finisher (ig/ref :xtdb.log.processor/block-finisher)}
           opts)})

(defmethod ig/init-key :xtdb.log/processor [_ {:keys [allocator ^DatabaseStorage db-storage ^DatabaseState db-state
                                                      ^IndexerConfig indexer-conf ^Database$Mode mode
                                                      compactor-for-db source-watchers db-catalog
                                                      buffer-pool indexer-for-db ^BlockFinisher block-finisher]
                                               {:keys [meter-registry]} :base}]
  (when (.getEnabled indexer-conf)
    (let [proc-factory (reify LogProcessor$ProcessorFactory
                         (openFollower [_ replica-watchers pending-block]
                           (FollowerLogProcessor. allocator buffer-pool db-state compactor-for-db
                                                  source-watchers replica-watchers
                                                  db-catalog pending-block))

                         (openLeaderSystem [_ replica-producer replica-watchers after-msg-id]
                           (util/with-close-on-catch [proc (LeaderLogProcessor. allocator
                                                                                db-storage replica-producer db-state
                                                                                indexer-for-db source-watchers replica-watchers
                                                                                (set (.getSkipTxs indexer-conf))
                                                                                db-catalog block-finisher
                                                                                (.getFlushDuration indexer-conf)
                                                                                meter-registry)
                                                      sub (Log/tailAll (.getSourceLog db-storage) after-msg-id proc)]
                             (reify LogProcessor$LeaderSystem
                               (getPendingBlock [_] (.getPendingBlock proc))
                               (close [_]
                                 (.close sub)
                                 (.close proc)))))

                         (openTransition [_ replica-producer replica-watchers]
                           (TransitionLogProcessor. allocator
                                                    buffer-pool db-state
                                                    (.getLiveIndex db-state)
                                                    block-finisher
                                                    replica-producer
                                                    source-watchers replica-watchers db-catalog)))

          log-processor (LogProcessor. proc-factory db-storage db-state source-watchers block-finisher meter-registry)]

      {:log-processor log-processor
       :consumer (when-not (= mode Database$Mode/READ_ONLY)
                   (.openGroupConsumer (.getSourceLog db-storage) log-processor))})))

(defmethod ig/halt-key! :xtdb.log/processor [_ {:keys [consumer log-processor]}]
  (util/close consumer)
  (util/close log-processor))
