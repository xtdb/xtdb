(ns xtdb.log.processor
  (:require [integrant.core :as ig]
            [xtdb.util :as util])
  (:import [xtdb.api IndexerConfig]
           [xtdb.database Database$Mode DatabaseState DatabaseStorage]
           [xtdb.indexer LogProcessor LogProcessor$System LogProcessor$SystemFactory
                         SourceLogProcessor]
           [xtdb.util MsgIdUtil]))

(defn- open-source-processor [{:keys [allocator ^DatabaseStorage db-storage ^DatabaseState db-state
                                      indexer-for-db compactor-for-db tx-source-for-db watchers db-catalog
                                      ^IndexerConfig indexer-conf block-flush-duration]
                                {:keys [meter-registry]} :base}
                              read-only?]
  (SourceLogProcessor. allocator meter-registry
                       db-storage db-state
                       indexer-for-db (.getLiveIndex db-state)
                       watchers compactor-for-db (set (.getSkipTxs indexer-conf))
                       read-only?
                       db-catalog
                       tx-source-for-db
                       block-flush-duration))

(defn- subscribe-source-log [^DatabaseStorage db-storage ^DatabaseState db-state ^SourceLogProcessor src-proc]
  (let [source-log (.getSourceLog db-storage)
        epoch (.getEpoch source-log)
        latest-processed-msg-id (.getLatestProcessedMsgId (.getBlockCatalog db-state))
        latest-offset (if latest-processed-msg-id
                        (if (= (MsgIdUtil/msgIdToEpoch latest-processed-msg-id) epoch)
                          (MsgIdUtil/msgIdToOffset latest-processed-msg-id)
                          -1)
                        -1)]
    (.tailAll source-log src-proc latest-offset)))

(defn- open-system [opts read-only?]
  (let [src-proc (open-source-processor opts read-only?)
        subscription (subscribe-source-log (:db-storage opts) (:db-state opts) src-proc)]
    (reify LogProcessor$System
      (close [_]
        (util/close subscription)
        (util/close src-proc)))))

(defmethod ig/expand-key :xtdb.log/processor [k opts]
  {k (into {:allocator (ig/ref :xtdb.db-catalog/allocator)
            :buffer-pool (ig/ref :xtdb/buffer-pool)
            :db-storage (ig/ref :xtdb.db-catalog/storage)
            :db-state (ig/ref :xtdb.db-catalog/state)
            :watchers (ig/ref :xtdb.db-catalog/watchers)
            :compactor-for-db (ig/ref :xtdb.compactor/for-db)
            :tx-source-for-db (ig/ref :xtdb.tx-source/for-db)
            :indexer-for-db (ig/ref :xtdb.indexer/for-db)}
           opts)})

(defmethod ig/init-key :xtdb.log/processor [_ {:keys [^IndexerConfig indexer-conf ^Database$Mode mode] :as opts}]
  (when (.getEnabled indexer-conf)
    (let [factory (reify LogProcessor$SystemFactory
                    (openReadWriteSystem [_] (open-system opts false))
                    (openReadOnlySystem [_] (open-system opts true)))]
      (LogProcessor. factory mode))))

(defmethod ig/halt-key! :xtdb.log/processor [_ processor]
  (util/close processor))
