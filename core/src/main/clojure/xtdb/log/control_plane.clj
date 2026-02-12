(ns xtdb.log.control-plane
  (:require [integrant.core :as ig]
            [xtdb.util :as util])
  (:import [xtdb.database DatabaseState DatabaseStorage]
           xtdb.indexer.ControlPlaneConsumer))

(defmethod ig/expand-key :xtdb.log/control-plane [k {:keys [base]}]
  {k {:base base
      :storage (ig/ref :xtdb.db-catalog/storage)
      :state (ig/ref :xtdb.db-catalog/state)}})

(defmethod ig/init-key :xtdb.log/control-plane
  [_ {{:keys [db-catalog]} :base
      :keys [^DatabaseStorage storage ^DatabaseState state]}]
  (let [source-log (.getSourceLog storage)
        block-cat (.getBlockCatalog state)
        cp (ControlPlaneConsumer. db-catalog (.getEpoch source-log)
                                 (or (.getLatestProcessedMsgId block-cat) -1))]
    {:consumer cp
     :subscription (.tailAll source-log cp (.getLatestProcessedOffset cp))}))

(defmethod ig/resolve-key :xtdb.log/control-plane [_ {:keys [consumer]}]
  consumer)

(defmethod ig/halt-key! :xtdb.log/control-plane [_ {:keys [consumer subscription]}]
  (util/close subscription)
  (util/close consumer))
