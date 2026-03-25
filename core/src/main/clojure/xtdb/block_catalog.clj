(ns xtdb.block-catalog
  (:require [integrant.core :as ig]
            [xtdb.db-catalog :as db]
            [xtdb.serde :as serde]
            [xtdb.time :as time])
  (:import (xtdb.block.proto Block TxKey)
           (xtdb.catalog BlockCatalog)
           xtdb.database.DatabaseStorage))

(defmethod ig/expand-key :xtdb/block-catalog [k opts]
  {k (into {:storage (ig/ref :xtdb.db-catalog/storage)}
           opts)})

(defmethod ig/init-key :xtdb/block-catalog [_ {:keys [db-name ^DatabaseStorage storage]}]
  (BlockCatalog. db-name (BlockCatalog/getLatestBlock (.getBufferPool storage))))

(defn <-TxKey [^TxKey tx-key]
  (serde/->TxKey (.getTxId tx-key) (time/micros->instant (.getSystemTime tx-key))))

(defn <-Block [^Block block]
  {:block-idx (.getBlockIndex block)
   :latest-completed-tx (<-TxKey (.getLatestCompletedTx block))
   :latest-processed-msg-id (.getLatestProcessedMsgId block)
   :table-names (set (.getTableNamesList block))
   :secondary-dbs (->> (.getSecondaryDatabasesMap block)
                       (into {} (map (juxt key (comp db/<-DatabaseConfig val)))))})
