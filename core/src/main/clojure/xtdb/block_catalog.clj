(ns xtdb.block-catalog
  (:require [integrant.core :as ig]
            [xtdb.db-catalog :as db]
            [xtdb.serde :as serde]
            [xtdb.time :as time])
  (:import (xtdb.block.proto Block TxKey)
           (xtdb.catalog BlockCatalog)))

(defmethod ig/expand-key :xtdb/block-catalog [k opts]
  {k (into {:buffer-pool (ig/ref :xtdb/buffer-pool)}
           opts)})

(defmethod ig/init-key :xtdb/block-catalog [_ {:keys [db-name buffer-pool]}]
  (BlockCatalog. db-name (BlockCatalog/getLatestBlock buffer-pool)))

(defn <-TxKey [^TxKey tx-key]
  (serde/->TxKey (.getTxId tx-key) (time/micros->instant (.getSystemTime tx-key))))

(defn <-Block [^Block block]
  {:block-idx (.getBlockIndex block)
   :latest-completed-tx (<-TxKey (.getLatestCompletedTx block))
   :latest-processed-msg-id (.getLatestProcessedMsgId block)
   :table-names (set (.getTableNamesList block))
   :secondary-dbs (->> (.getSecondaryDatabasesMap block)
                       (into {} (map (juxt key (comp db/<-DatabaseConfig val)))))})
