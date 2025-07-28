(ns xtdb.block-catalog
  (:require [integrant.core :as ig]
            [xtdb.db-catalog :as db]
            [xtdb.serde :as serde]
            [xtdb.time :as time])
  (:import (xtdb.block.proto Block TxKey)
           xtdb.catalog.BlockCatalog))

(defmethod ig/prep-key :xtdb/block-catalog [_ {:keys [db-name base]}]
  {:db-cat (when (= db-name "xtdb")
             (:db-cat base))
   :db-name db-name
   :buffer-pool (ig/ref :xtdb/buffer-pool)})

(defmethod ig/init-key :xtdb/block-catalog [_ {:keys [db-name db-cat buffer-pool]}]
  (BlockCatalog. db-name db-cat buffer-pool))

(defn- <-TxKey [^TxKey tx-key]
  (serde/->TxKey (.getTxId tx-key) (time/micros->instant (.getSystemTime tx-key))))

(defn <-Block [^Block block]
  {:block-idx (.getBlockIndex block)
   :latest-completed-tx (<-TxKey (.getLatestCompletedTx block))
   :latest-processed-msg-id (.getLatestProcessedMsgId block)
   :table-names (set (.getTableNamesList block))
   :databases (->> (for [db-conf (.getDatabasesList block)
                         :let [{:keys [db-name] :as db} (db/<-DatabaseConfig db-conf)]]
                     [db-name db])
                   (into {}))})
