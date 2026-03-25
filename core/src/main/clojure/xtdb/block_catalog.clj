(ns xtdb.block-catalog
  (:require [xtdb.db-catalog :as db]
            [xtdb.serde :as serde]
            [xtdb.time :as time])
  (:import (xtdb.block.proto Block TxKey)))

(defn <-TxKey [^TxKey tx-key]
  (serde/->TxKey (.getTxId tx-key) (time/micros->instant (.getSystemTime tx-key))))

(defn <-Block [^Block block]
  {:block-idx (.getBlockIndex block)
   :latest-completed-tx (<-TxKey (.getLatestCompletedTx block))
   :latest-processed-msg-id (.getLatestProcessedMsgId block)
   :table-names (set (.getTableNamesList block))
   :secondary-dbs (->> (.getSecondaryDatabasesMap block)
                       (into {} (map (juxt key (comp db/<-DatabaseConfig val)))))})
