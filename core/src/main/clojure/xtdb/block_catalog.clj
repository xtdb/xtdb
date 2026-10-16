(ns xtdb.block-catalog
  (:require [xtdb.db-catalog :as db]
            [xtdb.time :as time])
  (:import (xtdb.block.proto Block TableEntry TxKey)
           (xtdb.catalog TableCatalogKt)))

(defn <-TxKey [^TxKey tx-key]
  (TableCatalogKt/toTransactionKey tx-key))

(defn <-Block [^Block block]
  (cond-> {:block-idx (.getBlockIndex block)
           :latest-completed-tx (<-TxKey (.getLatestCompletedTx block))
           :latest-processed-msg-id (.getLatestProcessedMsgId block)
           :table-names (set (.getTableNamesList block))
           :tables (->> (.getTablesList block)
                        (into [] (map (fn [^TableEntry entry]
                                        {:oid (.getOid entry)
                                         :schema-name (.getSchemaName entry)
                                         :table-name (.getTableName entry)
                                         :slug (.getSlug entry)}))))
           :secondary-dbs (->> (.getSecondaryDatabasesMap block)
                               (into {} (map (juxt key (comp db/<-DatabaseConfig val)))))}

    (.hasLatestSystemTime block) (assoc :latest-system-time (time/micros->instant (.getLatestSystemTime block)))))
