(ns xtdb.indexer.live-index
  (:require [clojure.set :as set]
            [clojure.tools.logging :as log]
            [integrant.core :as ig]
            [xtdb.buffer-pool]
            [xtdb.db-catalog :as db]
            [xtdb.metrics :as metrics]
            [xtdb.table :as table]
            [xtdb.table-catalog :as table-cat]
            [xtdb.trie :as trie]
            [xtdb.trie-catalog :as trie-cat]
            [xtdb.util :as util])
  (:import (clojure.lang MapEntry)
           (java.lang AutoCloseable)
           (java.time Duration)
           (java.util HashMap List Map)
           (java.util.concurrent StructuredTaskScope$ShutdownOnFailure StructuredTaskScope$Subtask)
           (java.util.concurrent.locks StampedLock)
           (org.apache.arrow.memory BufferAllocator)
           (xtdb.api IndexerConfig TransactionKey)
           (xtdb.api.log Log Log$Message$TriesAdded)
           xtdb.api.storage.Storage
           xtdb.BufferPool
           (xtdb.catalog BlockCatalog TableCatalog)
           (xtdb.indexer LiveIndex$Snapshot LiveIndex$Tx LiveTable LiveTable$Snapshot LiveTable$Tx Snapshot Snapshot)
           (xtdb.log.proto TrieDetails)
           (xtdb.trie TrieCatalog)
           (xtdb.util RefCounter RowCounter)))

(defn open-live-idx-snap [^Map tables]
  (util/with-close-on-catch [snaps (HashMap.)]

    (doseq [[table ^LiveTable live-table] tables]
      (.put snaps table (.openSnapshot live-table)))

    (reify LiveIndex$Snapshot
      (getAllColumnFields [_] (update-vals snaps #(.getColumnFields ^LiveTable$Snapshot %)))

      (liveTable [_ table] (.get snaps table))

      (getLiveTables [_] (keys snaps))

      AutoCloseable
      (close [_] (util/close snaps)))))

(defn ->schema [^LiveIndex$Snapshot live-index-snap, ^TableCatalog table-catalog]
  (merge-with set/union
              (update-vals (.getFields table-catalog)
                           (comp set keys))
              (update-vals (some-> live-index-snap (.getAllColumnFields))
                           (comp set keys))))

(deftype LiveIndex [^BufferAllocator allocator, ^BufferPool buffer-pool, ^Log log
                    ^BlockCatalog block-cat, table-cat, ^TrieCatalog trie-cat

                    ^:volatile-mutable ^TransactionKey latest-completed-tx
                    ^Map tables,

                    ^:volatile-mutable ^Snapshot shared-snap
                    ^StampedLock snap-lock
                    ^RefCounter snap-ref-counter,

                    ^RowCounter row-counter, ^long rows-per-block

                    ^long log-limit, ^long page-limit
                    ^List skip-txs]
  xtdb.indexer.LiveIndex
  (getLatestCompletedTx [_] latest-completed-tx)

  (liveTable [_ table] (.get tables table))

  (startTx [this-idx tx-key]
    (let [table-txs (HashMap.)]
      (reify LiveIndex$Tx
        (liveTable [_ table]
          (.computeIfAbsent table-txs table
                            (fn [table]
                              (let [live-table (.liveTable this-idx table)
                                    new-live-table? (nil? live-table)
                                    ^LiveTable live-table (or live-table
                                                              (LiveTable. allocator buffer-pool table row-counter
                                                                          (partial trie/->live-trie log-limit page-limit)))]

                                (.startTx live-table tx-key new-live-table?)))))

        (commit [_]
          (let [snap-lock-stamp (.writeLock snap-lock)]
            (try
              (doseq [[table ^LiveTable$Tx live-table-tx] table-txs]
                (.put tables table (.commit live-table-tx)))

              (set! (.-latest-completed-tx this-idx) tx-key)

              (let [^Snapshot old-snap (.shared-snap this-idx)
                    ^Snapshot shared-snap (util/with-close-on-catch [live-index-snap (open-live-idx-snap tables)]
                                            (Snapshot. tx-key live-index-snap (->schema live-index-snap table-cat)))]
                (set! (.shared-snap this-idx) shared-snap)
                (some-> old-snap .close))

              (finally
                (.unlock snap-lock snap-lock-stamp)))))

        (abort [_]
          (doseq [^LiveTable$Tx live-table-tx (.values table-txs)]
            (.abort live-table-tx))

          (set! (.-latest-completed-tx this-idx) tx-key))

        (openSnapshot [_]
          (util/with-close-on-catch [snaps (HashMap.)]
            (doseq [[table ^LiveTable$Tx live-table-tx] table-txs]
              (.put snaps table (.openSnapshot live-table-tx)))

            (doseq [[table ^LiveTable live-table] tables]
              (.computeIfAbsent snaps table (fn [_] (.openSnapshot live-table))))

            (reify LiveIndex$Snapshot
              (getAllColumnFields [_] (update-vals snaps #(.getColumnFields ^LiveTable$Snapshot %)))
              (liveTable [_ table] (.get snaps table))
              (getLiveTables [_] (keys snaps))

              AutoCloseable
              (close [_] (util/close snaps)))))

        AutoCloseable
        (close [_]))))

  (openSnapshot [this]
    (let [snap-read-stamp (.readLock snap-lock)]
      (try
        (doto ^Snapshot (.-shared-snap this) .retain)
        (finally
          (.unlock snap-lock snap-read-stamp)))))

  (isFull [_]
    (>= (.getBlockRowCount row-counter) rows-per-block))

  (finishBlock [_ block-idx]
    (with-open [scope (StructuredTaskScope$ShutdownOnFailure.)]
      (let [tasks (vec (for [[table ^LiveTable live-table] tables]
                         (.fork scope (fn []
                                        (try
                                          (when-let [finished-block (.finishBlock live-table block-idx)]
                                            [table {:fields (.getFields finished-block)
                                                    :trie-key (.getTrieKey finished-block)
                                                    :row-count (.getRowCount finished-block)
                                                    :data-file-size (.getDataFileSize finished-block)
                                                    :trie-metadata (.getTrieMetadata finished-block)
                                                    :hlls (.getHllDeltas finished-block)}])
                                          (catch InterruptedException e
                                            (throw e))
                                          (catch Exception e
                                            (log/warn e "Error finishing block for table" live-table)
                                            (throw e)))))))]
        (.join scope)

        (let [table-metadata (-> tasks
                                 (->> (into {} (keep #(try
                                                        (.get ^StructuredTaskScope$Subtask %)
                                                        (catch Exception _
                                                          (throw (.exception ^StructuredTaskScope$Subtask %)))))))
                                 (util/rethrowing-cause))]
          (let [added-tries (for [[table {:keys [trie-key data-file-size trie-metadata state]}] table-metadata]
                              (trie/->trie-details table trie-key data-file-size trie-metadata state))]
            (.appendMessage log (Log$Message$TriesAdded. Storage/VERSION added-tries))
            (doseq [^TrieDetails added-trie added-tries]
              (.addTries trie-cat (table/->ref (.getTableName added-trie)) [added-trie] (.getSystemTime latest-completed-tx))))

          (let [all-tables (set (concat (keys table-metadata) (.getAllTables block-cat)))
                table->all-tries (->> all-tables
                                      (map (fn [table]
                                             (MapEntry/create table (->> (trie-cat/trie-state trie-cat table)
                                                                         trie-cat/all-tries))))
                                      (into {}))]
            (table-cat/finish-block! table-cat block-idx table-metadata table->all-tries))))))

  (nextBlock [this]
    (.nextBlock row-counter)

    (let [snap-lock-stamp (.writeLock snap-lock)]
      (try
        (let [^Snapshot shared-snap (.shared-snap this)]
          (.close shared-snap))

        (util/close tables)
        (.clear tables)
        (set! (.shared-snap this)
              (util/with-close-on-catch [live-index-snap (open-live-idx-snap tables)]
                (Snapshot. latest-completed-tx
                           live-index-snap
                           (->schema live-index-snap table-cat))))
        (finally
          (.unlock snap-lock snap-lock-stamp))))

    (when (and (not-empty skip-txs) (>= (:tx-id latest-completed-tx) (last skip-txs)))
      #_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
      (defonce -log-skip-txs-once
        (log/info "All XTDB_SKIP_TXS have been skipped and block has been finished - it is safe to remove the XTDB_SKIP_TXS environment variable."))))

  AutoCloseable
  (close [_]
    (some-> shared-snap .close)
    (util/close tables)
    (if-not (.tryClose snap-ref-counter (Duration/ofMinutes 1))
      (log/warn "Failed to shut down live-index after 60s due to outstanding watermarks.")
      (util/close allocator))))

(defmethod ig/prep-key :xtdb.indexer/live-index [_ {:keys [base, ^IndexerConfig indexer-conf]}]
  {:base base

   :allocator (ig/ref :xtdb.db-catalog/allocator)
   :buffer-pool (ig/ref :xtdb/buffer-pool)
   :block-cat (ig/ref :xtdb/block-catalog)
   :table-cat (ig/ref :xtdb/table-catalog)
   :log (ig/ref :xtdb/log)
   :trie-cat (ig/ref :xtdb/trie-catalog)

   :rows-per-block (.getRowsPerBlock indexer-conf)
   :log-limit (.getLogLimit indexer-conf)
   :page-limit (.getPageLimit indexer-conf)
   :skip-txs (.getSkipTxs indexer-conf)})

(defmethod ig/init-key :xtdb.indexer/live-index [_ {{:keys [meter-registry]} :base,
                                                    :keys [allocator, ^BlockCatalog block-cat, buffer-pool log trie-cat table-cat
                                                           ^long rows-per-block, ^long log-limit, ^long page-limit, skip-txs]}]
  (let [latest-completed-tx (.getLatestCompletedTx block-cat)]
    (util/with-close-on-catch [allocator (util/->child-allocator allocator "live-index")]
      (metrics/add-allocator-gauge meter-registry "live-index.allocator.allocated_memory" allocator)
      (let [tables (HashMap.)]
        (->LiveIndex allocator buffer-pool log
                     block-cat table-cat trie-cat
                     latest-completed-tx
                     tables

                     (Snapshot. latest-completed-tx (open-live-idx-snap tables) (->schema nil table-cat))
                     (StampedLock.)
                     (RefCounter.)

                     (RowCounter.) rows-per-block

                     log-limit page-limit skip-txs)))))

(defmethod ig/halt-key! :xtdb.indexer/live-index [_ live-idx]
  (util/close live-idx))

