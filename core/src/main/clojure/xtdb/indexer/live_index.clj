(ns xtdb.indexer.live-index
  (:require [clojure.set :as set]
            [clojure.tools.logging :as log]
            [integrant.core :as ig]
            [xtdb.buffer-pool]
            [xtdb.compactor :as c]
            [xtdb.metrics :as metrics]
            [xtdb.serde :as serde]
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
           (java.util.function Function)
           (org.apache.arrow.memory BufferAllocator)
           (xtdb.api IndexerConfig TransactionKey)
           (xtdb.api.log Log Log$Message$TriesAdded)
           xtdb.BufferPool
           xtdb.catalog.BlockCatalog
           (xtdb.indexer LiveIndex$Tx LiveIndex$Watermark LiveTable LiveTable$Tx LiveTable$Watermark Watermark)
           (xtdb.trie TrieCatalog)
           (xtdb.util RefCounter RowCounter)))

(defn open-live-idx-wm [^Map tables]
  (util/with-close-on-catch [wms (HashMap.)]

    (doseq [[table-name ^LiveTable live-table] tables]
      (.put wms table-name (.openWatermark live-table)))

    (reify LiveIndex$Watermark
      (getAllColumnFields [_] (update-vals wms #(.getColumnFields ^LiveTable$Watermark %)))

      (liveTable [_ table-name] (.get wms table-name))

      AutoCloseable
      (close [_] (util/close wms)))))

(defn ->schema [^LiveIndex$Watermark live-index-wm table-catalog]
  (merge-with set/union
              (update-vals (table-cat/all-column-fields table-catalog)
                           (comp set keys))
              (update-vals (some-> live-index-wm
                                   (.getAllColumnFields))
                           (comp set keys))))


(deftype LiveIndex [^BufferAllocator allocator, ^BufferPool buffer-pool, ^Log log, compactor
                    ^BlockCatalog block-cat, table-cat, ^TrieCatalog trie-cat

                    ^:volatile-mutable ^TransactionKey latest-completed-tx
                    ^:volatile-mutable ^TransactionKey latest-completed-block-tx
                    ^Map tables,

                    ^:volatile-mutable ^Watermark shared-wm
                    ^StampedLock wm-lock
                    ^RefCounter wm-cnt,

                    ^RowCounter row-counter, ^long rows-per-block

                    ^long log-limit, ^long page-limit
                    ^List skip-txs]
  xtdb.indexer.LiveIndex
  (getLatestCompletedTx [_] latest-completed-tx)
  (getLatestCompletedBlockTx [_] latest-completed-block-tx)

  (liveTable [_ table-name] (.get tables table-name))

  (startTx [this-idx tx-key]
    (let [table-txs (HashMap.)]
      (reify LiveIndex$Tx
        (liveTable [_ table-name]
          (.computeIfAbsent table-txs table-name
                            (reify Function
                              (apply [_ table-name]
                                (let [live-table (.liveTable this-idx table-name)
                                      new-live-table? (nil? live-table)
                                      ^LiveTable live-table (or live-table
                                                                (LiveTable. allocator buffer-pool table-name row-counter
                                                                            (partial trie/->live-trie log-limit page-limit)))]

                                  (.startTx live-table tx-key new-live-table?))))))

        (commit [_]
          (let [wm-lock-stamp (.writeLock wm-lock)]
            (try
              (doseq [[table-name ^LiveTable$Tx live-table-tx] table-txs]
                (.put tables table-name (.commit live-table-tx)))

              (set! (.-latest-completed-tx this-idx) tx-key)

              (let [^Watermark old-wm (.shared-wm this-idx)
                    ^Watermark shared-wm (util/with-close-on-catch [live-index-wm (open-live-idx-wm tables)]
                                           (Watermark. tx-key live-index-wm (->schema live-index-wm table-cat)))]
                (set! (.shared-wm this-idx) shared-wm)
                (some-> old-wm .close))

              (finally
                (.unlock wm-lock wm-lock-stamp))))

          (when (>= (.getBlockRowCount row-counter) rows-per-block)
            (.finishBlock this-idx)))

        (abort [_]
          (doseq [^LiveTable$Tx live-table-tx (.values table-txs)]
            (.abort live-table-tx))

          (set! (.-latest-completed-tx this-idx) tx-key)

          (when (>= (.getBlockRowCount row-counter) rows-per-block)
            (.finishBlock this-idx)))

        (openWatermark [_]
          (util/with-close-on-catch [wms (HashMap.)]
            (doseq [[table-name ^LiveTable$Tx live-table-tx] table-txs]
              (.put wms table-name (.openWatermark live-table-tx)))

            (doseq [[table-name ^LiveTable live-table] tables]
              (.computeIfAbsent wms table-name (fn [_] (.openWatermark live-table))))

            (reify LiveIndex$Watermark
              (getAllColumnFields [_] (update-vals wms #(.getColumnFields ^LiveTable$Watermark %)))
              (liveTable [_ table-name] (.get wms table-name))

              AutoCloseable
              (close [_] (util/close wms)))))

        AutoCloseable
        (close [_]))))

  (openWatermark [this]
    (let [wm-read-stamp (.readLock wm-lock)]
      (try
        (doto ^Watermark (.-shared-wm this) .retain)
        (finally
          (.unlock wm-lock wm-read-stamp)))))

  (finishBlock [this]
    (let [block-idx (.getBlockIdx row-counter)]

      (log/debugf "finishing block '%s'..." (util/->lex-hex-string block-idx))

      (with-open [scope (StructuredTaskScope$ShutdownOnFailure.)]
        (let [tasks (vec (for [[table-name ^LiveTable table] tables]
                           (.fork scope (fn []
                                          (try
                                            (when-let [finished-block (.finishBlock table block-idx)]
                                              [table-name {:fields (.getFields finished-block)
                                                           :trie-key (.getTrieKey finished-block)
                                                           :row-count (.getRowCount finished-block)
                                                           :data-file-size (.getDataFileSize finished-block)
                                                           :trie-metadata (.getTrieMetadata finished-block)}])
                                            (catch InterruptedException e
                                              (throw e))
                                            (catch Exception e
                                              (log/warn e "Error finishing block for table" table)
                                              (throw e)))))))]
          (.join scope)

          (let [table-metadata (-> tasks
                                   (->> (into {} (keep #(try
                                                          (.get ^StructuredTaskScope$Subtask %)
                                                          (catch Exception _
                                                            (throw (.exception ^StructuredTaskScope$Subtask %)))))))
                                   (util/rethrowing-cause))]
            (let [added-tries (for [[table-name {:keys [trie-key data-file-size trie-metadata]}] table-metadata]
                                (trie/->trie-details table-name trie-key data-file-size trie-metadata))]
              (.addTries trie-cat added-tries)
              @(.appendMessage log (Log$Message$TriesAdded. added-tries)))

            (let [all-tables (set (concat (keys table-metadata) (.getAllTableNames block-cat)))
                  table->current-tries (->> all-tables
                                            (map (fn [table-name]
                                                   (MapEntry/create table-name (->> (trie-cat/trie-state trie-cat table-name)
                                                                                    trie-cat/all-tries))))
                                            (into {}))
                  table-block-paths (table-cat/finish-block! table-cat block-idx table-metadata
                                                             table->current-tries)]
              (.finishBlock block-cat block-idx latest-completed-tx table-block-paths)))))

      (.nextBlock row-counter)

      (set! (.-latest_completed_block_tx this) latest-completed-tx)

      (let [wm-lock-stamp (.writeLock wm-lock)]
        (try
          (let [^Watermark shared-wm (.shared-wm this)]
            (.close shared-wm))

          (util/close tables)
          (.clear tables)
          (set! (.shared-wm this)
                (util/with-close-on-catch [live-index-wm (open-live-idx-wm tables)]
                  (Watermark.
                   latest-completed-tx
                   live-index-wm
                   (->schema live-index-wm table-cat))))
          (finally
            (.unlock wm-lock wm-lock-stamp))))

      (c/signal-block! compactor)
      (log/debugf "finished block 'b%s'." (util/->lex-hex-string block-idx))

      (when (and (not-empty skip-txs) (>= (:tx-id latest-completed-tx) (last skip-txs)))
        #_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
        (defonce -log-skip-txs-once
          (log/info "All XTDB_SKIP_TXS have been skipped and block has been finished - it is safe to remove the XTDB_SKIP_TXS environment variable.")))))

  (forceFlush [this record msg]
    (let [expected-last-block-tx-id (.getExpectedBlockTxId msg)
          latest-block-tx-id (some-> latest-completed-block-tx (.getTxId))]
      (when (= (or latest-block-tx-id -1) expected-last-block-tx-id)
        (.finishBlock this)))

    (set! (.latest-completed-tx this)
          (serde/->TxKey (.getLogOffset record)
                         (.getLogTimestamp record))))

  AutoCloseable
  (close [_]
    (some-> shared-wm .close)
    (util/close tables)
    (if-not (.tryClose wm-cnt (Duration/ofMinutes 1))
      (log/warn "Failed to shut down live-index after 60s due to outstanding watermarks.")
      (util/close allocator))))

(defmethod ig/prep-key :xtdb.indexer/live-index [_ config]
  {:allocator (ig/ref :xtdb/allocator)
   :buffer-pool (ig/ref :xtdb/buffer-pool)
   :block-cat (ig/ref :xtdb/block-catalog)
   :table-cat (ig/ref :xtdb/table-catalog)
   :compactor (ig/ref :xtdb/compactor)
   :log (ig/ref :xtdb/log)
   :trie-cat (ig/ref :xtdb/trie-catalog)
   :metrics-registry (ig/ref :xtdb.metrics/registry)
   :config config})

(defmethod ig/init-key :xtdb.indexer/live-index [_ {:keys [allocator, ^BlockCatalog block-cat, buffer-pool log trie-cat table-cat compactor ^IndexerConfig config metrics-registry]}]
  (let [block-idx (.getCurrentBlockIndex block-cat)
        latest-completed-tx (.getLatestCompletedTx block-cat)]
    (util/with-close-on-catch [allocator (util/->child-allocator allocator "live-index")]
      (metrics/add-allocator-gauge metrics-registry "live-index.allocator.allocated_memory" allocator)
      (let [tables (HashMap.)]
        (->LiveIndex allocator buffer-pool log compactor
                     block-cat table-cat trie-cat
                     latest-completed-tx latest-completed-tx
                     tables

                     (Watermark. nil (open-live-idx-wm tables) (->schema nil table-cat))
                     (StampedLock.)
                     (RefCounter.)

                     (RowCounter. (or (some-> block-idx inc) 0)) (.getRowsPerBlock config)

                     (.getLogLimit config) (.getPageLimit config)
                     (.getSkipTxs config))))))

(defmethod ig/halt-key! :xtdb.indexer/live-index [_ live-idx]
  (util/close live-idx))

(defn finish-block! [node]
  (.finishBlock ^xtdb.indexer.LiveIndex (util/component node :xtdb.indexer/live-index)))
