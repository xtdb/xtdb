(ns xtdb.compactor.reset
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [xtdb.db-catalog :as db]
            [xtdb.error :as err]
            [xtdb.log :as xt-log]
            [xtdb.node :as xtn]
            [xtdb.table :as xt-table]
            xtdb.node.impl
            [xtdb.trie-catalog :as trie-cat])
  (:import (xtdb.database Database)
           xtdb.table.TableRef
           (xtdb.trie Trie)))

(defn- table-matches? [^TableRef table table-names]
  "Check if table matches any of the specified table names"
  (when (seq table-names)
    (let [table-name (.getTableName table)
          schema-name (.getSchemaName table)]
      (some (fn [name-spec]
              (cond
                ;; exact table name match
                (= name-spec table-name) true
                ;; schema.table format
                (and (str/includes? name-spec ".")
                     (let [[schema table] (str/split name-spec #"\." 2)]
                       (and (= schema schema-name) (= table table-name)))) true
                :else false))
            table-names))))

(defn- filter-tables [all-tables table-names]
  "Filter tables based on specified table names. If no table names specified, return all tables."
  (if (seq table-names)
    (filter #(table-matches? % table-names) all-tables)
    all-tables))

(defn reset-compactor! [node-opts db-name {:keys [dry-run? table-names]}]
  (let [config (doto (xtn/->config node-opts)
                 (-> (.getCompactor) (.threads 0))
                 (.setServer nil))]

    (log/info "Starting a temporary node to catch up with any pending transactions on the tx-log")

    (with-open [node (.open config)]
      (let [^Database db (or (.databaseOrNull (db/<-node node) db-name)
                             (throw (err/incorrect ::db-not-found "Database not found" {:db-name db-name})))]
        (xt-log/send-flush-block-msg! db)
        (xt-log/sync-db db #xt/duration "PT5M")

        (log/info "Node caught up - resetting compaction...")

        (let [bp (.getBufferPool db)
              trie-cat (.getTrieCatalog db)
              live-idx (.getLiveIndex db)
              all-tables (.getTables trie-cat)
              selected-tables (filter-tables all-tables table-names)]
          
          ;; Validate table names if specified
          (when (seq table-names)
            (let [all-table-names (set (map (fn [^TableRef t] 
                                              (let [schema (.getSchemaName t)
                                                    table-name (.getTableName t)]
                                                (if (= schema "public")
                                                  table-name
                                                  (str schema "." table-name)))) 
                                            all-tables))
                  invalid-tables (remove (fn [table-name]
                                           (or (contains? all-table-names table-name)
                                               (some (fn [^TableRef t]
                                                       (table-matches? t [table-name]))
                                                     all-tables)))
                                         table-names)]
              (when (seq invalid-tables)
                (throw (err/incorrect ::table-not-found 
                                      (format "Tables not found: %s. Available tables: %s" 
                                              (str/join ", " invalid-tables)
                                              (str/join ", " (sort all-table-names))))))))
          
          (if (seq table-names)
            (log/infof "Processing %d specified table(s): %s" 
                      (count selected-tables)
                      (str/join ", " (map (fn [^TableRef t] 
                                            (let [schema (.getSchemaName t)
                                                  table-name (.getTableName t)]
                                              (if (= schema "public")
                                                table-name
                                                (str schema "." table-name))))
                                          selected-tables)))
            (log/infof "Processing all %d table(s)" (count selected-tables)))
          
          (let [compacted-file-keys (vec (for [^TableRef table selected-tables
                                               ^String trie-key (trie-cat/compacted-trie-keys (trie-cat/trie-state trie-cat table))

                                               ;; meta file first, as it's the marker
                                               file-key [(Trie/metaFilePath table trie-key)
                                                         (Trie/dataFilePath table trie-key)]]
                                           file-key))]
          (cond
            dry-run?
            (do
              (log/info "Dry run: no changes will be made."
                        "\n\nWhen you run this for real, ensure all nodes are stopped before running this command."
                        "\nThen, do not upgrade nodes until the reset is complete.")

              (log/info "WOULD delete:\n"
                        (->> (map #(str "  " %) compacted-file-keys)
                             (str/join "\n"))))

            (nil? (.getLatestCompletedTx live-idx))
            (log/error "No completed transactions found in the live index. Cannot reset compaction.")

            :else
            (do
              (log/info "Resetting compaction...")

              ;; Reset to L0 for selected tables only
              (if (seq table-names)
                ;; Reset only selected tables
                (let [table-cats (:!table-cats trie-cat)]
                  (doseq [^TableRef table selected-tables]
                    (log/debugf "Resetting table: %s" (.getTableName table))
                    (.compute table-cats table
                              (fn [_table table-cat]
                                ((requiring-resolve 'xtdb.trie-catalog/reset->l0) table-cat)))))
                ;; Reset all tables (existing behavior)
                (trie-cat/reset->l0! trie-cat))
              (xt-log/send-flush-block-msg! db)
              (xt-log/sync-db db #xt/duration "PT5M")
              (log/info "Reset complete. Deleting compacted files...")

              (doseq [file-key compacted-file-keys]
                (log/debugf "Deleting file: %s" file-key)
                (.deleteIfExists bp file-key)
                (log/debugf "Deleted file: %s" file-key))

              (log/info "Compacted files deleted - you can now upgrade the nodes."))))))))
