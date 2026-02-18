(ns xtdb.export
  (:require [clojure.tools.logging :as log]
            [xtdb.db-catalog :as db]
            [xtdb.error :as err]
            [xtdb.node :as xtn]
            [xtdb.table-catalog :as table-cat]
            [xtdb.trie-catalog :as trie-cat]
            [xtdb.util :as util])
  (:import [java.nio.file Path]
           (java.time Duration Instant ZoneId)
           (java.time.format DateTimeFormatter)
           java.time.temporal.ChronoUnit
           xtdb.catalog.BlockCatalog
           (xtdb.database Database)
           xtdb.api.storage.Storage
           xtdb.table.TableRef
           (xtdb.trie Trie)))

(defn- export-dir-path ^java.nio.file.Path [block-idx ^Instant now]
  (-> (util/->path "exports")
      (.resolve (format "b%s_%s"
                        (util/->lex-hex-string block-idx)
                        (.format (DateTimeFormatter/ofPattern "yyyyMMdd'T'HHmmss'Z'")
                                 (.atZone now (ZoneId/of "UTC")))))))

(defn- all-trie-keys [trie-cat ^TableRef table]
  (->> (trie-cat/all-tries
        (trie-cat/trie-state trie-cat table))
       (into []
             (comp (filter (comp #{:live :nascent} :state))
                   (map :trie-key)))))

(defn export-snapshot!
  ([node-opts db-name] (export-snapshot! node-opts db-name {}))

  ([node-opts db-name {:keys [dry-run?]}]
   (let [config (doto (xtn/->config node-opts)
                  (-> (.getCompactor) (.threads 0))
                  (-> (.getIndexer) (.enabled false))
                  (-> (.getTracer) (.enabled false))
                  (.setServer nil)
                  (.setFlightSql nil)
                  (.setHealthz nil))]

     (log/info (str "Starting minimal node to export snapshot of database: " db-name))

     (with-open [node (.open config)]
       (let [^Database db (or (.databaseOrNull (db/<-node node) db-name)
                              (throw (err/incorrect ::db-not-found
                                                    (format "Database not found: '%s'" db-name)
                                                    {:db-name db-name})))
             buffer-pool (.getBufferPool db)
             block-cat (.getBlockCatalog db)
             trie-cat (.getTrieCatalog db)
             
             start-time (Instant/now)]

         (when-let [block-idx (or (.getCurrentBlockIndex block-cat)
                                  (log/warn "No completed blocks found - aborting."))]

           (let [export-dir (export-dir-path block-idx start-time)
                 export-root (.resolve export-dir (Storage/storageRoot Storage/VERSION 0))
                 tables (.getTables trie-cat)]

             (log/infof "Export directory: %s" export-dir)
             (log/infof "Found %d tables to export" (count tables))

             (let [block-files [(BlockCatalog/blockFilePath block-idx)]
                   table-block-files (for [^TableRef table tables]
                                       (-> (Trie/getTablePath table)
                                           (table-cat/->table-block-metadata-obj-key block-idx)))
                   trie-files (for [^TableRef table tables
                                    :let [trie-keys (all-trie-keys trie-cat table)
                                          _ (log/infof "Table %s: %d active trie files" (pr-str table) (count trie-keys))]
                                    ^String trie-key trie-keys
                                    path [(Trie/metaFilePath table trie-key)
                                          (Trie/dataFilePath table trie-key)]]
                                path)
                   files-to-copy (vec (concat block-files table-block-files trie-files))]

               (log/infof "Copying %d block files, %d table-block files, %d trie files (total: %d)"
                          (count block-files) (count table-block-files) (count trie-files) (count files-to-copy))

               (if dry-run?
                 (do
                   (println "DRY RUN: No files will be copied")
                   (println "Files that would be copied:")
                   (doseq [src-path files-to-copy]
                     (println (str src-path))))

                 (do
                   (log/info "Starting file copy...")

                   (doseq [^Path path files-to-copy]
                     (try
                       (let [dest-path (.resolve export-root path)]
                         (log/debugf "Copying %s -> %s" path dest-path)
                         (.copyObject buffer-pool path dest-path))
                       (catch Exception e
                         (log/errorf e "Failed to copy %s" path)
                         (throw e))))

                   (log/infof "Export complete! Copied %d files in %s"
                              (count files-to-copy) (-> (Duration/between start-time (Instant/now))
                                                        (.truncatedTo ChronoUnit/MILLIS)))

                   (log/infof "Snapshot saved to: %s" export-dir)))

               {:block-idx block-idx
                :export-time start-time
                :export-dir export-dir
                :tables (count tables)
                :file-count (count files-to-copy)
                :dry-run? dry-run?}))))))))
