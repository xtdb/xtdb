(ns xtdb.compactor.reset
  (:require [clojure.pprint :as pp]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [xtdb.db-catalog :as db]
            [xtdb.error :as err]
            [xtdb.log :as xt-log]
            [xtdb.node :as xtn]
            xtdb.node.impl
            [xtdb.trie :as trie]
            [xtdb.trie-catalog :as trie-cat])
  (:import (java.nio.file Path)
           (xtdb.api.storage ObjectStore$StoredObject)
           (xtdb.database Database)
           (xtdb.storage BufferPool)
           xtdb.table.TableRef
           (xtdb.trie Trie)))

(defn- meta-file->trie-key
  "\"…/meta/l00-rc-b00.arrow\" → \"l00-rc-b00\"."
  [^Path path]
  (let [file-name (str (.getFileName path))
        dot (str/last-index-of file-name ".")]
    (cond-> file-name dot (subs 0 dot))))

(defn- list-data-file-sizes
  "Build a `trie-key → data-file-size` lookup from the table's data dir."
  [^BufferPool bp ^TableRef table]
  (into {} (map (fn [^ObjectStore$StoredObject obj]
                  [(meta-file->trie-key (.getKey obj)) (.getSize obj)]))
        (.listAllObjects bp (Trie/dataFileDir table))))

(defn- list-l0-entries
  "Enumerate the L0 trie files for a table directly from the object store.
   The catalog is no longer the authoritative source for L0 — see `trie-cat/reset->l0!`.
   Meta is the completion marker (deleted last by GC), so we list it first; data-file-size
   comes from the paired data file. Skips meta files whose data file is missing — those are
   half-written tries that the GC's data-then-meta deletion order leaves transiently visible.
   `:trie-metadata` is left absent — the post-reset `CatalogEntry.getTemporalMetadata` fallback
   handles that until the next compaction re-derives it."
  [^BufferPool bp ^TableRef table]
  (let [data-sizes (list-data-file-sizes bp table)]
    (->> (.listAllObjects bp (Trie/metaFileDir table))
         (keep (fn [^ObjectStore$StoredObject obj]
                 (let [trie-key (meta-file->trie-key (.getKey obj))]
                   (when-some [parsed (trie/parse-trie-key trie-key)]
                     (when-some [data-size (and (zero? ^long (:level parsed))
                                                (data-sizes trie-key))]
                       (assoc parsed :data-file-size data-size)))))))))

(defn- enumerate-l0s [^BufferPool bp tables]
  (into {} (for [^TableRef table tables]
             [table (vec (list-l0-entries bp table))])))

(defn reset-compactor! [node-opts ^String db-name {:keys [dry-run?]}]
  (let [config (doto (xtn/->config node-opts)
                 (-> (.getCompactor) (.threads 0))
                 (.setServer nil)
                 (.setFlightSql nil))]

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
              tables (.getTables trie-cat)
              compacted-file-keys (vec (for [^TableRef table tables
                                             ^String trie-key (trie-cat/compacted-trie-keys (trie-cat/trie-state trie-cat table))

                                             ;; meta file first, as it's the marker
                                             file-key [(Trie/metaFilePath table trie-key)
                                                       (Trie/dataFilePath table trie-key)]]
                                         file-key))
              ;; Source of truth for L0 is the object store, not the catalog: catalog L0s
              ;; are dropped on supersession, so by the time we get here the catalog will be
              ;; missing every L0 that's already been consumed by an L1C we're about to wipe.
              table->l0-entries (enumerate-l0s bp tables)]
          (cond
            dry-run?
            (do
              (log/info "Dry run: no changes will be made."
                        "\n\nWhen you run this for real, ensure all nodes are stopped before running this command."
                        "\nThen, do not upgrade nodes until the reset is complete.")

              (letfn [(freeze-state []
                        (->> (for [[tbl {:keys [tries]}] (:!table-cats trie-cat)]
                               [tbl (->> (for [[k tries] tries
                                               :let [{:keys [live garbage]} tries]]
                                           [k {:live (count live)
                                               :garbage (count garbage)}])
                                         (into {}))])
                             (into {})))]
                (let [state-before (freeze-state)]
                  (trie-cat/reset->l0! trie-cat table->l0-entries)
                  (let [state-after (freeze-state)]
                    (log/info "Trie catalog state diff:")
                    (pp/pprint (->> (for [tbl (set/union (set (keys state-before))
                                                         (set (keys state-after)))
                                          :let [parts-before (get state-before tbl)
                                                parts-after (get state-after tbl)]
                                          part (set/union (set (keys parts-before))
                                                          (set (keys parts-after)))
                                          :let [{live-before :live, garbage-before :garbage} (get parts-before part)
                                                {live-after :live, garbage-after :garbage} (get parts-after part)]
                                          :when (or (not= live-before live-after)
                                                    (not= garbage-before garbage-after))]
                                      {:tbl tbl
                                       :part part
                                       :live {:before live-before, :after live-after}
                                       :garbage {:before garbage-before, :after garbage-after}})
                                    (group-by :tbl))))))

              (log/info "WOULD delete:\n"
                        (->> (map #(str "  " %) compacted-file-keys)
                             (str/join "\n"))))

            (nil? (.getLatestCompletedTx live-idx))
            (log/error "No completed transactions found in the live index. Cannot reset compaction.")

            :else
            (do
              (log/info "Resetting compaction...")

              (trie-cat/reset->l0! trie-cat table->l0-entries)
              (xt-log/send-flush-block-msg! db)
              (xt-log/sync-db db #xt/duration "PT5M")
              (log/info "Reset complete. Deleting compacted files...")

              (doseq [file-key compacted-file-keys]
                (log/debugf "Deleting file: %s" file-key)
                (.deleteIfExists bp file-key)
                (log/debugf "Deleted file: %s" file-key))

              (log/info "Compacted files deleted - you can now upgrade the nodes."))))))))
