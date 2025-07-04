(ns xtdb.compactor.reset
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [integrant.core :as ig]
            [xtdb.node :as xtn]
            xtdb.node.impl
            [xtdb.trie-catalog :as cat])
  (:import (xtdb.api Xtdb$Config)
           xtdb.BufferPool
           xtdb.indexer.LiveIndex
           (xtdb.trie Trie TrieCatalog)))

(defn reset-system [^Xtdb$Config opts]
  (-> {:xtdb/config opts
       :xtdb/allocator {}
       :xtdb.metrics/registry {}
       :xtdb/buffer-pool (.getStorage opts)
       :xtdb/block-catalog {}
       :xtdb/table-catalog {}
       :xtdb/trie-catalog {}
       :xtdb.indexer/live-index (.getIndexer opts)
       :xtdb/compactor (doto (.getCompactor opts)
                         (.threads 0))
       :xtdb.metadata/metadata-manager {}
       :xtdb/log (.getLog opts)}

      (doto ig/load-namespaces)))

(defn reset-compactor! [node-opts {:keys [dry-run?]}]
  (let [system (-> (reset-system (xtn/->config node-opts))
                   ig/prep ig/init)

        ^BufferPool bp (:xtdb/buffer-pool system)
        ^TrieCatalog trie-cat (:xtdb/trie-catalog system)
        ^LiveIndex live-idx (:xtdb.indexer/live-index system)
        compacted-file-keys (vec (for [table-name (.getTableNames trie-cat)
                                       trie-key (cat/compacted-trie-keys (cat/trie-state trie-cat table-name))

                                       ;; meta file first, as it's the marker
                                       file-key [(Trie/metaFilePath table-name trie-key)
                                                 (Trie/dataFilePath table-name trie-key)]]
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

        (cat/reset->l0! trie-cat)
        (.finishBlock live-idx)
        (log/info "Reset complete. Deleting compacted files...")

        (doseq [file-key compacted-file-keys]
          (log/debugf "Deleting file: %s" file-key)
          (.deleteIfExists bp file-key)
          (log/debugf "Deleted file: %s" file-key))

        (log/info "Compacted files deleted - you can now upgrade the nodes.")))))
