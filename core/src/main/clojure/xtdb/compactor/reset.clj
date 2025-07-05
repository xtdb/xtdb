(ns xtdb.compactor.reset
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [xtdb.block-catalog :as block-cat]
            [xtdb.buffer-pool :as bp]
            [xtdb.indexer.live-index :as li]
            [xtdb.log :as xt-log]
            [xtdb.node :as xtn]
            xtdb.node.impl
            [xtdb.protocols :as xtp]
            [xtdb.trie-catalog :as trie-cat])
  (:import (xtdb.trie Trie)))

(defn reset-compactor! [node-opts {:keys [dry-run?]}]
  (let [config (doto (xtn/->config node-opts)
                 (-> (.getCompactor) (.threads 0))
                 (.setServer nil))]

    (log/info "Starting a temporary node to catch up with any pending transactions on the tx-log")

    (with-open [node (.open config)]
      (xt-log/await-tx node (xtp/latest-submitted-tx-id node) #xt/duration "PT5M")
      (xt-log/finish-block! node)

      (log/info "Node caught up - resetting compaction...")

      (let [bp (bp/<-node node)
            log-proc (xt-log/node->log-proc node)
            block-cat (block-cat/<-node node)
            trie-cat (trie-cat/trie-catalog node)
            live-idx (li/<-node node)
            compacted-file-keys (vec (for [table-name (.getTableNames trie-cat)
                                           trie-key (trie-cat/compacted-trie-keys (trie-cat/trie-state trie-cat table-name))

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

            (trie-cat/reset->l0! trie-cat)
            (.finishBlock log-proc)
            (log/info "Reset complete. Deleting compacted files...")

            (doseq [file-key compacted-file-keys]
              (log/debugf "Deleting file: %s" file-key)
              (.deleteIfExists bp file-key)
              (log/debugf "Deleted file: %s" file-key))

            (log/info "Compacted files deleted - you can now upgrade the nodes.")))))))
