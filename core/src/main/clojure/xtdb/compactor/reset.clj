(ns xtdb.compactor.reset
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [xtdb.db-catalog :as db]
            [xtdb.error :as err]
            [xtdb.log :as xt-log]
            [xtdb.node :as xtn]
            xtdb.node.impl
            [xtdb.trie-catalog :as trie-cat])
  (:import (xtdb.database Database)
           xtdb.table.TableRef
           (xtdb.trie Trie)))

(defn reset-compactor! [node-opts db-name {:keys [dry-run?]}]
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
              compacted-file-keys (vec (for [^TableRef table (.getTables trie-cat)
                                             ^String trie-key (trie-cat/compacted-trie-keys-syn-l3h (trie-cat/trie-state trie-cat table))

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

              (trie-cat/reset->l2h! trie-cat)
              (xt-log/send-flush-block-msg! db)
              (xt-log/sync-db db #xt/duration "PT5M")
              (log/info "Reset complete. Deleting compacted files...")

              (doseq [file-key compacted-file-keys]
                (log/debugf "Deleting file: %s" file-key)
                (.deleteIfExists bp file-key)
                (log/debugf "Deleted file: %s" file-key))

              (log/info "Compacted files deleted - you can now upgrade the nodes."))))))))
