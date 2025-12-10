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
            [xtdb.trie-catalog :as trie-cat])
  (:import (xtdb.database Database)
           xtdb.table.TableRef
           (xtdb.trie Trie)))

(defn reset-compactor! [node-opts db-name {:keys [dry-run?]}]
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
              compacted-file-keys (vec (for [^TableRef table (.getTables trie-cat)
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

              (letfn [(freeze-state []
                        (->> (for [[tbl {:keys [tries]}] (:!table-cats trie-cat)]
                               [tbl (->> (for [[k tries] tries
                                               :let [{:keys [live garbage]} tries]]
                                           [k {:live (count live)
                                               :garbage (count garbage)}])
                                         (into {}))])
                             (into {})))]
                (let [state-before (freeze-state)]
                  (trie-cat/reset->l0! trie-cat)
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

              (trie-cat/reset->l0! trie-cat)
              (xt-log/send-flush-block-msg! db)
              (xt-log/sync-db db #xt/duration "PT5M")
              (log/info "Reset complete. Deleting compacted files...")

              (doseq [file-key compacted-file-keys]
                (log/debugf "Deleting file: %s" file-key)
                (.deleteIfExists bp file-key)
                (log/debugf "Deleted file: %s" file-key))

              (log/info "Compacted files deleted - you can now upgrade the nodes."))))))))
