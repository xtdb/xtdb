(ns xtdb.database
  (:require [integrant.core :as ig]
            [xtdb.util :as util])
  (:import [xtdb.database Database]))

(defmethod ig/prep-key :xtdb/database [_ _]
  {:allocator (ig/ref :xtdb/allocator)
   :block-cat (ig/ref :xtdb/block-catalog)
   :table-cat (ig/ref :xtdb/table-catalog)
   :trie-cat (ig/ref :xtdb/trie-catalog)
   :live-idx (ig/ref :xtdb.indexer/live-index)
   :log (ig/ref :xtdb/log)
   :buffer-pool (ig/ref :xtdb/buffer-pool)
   :metadata-mgr (ig/ref :xtdb.metadata/metadata-manager)})

(defmethod ig/init-key :xtdb/database [_ {:keys [allocator block-cat table-cat trie-cat log buffer-pool live-idx metadata-mgr]}]
  (util/with-close-on-catch [allocator (util/->child-allocator allocator "database/xtdb")]
    (Database. "xtdb"
               allocator block-cat table-cat trie-cat
               log buffer-pool metadata-mgr live-idx)))

(defmethod ig/halt-key! :xtdb/database [_ db]
  (util/close db))

(defn <-node ^xtdb.database.Database [node]
  (util/component node :xtdb/database))
