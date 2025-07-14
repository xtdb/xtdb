(ns xtdb.database
  (:require [integrant.core :as ig]
            [xtdb.util :as util]))

(defrecord XtDatabase [block-cat table-cat trie-cat buffer-pool live-idx scan-emitter]
  xtdb.database.Database
  (getBlockCatalog [_] block-cat)
  (getTableCatalog [_] table-cat)
  (getTrieCatalog [_] trie-cat)
  (getBufferPool [_] buffer-pool)
  (getLiveIndex [_] live-idx)
  (getScanEmitter [_] scan-emitter))

(defmethod ig/prep-key :xtdb/database [_ _]
  {:block-cat (ig/ref :xtdb/block-catalog)
   :table-cat (ig/ref :xtdb/table-catalog)
   :trie-cat (ig/ref :xtdb/trie-catalog)
   :live-idx (ig/ref :xtdb.indexer/live-index)
   :buffer-pool (ig/ref :xtdb/buffer-pool)
   :scan-emitter (ig/ref :xtdb.operator.scan/scan-emitter)})

(defmethod ig/init-key :xtdb/database [_ {:keys [block-cat table-cat trie-cat buffer-pool live-idx scan-emitter]}]
  (->XtDatabase block-cat table-cat trie-cat buffer-pool live-idx scan-emitter))

(defmethod ig/halt-key! :xtdb/database [_ _])

(defn <-node ^ xtdb.database.Database [node]
  (util/component node :xtdb/database))
