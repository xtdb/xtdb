(ns xtdb.database
  (:require [integrant.core :as ig]
            [xtdb.util :as util])
  (:import [xtdb.database Database]))

(defrecord XtDatabase [db-name allocator block-cat table-cat trie-cat buffer-pool live-idx scan-emitter]
  Database
  (getName [_] db-name)
  (getAllocator [_] allocator)
  (getBlockCatalog [_] block-cat)
  (getTableCatalog [_] table-cat)
  (getTrieCatalog [_] trie-cat)
  (getBufferPool [_] buffer-pool)
  (getLiveIndex [_] live-idx)
  (getScanEmitter [_] scan-emitter)

  (close [_]
    (util/close allocator)))

(defmethod print-method XtDatabase [^Database db ^java.io.Writer w]
  (.write w (str "#<XtdbDatabase " (pr-str (.getName db)) ">")))

(defmethod ig/prep-key :xtdb/database [_ _]
  {:allocator (ig/ref :xtdb/allocator)
   :block-cat (ig/ref :xtdb/block-catalog)
   :table-cat (ig/ref :xtdb/table-catalog)
   :trie-cat (ig/ref :xtdb/trie-catalog)
   :live-idx (ig/ref :xtdb.indexer/live-index)
   :buffer-pool (ig/ref :xtdb/buffer-pool)
   :scan-emitter (ig/ref :xtdb.operator.scan/scan-emitter)})

(defmethod ig/init-key :xtdb/database [_ {:keys [allocator block-cat table-cat trie-cat buffer-pool live-idx scan-emitter]}]
  (util/with-close-on-catch [allocator (util/->child-allocator allocator "database/xtdb")]
    (->XtDatabase "xtdb"
                  allocator block-cat table-cat trie-cat
                  buffer-pool live-idx scan-emitter)))

(defmethod ig/halt-key! :xtdb/database [_ db]
  (util/close db))

(defn <-node ^ xtdb.database.Database [node]
  (util/component node :xtdb/database))
