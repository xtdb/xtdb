(ns xtdb.indexer.live-index
  (:require [integrant.core :as ig]
            [xtdb.util :as util])
  (:import (xtdb.api IndexerConfig)
           (xtdb.catalog BlockCatalog TableCatalog)
           xtdb.indexer.LiveIndex))

(defmethod ig/expand-key :xtdb.indexer/live-index [k opts]
  {k (into {:allocator (ig/ref :xtdb.db-catalog/allocator)
            :block-cat (ig/ref :xtdb/block-catalog)
            :table-cat (ig/ref :xtdb/table-catalog)}
           opts)})

(defmethod ig/init-key :xtdb.indexer/live-index [_ {:keys [allocator ^BlockCatalog block-cat ^TableCatalog table-cat
                                                           db-name ^IndexerConfig indexer-conf]}]
  (LiveIndex/open allocator block-cat table-cat db-name indexer-conf))

(defmethod ig/halt-key! :xtdb.indexer/live-index [_ live-idx]
  (util/close live-idx))
