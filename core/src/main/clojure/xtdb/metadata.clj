(ns xtdb.metadata
  (:require [integrant.core :as ig]
            [xtdb.util :as util])
  (:import xtdb.storage.BufferPool
           (xtdb.metadata PageMetadata)))

(defmethod ig/prep-key ::metadata-manager [_ _]
  {:allocator (ig/ref :xtdb.db-catalog/allocator)
   :buffer-pool (ig/ref :xtdb/buffer-pool)})

(defmethod ig/init-key ::metadata-manager [_ {:keys [allocator, ^BufferPool buffer-pool, cache-size], :or {cache-size 128}}]
  (PageMetadata/factory allocator buffer-pool cache-size))

(defmethod ig/halt-key! ::metadata-manager [_ mgr]
  (util/try-close mgr))

