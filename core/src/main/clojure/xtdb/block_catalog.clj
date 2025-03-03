(ns xtdb.block-catalog
  (:require [integrant.core :as ig]
            [xtdb.util :as util])
  (:import xtdb.catalog.BlockCatalog))

(defmethod ig/prep-key :xtdb/block-catalog [_ _]
  {:buffer-pool (ig/ref :xtdb/buffer-pool)})

(defmethod ig/init-key :xtdb/block-catalog [_ {:keys [buffer-pool]}]
  (BlockCatalog. buffer-pool))

(defn <-node ^xtdb.catalog.BlockCatalog [node]
  (util/component node :xtdb/block-catalog))
