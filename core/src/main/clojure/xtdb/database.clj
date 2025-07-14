(ns xtdb.database
  (:require [integrant.core :as ig]
            [xtdb.util :as util]))

(defrecord Database [block-cat table-cat scan-emitter]
  xtdb.query.Database
  (getBlockCatalog [_] block-cat)
  (getTableCatalog [_] table-cat)
  (getScanEmitter [_] scan-emitter))

(defmethod ig/prep-key :xtdb/database [_ _]
  {:block-cat (ig/ref :xtdb/block-catalog)
   :table-cat (ig/ref :xtdb/table-catalog)
   :scan-emitter (ig/ref :xtdb.operator.scan/scan-emitter)})

(defmethod ig/init-key :xtdb/database [_ {:keys [block-cat table-cat scan-emitter]}]
  (->Database block-cat table-cat scan-emitter))

(defmethod ig/halt-key! :xtdb/database [_ _])

(defn <-node ^xtdb.query.Database [node]
  (util/component node :xtdb/database))
