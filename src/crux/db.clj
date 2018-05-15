(ns crux.db)

(defprotocol Datasource
  (entities [this])
  (entities-for-attribute-value [this a v])
  (attr-val [this eid attr]))

(defprotocol Indexer
  (index [this txs transact-time])
  (store-index-meta [this k v])
  (read-index-meta [this k]))
