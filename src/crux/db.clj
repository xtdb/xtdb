(ns crux.db)

(defprotocol Datasource
  (entities [this])
  (entities-for-attribute-value [this a min-v max-v]))

(defprotocol Entity
  (attr-val [this attr])
  (->id [this]))

(defprotocol Indexer
  (index [this txs transact-time])
  (store-index-meta [this k v])
  (read-index-meta [this k]))
