(ns crux.db)

(defprotocol Datasource
  (entities [this])
  (entities-for-attribute-value [this a min-v max-v]))

(defprotocol Entity
  (attr-val [this attr])
  (->id [this]))

(defprotocol Indexer
  (index-doc [this content-hash doc])
  (index-tx [this tx-ops tx-time tx-id])
  (store-index-meta [this k v])
  (read-index-meta [this k]))
