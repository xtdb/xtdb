(ns crux.db)

(defprotocol Datasource
  (entities [this])
  (entities-for-attribute-value [this a min-v max-v]))

(defprotocol Entity
  (attr-val [this attr])
  (->id [this])
  (eq? [this another]))

(defprotocol Indexer
  (index-doc [this content-hash doc])
  (index-tx [this tx-ops tx-time tx-id])
  (store-index-meta [this k v])
  (read-index-meta [this k]))

(defprotocol TxLog
  (submit-doc [this content-hash doc])
  (submit-tx [this tx-ops]))
