(ns crux.db)

(defprotocol Datasource
  (^java.io.Closeable new-query-context [this])
  (entities [this qc])
  (entities-for-attribute-value [this qc a min-v max-v])
  (entity [this qc eid])
  (entity-history [this qc eid]))

(defprotocol Entity
  (attr-val [this attr])
  (->id [this])
  (->map [this])
  (->business-time [this])
  (eq? [this another]))

(defprotocol Index
  (-seek-values [this k])
  (-next-values [this]))

(defprotocol Indexer
  (index-doc [this content-hash doc])
  (index-tx [this tx-ops tx-time tx-id])
  (store-index-meta [this k v])
  (read-index-meta [this k]))

(defprotocol TxLog
  (submit-doc [this content-hash doc])
  (submit-tx [this tx-ops]))

(defprotocol ObjectStore
  (get-objects [this ks])
  (put-objects [this kvs])
  (delete-objects [this kvs]))
