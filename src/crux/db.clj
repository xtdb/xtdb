(ns crux.db)

(defprotocol Index
  (seek-values [this k]))

(defprotocol OrderedIndex
  (next-values [this]))

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
