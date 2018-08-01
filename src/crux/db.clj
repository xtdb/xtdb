(ns crux.db)

(defprotocol Index
  (seek-values [this k]))

(defprotocol OrderedIndex
  (next-values [this]))

(defprotocol LayeredIndex
  (open-level [this])
  (close-level [this])
  (max-depth [this]))

(defprotocol Indexer
  (index-doc [this content-hash doc])
  (index-tx [this tx-ops tx-time tx-id])
  (store-index-meta [this k v])
  (read-index-meta [this k]))

(defprotocol TxLog
  (submit-doc [this content-hash doc])
  (submit-tx [this tx-ops]))

;; NOTE: The snapshot parameter here is an optimisation to avoid keep
;; opening snapshots and allow caching of iterators. A non-KV backed
;; object store could choose to ignore it, but it would be nice to
;; hide it.
(defprotocol ObjectStore
  (get-objects [this snapshot ks])
  (put-objects [this kvs])
  (delete-objects [this kvs]))
