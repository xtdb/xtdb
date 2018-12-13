(ns crux.db
  (:import java.io.Closeable))

(defprotocol Index
  (seek-values [this k])
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
  (submit-tx [this tx-ops])
  (new-tx-log-context ^java.io.Closeable [this])
  (tx-log [this tx-log-context]))

;; NOTE: The snapshot parameter here is an optimisation to avoid keep
;; opening snapshots and allow caching of iterators. A non-KV backed
;; object store could choose to ignore it, but it would be nice to
;; hide it.
(defprotocol ObjectStore
  (get-single-object [this snapshot k])
  (get-objects [this snapshot ks])
  (put-objects [this kvs])
  (delete-objects [this kvs]))
