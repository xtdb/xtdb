(ns crux.db
  (:import java.io.Closeable))

;; tag::Index[]
(defprotocol Index
  (seek-values [this k])
  (next-values [this]))
;; end::Index[]

;; tag::LayeredIndex[]
(defprotocol LayeredIndex
  (open-level [this])
  (close-level [this])
  (max-depth [this]))
;; end::LayeredIndex[]

;; tag::Indexer[]
(defprotocol Indexer
  (index-doc [this content-hash doc])
  (index-tx [this tx-events tx-time tx-id])
  (docs-exist? [this content-hashes])
  (store-index-meta [this k v])
  (read-index-meta [this k]))
;; end::Indexer[]

;; tag::TxLog[]
(defprotocol TxLog
  (submit-doc [this content-hash doc])
  (submit-tx [this tx-ops])
  (new-tx-log-context ^java.io.Closeable [this])
  (tx-log [this tx-log-context from-tx-id]))
;; end::TxLog[]

;; NOTE: The snapshot parameter here is an optimisation to avoid keep
;; opening snapshots and allow caching of iterators. A non-KV backed
;; object store could choose to ignore it, but it would be nice to
;; hide it.
;; tag::ObjectStore[]
(defprotocol ObjectStore
  (init [this partial-node options])
  (get-single-object [this snapshot k])
  (get-objects [this snapshot ks])
  (known-keys? [this snapshot ks])
  (put-objects [this kvs])
  (delete-objects [this kvs]))
;; end::ObjectStore[]
