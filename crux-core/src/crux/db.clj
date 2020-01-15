(ns crux.db
  (:import java.io.Closeable
           java.util.Iterator
           crux.api.ITxLog))

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
  (index-docs [this docs])
  (index-tx [this tx tx-events])
  (docs-exist? [this content-hashes])
  (store-index-meta [this k v])
  (read-index-meta [this k]))
;; end::Indexer[]

;; tag::TxLog[]
(defprotocol TxLog
  (submit-tx [this tx-ops])
  (open-tx-log ^crux.api.ITxLog [this from-tx-id])
  (latest-submitted-tx [this]))
;; end::TxLog[]

(defprotocol RemoteDocumentStore
  (submit-docs [this id-and-docs]))

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

(defrecord CloseableTxLogIterator [close-fn ^Iterator lazy-seq-iterator]
  ITxLog
  (next [this]
    (.next lazy-seq-iterator))

  (hasNext [this]
    (.hasNext lazy-seq-iterator))

  (close [_]
    (close-fn)))

(defn ->closeable-tx-log-iterator [close-fn ^Iterable sq]
  (->CloseableTxLogIterator close-fn (.iterator (lazy-seq sq))))
