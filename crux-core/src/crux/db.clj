(ns ^:no-doc crux.db)

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
  (unindex-docs [this docs])
  (index-entity-txs [this tx entity-txs])
  (mark-tx-as-failed [this tx])
  (store-index-meta [this k v])
  (read-index-meta [this k])
  (latest-completed-tx [this])
  (tx-failed? [this tx-id])
  (open-index-store ^java.io.Closeable [this]))
;; end::Indexer[]

;; tag::IndexStore[]
(defprotocol IndexStore
  (av [this a min-v entity-resolver-fn])
  (ave [this a v min-e entity-resolver-fn])
  (ae [this a min-e entity-resolver-fn])
  (aev [this a e min-v entity-resolver-fn])
  (entity-as-of [this eid valid-time transact-time])
  (open-entity-history ^crux.api.ICursor [this eid sort-order opts])
  (all-content-hashes [this eid])
  (decode-value [this a content-hash value-buffer])
  (encode-value [this value])
  (open-nested-index-store ^java.io.Closeable [this]))
;; end::IndexStore[]

;; tag::TxLog[]
(defprotocol TxLog
  (submit-tx [this tx-events])
  (open-tx-log ^crux.api.ICursor [this after-tx-id])
  (latest-submitted-tx [this]))
;; end::TxLog[]

(defprotocol TxConsumer
  (consumer-error [this]))

(defprotocol DocumentStore
  (submit-docs [this id-and-docs])
  (fetch-docs [this ids]))

;; NOTE: The snapshot parameter here is an optimisation to avoid keep
;; opening snapshots and allow caching of iterators. A non-KV backed
;; object store could choose to ignore it, but it would be nice to
;; hide it.
;; tag::ObjectStore[]
(defprotocol ObjectStore
  (get-single-object [this snapshot k])
  (get-objects [this snapshot ks])
  (put-objects [this kvs]))
;; end::ObjectStore[]
