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
  (new-attribute-value-entity-index-pair [this a entity-as-of-idx])
  (new-attribute-entity-value-index-pair [this a entity-as-of-idx])
  (new-entity-as-of-index [this valid-time transact-time])
  (entity-valid-time-history [this eid start-valid-time transact-time ascending?])
  (entity-history-range [this eid valid-time-start transaction-time-start valid-time-end transaction-time-end])
  (all-content-hashes [this eid])
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
