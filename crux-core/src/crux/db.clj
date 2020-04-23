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
  (index-tx [this tx tx-events])
  (store-index-meta [this k v])
  (read-index-meta [this k])
  (latest-completed-tx [this])
  (open-index-store ^java.io.Closeable [this]))
;; end::Indexer[]

;; tag::IndexStore[]
(defprotocol IndexStore
  ;; AVE
  (new-doc-attribute-value-entity-value-index [this a])
  ;; this way of passing other indexes into each other could be hidden
  ;; by having a function that creates both.
  (new-doc-attribute-value-entity-entity-index [this a v-doc-idx entity-as-of-idx])
  ;; AEV
  (new-doc-attribute-entity-value-entity-index [this a entity-as-of-idx])
  (new-doc-attribute-entity-value-value-index [this a e-doc-idx])

  ;; optimisation, not strictly necessary, can be implemented in other ways
  (or-known-triple-fast-path [this eid a v valid-time transact-time])

  ;; bitemporal index
  (new-entity-as-of-index [this valid-time transact-time])

  ;; history seqs, could be redone as lower-level indexes

  ;; valid time history
  (entity-history-seq-ascending [this eid valid-time transact-time])
  (entity-history-seq-descending [this eid valid-time transact-time])

  ;; full history
  (entity-history [this eid])
  (entity-history-range [this eid valid-time-start transaction-time-start valid-time-end transaction-time-end])

  ;; for hasTxCommitted
  (tx-failed? [this tx-id])

  ;; to allow nested scoping of resources, maybe not needed
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
