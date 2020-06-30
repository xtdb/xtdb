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
  (unindex-eids [this eids])
  (index-entity-txs [this tx entity-txs])
  (mark-tx-as-failed [this tx])
  (store-index-meta [this k v])
  (read-index-meta [this k] [this k not-found])
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
  (entity-as-of-resolver [this eid valid-time transact-time])
  (entity-as-of ^crux.codec.EntityTx [this eid valid-time transact-time])
  (entity-history [this eid sort-order opts])
  (decode-value [this value-buffer])
  (encode-value [this value])
  (open-nested-index-store ^java.io.Closeable [this]))
;; end::IndexStore[]

;; tag::TxLog[]
(defprotocol TxLog
  (submit-tx [this tx-events])
  (open-tx-log ^crux.api.ICursor [this after-tx-id])
  (latest-submitted-tx [this]))
;; end::TxLog[]

(defprotocol TxIngester
  (begin-tx [tx-ingester tx])
  (ingester-error [tx-ingester]))

(defprotocol InFlightTx
  (index-tx-events [in-flight-tx tx-events])
  (commit [in-flight-tx])
  (abort [in-flight-tx]))

(defprotocol DocumentStore
  (submit-docs [this id-and-docs])
  (fetch-docs [this ids]))
