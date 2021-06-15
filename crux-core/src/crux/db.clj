(ns ^:no-doc crux.db
  (:require [clojure.set :as set]
            [crux.codec :as c]))

(defprotocol Index
  (seek-values [this k])
  (next-values [this]))

(defprotocol LayeredIndex
  (open-level [this])
  (close-level [this])
  (max-depth [this]))

(defprotocol IndexStoreTx
  (index-docs [this docs])
  (unindex-eids [this eids])
  (index-entity-txs [this entity-txs])
  (commit-index-tx [this])
  (abort-index-tx [this]))

(defprotocol IndexStore
  (exclusive-avs [this eids])
  (store-index-meta [this k v])
  (tx-failed? [this tx-id])
  (begin-index-tx [index-store tx fork-at])
  (latest-completed-tx [this]))

(defprotocol IndexMeta
  (-read-index-meta [this k not-found]))

(defn read-index-meta
  ([index-meta k] (-read-index-meta index-meta k nil))
  ([index-meta k not-found] (-read-index-meta index-meta k not-found)))

(defprotocol IndexSnapshotFactory
  (open-index-snapshot ^java.io.Closeable [this]))

(defprotocol AttributeStats
  (all-attrs [this])
  (doc-count [this attr])
  (^double value-cardinality [this attr])
  (^double eid-cardinality [this attr]))

(defprotocol IndexSnapshot
  (av [this a min-v])
  (ave [this a v min-e entity-resolver-fn])
  (ae [this a min-e])
  (aev [this a e min-v entity-resolver-fn])
  (entity [this e c])
  (entity-as-of-resolver [this eid valid-time tx-id])
  (entity-as-of ^crux.codec.EntityTx [this eid valid-time tx-id])
  (entity-history [this eid sort-order opts])
  (decode-value [this value-buffer])
  (encode-value [this value])
  (resolve-tx [this tx])
  (open-nested-index-snapshot ^java.io.Closeable [this]))

(defprotocol TxLog
  (submit-tx [this tx-events])
  (open-tx-log ^crux.api.ICursor [this after-tx-id])
  (latest-submitted-tx [this])
  (^java.util.concurrent.CompletableFuture subscribe [_ after-tx-id f]
   "f takes Future + tx - complete the future to stop the subscription.
    or, outside of f, complete the future returned from this function to stop the subscription."))

(defprotocol InFlightTx
  (index-tx-events [in-flight-tx tx-events])
  (commit [in-flight-tx])
  (abort [in-flight-tx]))

(defprotocol TxIndexer
  (begin-tx [tx-indexer tx fork-at]))

(defprotocol TxIngester
  (ingester-error [tx-ingester]))

(defprotocol DocumentStore
  "Once `submit-docs` function returns successfully, any call to `fetch-docs` across the cluster must return the submitted docs."
  (submit-docs [this id-and-docs])
  (fetch-docs [this ids]))
