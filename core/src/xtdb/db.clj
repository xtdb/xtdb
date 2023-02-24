(ns ^:no-doc xtdb.db)

(defprotocol Index
  (seek-values [this k])
  (next-values [this]))

(defprotocol LayeredIndex
  (open-level [this])
  (close-level [this])
  (max-depth [this]))

(defprotocol IndexStoreTx
  (index-tx [this tx])
  (index-docs [this docs])
  (unindex-eids [this base-snapshot eids])
  (index-entity-txs [this entity-txs])
  (commit-index-tx [this])
  (abort-index-tx [this tx docs]))

(defprotocol IndexStore
  (store-index-meta [this k v])
  (tx-failed? [this tx-id])
  (begin-index-tx [this])
  (index-stats [this docs]))

(defprotocol ReplicatorTx
  (index-replicator-tx [replicator-tx tx])
  (index-replicator-docs [replicator-tx docs])
  (index-coords [replicator-tx coords])
  (commit-replicator-tx [replictor-tx])
  (abort-replicator-tx [replicator-tx]))

(defprotocol Replicator
  (begin-replicator-tx [replicator]))

(defprotocol LatestCompletedTx
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
  (doc-value-count [this attr])
  (^double value-cardinality [this attr])
  (^double eid-cardinality [this attr]))

(defprotocol ValueSerde
  (decode-value [this value-buffer])
  (encode-value [this value]))

(defprotocol IndexSnapshot
  (av [this a min-v])
  (ave [this a v min-e entity-resolver-fn])
  (ae [this a min-e])
  (aev [this a e min-v entity-resolver-fn])
  (entity [this e c])
  (entity-as-of-resolver [this eid valid-time tx-id])
  (entity-as-of ^xtdb.codec.EntityTx [this eid valid-time tx-id])
  (entity-history [this eid sort-order opts])
  (resolve-tx [this tx])
  (open-nested-index-snapshot ^java.io.Closeable [this]))

(defprotocol TxLog
  (submit-tx [this tx-events] [this tx-events opts])
  (open-tx-log ^xtdb.api.ICursor [this after-tx-id opts]
   "Returns a serialized iterator (cursor) over transactions (see spec ::xtdb.api/tx) in the log after the given tx id.

   The cursor:

   - is free to return log entries introduced after the creation of the cursor.
   - does not necessarily block when it runs out of entries, the sequence can just end. Do not expect it to be infinite or blocking.
   - it can appear infinite regardless of any blocking or polling behaviour, given frequent enough writes to the log.
   - should be closed with .close to free any resources such as threads or connections held by the cursor.")
  (latest-submitted-tx [this])
  (^java.util.concurrent.CompletableFuture subscribe [_ after-tx-id f]
   "f takes Future + txs - complete the future to stop the subscription.
    or, outside of f, complete the future returned from this function to stop the subscription."))

(defprotocol InFlightTx
  (index-tx-docs [in-flight-tx docs])
  (index-tx-events [in-flight-tx tx] "Returns a map of :committing? (true/false) representing whether the transactions were indexed, :indexed-docs (any new docs indexed during the expansion of tx fns)")
  (commit [in-flight-tx tx])
  (abort [in-flight-tx tx]))

(defprotocol TxIndexer
  (begin-tx [tx-indexer]))

(defprotocol TxIngester
  (ingester-error [tx-ingester]))

(defprotocol DocumentStore
  "Once `submit-docs` function returns successfully, any call to `fetch-docs` across the cluster must return the submitted docs."
  (submit-docs [this id-and-docs])
  (fetch-docs [this ids]
    "Fetches documents from storage. Returning a map containing entries where found [id, document content (deserialized map)].

    Input ids are converted to id hashes via xtdb.codec/new-id, the hash is looked up, not necessarily the supplied id.

    Thrown exceptions may contain a :cognitect.anomalies/category key to inform callers of the nature of
    any error, without knowledge of the implementation of DocumentStore.

    Returned ids are always of the type xtdb.codec.Id, as such they may not share a representation (or equality) with the supplied ids,
    map an input id through xtdb.codec/new-id to find its corresponding entry."))
