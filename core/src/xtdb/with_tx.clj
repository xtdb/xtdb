(ns xtdb.with-tx
  (:require [xtdb.api :as xt]
            [xtdb.bus :as bus]
            [xtdb.cache]
            [xtdb.codec :as c]
            [xtdb.db :as db]
            [xtdb.io :as xio]
            [xtdb.kv.index-store :as index-store]
            [xtdb.kv.mutable-kv :as mut-kv]
            [xtdb.tx.conform :as txc]
            [xtdb.tx.event :as txe]
            [xtdb.kv :as kv]
            [xtdb.fork :as fork])
  (:import java.util.Date
           xtdb.codec.EntityTx))

(defn- date-min [^Date d1, ^Date d2]
  (if (and d1 d2)
    (Date. (min (.getTime d1) (.getTime d2)))
    (or d1 d2)))

(defn long-min [^Long l1, ^Long l2]
  (if (and l1 l2)
    (min l1 l2)
    (or l1 l2)))

(defn- inc-date [^Date d1]
  (Date. (inc (.getTime d1))))

(defrecord CappedIndexSnapshot [index-snapshot capped-valid-time capped-tx-id]
  db/IndexSnapshot
  (av [_ a min-v] (db/av index-snapshot a min-v))
  (ave [_ a v min-e entity-resolver-fn] (db/ave index-snapshot a v min-e entity-resolver-fn))
  (ae [_ a min-e] (db/ae index-snapshot a min-e))
  (aev [_ a e min-v entity-resolver-fn] (db/aev index-snapshot a e min-v entity-resolver-fn))
  (entity [_ e c] (db/entity index-snapshot e c))

  (entity-as-of-resolver [this eid valid-time tx-id]
    (some-> ^EntityTx (db/entity-as-of this eid valid-time tx-id)
            (.content-hash)
            c/->id-buffer))

  (entity-as-of [_ eid valid-time tx-id]
    (when capped-tx-id
      (db/entity-as-of index-snapshot eid
                       (date-min valid-time capped-valid-time)
                       (long-min tx-id capped-tx-id))))

  (entity-history [_ eid sort-order opts]
    (when capped-tx-id
      (db/entity-history index-snapshot eid sort-order
                         (case sort-order
                           :asc (-> opts
                                    (cond-> capped-valid-time (update :end-valid-time date-min (inc-date capped-valid-time)))
                                    (update-in [:end-tx ::xt/tx-id] long-min (inc capped-tx-id)))
                           :desc (-> opts
                                     (cond-> capped-valid-time (update :start-valid-time date-min capped-valid-time))
                                     (update-in [:start-tx ::xt/tx-id] long-min capped-tx-id))))))

  (resolve-tx [_ tx] (db/resolve-tx index-snapshot tx))

  (open-nested-index-snapshot ^java.io.Closeable [_]
    (->CappedIndexSnapshot (db/open-nested-index-snapshot index-snapshot)
                           capped-valid-time
                           capped-tx-id))

  db/ValueSerde
  (decode-value [_ value-buffer] (db/decode-value index-snapshot value-buffer))
  (encode-value [_ value] (db/encode-value index-snapshot value))

  db/AttributeStats
  (all-attrs [_] (db/all-attrs index-snapshot))
  (doc-count [_ attr] (db/doc-count index-snapshot attr))
  (doc-value-count [_ attr] (db/doc-value-count index-snapshot attr))
  (value-cardinality [_ attr] (db/value-cardinality index-snapshot attr))
  (eid-cardinality [_ attr] (db/eid-cardinality index-snapshot attr))

  db/IndexMeta
  (-read-index-meta [_ k not-found]
    (db/-read-index-meta index-snapshot k not-found))

  java.io.Closeable
  (close [_] (xio/try-close index-snapshot)))

(defrecord SpeculativeKvIndexStoreTx [base-index-store index-store-tx, valid-time, tx-id, !evicted-eids]
  db/IndexStoreTx
  (index-tx [_ tx]
    (db/index-tx index-store-tx tx))

  (index-docs [_ docs]
    (db/index-docs index-store-tx docs))

  (unindex-eids [_ _ eids]
    (when (seq eids)
      (swap! !evicted-eids into eids)))

  (index-entity-txs [_ entity-txs]
    (db/index-entity-txs index-store-tx entity-txs))

  (commit-index-tx [_]
    (throw (UnsupportedOperationException. "Cannot commit from a speculative tx.")))

  (abort-index-tx [_ _ _]
    (throw (UnsupportedOperationException. "Cannot commit from a speculative tx.")))

  db/IndexSnapshotFactory
  (open-index-snapshot [_]
    (fork/->MergedIndexSnapshot (-> (db/open-index-snapshot base-index-store)
                                    (->CappedIndexSnapshot valid-time tx-id))
                                (db/open-index-snapshot index-store-tx)
                                @!evicted-eids)))

(defn ->db [{:keys [index-store tx-indexer valid-time tx-id]} tx-ops]
  (let [valid-time valid-time

        conformed-tx-ops (map txc/conform-tx-op tx-ops)
        docs (->> conformed-tx-ops
                  (into {} (comp (mapcat :docs)
                                 (xio/map-vals c/xt->crux))))

        tx (merge {::xt/valid-time valid-time
                   ::txe/tx-events (map txc/->tx-event conformed-tx-ops)}
                  (if-let [latest-completed-tx (db/latest-completed-tx index-store)]
                    {::xt/tx-id (inc (long (::xt/tx-id latest-completed-tx)))
                     ::xt/tx-time (Date. (max (System/currentTimeMillis)
                                              (inc (.getTime ^Date (::xt/tx-time latest-completed-tx)))))}
                    {::xt/tx-time (Date.)
                     ::xt/tx-id 0}))

        delta-index-store (index-store/->kv-index-store {:kv-store (mut-kv/->mutable-kv-store)
                                                         :cav-cache (xtdb.cache/->cache {:cache-size (* 128 1024)})
                                                         :canonical-buffer-cache (xtdb.cache/->cache {:cache-size (* 128 1024)})
                                                         :stats-kvs-cache (xtdb.cache/->cache {:cache-size (* 128 1024)})})

        delta-index-store-tx (db/begin-index-tx delta-index-store)
        forked-index-store-tx (->SpeculativeKvIndexStoreTx index-store delta-index-store-tx valid-time tx-id (atom #{}))

        in-flight-tx (db/begin-tx (assoc tx-indexer
                                         :bus (reify xtdb.bus/EventSink
                                                (send [_ _]))
                                         :index-store (reify xtdb.db/IndexStore
                                                        (begin-index-tx [_]
                                                          forked-index-store-tx))))]

    (db/submit-docs in-flight-tx docs)
    (db/index-tx-docs in-flight-tx docs)
    (when (:committing? (db/index-tx-events in-flight-tx tx))
      (xt/db in-flight-tx valid-time))))
