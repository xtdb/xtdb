(ns crux.kv-indexer
  (:require [crux.codec :as c]
            [crux.db :as db]
            [crux.index :as idx]
            [crux.io :as cio]
            [crux.kv :as kv]
            [crux.lru :as lru]
            [crux.memory :as mem]
            [crux.status :as status])
  (:import crux.codec.EntityTx
           java.io.Closeable
           java.util.function.Supplier
           org.agrona.ExpandableDirectByteBuffer))

(defn etx->kvs [^EntityTx etx]
  [[(c/encode-entity+vt+tt+tx-id-key-to
     nil
     (c/->id-buffer (.eid etx))
     (.vt etx)
     (.tt etx)
     (.tx-id etx))
    (c/->id-buffer (.content-hash etx))]
   [(c/encode-entity+z+tx-id-key-to
     nil
     (c/->id-buffer (.eid etx))
     (c/encode-entity-tx-z-number (.vt etx) (.tt etx))
     (.tx-id etx))
    (c/->id-buffer (.content-hash etx))]])

(def ^:private ^ThreadLocal value-buffer-tl
  (ThreadLocal/withInitial
   (reify Supplier
     (get [_]
       (ExpandableDirectByteBuffer.)))))

(defrecord KvIndexStore [object-store snapshot]
  Closeable
  (close [_]
    (cio/try-close snapshot))

  kv/KvSnapshot
  (new-iterator ^java.io.Closeable [this]
    (kv/new-iterator snapshot))

  (get-value [this k]
    (kv/get-value snapshot k))

  db/IndexStore
  (av [this a min-v entity-resolver-fn]
    (idx/av this a min-v entity-resolver-fn))

  (ave [this a v min-e entity-resolver-fn]
    (idx/ave this a v min-e entity-resolver-fn))

  (ae [this a min-e entity-resolver-fn]
    (idx/ae this a min-e entity-resolver-fn))

  (aev [this a e min-v entity-resolver-fn]
    (idx/aev this a e min-v entity-resolver-fn))

  (entity-as-of [this valid-time transact-time eid]
    (with-open [i (kv/new-iterator snapshot)]
      (idx/entity-as-of i valid-time transact-time eid)))

  (entity-history-range [this eid valid-time-start transaction-time-start valid-time-end transaction-time-end]
    (idx/entity-history-range snapshot eid valid-time-start transaction-time-start valid-time-end transaction-time-end))

  (open-entity-history [this eid sort-order opts]
    (let [i (kv/new-iterator snapshot)
          entity-history-seq (case sort-order
                               :asc idx/entity-history-seq-ascending
                               :desc idx/entity-history-seq-descending)]
      (cio/->cursor #(.close i)
                    (entity-history-seq i eid opts))))

  (all-content-hashes [this eid]
    (idx/all-content-hashes snapshot eid))

  (decode-value [this a content-hash value-buffer]
    (assert (some? value-buffer) (str a))
    (if (c/can-decode-value-buffer? value-buffer)
      (c/decode-value-buffer value-buffer)
      (let [doc (db/get-document this content-hash)
            value-or-values (get doc a)]
        (if-not (idx/multiple-values? value-or-values)
          value-or-values
          (loop [[x & xs] (idx/vectorize-value value-or-values)]
            (if (mem/buffers=? value-buffer (c/value->buffer x (.get value-buffer-tl)))
              x
              (when xs
                (recur xs))))))))

  (encode-value [this value]
    (c/->value-buffer value))

  (get-document [this content-hash]
    (db/get-single-object object-store snapshot content-hash))

  (open-nested-index-store [this]
    (->KvIndexStore object-store (lru/new-cached-snapshot snapshot false))))

(defrecord KvIndexer [kv-store object-store]
  db/Indexer
  (index-docs [this docs]
    (let [doc-idx-keys (when (seq docs)
                         (->> docs
                              (mapcat (fn [[k doc]] (idx/doc-idx-keys k doc)))))

          _ (some->> (seq doc-idx-keys) (idx/store-doc-idx-keys kv-store))]

      (db/put-objects object-store docs)

      (->> doc-idx-keys (transduce (map mem/capacity) +))))

  (unindex-docs [this docs]
    (->> docs
         (mapcat (fn [[k doc]] (idx/doc-idx-keys k doc)))
         (idx/delete-doc-idx-keys kv-store)))

  (mark-tx-as-failed [this {:crux.tx/keys [tx-id] :as tx}]
    (kv/store kv-store [(idx/meta-kv ::latest-completed-tx tx)
                        [(c/encode-failed-tx-id-key-to nil tx-id) c/empty-buffer]]))

  (index-entity-txs [this tx entity-txs]
    (kv/store kv-store (->> (conj (mapcat etx->kvs entity-txs)
                                  (idx/meta-kv ::latest-completed-tx tx))
                            (into (sorted-map-by mem/buffer-comparator)))))

  (store-index-meta [_ k v]
    (idx/store-meta kv-store k v))

  (read-index-meta [_  k]
    (idx/read-meta kv-store k))

  (latest-completed-tx [this]
    (db/read-index-meta this ::latest-completed-tx))

  (tx-failed? [this tx-id]
    (with-open [snapshot (kv/new-snapshot kv-store)]
      (nil? (kv/get-value snapshot (c/encode-failed-tx-id-key-to nil tx-id)))))

  (open-index-store [this]
    (->KvIndexStore object-store (lru/new-cached-snapshot (kv/new-snapshot kv-store) true)))

  status/Status
  (status-map [this]
    {:crux.index/index-version (idx/current-index-version kv-store)
     :crux.doc-log/consumer-state (db/read-index-meta this :crux.doc-log/consumer-state)
     :crux.tx-log/consumer-state (db/read-index-meta this :crux.tx-log/consumer-state)}))

(def kv-indexer
  {:start-fn (fn [{:crux.node/keys [kv-store object-store]} args]
               (->KvIndexer kv-store object-store))
   :deps [:crux.node/kv-store :crux.node/object-store]})
