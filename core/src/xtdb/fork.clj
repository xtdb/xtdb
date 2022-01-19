(ns ^:no-doc xtdb.fork
  (:require [clojure.set :as set]
            [xtdb.codec :as c]
            [xtdb.db :as db]
            [xtdb.io :as xio]
            [xtdb.memory :as mem])
  (:import xtdb.codec.EntityTx
           org.agrona.DirectBuffer
           java.util.Date))

(defn merge-seqs
  ([persistent transient] (merge-seqs persistent transient #(.compare mem/buffer-comparator %1 %2)))
  ([persistent transient compare]
   (letfn [(merge-seqs* [persistent transient]
             (lazy-seq
              (let [[i1 & more-persistent] persistent
                    [m1 & more-transient] transient]
                (cond
                  (empty? persistent) transient
                  (empty? transient) persistent

                  :else (let [cmp (compare i1 m1)]
                          (cond
                            (neg? cmp) (cons i1 (merge-seqs* more-persistent transient))
                            (zero? cmp) (cons m1 (merge-seqs* more-persistent more-transient))
                            :else (cons m1 (merge-seqs* persistent more-transient))))))))]
     (merge-seqs* persistent transient))))

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
                                    (update :end-tx-id long-min (inc capped-tx-id)))
                           :desc (-> opts
                                     (cond-> capped-valid-time (update :start-valid-time date-min capped-valid-time))
                                     (update :start-tx-id long-min capped-tx-id))))))

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
  (value-cardinality [_ attr] (db/value-cardinality index-snapshot attr))
  (eid-cardinality [_ attr] (db/eid-cardinality index-snapshot attr))

  db/IndexMeta
  (-read-index-meta [_ k not-found]
    (db/-read-index-meta index-snapshot k not-found))

  java.io.Closeable
  (close [_] (xio/try-close index-snapshot)))

(defrecord MergedIndexSnapshot [persistent-index-snapshot transient-index-snapshot evicted-eids]
  db/IndexSnapshot
  (av [_ a min-v]
    (merge-seqs (db/av persistent-index-snapshot a min-v)
                (db/av transient-index-snapshot a min-v)))

  (ave [_ a v min-e entity-resolver-fn]
    (merge-seqs (db/ave persistent-index-snapshot a v min-e entity-resolver-fn)
                (db/ave transient-index-snapshot a v min-e entity-resolver-fn)))

  (ae [_ a min-e]
    (merge-seqs (db/ae persistent-index-snapshot a min-e)
                (db/ae transient-index-snapshot a min-e)))

  (aev [_ a e min-v entity-resolver-fn]
    (merge-seqs (db/aev persistent-index-snapshot a e min-v entity-resolver-fn)
                (db/aev transient-index-snapshot a e min-v entity-resolver-fn)))

  (entity [_ e c]
    (or (db/entity transient-index-snapshot e c)
        (db/entity persistent-index-snapshot e c)))

  (entity-as-of-resolver [this eid valid-time tx-id]
    (let [eid (if (instance? DirectBuffer eid)
                (if (c/id-buffer? eid)
                  eid
                  (db/decode-value this eid))
                eid)
          eid-buffer (c/->id-buffer eid)]
      (some-> ^EntityTx (db/entity-as-of this eid-buffer valid-time tx-id)
              (.content-hash)
              c/->id-buffer)))

  (entity-as-of [this eid valid-time tx-id]
    (->> [(when-not (contains? (into #{} (map #(c/->id-buffer %)) evicted-eids) (c/->id-buffer eid))
            (db/entity-as-of persistent-index-snapshot eid valid-time tx-id))
          (db/entity-as-of transient-index-snapshot eid valid-time tx-id)]
         (remove nil?)
         (sort-by (juxt #(.vt ^EntityTx %) #(.tx-id ^EntityTx %)))
         last))

  (entity-history [this eid sort-order opts]
    (merge-seqs (when-not (contains? evicted-eids eid)
                  (db/entity-history persistent-index-snapshot eid sort-order opts))
                (db/entity-history transient-index-snapshot eid sort-order opts)

                (case [sort-order (boolean (:with-corrections? opts))]
                  [:asc false] #(compare (.vt ^EntityTx %1) (.vt ^EntityTx %2))
                  [:asc true] #(compare [(.vt ^EntityTx %1) (.tx-id ^EntityTx %1)]
                                        [(.vt ^EntityTx %2) (.tx-id ^EntityTx %2)])
                  [:desc false] #(compare (.vt ^EntityTx %2) (.vt ^EntityTx %1))
                  [:desc true] #(compare [(.vt ^EntityTx %2) (.tx-id ^EntityTx %2)]
                                         [(.vt ^EntityTx %1) (.tx-id ^EntityTx %1)]))))

  (resolve-tx [_ tx]
    (or (db/resolve-tx transient-index-snapshot tx)
        (db/resolve-tx persistent-index-snapshot tx)))

  (open-nested-index-snapshot ^java.io.Closeable [_]
    (->MergedIndexSnapshot (db/open-nested-index-snapshot persistent-index-snapshot)
                           (db/open-nested-index-snapshot transient-index-snapshot)
                           evicted-eids))

  db/ValueSerde
  (decode-value [_ value-buffer]
    (or (db/decode-value transient-index-snapshot value-buffer)
        (db/decode-value persistent-index-snapshot value-buffer)))

  (encode-value [_ value]
    (db/encode-value transient-index-snapshot value))

  db/AttributeStats
  (all-attrs [_]
    (set/union (db/all-attrs persistent-index-snapshot)
               (db/all-attrs transient-index-snapshot)))

  (doc-count [_ attr]
    (or (db/doc-count transient-index-snapshot attr)
        (db/doc-count persistent-index-snapshot attr)))

  (value-cardinality [_ attr]
    (or (db/value-cardinality transient-index-snapshot attr)
        (db/value-cardinality persistent-index-snapshot attr)))

  (eid-cardinality [_ attr]
    (or (db/eid-cardinality transient-index-snapshot attr)
        (db/eid-cardinality persistent-index-snapshot attr)))

  db/IndexMeta
  (-read-index-meta [_ k not-found]
    (let [v (db/read-index-meta transient-index-snapshot k ::not-found)]
      (if (not= v ::not-found)
        v
        (db/read-index-meta persistent-index-snapshot k not-found))))

  java.io.Closeable
  (close [_]
    (xio/try-close transient-index-snapshot)
    (xio/try-close persistent-index-snapshot)))

(defprotocol DocumentStoreTx
  (abort-doc-store-tx [document-store-tx])
  (commit-doc-store-tx [document-store-tx]))

(defrecord ForkedDocumentStore [doc-store !docs]
  db/DocumentStore
  (submit-docs [_ docs]
    (swap! !docs into docs))

  (fetch-docs [_ ids]
    (let [overridden-docs (select-keys @!docs ids)]
      (into overridden-docs (db/fetch-docs doc-store (set/difference (set ids) (set (keys overridden-docs)))))))

  DocumentStoreTx
  (abort-doc-store-tx [_]
    (when-let [docs (not-empty (->> @!docs (into {} (filter (comp :crux.db.fn/failed? val)))))]
      (db/submit-docs doc-store docs)))

  (commit-doc-store-tx [_]
    (when-let [docs (not-empty @!docs)]
      (db/submit-docs doc-store docs))))

(defn begin-document-store-tx [doc-store]
  (->ForkedDocumentStore doc-store (atom {})))
