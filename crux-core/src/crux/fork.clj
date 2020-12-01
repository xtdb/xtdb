(ns ^:no-doc crux.fork
  (:require [clojure.set :as set]
            [crux.codec :as c]
            [crux.db :as db]
            [crux.io :as cio]
            [crux.memory :as mem])
  (:import crux.codec.EntityTx
           org.agrona.DirectBuffer
           java.util.Date))

(defn- merge-seqs
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

(defrecord ForkedIndexSnapshot [persistent-index-snapshot close-persistent-index-snapshot? transient-index-snapshot
                                evicted-eids
                                capped-valid-time capped-tx-id]
  db/IndexSnapshot
  (av [this a min-v]
    (merge-seqs (db/av persistent-index-snapshot a min-v)
                (db/av transient-index-snapshot a min-v)))

  (ave [this a v min-e entity-resolver-fn]
    (merge-seqs (db/ave persistent-index-snapshot a v min-e entity-resolver-fn)
                (db/ave transient-index-snapshot a v min-e entity-resolver-fn)))

  (ae [this a min-e]
    (merge-seqs (db/ae persistent-index-snapshot a min-e)
                (db/ae transient-index-snapshot a min-e)))

  (aev [this a e min-v entity-resolver-fn]
    (merge-seqs (db/aev persistent-index-snapshot a e min-v entity-resolver-fn)
                (db/aev transient-index-snapshot a e min-v entity-resolver-fn)))

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
    (->> [(when capped-tx-id
            (when-not (contains? (into #{} (map #(c/->id-buffer %)) evicted-eids) eid)
              (db/entity-as-of persistent-index-snapshot eid
                               (date-min valid-time capped-valid-time)
                               (long-min tx-id capped-tx-id))))
          (db/entity-as-of transient-index-snapshot eid valid-time tx-id)]
         (remove nil?)
         (sort-by (juxt #(.vt ^EntityTx %) #(.tx-id ^EntityTx %)))
         last))

  (entity-history [this eid sort-order opts]
    (merge-seqs (when capped-tx-id
                  (when-not (contains? evicted-eids eid)
                    (db/entity-history persistent-index-snapshot eid sort-order
                                       (case sort-order
                                         :asc (-> opts
                                                  (cond-> capped-valid-time (update :end-valid-time date-min (inc-date capped-valid-time)))
                                                  (update :end-tx-id long-min (inc capped-tx-id)))
                                         :desc (-> opts
                                                   (cond-> capped-valid-time (update :start-valid-time date-min capped-valid-time))
                                                   (update :start-tx-id long-min capped-tx-id))))))
                (db/entity-history transient-index-snapshot eid sort-order opts)

                (case [sort-order (boolean (:with-corrections? opts))]
                  [:asc false] #(compare (.vt ^EntityTx %1) (.vt ^EntityTx %2))
                  [:asc true] #(compare [(.vt ^EntityTx %1) (.tx-id ^EntityTx %1)]
                                        [(.vt ^EntityTx %2) (.tx-id ^EntityTx %2)])
                  [:desc false] #(compare (.vt ^EntityTx %2) (.vt ^EntityTx %1))
                  [:desc true] #(compare [(.vt ^EntityTx %2) (.tx-id ^EntityTx %2)]
                                         [(.vt ^EntityTx %1) (.tx-id ^EntityTx %1)]))))

  (decode-value [this value-buffer]
    (or (db/decode-value transient-index-snapshot value-buffer)
        (db/decode-value persistent-index-snapshot value-buffer)))

  (encode-value [this value]
    (db/encode-value transient-index-snapshot value))

  (resolve-tx [this tx]
    (or (db/resolve-tx transient-index-snapshot tx)
        (db/resolve-tx persistent-index-snapshot tx)))

  (open-nested-index-snapshot ^java.io.Closeable [this]
    (->ForkedIndexSnapshot (db/open-nested-index-snapshot persistent-index-snapshot)
                           true
                           (db/open-nested-index-snapshot transient-index-snapshot)
                           evicted-eids
                           capped-valid-time
                           capped-tx-id))

  java.io.Closeable
  (close [_]
    (cio/try-close transient-index-snapshot)
    (when close-persistent-index-snapshot?
      (cio/try-close persistent-index-snapshot))))

(defrecord ForkedIndexStore [persistent-index-store persistent-index-snapshot transient-index-store
                             !indexed-docs !evicted-eids !etxs
                             capped-valid-time capped-tx-id]
  db/IndexStore
  (index-docs [this docs]
    (swap! !indexed-docs into docs)
    (db/index-docs transient-index-store docs))

  (unindex-eids [this eids]
    (swap! !evicted-eids set/union (set eids))
    (db/unindex-eids transient-index-store eids)

    (with-open [transient-index-snapshot (db/open-index-snapshot transient-index-store)]
      (let [tombstones (->> (for [eid eids
                                  etx (concat (db/entity-history persistent-index-snapshot eid :asc
                                                                 {:with-corrections? true})
                                              (db/entity-history transient-index-snapshot eid :asc
                                                                 {:with-corrections? true}))
                                  :let [content-hash (.content-hash ^EntityTx etx)]
                                  :when content-hash]
                              [content-hash {:crux.db/id eid, :crux.db/evicted? true}])
                            (into {}))]
        {:tombstones tombstones})))

  (index-entity-txs [this tx entity-txs]
    (swap! !etxs into (->> entity-txs
                           (into {} (map (juxt (fn [^EntityTx etx]
                                                 [(.eid etx) (.vt etx) (.tt etx) (.tx-id etx)])
                                               identity)))))
    (db/index-entity-txs transient-index-store tx entity-txs))

  (store-index-meta [this k v]
    (db/store-index-meta transient-index-store k v))

  (read-index-meta [this k]
    (db/read-index-meta this k nil))

  (read-index-meta [this k not-found]
    (let [v (db/read-index-meta transient-index-store k ::not-found)]
      (if (not= v ::not-found)
        v
        (db/read-index-meta persistent-index-store k not-found))))

  (latest-completed-tx [this]
    (or (db/latest-completed-tx transient-index-store)
        (db/latest-completed-tx persistent-index-store)))

  (mark-tx-as-failed [this tx]
    (db/mark-tx-as-failed transient-index-store tx))

  (tx-failed? [this tx-id]
    (or (db/tx-failed? transient-index-store tx-id)
        (db/tx-failed? persistent-index-store tx-id)))

  (open-index-snapshot ^java.io.Closeable [this]
    (->ForkedIndexSnapshot (or persistent-index-snapshot (db/open-index-snapshot persistent-index-store))
                           (nil? persistent-index-snapshot)
                           (db/open-index-snapshot transient-index-store)
                           @!evicted-eids
                           capped-valid-time
                           capped-tx-id)))

(defn indexed-docs [index-store]
  @(:!indexed-docs index-store))

(defn newly-evicted-eids [index-store]
  @(:!evicted-eids index-store))

(defn new-etxs [index-store]
  (vals @(:!etxs index-store)))

(defn ->forked-index-store [persistent-index-store transient-index-store
                            capped-valid-time capped-tx-id]
  (->ForkedIndexStore persistent-index-store nil transient-index-store
                      (atom {}) (atom #{}) (atom {})
                      capped-valid-time capped-tx-id))

(defrecord ForkedDocumentStore [doc-store !docs]
  db/DocumentStore
  (submit-docs [_ docs]
    (swap! !docs into docs))

  (fetch-docs [_ ids]
    (let [overridden-docs (select-keys @!docs ids)]
      (into overridden-docs (db/fetch-docs doc-store (set/difference (set ids) (set (keys overridden-docs))))))))

(defn new-docs [doc-store]
  @(:!docs doc-store))

(defn ->forked-document-store [doc-store]
  (->ForkedDocumentStore doc-store (atom {})))
