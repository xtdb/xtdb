(ns crux.kv-indexer
  (:require [crux.codec :as c]
            [crux.db :as db]
            [crux.index :as idx]
            [crux.io :as cio]
            [crux.kv :as kv]
            [crux.lru :as lru]
            [crux.memory :as mem]
            [crux.status :as status])
  (:import (crux.codec EntityTx EntityValueContentHash)
           java.io.Closeable
           java.util.function.Supplier
           (clojure.lang MapEntry)
           (org.agrona DirectBuffer ExpandableDirectByteBuffer)))

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

(defrecord PrefixKvIterator [i ^DirectBuffer prefix]
  kv/KvIterator
  (seek [_ k]
    (when-let [k (kv/seek i k)]
      (when (mem/buffers=? k prefix (.capacity prefix))
        k)))

  (next [_]
    (when-let [k (kv/next i)]
      (when (mem/buffers=? k prefix (.capacity prefix))
        k)))

  (value [_]
    (kv/value i))

  Closeable
  (close [_]
    (.close ^Closeable i)))

(defn new-prefix-kv-iterator ^java.io.Closeable [i prefix]
  (->PrefixKvIterator i prefix))

(defn all-keys-in-prefix
  ([i prefix] (all-keys-in-prefix i prefix prefix {}))
  ([i prefix seek-k] (all-keys-in-prefix i prefix seek-k {}))
  ([i ^DirectBuffer prefix, ^DirectBuffer seek-k, {:keys [entries? reverse?]}]
   (letfn [(step [k]
             (lazy-seq
              (when (and k (mem/buffers=? prefix k (.capacity prefix)))
                (cons (if entries?
                        (MapEntry/create (mem/copy-to-unpooled-buffer k) (mem/copy-to-unpooled-buffer (kv/value i)))
                        (mem/copy-to-unpooled-buffer k))
                      (step (if reverse? (kv/prev i) (kv/next i)))))))]
     (step (if reverse?
             (when (kv/seek i (-> seek-k (mem/copy-buffer) (mem/inc-unsigned-buffer!)))
               (kv/prev i))
             (kv/seek i seek-k))))))

(defn- buffer-or-value-buffer [v]
  (cond
    (instance? DirectBuffer v)
    v

    (some? v)
    (c/->value-buffer v)

    :else
    c/empty-buffer))

(defn- buffer-or-id-buffer [v]
  (cond
    (instance? DirectBuffer v)
    v

    (some? v)
    (c/->id-buffer v)

    :else
    c/empty-buffer))

(defn- inc-unsigned-prefix-buffer [buffer prefix-size]
  (mem/inc-unsigned-buffer! (mem/limit-buffer (mem/copy-buffer buffer prefix-size (.get idx/seek-buffer-tl)) prefix-size)))

(defn- ->entity-tx [[k v]]
  (-> (c/decode-entity+vt+tt+tx-id-key-from k)
      (idx/enrich-entity-tx v)))

(defn entity-history-seq-ascending
  ([i eid] ([i eid] (entity-history-seq-ascending i eid {})))
  ([i eid {{^Date start-vt :crux.db/valid-time, ^Date start-tt :crux.tx/tx-time} :start
           {^Date end-vt :crux.db/valid-time, ^Date end-tt :crux.tx/tx-time} :end
           :keys [with-corrections?]}]
   (let [seek-k (c/encode-entity+vt+tt+tx-id-key-to nil (c/->id-buffer eid) start-vt)]
     (-> (all-keys-in-prefix i (mem/limit-buffer seek-k (+ c/index-id-size c/id-size)) seek-k
                             {:reverse? true, :entries? true})
         (->> (map ->entity-tx))
         (cond->> end-vt (take-while (fn [^EntityTx entity-tx]
                                       (neg? (compare (.vt entity-tx) end-vt))))
                  start-tt (remove (fn [^EntityTx entity-tx]
                                     (neg? (compare (.tt entity-tx) start-tt))))
                  end-tt (filter (fn [^EntityTx entity-tx]
                                   (neg? (compare (.tt entity-tx) end-tt)))))
         (cond-> (not with-corrections?) (->> (partition-by :vt)
                                              (map last)))))))

(defn entity-history-seq-descending
  ([i eid] (entity-history-seq-descending i eid {}))
  ([i eid {{^Date start-vt :crux.db/valid-time, ^Date start-tt :crux.tx/tx-time} :start
           {^Date end-vt :crux.db/valid-time, ^Date end-tt :crux.tx/tx-time} :end
           :keys [with-corrections?]}]
   (let [seek-k (c/encode-entity+vt+tt+tx-id-key-to nil (c/->id-buffer eid) start-vt)]
     (-> (all-keys-in-prefix i (-> seek-k (mem/limit-buffer (+ c/index-id-size c/id-size))) seek-k
                             {:entries? true})
         (->> (map ->entity-tx))
         (cond->> end-vt (take-while (fn [^EntityTx entity-tx]
                                         (pos? (compare (.vt entity-tx) end-vt))))
                  start-tt (remove (fn [^EntityTx entity-tx]
                                    (pos? (compare (.tt entity-tx) start-tt))))
                  end-tt (filter (fn [^EntityTx entity-tx]
                                   (pos? (compare (.tt entity-tx) end-tt)))))
         (cond-> (not with-corrections?) (->> (partition-by :vt)
                                              (map first)))))))

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
    (let [attr-buffer (c/->id-buffer a)
          prefix (c/encode-avec-key-to nil attr-buffer)
          i (new-prefix-kv-iterator (kv/new-iterator this) prefix)]
      (some->> (c/encode-avec-key-to
                (.get idx/seek-buffer-tl)
                attr-buffer
                (buffer-or-value-buffer min-v))
               (kv/seek i)
               ((fn step [^DirectBuffer k]
                  (when k
                    (cons (MapEntry/create (.value (c/decode-avec-key->evc-from k))
                                           :crux.index.binary-placeholder/value)
                          (lazy-seq
                           (some->> (inc-unsigned-prefix-buffer k (- (.capacity k) c/id-size c/id-size))
                                    (kv/seek i)
                                    (step))))))))))

  (ave [this a v min-e entity-resolver-fn]
    (let [attr-buffer (c/->id-buffer a)
          value-buffer (buffer-or-value-buffer v)
          prefix (c/encode-avec-key-to nil attr-buffer value-buffer)
          i (new-prefix-kv-iterator (kv/new-iterator this) prefix)]
      (some->> (c/encode-avec-key-to
                (.get idx/seek-buffer-tl)
                attr-buffer
                value-buffer
                (buffer-or-id-buffer min-e))
               (kv/seek i)
               ((fn step [^DirectBuffer k]
                  (when k
                    (let [eid (.eid (c/decode-avec-key->evc-from k))
                          eid-buffer (c/->id-buffer eid)]
                      (concat
                       (when-let [^EntityTx entity-tx (entity-resolver-fn eid-buffer)]
                         (let [version-k (c/encode-avec-key-to
                                          (.get idx/seek-buffer-tl)
                                          attr-buffer
                                          value-buffer
                                          eid-buffer
                                          (c/->id-buffer (.content-hash entity-tx)))]
                           (when (kv/get-value this version-k)
                             [(MapEntry/create eid-buffer entity-tx)])))
                       (lazy-seq
                        (some->> (inc-unsigned-prefix-buffer k (- (.capacity k) c/id-size))
                                 (kv/seek i)
                                 (step)))))))))))

  (ae [this a min-e entity-resolver-fn]
    (let [attr-buffer (c/->id-buffer a)
          prefix (c/encode-aecv-key-to nil attr-buffer)
          i (new-prefix-kv-iterator (kv/new-iterator this) prefix)]
      (some->> (c/encode-aecv-key-to
                (.get idx/seek-buffer-tl)
                attr-buffer
                (buffer-or-id-buffer min-e))
               (kv/seek i)
               ((fn step [^DirectBuffer k]
                  (when k
                    (let [eid (.eid (c/decode-aecv-key->evc-from k))
                          eid-buffer (c/->id-buffer eid)]
                      (concat
                       (when (entity-resolver-fn eid-buffer)
                         [(MapEntry/create eid-buffer :crux.index.binary-placeholder/entity)])
                       (lazy-seq
                        (some->> (inc-unsigned-prefix-buffer k (- (.capacity k) c/id-size c/id-size))
                                 (kv/seek i)
                                 (step)))))))))))

  (aev [this a e min-v entity-resolver-fn]
    (let [attr-buffer (c/->id-buffer a)
          eid-buffer (buffer-or-id-buffer e)
          ^EntityTx entity-tx (entity-resolver-fn eid-buffer)
          content-hash-buffer (c/->id-buffer (.content-hash entity-tx))
          prefix (c/encode-aecv-key-to nil attr-buffer eid-buffer content-hash-buffer)
          i (new-prefix-kv-iterator (kv/new-iterator this) prefix)]
      (some->> (c/encode-aecv-key-to
                (.get idx/seek-buffer-tl)
                attr-buffer
                eid-buffer
                content-hash-buffer
                (buffer-or-value-buffer min-v))
               (kv/seek i)
               ((fn step [^DirectBuffer k]
                  (when k
                    (cons (MapEntry/create (.value (c/decode-aecv-key->evc-from k))
                                           entity-tx)
                          (lazy-seq (step (kv/next i))))))))))

  (entity-as-of [this valid-time transact-time eid]
    (with-open [i (kv/new-iterator snapshot)]
      (idx/entity-as-of i valid-time transact-time eid)))

  (entity-history-range [this eid valid-time-start transaction-time-start valid-time-end transaction-time-end]
    (idx/entity-history-range snapshot eid valid-time-start transaction-time-start valid-time-end transaction-time-end))

  (open-entity-history [this eid sort-order opts]
    (let [i (kv/new-iterator snapshot)
          entity-history-seq (case sort-order
                               :asc entity-history-seq-ascending
                               :desc entity-history-seq-descending)]
      (cio/->cursor #(.close i)
                    (entity-history-seq i eid opts))))

  (all-content-hashes [this eid]
    (with-open [i (kv/new-iterator snapshot)]
      (->> (all-keys-in-prefix i (c/encode-aecv-key-to (.get idx/seek-buffer-tl) (c/->id-buffer :crux.db/id) (c/->id-buffer eid)))
           (map c/decode-aecv-key->evc-from)
           (map #(.content-hash ^EntityValueContentHash %))
           (set))))

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

(defn content-idx-kvs [content-hash doc]
  (let [id (c/->id-buffer (:crux.db/id doc))
        content-hash (c/->id-buffer content-hash)]
    (->> (for [[k v] doc
               :let [k (c/->id-buffer k)]
               v (idx/vectorize-value v)
               :let [v (c/->value-buffer v)]
               :when (pos? (.capacity v))]
           [(MapEntry/create (c/encode-avec-key-to nil k v id content-hash) c/empty-buffer)
            (MapEntry/create (c/encode-aecv-key-to nil k id content-hash v) c/empty-buffer)])
         (apply concat))))

(defrecord KvIndexer [kv-store object-store]
  db/Indexer
  (index-docs [this docs]
    (let [content-idx-kvs (when (seq docs)
                             (->> docs
                                  (mapcat (fn [[k doc]] (content-idx-kvs k doc)))))

          _ (some->> (seq content-idx-kvs) (kv/store kv-store))]

      (db/put-objects object-store docs)

      (->> content-idx-kvs (transduce (comp (mapcat seq) (map mem/capacity)) +))))

  (unindex-docs [this docs]
    (->> docs
         (mapcat (fn [[k doc]] (content-idx-kvs k doc)))
         keys
         (kv/delete kv-store)))

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
