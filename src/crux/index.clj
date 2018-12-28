(ns crux.index
  (:require [clojure.tools.logging :as log]
            [clojure.set :as set]
            [crux.codec :as c]
            [crux.db :as db]
            [crux.kv :as kv]
            [crux.memory :as mem]
            [taoensso.nippy :as nippy])
  (:import java.io.Closeable
           [java.util Arrays Collections Comparator]
           [org.agrona DirectBuffer ExpandableDirectByteBuffer]
           [crux.codec EntityTx Id]))

(set! *unchecked-math* :warn-on-boxed)

;; NOTE: A buffer returned from an kv/KvIterator can only be assumed
;; to be valid until the next call on the same iterator. In practice
;; this limitation is only for RocksJNRKv.
;; TODO: It would be nice to make this explicit somehow.

;; Indexes

(defrecord PrefixKvIterator [i ^DirectBuffer prefix]
  kv/KvIterator
  (seek [_ k]
    (when-let [k (kv/seek i k)]
      (when (mem/buffers=? k prefix (mem/capacity prefix))
        k)))

  (next [_]
    (when-let [k (kv/next i)]
      (when (mem/buffers=? k prefix (mem/capacity prefix))
        k)))

  (value [_]
    (kv/value i))

  Closeable
  (close [_]
    (.close ^Closeable i)))

(defn new-prefix-kv-iterator ^java.io.Closeable [i prefix]
  (->PrefixKvIterator i prefix))

;; AVE

(defn- attribute-value+placeholder [k peek-state]
  (let [value (.value (c/decode-attribute+value+entity+content-hash-key->value+entity+content-hash-from k))]
    (reset! peek-state {:last-k k :value value})
    [value :crux.index.binary-placeholder/value]))

(defrecord DocAttributeValueEntityValueIndex [i ^DirectBuffer attr eb peek-state]
  db/Index
  (seek-values [this k]
    (when-let [k (->> (c/encode-attribute+value+entity+content-hash-key-to
                       eb
                       attr
                       (or k c/empty-buffer))
                      (kv/seek i))]
      (attribute-value+placeholder k peek-state)))

  (next-values [this]
    (let [{:keys [last-k]} @peek-state
          prefix-size (- (mem/capacity last-k) c/id-size c/id-size)]
      (when-let [k (some->> (mem/inc-unsigned-buffer! (mem/limit-buffer (mem/copy-buffer last-k prefix-size eb)
                                                                        prefix-size))
                            (kv/seek i))]
        (attribute-value+placeholder k peek-state)))))

(defn new-doc-attribute-value-entity-value-index [snapshot attr]
  (let [attr (c/->id-buffer attr)
        prefix (c/encode-attribute+value+entity+content-hash-key-to nil attr)]
    (->DocAttributeValueEntityValueIndex (new-prefix-kv-iterator (kv/new-iterator snapshot) prefix) attr (ExpandableDirectByteBuffer.) (atom nil))))

(defn- attribute-value-entity-entity+value [i ^DirectBuffer current-k attr value entity-as-of-idx eb peek-eb peek-state]
  (loop [k current-k]
    (let [limit (- (mem/capacity k) c/id-size)]
      (reset! peek-state (mem/inc-unsigned-buffer! (mem/limit-buffer (mem/copy-buffer k limit peek-eb) limit))))
    (or (let [eid (.eid (c/decode-attribute+value+entity+content-hash-key->value+entity+content-hash-from k))
              eid-buffer (mem/copy-buffer (c/->id-buffer eid))
              [_ ^EntityTx entity-tx] (db/seek-values entity-as-of-idx eid-buffer)]
          (when entity-tx
            (let [version-k (c/encode-attribute+value+entity+content-hash-key-to
                             eb
                             attr
                             value
                             eid-buffer
                             (c/->id-buffer (.content-hash entity-tx)))]
              (when-let [found-k (kv/seek i version-k)]
                (when (mem/buffers=? version-k found-k)
                  [eid-buffer entity-tx])))))
        (when-let [k (some->> @peek-state (kv/seek i))]
          (recur k)))))

(defn- attribute-value-value+prefix-iterator [i ^DocAttributeValueEntityValueIndex value-entity-value-idx attr prefix-eb]
  (let [{:keys [value]} @(.peek-state value-entity-value-idx)
        prefix (c/encode-attribute+value+entity+content-hash-key-to prefix-eb attr value)]
    [value (new-prefix-kv-iterator i prefix)]))

(defrecord DocAttributeValueEntityEntityIndex [i ^DirectBuffer attr value-entity-value-idx entity-as-of-idx prefix-eb peek-eb eb peek-state]
  db/Index
  (seek-values [this k]
    (let [[value i] (attribute-value-value+prefix-iterator i value-entity-value-idx attr prefix-eb)]
      (when-let [k (->> (c/encode-attribute+value+entity+content-hash-key-to
                         eb
                         attr
                         value
                         (or k c/empty-buffer))
                        (kv/seek i))]
        (attribute-value-entity-entity+value i k attr value entity-as-of-idx eb peek-eb peek-state))))

  (next-values [this]
    (let [[value i] (attribute-value-value+prefix-iterator i value-entity-value-idx attr prefix-eb)]
      (when-let [k (some->> @peek-state (kv/seek i))]
        (attribute-value-entity-entity+value i k attr value entity-as-of-idx eb peek-eb peek-state)))))

(defn new-doc-attribute-value-entity-entity-index [snapshot attr value-entity-value-idx entity-as-of-idx]
  (->DocAttributeValueEntityEntityIndex (kv/new-iterator snapshot) (c/->id-buffer attr) value-entity-value-idx entity-as-of-idx
                                        (ExpandableDirectByteBuffer.) (ExpandableDirectByteBuffer.) (ExpandableDirectByteBuffer.) (atom nil)))

;; AEV

(defn- attribute-entity+placeholder [k attr entity-as-of-idx peek-state]
  (let [eid (.eid (c/decode-attribute+entity+value+content-hash-key->entity+value+content-hash-from k))
        eid-buffer (c/->id-buffer eid)
        [_ entity-tx] (db/seek-values entity-as-of-idx eid-buffer)]
    (reset! peek-state {:last-k k :entity-tx entity-tx})
    (if entity-tx
      [eid-buffer :crux.index.binary-placeholder/entity]
      ::deleted-entity)))

(defrecord DocAttributeEntityValueEntityIndex [i ^DirectBuffer attr entity-as-of-idx eb peek-state]
  db/Index
  (seek-values [this k]
    (when-let [k (->> (c/encode-attribute+entity+value+content-hash-key-to
                       eb
                       attr
                       (or k c/empty-buffer))
                      (kv/seek i))]
      (let [placeholder (attribute-entity+placeholder k attr entity-as-of-idx peek-state)]
        (if (= ::deleted-entity placeholder)
          (db/next-values this)
          placeholder))))

  (next-values [this]
    (let [{:keys [last-k]} @peek-state
          prefix-size (+ c/index-id-size c/id-size c/id-size)]
      (when-let [k (some->> (mem/inc-unsigned-buffer! (mem/limit-buffer (mem/copy-buffer last-k prefix-size eb) prefix-size))
                            (kv/seek i))]
        (let [placeholder (attribute-entity+placeholder k attr entity-as-of-idx peek-state)]
          (if (= ::deleted-entity placeholder)
            (db/next-values this)
            placeholder))))))

(defn new-doc-attribute-entity-value-entity-index [snapshot attr entity-as-of-idx]
  (let [attr (c/->id-buffer attr)
        prefix (c/encode-attribute+entity+value+content-hash-key-to nil attr)]
    (->DocAttributeEntityValueEntityIndex (new-prefix-kv-iterator (kv/new-iterator snapshot) prefix) attr entity-as-of-idx
                                          (ExpandableDirectByteBuffer.) (atom nil))))

(defn- attribute-entity-value-value+entity [i ^DirectBuffer current-k attr ^EntityTx entity-tx eb peek-eb peek-state]
  (when entity-tx
    (let [eid (.eid entity-tx)
          content-hash (.content-hash entity-tx)]
      (loop [k current-k]
        (let [limit (- (mem/capacity k) c/id-size)]
          (reset! peek-state (mem/inc-unsigned-buffer! (mem/limit-buffer (mem/copy-buffer k limit peek-eb) limit))))
        (or (let [value (mem/copy-buffer (.value (c/decode-attribute+entity+value+content-hash-key->entity+value+content-hash-from k)))
                  version-k (c/encode-attribute+entity+value+content-hash-key-to
                             eb
                             attr
                             (c/->id-buffer eid)
                             value
                             (c/->id-buffer content-hash))]
              (when-let [found-k (kv/seek i version-k)]
                (when (mem/buffers=? version-k found-k)
                  [value entity-tx])))
            (when-let [k (some->> @peek-state (kv/seek i))]
              (recur k)))))))

(defn- attribute-value-entity-tx+prefix-iterator [i ^DocAttributeEntityValueEntityIndex entity-value-entity-idx attr prefix-eb]
  (let [{:keys [^EntityTx entity-tx]} @(.peek-state entity-value-entity-idx)
        prefix (c/encode-attribute+entity+value+content-hash-key-to prefix-eb attr (c/->id-buffer (.eid entity-tx)))]
    [entity-tx (new-prefix-kv-iterator i prefix)]))

(defrecord DocAttributeEntityValueValueIndex [i ^DirectBuffer attr entity-value-entity-idx prefix-eb peek-eb eb peek-state]
  db/Index
  (seek-values [this k]
    (let [[^EntityTx entity-tx i] (attribute-value-entity-tx+prefix-iterator i entity-value-entity-idx attr prefix-eb)]
      (when-let [k (->> (c/encode-attribute+entity+value+content-hash-key-to
                         eb
                         attr
                         (c/->id-buffer (.eid entity-tx))
                         (or k c/empty-buffer))
                        (kv/seek i))]
        (attribute-entity-value-value+entity i k attr entity-tx eb peek-eb peek-state))))

  (next-values [this]
    (let [[entity-tx i] (attribute-value-entity-tx+prefix-iterator i entity-value-entity-idx attr prefix-eb)]
      (when-let [k (some->> @peek-state (kv/seek i))]
        (attribute-entity-value-value+entity i k attr entity-tx eb peek-eb peek-state)))))

(defn new-doc-attribute-entity-value-value-index [snapshot attr entity-value-entity-idx]
  (->DocAttributeEntityValueValueIndex (kv/new-iterator snapshot) (c/->id-buffer attr) entity-value-entity-idx
                                       (ExpandableDirectByteBuffer.) (ExpandableDirectByteBuffer.) (ExpandableDirectByteBuffer.) (atom nil)))

;; Range Constraints

(defrecord PredicateVirtualIndex [idx pred seek-k-fn]
  db/Index
  (seek-values [this k]
    (when-let [value+results (db/seek-values idx (seek-k-fn k))]
      (when (pred (first value+results))
        value+results)))

  (next-values [this]
    (when-let [value+results (db/next-values idx)]
      (when (pred (first value+results))
        value+results))))

(defn- value-comparsion-predicate
  ([compare-pred compare-v]
   (value-comparsion-predicate compare-pred compare-v Integer/MAX_VALUE))
  ([compare-pred ^DirectBuffer compare-v max-length]
   (if compare-v
     (fn [value]
       (and value (compare-pred (mem/compare-buffers value compare-v max-length))))
     (constantly true))))

(defn new-less-than-equal-virtual-index [idx max-v]
  (let [pred (value-comparsion-predicate (comp not pos?) (c/->value-buffer max-v))]
    (->PredicateVirtualIndex idx pred identity)))

(defn new-less-than-virtual-index [idx max-v]
  (let [pred (value-comparsion-predicate neg? (c/->value-buffer max-v))]
    (->PredicateVirtualIndex idx pred identity)))

(defn new-greater-than-equal-virtual-index [idx min-v]
  (let [min-v (c/->value-buffer min-v)
        pred (value-comparsion-predicate (comp not neg?) min-v)]
    (->PredicateVirtualIndex idx pred (fn [k]
                                        (if (pred k)
                                          k
                                          min-v)))))

(defrecord GreaterThanVirtualIndex [idx]
  db/Index
  (seek-values [this k]
    (or (db/seek-values idx k)
        (db/next-values idx)))

  (next-values [this]
    (db/next-values idx)))

(defn new-greater-than-virtual-index [idx min-v]
  (let [min-v (c/->value-buffer min-v)
        pred (value-comparsion-predicate pos? min-v)
        idx (->PredicateVirtualIndex idx pred (fn [k]
                                                (if (pred k)
                                                  k
                                                  min-v)))]
    (->GreaterThanVirtualIndex idx)))

(defn new-prefix-equal-virtual-index [idx ^DirectBuffer prefix-v]
  (let [seek-k-pred (value-comparsion-predicate (comp not neg?) prefix-v (mem/capacity prefix-v))
        pred (value-comparsion-predicate zero? prefix-v (mem/capacity prefix-v))]
    (->PredicateVirtualIndex idx pred (fn [k]
                                        (if (seek-k-pred k)
                                          k
                                          prefix-v)))))

(defn wrap-with-range-constraints [idx range-constraints]
  (if range-constraints
    (range-constraints idx)
    idx))

;; Meta

(defn store-meta [kv k v]
  (kv/store kv [[(c/encode-meta-key-to nil (c/->id-buffer k))
                 (mem/->off-heap (nippy/fast-freeze v))]]))

(defn read-meta [kv k]
  (let [seek-k (c/encode-meta-key-to nil (c/->id-buffer k))]
    (with-open [snapshot (kv/new-snapshot kv)
                i (kv/new-iterator snapshot)]
      (when-let [k (kv/seek i seek-k)]
        (when (mem/buffers=? seek-k k)
          (nippy/fast-thaw (mem/->on-heap (kv/value i))))))))

;; Object Store

(defn normalize-value [v]
  (cond-> v
    (not (or (vector? v)
             (set? v))) (vector)))

(defn- normalize-doc [doc]
  (->> (for [[k v] doc]
         [k (normalize-value v)])
       (into {})))

(defn- doc-predicate-stats [normalized-doc]
  (->> (for [[k v] normalized-doc]
         [k (count v)])
       (into {})))

(defn- update-predicate-stats [kv f normalized-doc]
  (->> (doc-predicate-stats normalized-doc)
       (merge-with f (read-meta kv :crux.kv/stats))
       (store-meta kv :crux.kv/stats)))

(defn index-doc [kv content-hash doc]
  (let [id (c/->id-buffer (:crux.db/id doc))
        content-hash (c/->id-buffer content-hash)
        normalized-doc (normalize-doc doc)]
    (kv/store kv (->> (for [[k v] normalized-doc
                            :let [k (c/->id-buffer k)]
                            v v
                            :let [v (c/->value-buffer v)]
                            :when (pos? (mem/capacity v))]
                        [[(c/encode-attribute+value+entity+content-hash-key-to nil k v id content-hash)
                          c/empty-buffer]
                         [(c/encode-attribute+entity+value+content-hash-key-to nil k id v content-hash)
                          c/empty-buffer]])
                      (reduce into [])))
    (update-predicate-stats kv + normalized-doc)))

(defn delete-doc-from-index [kv content-hash doc]
  (let [id (c/->id-buffer (:crux.db/id doc))
        content-hash (c/->id-buffer content-hash)
        normalized-doc (normalize-doc doc)]
    (kv/delete kv (->> (for [[k v] normalized-doc
                             :let [k (c/->id-buffer k)]
                             v v
                             :let [v (c/->value-buffer v)]
                             :when (pos? (mem/capacity v))]
                         [(c/encode-attribute+value+entity+content-hash-key-to nil k v id content-hash)
                          (c/encode-attribute+entity+value+content-hash-key-to nil k id v content-hash)])
                       (reduce into [])))
    (update-predicate-stats kv - normalized-doc)))

(defrecord KvObjectStore [kv]
  Closeable
  (close [_])

  db/ObjectStore
  (get-single-object [this snapshot k]
    (with-open [i (kv/new-iterator snapshot)]
      (let [doc-key (c/->id-buffer k)
            seek-k (c/encode-doc-key-to nil doc-key)
            k (kv/seek i seek-k)]
        (when (and k (mem/buffers=? seek-k k))
          (nippy/fast-thaw (mem/->on-heap (kv/value i)))))))

  (get-objects [this snapshot ks]
    (with-open [i (kv/new-iterator snapshot)]
      (->> (for [k ks
                 :let [seek-k (c/encode-doc-key-to nil (c/->id-buffer k))
                       k (kv/seek i seek-k)]
                 :when (and k (mem/buffers=? seek-k k))]
             [(c/safe-id (c/decode-doc-key-from k))
              (nippy/fast-thaw (mem/->on-heap (kv/value i)))])
           (into {}))))

  (put-objects [this kvs]
    (kv/store kv (for [[k v] kvs]
                   [(c/encode-doc-key-to nil (c/->id-buffer k))
                    (mem/->off-heap (nippy/fast-freeze v))])))

  (delete-objects [this ks]
    (kv/delete kv (for [k ks]
                    (c/encode-doc-key-to nil (c/->id-buffer k)))))

  Closeable
  (close [_]))

;; Utils

(defn all-keys-in-prefix
  ([i prefix]
   (all-keys-in-prefix i prefix false))
  ([i ^DirectBuffer prefix entries?]
   ((fn step [f-cons f-next]
      (lazy-seq
       (let [k (f-cons)]
         (when (and k (mem/buffers=? prefix k (mem/capacity prefix)))
           (cons (if entries?
                   [k (kv/value i)]
                   k) (step f-next f-next))))))
    #(kv/seek i prefix) #(kv/next i))))

(defn idx->seq [idx]
  (when-let [result (db/seek-values idx nil)]
    (->> (repeatedly #(db/next-values idx))
         (take-while identity)
         (cons result))))

;; Entities

(defn- ^EntityTx enrich-entity-tx [entity-tx ^DirectBuffer content-hash]
  (assoc entity-tx :content-hash (when (pos? (mem/capacity content-hash))
                                   (c/new-id content-hash))))

(defrecord EntityAsOfIndex [i business-time transact-time eb]
  db/Index
  (db/seek-values [this k]
    (let [prefix-size (+ c/index-id-size c/id-size)
          seek-k (c/encode-entity+bt+tt+tx-id-key-to
                  eb
                  k
                  business-time
                  transact-time
                  nil)]
      (loop [k (kv/seek i seek-k)]
        (when (and k (mem/buffers=? seek-k k prefix-size))
          (let [v (kv/value i)
                entity-tx (c/decode-entity+bt+tt+tx-id-key-from k)]
            (if (<= (compare (.tt entity-tx) transact-time) 0)
              (when-not (mem/buffers=? c/nil-id-buffer v)
                [(c/->id-buffer (.eid entity-tx))
                 (enrich-entity-tx entity-tx v)])
              (recur (kv/next i))))))))

  (db/next-values [this]
    (throw (UnsupportedOperationException.))))

(defn new-entity-as-of-index [snapshot business-time transact-time]
  (->EntityAsOfIndex (kv/new-iterator snapshot) business-time transact-time (ExpandableDirectByteBuffer.)))

(defn safe-entity-tx [entity-tx]
  (-> entity-tx
      (update :eid c/safe-id)
      (update :content-hash c/safe-id)))

(defn entities-at [snapshot eids business-time transact-time]
  (let [entity-as-of-idx (new-entity-as-of-index snapshot business-time transact-time)]
    (some->> (for [eid eids
                   :let [[_ entity-tx] (db/seek-values entity-as-of-idx (c/->id-buffer eid))]
                   :when entity-tx]
               (safe-entity-tx entity-tx))
             (not-empty)
             (vec))))

(defn all-entities [snapshot business-time transact-time]
  (with-open [i (kv/new-iterator snapshot)]
    (let [eids (->> (all-keys-in-prefix i (c/encode-entity+bt+tt+tx-id-key-to nil))
                    (map (comp :eid c/decode-entity+bt+tt+tx-id-key-from))
                    (distinct))]
      (entities-at snapshot eids business-time transact-time))))

(defn entity-history [snapshot eid]
  (with-open [i (kv/new-iterator snapshot)]
    (let [seek-k (c/encode-entity+bt+tt+tx-id-key-to nil (c/->id-buffer eid))]
      (vec (for [[k v] (all-keys-in-prefix i seek-k true)]
             (-> (c/decode-entity+bt+tt+tx-id-key-from k)
                 (enrich-entity-tx v)
                 (safe-entity-tx)))))))

;; Join

(extend-protocol db/LayeredIndex
  Object
  (open-level [_])
  (close-level [_])
  (max-depth [_] 1))

(def ^:private sorted-virtual-index-key-comparator
  (reify Comparator
    (compare [_ [a] [b]]
      (mem/compare-buffers (or a c/nil-id-buffer)
                           (or b c/nil-id-buffer)))))

(defrecord SortedVirtualIndex [values seq-state]
  db/Index
  (seek-values [this k]
    (let [idx (Collections/binarySearch values
                                        [k]
                                        sorted-virtual-index-key-comparator)
          [x & xs] (subvec values (if (neg? idx)
                                    (dec (- idx))
                                    idx))
          {:keys [fst]} (reset! seq-state {:fst x :rest xs})]
      (if fst
        fst
        (reset! seq-state nil))))

  (next-values [this]
    (let [{:keys [fst]} (swap! seq-state (fn [{[x & xs] :rest
                                               :as seq-state}]
                                           (assoc seq-state :fst x :rest xs)))]
      (when fst
        fst))))

(defn new-sorted-virtual-index [idx-or-seq]
  (let [idx-as-seq (if (satisfies? db/Index idx-or-seq)
                     (idx->seq idx-or-seq)
                     idx-or-seq)]
    (->SortedVirtualIndex
     (->> idx-as-seq
          (sort-by first mem/buffer-comparator)
          (distinct)
          (vec))
     (atom nil))))

(defrecord OrVirtualIndex [indexes peek-state]
  db/Index
  (seek-values [this k]
    (reset! peek-state (vec (for [idx indexes]
                              (db/seek-values idx k))))
    (db/next-values this))

  (next-values [this]
    (let [[n value] (->> (map-indexed vector @peek-state)
                         (remove (comp nil? second))
                         (sort-by (comp first second) mem/buffer-comparator)
                         (first))]
      (when n
        (swap! peek-state assoc n (db/next-values (get indexes n))))
      value)))

(defn new-or-virtual-index [indexes]
  (->OrVirtualIndex indexes (atom nil)))

(defn or-known-triple-fast-path [snapshot e a v business-time transact-time]
  (when-let [[^EntityTx entity-tx] (entities-at snapshot [e] business-time transact-time)]
    (let [version-k (c/encode-attribute+entity+value+content-hash-key-to
                     nil
                     a
                     (c/->id-buffer (.eid entity-tx))
                     v
                     (c/->id-buffer (.content-hash entity-tx)))]
      (with-open [i (new-prefix-kv-iterator (kv/new-iterator snapshot) version-k)]
        (when (kv/seek i version-k)
          entity-tx)))))

(defn- new-unary-join-iterator-state [idx [value results]]
  (let [result-name (:name idx)]
    {:idx idx
     :key (or value c/nil-id-buffer)
     :result-name result-name
     :results (when (and result-name results)
                {result-name results})}))

(defrecord UnaryJoinVirtualIndex [indexes iterators-thunk-state]
  db/Index
  (seek-values [this k]
    (->> #(let [iterators (->> (for [idx indexes]
                                 (new-unary-join-iterator-state idx (db/seek-values idx k)))
                               (sort-by :key mem/buffer-comparator)
                               (vec))]
            {:iterators iterators :index 0})
         (reset! iterators-thunk-state))
    (db/next-values this))

  (next-values [this]
    (when-let [iterators-thunk @iterators-thunk-state]
      (when-let [{:keys [iterators ^long index]} (iterators-thunk)]
        (let [{:keys [key result-name idx]} (get iterators index)
              max-index (mod (dec index) (count iterators))
              max-k (:key (get iterators max-index))
              match? (mem/buffers=? key max-k)]
          (->> #(let [next-value+results (if match?
                                           (do (log/debug :next result-name)
                                               (db/next-values idx))
                                           (do (log/debug :seek result-name (mem/buffer->hex max-k))
                                               (db/seek-values idx max-k)))]
                  (when next-value+results
                    {:iterators (assoc iterators index (new-unary-join-iterator-state idx next-value+results))
                     :index (mod (inc index) (count iterators))}))
               (reset! iterators-thunk-state))
          (if match?
            (let [names (map :result-name iterators)]
              (log/debug :match names (mem/buffer->hex max-k))
              (when-let [result (->> (map :results iterators)
                                     (apply merge))]
                [max-k result]))
            (recur))))))

  db/LayeredIndex
  (open-level [this]
    (doseq [idx indexes]
      (db/open-level idx)))

  (close-level [this]
    (doseq [idx indexes]
      (db/close-level idx)))

  (max-depth [this]
    1))

(defn new-unary-join-virtual-index [indexes]
  (->UnaryJoinVirtualIndex indexes (atom nil)))

(defn constrain-join-result-by-empty-names [join-keys join-results]
  (when (not-any? nil? (vals join-results))
    join-results))

(defrecord NAryJoinLayeredVirtualIndex [unary-join-indexes depth-state]
  db/Index
  (seek-values [this k]
    (db/seek-values (get unary-join-indexes @depth-state) k))

  (next-values [this]
    (db/next-values (get unary-join-indexes @depth-state)))

  db/LayeredIndex
  (open-level [this]
    (db/open-level (get unary-join-indexes @depth-state))
    (swap! depth-state inc)
    nil)

  (close-level [this]
    (db/close-level (get unary-join-indexes (dec (long @depth-state))))
    (swap! depth-state dec)
    nil)

  (max-depth [this]
    (count unary-join-indexes)))

(defn new-n-ary-join-layered-virtual-index [indexes]
  (->NAryJoinLayeredVirtualIndex indexes (atom 0)))

(defrecord BinaryJoinLayeredVirtualIndex [index-and-depth-state]
  db/Index
  (seek-values [this k]
    (let [{:keys [indexes depth]} @index-and-depth-state]
      (db/seek-values (get indexes depth) k)))

  (next-values [this]
    (let [{:keys [indexes depth]} @index-and-depth-state]
      (db/next-values (get indexes depth))))

  db/LayeredIndex
  (open-level [this]
    (let [{:keys [indexes depth]} @index-and-depth-state]
      (db/open-level (get indexes depth)))
    (swap! index-and-depth-state update :depth inc)
    nil)

  (close-level [this]
    (let [{:keys [indexes depth]} @index-and-depth-state]
      (db/close-level (get indexes (dec (long depth)))))
    (swap! index-and-depth-state update :depth dec)
    nil)

  (max-depth [this]
    (count (:indexes @index-and-depth-state))))

(defn new-binary-join-virtual-index
  ([]
   (new-binary-join-virtual-index nil nil))
  ([lhs-index rhs-index]
   (->BinaryJoinLayeredVirtualIndex (atom {:depth 0
                                           :indexes [lhs-index rhs-index]}))))

(defn update-binary-join-order! [binary-join-index lhs-index rhs-index]
  (swap! (:index-and-depth-state binary-join-index) assoc :indexes [lhs-index rhs-index])
  binary-join-index)

(defn- build-constrained-result [constrain-result-fn result-stack [max-k new-values]]
  (let [[max-ks parent-result] (last result-stack)
        join-keys (conj (or max-ks []) max-k)]
    (when-let [join-results (->> (merge parent-result new-values)
                                 (constrain-result-fn join-keys)
                                 (not-empty))]
      (conj result-stack [join-keys join-results]))))

(defrecord NAryConstrainingLayeredVirtualIndex [n-ary-index constrain-result-fn walk-state]
  db/Index
  (seek-values [this k]
    (when-let [values (db/seek-values n-ary-index k)]
      (if-let [result (build-constrained-result constrain-result-fn (:result-stack @walk-state) values)]
        (do (swap! walk-state assoc :last result)
            [(first values) (second (last result))])
        (db/next-values this))))

  (next-values [this]
    (when-let [values (db/next-values n-ary-index)]
      (if-let [result (build-constrained-result constrain-result-fn (:result-stack @walk-state) values)]
        (do (swap! walk-state assoc :last result)
            [(first values) (second (last result))])
        (recur))))

  db/LayeredIndex
  (open-level [this]
    (db/open-level n-ary-index)
    (swap! walk-state #(assoc % :result-stack (:last %)))
    nil)

  (close-level [this]
    (db/close-level n-ary-index)
    (swap! walk-state update :result-stack pop)
    nil)

  (max-depth [this]
    (db/max-depth n-ary-index)))

(defn new-n-ary-constraining-layered-virtual-index [idx constrain-result-fn]
  (->NAryConstrainingLayeredVirtualIndex idx constrain-result-fn (atom {:result-stack [] :last nil})))

(defn layered-idx->seq [idx]
  (let [max-depth (long (db/max-depth idx))
        build-result (fn [max-ks [max-k new-values]]
                       (when new-values
                         (conj max-ks max-k)))
        build-leaf-results (fn [max-ks idx]
                             (for [result (idx->seq idx)
                                   :let [leaf-key (build-result max-ks result)]
                                   :when leaf-key]
                               [leaf-key (last result)]))
        step (fn step [max-ks ^long depth needs-seek?]
               (when (Thread/interrupted)
                 (throw (InterruptedException.)))
               (let [close-level (fn []
                                   (when (pos? depth)
                                     (lazy-seq
                                      (db/close-level idx)
                                      (step (pop max-ks) (dec depth) false))))
                     open-level (fn [result]
                                  (db/open-level idx)
                                  (if-let [max-ks (build-result max-ks result)]
                                    (step max-ks (inc depth) true)
                                    (do (db/close-level idx)
                                        (step max-ks depth false))))]
                 (if (= depth (dec max-depth))
                   (concat (build-leaf-results max-ks idx)
                           (close-level))
                   (if-let [result (if needs-seek?
                                     (db/seek-values idx nil)
                                     (db/next-values idx))]
                     (open-level result)
                     (close-level)))))]
    (when (pos? max-depth)
      (step [] 0 true))))

(defn- relation-virtual-index-depth ^long [iterators-state]
  (dec (count (:indexes @iterators-state))))

(defrecord RelationVirtualIndex [relation-name max-depth layered-range-constraints iterators-state]
  db/Index
  (seek-values [this k]
    (let [{:keys [indexes]} @iterators-state]
      (when-let [idx (last indexes)]
        (let [[k {:keys [value child-idx]}] (db/seek-values idx k)]
          (swap! iterators-state merge {:child-idx child-idx
                                        :needs-seek? false})
          (when k
            [k value])))))

  (next-values [this]
    (let [{:keys [needs-seek? indexes]} @iterators-state]
      (if needs-seek?
        (db/seek-values this nil)
        (when-let [idx (last indexes)]
          (let [[k {:keys [value child-idx]}] (db/next-values idx)]
            (swap! iterators-state assoc :child-idx child-idx)
            (when k
              [k value]))))))

  db/LayeredIndex
  (open-level [this]
    (when (= max-depth (relation-virtual-index-depth iterators-state))
      (throw (IllegalStateException. (str "Cannot open level at max depth: " max-depth))))
    (swap! iterators-state
           (fn [{:keys [indexes child-idx]}]
             {:indexes (conj indexes child-idx)
              :child-idx nil
              :needs-seek? true}))
    nil)

  (close-level [this]
    (when (zero? (relation-virtual-index-depth iterators-state))
      (throw (IllegalStateException. "Cannot close level at root.")))
    (swap! iterators-state (fn [{:keys [indexes]}]
                             {:indexes (pop indexes)
                              :child-idx nil
                              :needs-seek? false}))
    nil)

  (max-depth [this]
    max-depth))

(defn- build-nested-index [tuples [range-constraints & next-range-constraints]]
  (-> (new-sorted-virtual-index
       (for [prefix (partition-by first tuples)
             :let [value (ffirst prefix)]]
         [(c/->value-buffer value)
          {:value value
           :child-idx (when (seq (next (first prefix)))
                        (build-nested-index (map next prefix) next-range-constraints))}]))
      (wrap-with-range-constraints range-constraints)))

(defn update-relation-virtual-index!
  ([relation tuples]
   (update-relation-virtual-index! relation tuples (:layered-range-constraints relation)))
  ([relation tuples layered-range-constraints]
   (reset! (:iterators-state relation)
           {:indexes [(binding [nippy/*freeze-fallback* :write-unfreezable]
                        (build-nested-index tuples layered-range-constraints))]
            :child-idx nil
            :needs-seek? true})
   relation))

(defn new-relation-virtual-index
  ([relation-name tuples max-depth]
   (new-relation-virtual-index relation-name tuples max-depth nil))
  ([relation-name tuples max-depth layered-range-constraints]
   (let [iterators-state (atom nil)]
     (update-relation-virtual-index! (->RelationVirtualIndex relation-name max-depth layered-range-constraints iterators-state) tuples))))
