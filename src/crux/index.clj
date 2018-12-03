(ns crux.index
  (:require [clojure.tools.logging :as log]
            [clojure.set :as set]
            [crux.byte-utils :as bu]
            [crux.codec :as c]
            [crux.kv-store :as ks]
            [crux.db :as db]
            [crux.lru :as lru]
            [taoensso.nippy :as nippy])
  (:import java.io.Closeable
           [java.util Arrays Collections Comparator Date]
           crux.codec.EntityTx))

;; TODO: Consider doing the big ns rename, where this potentially
;; becomes crux.index and current crux.index becomes something like
;; crux.codec.

(set! *unchecked-math* :warn-on-boxed)

;; Indexes

(defrecord PrefixKvIterator [i ^bytes prefix]
  ks/KvIterator
  (seek [_ k]
    (when-let [k (ks/seek i k)]
      (when (bu/bytes=? k prefix (alength prefix))
        k)))

  (next [_]
    (when-let [k (ks/next i)]
      (when (bu/bytes=? k prefix (alength prefix))
        k)))

  (value [_]
    (ks/value i))

  Closeable
  (close [_]
    (.close ^Closeable i)))

(defn ^java.io.Closeable new-prefix-kv-iterator [i prefix]
  (->PrefixKvIterator i prefix))

;; AVE

(defn- attribute-value+placeholder [k peek-state]
  (let [[value] (c/decode-attribute+value+entity+content-hash-key->value+entity+content-hash k)]
    (reset! peek-state {:last-k k :value value})
    [value :crux.index.binary-placeholder/value]))

(defrecord DocAttributeValueEntityValueIndex [i attr peek-state]
  db/Index
  (seek-values [this k]
    (when-let [k (->> (or k c/empty-byte-array)
                      (c/encode-attribute+value+entity+content-hash-key (c/id->bytes attr))
                      (ks/seek i))]
      (attribute-value+placeholder k peek-state)))

  (next-values [this]
    (let [{:keys [^bytes last-k]} @peek-state
          prefix-size (- (alength last-k) c/id-size c/id-size)]
      (when-let [k (some->> (bu/inc-unsigned-bytes! (Arrays/copyOf last-k prefix-size))
                            (ks/seek i))]
        (attribute-value+placeholder k peek-state)))))

(defn new-doc-attribute-value-entity-value-index [snapshot attr]
  (let [prefix (c/encode-attribute+value+entity+content-hash-key (c/id->bytes attr))]
    (->DocAttributeValueEntityValueIndex (new-prefix-kv-iterator (ks/new-iterator snapshot) prefix) attr (atom nil))))

(defn- attribute-value-entity-entity+value [i ^bytes current-k attr value entity-as-of-idx peek-state]
  (loop [k current-k]
    (reset! peek-state (bu/inc-unsigned-bytes! (Arrays/copyOf k (- (alength k) c/id-size))))
    (or (let [[_ entity] (c/decode-attribute+value+entity+content-hash-key->value+entity+content-hash k)
              [_ ^EntityTx entity-tx] (db/seek-values entity-as-of-idx (c/id->bytes entity))]
          (when entity-tx
            (let [version-k (c/encode-attribute+value+entity+content-hash-key
                             (c/id->bytes attr)
                             value
                             (c/id->bytes entity)
                             (c/id->bytes (.content-hash entity-tx)))]
              (when-let [found-k (ks/seek i version-k)]
                (when (bu/bytes=? version-k found-k)
                  [(c/id->bytes entity) entity-tx])))))
        (when-let [k (some->> @peek-state (ks/seek i))]
          (recur k)))))

(defn- attribute-value-value+prefix-iterator [i value-entity-value-idx attr]
  (let [{:keys [value]} @(:peek-state value-entity-value-idx)
        prefix (c/encode-attribute+value+entity+content-hash-key (c/id->bytes attr) value)]
    [value (new-prefix-kv-iterator i prefix)]))

(defrecord DocAttributeValueEntityEntityIndex [i attr value-entity-value-idx entity-as-of-idx peek-state]
  db/Index
  (seek-values [this k]
    (let [[value i] (attribute-value-value+prefix-iterator i value-entity-value-idx attr)]
      (when-let [k (->> (c/encode-attribute+value+entity+content-hash-key
                         (c/id->bytes attr)
                         value
                         (if k
                           (c/id->bytes k)
                           c/empty-byte-array))
                        (ks/seek i))]
        (attribute-value-entity-entity+value i k attr value entity-as-of-idx peek-state))))

  (next-values [this]
    (let [[value i] (attribute-value-value+prefix-iterator i value-entity-value-idx attr)]
      (when-let [k (some->> @peek-state (ks/seek i))]
        (attribute-value-entity-entity+value i k attr value entity-as-of-idx peek-state)))))

(defn new-doc-attribute-value-entity-entity-index [snapshot attr value-entity-value-idx entity-as-of-idx]
  (->DocAttributeValueEntityEntityIndex (ks/new-iterator snapshot) attr value-entity-value-idx entity-as-of-idx (atom nil)))

;; AEV

(defn- attribute-entity+placeholder [k attr entity-as-of-idx peek-state]
  (let [[e] (c/decode-attribute+entity+value+content-hash-key->entity+value+content-hash k)
        [_ entity-tx] (db/seek-values entity-as-of-idx (c/id->bytes e))]
    (reset! peek-state {:last-k k :entity-tx entity-tx})
    (if entity-tx
      [(c/id->bytes e) :crux.index.binary-placeholder/entity]
      ::deleted-entity)))

(defrecord DocAttributeEntityValueEntityIndex [i attr entity-as-of-idx peek-state]
  db/Index
  (seek-values [this k]
    (when-let [k (->> (if k
                        (c/id->bytes k)
                        c/empty-byte-array)
                      (c/encode-attribute+entity+value+content-hash-key (c/id->bytes attr))
                      (ks/seek i))]
      (let [placeholder (attribute-entity+placeholder k attr entity-as-of-idx peek-state)]
        (if (= ::deleted-entity placeholder)
          (db/next-values this)
          placeholder))))

  (next-values [this]
    (let [{:keys [^bytes last-k]} @peek-state
          prefix-size (+ c/index-id-size c/id-size c/id-size)]
      (when-let [k (some->> (bu/inc-unsigned-bytes! (Arrays/copyOf last-k prefix-size))
                            (ks/seek i))]
        (let [placeholder (attribute-entity+placeholder k attr entity-as-of-idx peek-state)]
          (if (= ::deleted-entity placeholder)
            (db/next-values this)
            placeholder))))))

(defn new-doc-attribute-entity-value-entity-index [snapshot attr entity-as-of-idx]
  (let [prefix (c/encode-attribute+entity+value+content-hash-key (c/id->bytes attr))]
    (->DocAttributeEntityValueEntityIndex (new-prefix-kv-iterator (ks/new-iterator snapshot) prefix) attr entity-as-of-idx (atom nil))))

(defn- attribute-entity-value-value+entity [i ^bytes current-k attr ^EntityTx entity-tx peek-state]
  (when entity-tx
    (loop [k current-k]
      (reset! peek-state (bu/inc-unsigned-bytes! (Arrays/copyOf k (- (alength k) c/id-size))))
      (or (let [[_ value] (c/decode-attribute+entity+value+content-hash-key->entity+value+content-hash k)]
            (let [version-k (c/encode-attribute+entity+value+content-hash-key
                             (c/id->bytes attr)
                             (c/id->bytes (.eid entity-tx))
                             value
                             (c/id->bytes (.content-hash entity-tx)))]
              (when-let [found-k (ks/seek i version-k)]
                (when (bu/bytes=? version-k found-k)
                  [value entity-tx]))))
          (when-let [k (some->> @peek-state (ks/seek i))]
            (recur k))))))

(defn- attribute-value-entity-tx+prefix-iterator [i entity-value-entity-idx attr]
  (let [{:keys [^EntityTx entity-tx]} @(:peek-state entity-value-entity-idx)
        prefix (c/encode-attribute+entity+value+content-hash-key (c/id->bytes attr) (c/id->bytes (.eid entity-tx)))]
    [entity-tx (new-prefix-kv-iterator i prefix)]))

(defrecord DocAttributeEntityValueValueIndex [i attr entity-value-entity-idx peek-state]
  db/Index
  (seek-values [this k]
    (let [[^EntityTx entity-tx i] (attribute-value-entity-tx+prefix-iterator i entity-value-entity-idx attr)]
      (when-let [k (->> (c/encode-attribute+entity+value+content-hash-key
                         (c/id->bytes attr)
                         (c/id->bytes (.eid entity-tx))
                         (or k c/empty-byte-array))
                        (ks/seek i))]
        (attribute-entity-value-value+entity i k attr entity-tx peek-state))))

  (next-values [this]
    (let [[entity-tx i] (attribute-value-entity-tx+prefix-iterator i entity-value-entity-idx attr)]
      (when-let [k (some->> @peek-state (ks/seek i))]
        (attribute-entity-value-value+entity i k attr entity-tx peek-state)))))

(defn new-doc-attribute-entity-value-value-index [snapshot attr entity-value-entity-idx]
  (->DocAttributeEntityValueValueIndex (ks/new-iterator snapshot) attr entity-value-entity-idx (atom nil)))

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
  ([compare-pred ^bytes compare-v max-length]
   (if compare-v
     (fn [value]
       (and value (compare-pred (bu/compare-bytes value compare-v max-length))))
     (constantly true))))

(defn new-less-than-equal-virtual-index [idx max-v]
  (let [pred (value-comparsion-predicate (comp not pos?) (c/value->bytes max-v))]
    (->PredicateVirtualIndex idx pred identity)))

(defn new-less-than-virtual-index [idx max-v]
  (let [pred (value-comparsion-predicate neg? (c/value->bytes max-v))]
    (->PredicateVirtualIndex idx pred identity)))

(defn new-greater-than-equal-virtual-index [idx min-v]
  (let [min-v (c/value->bytes min-v)
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
  (let [min-v (c/value->bytes min-v)
        pred (value-comparsion-predicate pos? min-v)
        idx (->PredicateVirtualIndex idx pred (fn [k]
                                                (if (pred k)
                                                  k
                                                  min-v)))]
    (->GreaterThanVirtualIndex idx)))

(defn new-prefix-equal-virtual-index [idx ^bytes prefix-v]
  (let [seek-k-pred (value-comparsion-predicate (comp not neg?) prefix-v (alength prefix-v))
        pred (value-comparsion-predicate zero? prefix-v (alength prefix-v))]
    (->PredicateVirtualIndex idx pred (fn [k]
                                        (if (seek-k-pred k)
                                          k
                                          prefix-v)))))

(defn wrap-with-range-constraints [idx range-constraints]
  (if range-constraints
    (range-constraints idx)
    idx))

;; Doc Store

(defn normalize-value [v]
  (cond-> v
    (not (or (vector? v)
             (set? v))) (vector)))

(defn index-doc [kv content-hash doc]
  (let [id (c/id->bytes (:crux.db/id doc))
        content-hash (c/id->bytes content-hash)]
    (ks/store kv (->> (for [[k v] doc
                            v (normalize-value v)
                            :let [k (c/id->bytes k)
                                  v (c/value->bytes v)]
                            :when (seq v)]
                        [[(c/encode-attribute+value+entity+content-hash-key k v id content-hash)
                          c/empty-byte-array]
                         [(c/encode-attribute+entity+value+content-hash-key k id v content-hash)
                          c/empty-byte-array]])
                      (reduce into [])))))

(defn delete-doc-from-index [kv content-hash doc]
  (let [id (c/id->bytes (:crux.db/id doc))
        content-hash (c/id->bytes content-hash)]
    (ks/delete kv (->> (for [[k v] doc
                             v (normalize-value v)
                             :let [k (c/id->bytes k)
                                   v (c/value->bytes v)]
                             :when (seq v)]
                         [(c/encode-attribute+value+entity+content-hash-key k v id content-hash)
                          (c/encode-attribute+entity+value+content-hash-key k id v content-hash)])
                       (reduce into [])))))

(defrecord DocObjectStore [kv]
  Closeable
  (close [_])

  db/ObjectStore
  (get-objects [this snapshot ks]
    (with-open [i (ks/new-iterator snapshot)]
      (->> (for [seek-k (->> (map (comp c/encode-doc-key c/id->bytes) ks)
                             (sort bu/bytes-comparator))
                 :let [k (ks/seek i seek-k)]
                 :when (and k (bu/bytes=? seek-k k))]
             [(c/decode-doc-key k)
              (nippy/fast-thaw (ks/value i))])
           (into {}))))

  (put-objects [this kvs]
    (ks/store kv (for [[k v] kvs]
                   [(c/encode-doc-key (c/id->bytes k))
                    (nippy/fast-freeze v)])))

  (delete-objects [this ks]
    (ks/delete kv (map (comp c/encode-doc-key c/id->bytes) ks)))

  Closeable
  (close [_]))

;; Meta

(defn store-meta [kv k v]
  (ks/store kv [[(c/encode-meta-key (c/id->bytes k))
                 (nippy/fast-freeze v)]]))

(defn read-meta [kv k]
  (let [seek-k (c/encode-meta-key (c/id->bytes k))]
    (with-open [snapshot (ks/new-snapshot kv)
                i (ks/new-iterator snapshot)]
      (when-let [k (ks/seek i seek-k)]
        (when (bu/bytes=? seek-k k)
          (nippy/fast-thaw (ks/value i)))))))

;; Utils

(defn- all-keys-in-prefix
  ([i prefix]
   (all-keys-in-prefix i prefix false))
  ([i ^bytes prefix entries?]
   ((fn step [f-cons f-next]
      (lazy-seq
       (let [k (f-cons)]
         (when (and k (bu/bytes=? prefix k (alength prefix)))
           (cons (if entries?
                   [k (ks/value i)]
                   k) (step f-next f-next))))))
    #(ks/seek i prefix) #(ks/next i))))

(defn idx->seq [idx]
  (when-let [result (db/seek-values idx nil)]
    (->> (repeatedly #(db/next-values idx))
         (take-while identity)
         (cons result))))

(def ^:const default-await-tx-timeout 10000)

(defn await-tx-time
  ([kv transact-time]
   (await-tx-time kv transact-time default-await-tx-timeout))
  ([kv transact-time ^long timeout]
   (let [timeout-at (+ timeout (System/currentTimeMillis))
         epoch (Date. 0)]
     (while (pos? (compare transact-time (or (read-meta kv :crux.tx-log/tx-time)
                                             epoch)))
       (Thread/sleep 100)
       (when (>= (System/currentTimeMillis) timeout-at)
         (throw (IllegalStateException.
                 (str "Timed out waiting for: " transact-time
                      " index has:" (read-meta kv :crux.tx-log/tx-time))))))
     (read-meta kv :crux.tx-log/tx-time))))

;; Entities

(declare ^{:tag 'java.io.Closeable} new-cached-snapshot)

(defn- ^EntityTx enrich-entity-tx [entity-tx content-hash]
  (assoc entity-tx :content-hash (some-> content-hash not-empty c/new-id)))

(defrecord EntityAsOfIndex [i business-time transact-time]
  db/Index
  (db/seek-values [this k]
    (let [prefix-size (+ c/index-id-size c/id-size)
          seek-k (c/encode-entity+bt+tt+tx-id-key
                  k
                  business-time
                  transact-time)]
      (loop [k (ks/seek i seek-k)]
        (when (and k (bu/bytes=? seek-k k prefix-size))
          (let [v (ks/value i)
                entity-tx (-> (c/decode-entity+bt+tt+tx-id-key k)
                              (enrich-entity-tx v))]
            (if (<= (compare (.tt entity-tx) transact-time) 0)
              (when-not (bu/bytes=? c/nil-id-bytes v)
                [(c/id->bytes (.eid entity-tx)) entity-tx])
              (recur (ks/next i))))))))

  (db/next-values [this]
    (throw (UnsupportedOperationException.))))

(defn new-entity-as-of-index [snapshot business-time transact-time]
  (->EntityAsOfIndex (ks/new-iterator snapshot) business-time transact-time))

(defn entities-at [snapshot entities business-time transact-time]
  (let [entity-as-of-idx (new-entity-as-of-index snapshot business-time transact-time)]
    (some->> (for [entity entities
                   :let [[_ entity-tx] (db/seek-values entity-as-of-idx (c/id->bytes entity))]
                   :when entity-tx]
               entity-tx)
             (not-empty)
             (vec))))

(defn all-entities [snapshot business-time transact-time]
  (with-open [i (ks/new-iterator snapshot)]
    (let [eids (->> (all-keys-in-prefix i (c/encode-entity+bt+tt+tx-id-key))
                    (map (comp :eid c/decode-entity+bt+tt+tx-id-key))
                    (distinct))]
      (entities-at snapshot eids business-time transact-time))))

(defn entity-history [snapshot entity]
  (with-open [i (ks/new-iterator snapshot)]
    (let [seek-k (c/encode-entity+bt+tt+tx-id-key (c/id->bytes entity))]
      (vec (for [[k v] (all-keys-in-prefix i seek-k true)]
             (-> (c/decode-entity+bt+tt+tx-id-key k)
                 (enrich-entity-tx v)))))))

;; Join

(extend-protocol db/LayeredIndex
  Object
  (open-level [_])
  (close-level [_])
  (max-depth [_] 1))

(def ^:private sorted-virtual-index-key-comparator
  (reify Comparator
    (compare [_ [a] [b]]
      (bu/compare-bytes (or a c/nil-id-bytes)
                        (or b c/nil-id-bytes)))))

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
          (sort-by first bu/bytes-comparator)
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
                         (sort-by (comp first second) bu/bytes-comparator)
                         (first))]
      (when n
        (swap! peek-state assoc n (db/next-values (get indexes n))))
      value)))

(defn new-or-virtual-index [indexes]
  (->OrVirtualIndex indexes (atom nil)))

(defn or-known-triple-fast-path [snapshot e a v business-time transact-time]
  (when-let [[^EntityTx entity-tx] (entities-at snapshot [e] business-time transact-time)]
    (let [version-k (c/encode-attribute+entity+value+content-hash-key
                     (c/id->bytes a)
                     (c/id->bytes (.eid entity-tx))
                     (c/value->bytes v)
                     (c/id->bytes (.content-hash entity-tx)))]
      (with-open [i (new-prefix-kv-iterator (ks/new-iterator snapshot) version-k)]
        (when (ks/seek i version-k)
          entity-tx)))))

(defn- new-unary-join-iterator-state [idx [value results]]
  (let [result-name (:name idx)]
    {:idx idx
     :key (or value c/nil-id-bytes)
     :result-name result-name
     :results (when (and result-name results)
                {result-name results})}))

(defrecord UnaryJoinVirtualIndex [indexes iterators-thunk-state]
  db/Index
  (seek-values [this k]
    (->> #(let [iterators (->> (for [idx indexes]
                                 (new-unary-join-iterator-state idx (db/seek-values idx k)))
                               (sort-by :key bu/bytes-comparator)
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
              match? (bu/bytes=? key max-k)]
          (->> #(let [next-value+results (if match?
                                           (do (log/debug :next result-name)
                                               (db/next-values idx))
                                           (do (log/debug :seek result-name (bu/bytes->hex max-k))
                                               (db/seek-values idx max-k)))]
                  (when next-value+results
                    {:iterators (assoc iterators index (new-unary-join-iterator-state idx next-value+results))
                     :index (mod (inc index) (count iterators))}))
               (reset! iterators-thunk-state))
          (if match?
            (let [names (map :result-name iterators)]
              (log/debug :match names (bu/bytes->hex max-k))
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
               (let [close-level (fn []
                                   (when (pos? depth)
                                     (db/close-level idx)
                                     (step (pop max-ks) (dec depth) false)))
                     open-level (fn [result]
                                  (db/open-level idx)
                                  (if-let [max-ks (build-result max-ks result)]
                                    (step max-ks (inc depth) true)
                                    (do (db/close-level idx)
                                        (step max-ks depth false))))]
                 (if (= depth (dec max-depth))
                   (concat (build-leaf-results max-ks idx)
                           (lazy-seq
                            (close-level)))
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
         [(c/value->bytes value)
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

;; Caching

(defrecord CachedObjectStore [cache object-store]
  db/ObjectStore
  (get-objects [this snapshot ks]
    (->> (for [k ks]
           [k (lru/compute-if-absent
               cache
               k
               #(get (db/get-objects object-store snapshot [%]) %))])
         (into {})))

  (put-objects [this kvs]
    (db/put-objects object-store kvs))

  (delete-objects [this ks]
    (doseq [k ks]
      (lru/evict cache k))
    (db/delete-objects object-store ks))

  Closeable
  (close [_]))

(def ^:const default-doc-cache-size 10240)

(defn new-cached-object-store
  ([kv]
   (new-cached-object-store kv default-doc-cache-size))
  ([kv cache-size]
   (->CachedObjectStore (lru/get-named-cache kv ::doc-cache cache-size)
                        (->DocObjectStore kv))))

(defn- ensure-iterator-open [closed-state]
  (when @closed-state
    (throw (IllegalStateException. "Iterator closed."))))

(defrecord CachedIterator [i closed-state]
  ks/KvIterator
  (seek [_ k]
    (locking i
      (ensure-iterator-open closed-state)
      (ks/seek i k)))

  (next [_]
    (locking i
      (ensure-iterator-open closed-state)
      (ks/next i)))

  (value [_]
    (locking i
      (ensure-iterator-open closed-state)
      (ks/value i)))

  Closeable
  (close [_]
    (ensure-iterator-open closed-state)
    (reset! closed-state true)))

(defrecord CachedSnapshot [^Closeable snapshot close-snapshot? iterators-state]
  ks/KvSnapshot
  (new-iterator [_]
    (if-let [i (->> @iterators-state
                    (filter (comp deref :closed-state))
                    (first))]
      (if (compare-and-set! (:closed-state i) true false)
        i
        (recur))
      (let [i (->CachedIterator (ks/new-iterator snapshot) (atom false))]
        (swap! iterators-state conj i)
        i)))

  Closeable
  (close [_]
    (doseq [{:keys [^Closeable i closed-state]} @iterators-state]
      (locking i
        (reset! closed-state true)
        (.close i)))
    (when close-snapshot?
      (.close snapshot))))

(defn ^crux.index.CachedSnapshot new-cached-snapshot [snapshot close-snapshot?]
  (->CachedSnapshot snapshot close-snapshot? (atom #{})))
