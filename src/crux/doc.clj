(ns crux.doc
  (:require [clojure.tools.logging :as log]
            [clojure.set :as set]
            [clojure.spec.alpha :as s]
            [crux.byte-utils :as bu]
            [crux.index :as idx]
            [crux.kv-store :as ks]
            [crux.db :as db]
            [crux.lru :as lru]
            [taoensso.nippy :as nippy])
  (:import [java.io Closeable]
           [java.util Collections Comparator Date]
           [crux.index EntityTx]))

(set! *unchecked-math* :warn-on-boxed)

;; Docs

(defn- attribute-value+content-hashes-for-current-key [i ^bytes current-k attr peek-state]
  (let [seek-k (idx/encode-attribute+value-prefix-key attr idx/empty-byte-array)
        prefix-size (- (alength current-k) idx/id-size)]
    (when (bu/bytes=? seek-k current-k (alength seek-k))
      (loop [acc []
             k current-k]
        (if-let [value+content-hash (when (and k (bu/bytes=? current-k k prefix-size))
                                      (idx/decode-attribute+value+content-hash-key->value+content-hash k))]
          (recur (conj acc value+content-hash)
                 (ks/-next i))
          (do (reset! peek-state k)
              (when (seq acc)
                [(ffirst acc) (mapv second acc)])))))))

(defrecord DocAttributeValueIndex [i attr peek-state]
  db/Index
  (-seek-values [this k]
    (when-let [k (->> (or k idx/empty-byte-array)
                      (idx/encode-attribute+value-prefix-key attr)
                      (ks/-seek i))]
      (attribute-value+content-hashes-for-current-key i k attr peek-state)))

  db/OrderedIndex
  (-next-values [this]
    (when-let [k (or @peek-state (ks/-next i))]
      (attribute-value+content-hashes-for-current-key i k attr peek-state))))

(defrecord DocLiteralAttributeValueIndex [i attr value]
  db/Index
  (-seek-values [this k]
    (let [seek-k (idx/encode-attribute+value+content-hash-key attr value (or k idx/empty-byte-array))]
      (when-let [^bytes k (ks/-seek i seek-k)]
        (when (bu/bytes=? k seek-k (- (alength k) idx/id-size))
          (let [[_ content-hash] (idx/decode-attribute+value+content-hash-key->value+content-hash k)]
            [(idx/value->bytes content-hash) [content-hash]])))))

  db/OrderedIndex
  (-next-values [this]
    (when-let [^bytes k (ks/-next i)]
      (let [seek-k (idx/encode-attribute+value-prefix-key attr value)]
        (when (bu/bytes=? k seek-k (- (alength k) idx/id-size))
          (let [[_ content-hash] (idx/decode-attribute+value+content-hash-key->value+content-hash k)]
            [(idx/value->bytes content-hash) [content-hash]]))))))

(defrecord PredicateVirtualIndex [idx pred seek-k-fn]
  db/Index
  (-seek-values [this k]
    (when-let [value+results (db/-seek-values idx (seek-k-fn k))]
      (when (pred (first value+results))
        value+results)))

  db/OrderedIndex
  (-next-values [this]
    (when-let [value+results (db/-next-values idx)]
      (when (pred (first value+results))
        value+results))))

(defn- value-comparsion-predicate [compare-pred v]
  (if v
    (let [seek-k (idx/value->bytes v)]
      (fn [value]
        (compare-pred (bu/compare-bytes value seek-k))))
    (constantly true)))

(defn new-less-than-equal-virtual-index [idx max-v]
  (let [pred (value-comparsion-predicate (comp not pos?) max-v)]
    (->PredicateVirtualIndex idx pred identity)))

(defn new-less-than-virtual-index [idx max-v]
  (let [pred (value-comparsion-predicate neg? max-v)]
    (->PredicateVirtualIndex idx pred identity)))

(defn new-greater-than-equal-virtual-index [idx min-v]
  (let [pred (value-comparsion-predicate (comp not neg?) min-v)]
    (->PredicateVirtualIndex idx pred (fn [k]
                                        (if (pred (idx/value->bytes k))
                                          k
                                          min-v)))))

(defrecord GreaterThanVirtualIndex [predicate-idx]
  db/Index
  (-seek-values [this k]
    (or (db/-seek-values predicate-idx k)
        (db/-next-values predicate-idx)))

  db/OrderedIndex
  (-next-values [this]
    (db/-next-values predicate-idx)))

(defn new-greater-than-virtual-index [idx min-v]
  (let [pred (value-comparsion-predicate pos? min-v)
        idx (->PredicateVirtualIndex idx pred (fn [k]
                                                (if (pred (idx/value->bytes k))
                                                  k
                                                  min-v)))]
    (->GreaterThanVirtualIndex idx)))

(defn new-equal-virtual-index [idx v]
  (let [seek-k-pred (value-comparsion-predicate (comp not neg?) v)
        pred (value-comparsion-predicate zero? v)]
    (->PredicateVirtualIndex idx pred (fn [k]
                                        (if (seek-k-pred (idx/value->bytes k))
                                          k
                                          v)))))

(defn- new-doc-attribute-value-index [i attr]
  (->DocAttributeValueIndex i attr (atom nil)))

(defn- wrap-with-range-constraints [idx range-constraints]
  (if range-constraints
    (range-constraints idx)
    idx))

(defn normalize-value [v]
  (cond-> v
    (not (or (vector? v)
             (set? v))) (vector)))

(defn index-doc [kv content-hash doc]
  (ks/store kv (for [[k v] doc
                     v (normalize-value v)]
                 [(idx/encode-attribute+value+content-hash-key k v content-hash)
                  idx/empty-byte-array])))

(defn delete-doc-from-index [kv content-hash doc]
  (ks/delete kv (for [[k v] doc
                      v (normalize-value v)]
                  (idx/encode-attribute+value+content-hash-key k v content-hash))))

(defrecord DocObjectStore [kv]
  db/ObjectStore
  (get-objects [this ks]
    (with-open [snapshot (ks/new-snapshot kv)
                i (ks/new-iterator snapshot)]
      (->> (for [seek-k (->> (map idx/encode-doc-key ks)
                             (sort bu/bytes-comparator))
                 :let [k (ks/-seek i seek-k)]
                 :when (and k (bu/bytes=? seek-k k))]
             [(idx/decode-doc-key k)
              (nippy/fast-thaw (ks/-value i))])
           (into {}))))

  (put-objects [this kvs]
    (ks/store kv (for [[k v] kvs]
                   [(idx/encode-doc-key k)
                    (nippy/fast-freeze v)])))

  (delete-objects [this ks]
    (ks/delete kv (map idx/encode-doc-key ks)))

  Closeable
  (close [_]))

;; Meta

(defn store-meta [kv k v]
  (ks/store kv [[(idx/encode-meta-key k)
                 (nippy/fast-freeze v)]]))

(defn read-meta [kv k]
  (with-open [snapshot (ks/new-snapshot kv)
              i (ks/new-iterator snapshot)]
    (when-let [k (ks/-seek i (idx/encode-meta-key k))]
      (nippy/fast-thaw (ks/-value i)))))

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
                   [k (ks/-value i)]
                   k) (step f-next f-next))))))
    #(ks/-seek i prefix) #(ks/-next i))))

(defn idx->seq [idx]
  (when-let [result (db/-seek-values idx nil)]
    (->> (repeatedly #(db/-next-values idx))
         (take-while identity)
         (cons result))))

;; Entities

(declare ^{:tag 'java.io.Closeable} new-cached-snapshot)

(defn- ^EntityTx enrich-entity-tx [entity-tx content-hash]
  (assoc entity-tx :content-hash (some-> content-hash not-empty idx/new-id)))

(defrecord EntityAsOfIndex [i business-time transact-time]
  db/Index
  (db/-seek-values [this k]
    (let [prefix-size (+ Short/BYTES idx/id-size)
          seek-k (idx/encode-entity+bt+tt-prefix-key
                  k
                  business-time
                  transact-time)]
      (loop [k (ks/-seek i seek-k)]
        (when (and k (bu/bytes=? seek-k k prefix-size))
          (let [v (ks/-value i)
                entity-tx (-> (idx/decode-entity+bt+tt+tx-id-key k)
                              (enrich-entity-tx v))]
            (if (<= (compare (.tt entity-tx) transact-time) 0)
              (when-not (bu/bytes=? idx/nil-id-bytes v)
                [(idx/id->bytes (.eid entity-tx)) [entity-tx]])
              (recur (ks/-next i)))))))))

(defn entities-at [snapshot entities business-time transact-time]
  (with-open [i (ks/new-iterator snapshot)]
    (let [entity-as-of-idx (->EntityAsOfIndex i business-time transact-time)]
      (some->> (for [entity entities
                     :let [[_ [entity-tx]] (db/-seek-values entity-as-of-idx entity)]
                     :when entity-tx]
                 entity-tx)
               (not-empty)
               (vec)))))

(defrecord ContentHashEntityIndex [i]
  db/Index
  (db/-seek-values [this k]
    (when-let [value+results (->> (idx/encode-content-hash-prefix-key k)
                                  (all-keys-in-prefix i)
                                  (map idx/decode-content-hash+entity-key->entity)
                                  (seq))]
      [(ffirst value+results) (mapv second value+results)])))

(defn- value+content-hashes->value+entities [content-hash-entity-idx entity-as-of-idx value+content-hashes]
  (when-let [[v content-hashes] value+content-hashes]
    (when-let [entity-txs (->> (for [content-hash content-hashes
                                     :let [[_ entities] (db/-seek-values content-hash-entity-idx content-hash)]
                                     entity entities
                                     :let [[_ [entity-tx]] (db/-seek-values entity-as-of-idx entity)]
                                     :when (= content-hash (.content-hash ^EntityTx entity-tx))]
                                 entity-tx)
                               (seq))]
      [v (vec entity-txs)])))

(defrecord EntityAttributeValueVirtualIndex [doc-idx content-hash-entity-idx entity-as-of-idx]
  db/Index
  (-seek-values [this k]
    (->> (db/-seek-values doc-idx k)
         (value+content-hashes->value+entities content-hash-entity-idx entity-as-of-idx)))

  db/OrderedIndex
  (-next-values [this]
    (->> (db/-next-values doc-idx)
         (value+content-hashes->value+entities content-hash-entity-idx entity-as-of-idx))))

(defn entities-by-attribute-value-at [snapshot attr range-constraints business-time transact-time]
  (with-open [di (ks/new-iterator snapshot)
              ci (ks/new-iterator snapshot)
              ei (ks/new-iterator snapshot)]
    (let [doc-idx (-> (new-doc-attribute-value-index di attr)
                      (wrap-with-range-constraints range-constraints))
          content-hash-entity-idx (->ContentHashEntityIndex ci)
          entity-as-of-idx (->EntityAsOfIndex ei business-time transact-time)
          entity-attribute-idx (->EntityAttributeValueVirtualIndex doc-idx content-hash-entity-idx entity-as-of-idx)]
      (->> (idx->seq entity-attribute-idx)
           (mapcat second)
           (vec)))))

(defn all-entities [snapshot business-time transact-time]
  (with-open [i (ks/new-iterator snapshot)]
    (let [eids (->> (all-keys-in-prefix i (idx/encode-entity+bt+tt-prefix-key))
                    (map (comp :eid idx/decode-entity+bt+tt+tx-id-key))
                    (distinct))]
      (entities-at snapshot eids business-time transact-time))))

(defn entity-history [snapshot entity]
  (with-open [i (ks/new-iterator snapshot)]
    (let [seek-k (idx/encode-entity+bt+tt-prefix-key entity)]
      (vec (for [[k v] (all-keys-in-prefix i seek-k true)]
             (-> (idx/decode-entity+bt+tt+tx-id-key k)
                 (enrich-entity-tx v)))))))

;; Join

(defrecord LiteralEntityAttributeValuesVirtualIndex [object-store entity-as-of-idx entity attr attr-state]
  db/Index
  (-seek-values [this k]
    (if-let [entity-tx (get @attr-state :entity-tx (let [[_ [entity-tx]] (db/-seek-values entity-as-of-idx entity)]
                                                     entity-tx))]
      (let [content-hash (.content-hash ^EntityTx entity-tx)
            values (get @attr-state :values
                        (->> (get-in (db/get-objects object-store [content-hash])
                                     [content-hash attr])
                             (normalize-value)
                             (map idx/value->bytes)
                             (into (sorted-set-by bu/bytes-comparator))))
            [x & xs] (subseq values >= (idx/value->bytes k))
            {:keys [first]} (reset! attr-state {:first x :rest xs :entity-tx entity-tx :values values})]
        (when first
          [first [entity-tx]]))
      (reset! attr-state nil)))

  db/OrderedIndex
  (-next-values [this]
    (let [{:keys [first entity-tx]} (swap! attr-state (fn [{[x & xs] :rest
                                                            :as attr-state}]
                                                        (assoc attr-state :first x :rest xs)))]
      (when first
        [first [entity-tx]]))))

(defn new-literal-entity-attribute-values-virtual-index [object-store snapshot entity attr range-constraints business-time transact-time]
  (let [entity-as-of-idx (->EntityAsOfIndex (ks/new-iterator snapshot) business-time transact-time)]
    (-> (->LiteralEntityAttributeValuesVirtualIndex object-store entity-as-of-idx entity attr (atom nil))
        (wrap-with-range-constraints range-constraints))))

(defrecord SortedVirtualIndex [values seq-state]
  db/Index
  (-seek-values [this k]
    (let [idx (Collections/binarySearch values [(idx/value->bytes k)]
                                        (reify Comparator
                                          (compare [_ [a] [b]]
                                            (bu/compare-bytes (or a idx/nil-id-bytes)
                                                              (or b idx/nil-id-bytes)))))
          [x & xs] (subvec values (if (neg? idx)
                                    (dec (- idx))
                                    idx))
          {:keys [first]} (reset! seq-state {:first x :rest xs})]
      (if first
        first
        (reset! seq-state nil))))

  db/OrderedIndex
  (-next-values [this]
    (let [{:keys [first]} (swap! seq-state (fn [{[x & xs] :rest
                                                 :as seq-state}]
                                             (assoc seq-state :first x :rest xs)))]
      (when first
        first))))

(defn new-sorted-virtual-index [idx]
  (->SortedVirtualIndex
   (->> (if (satisfies? db/OrderedIndex idx)
          (idx->seq idx)
          idx)
        (sort-by first bu/bytes-comparator)
        (vec))
   (atom nil)))

(defrecord OrVirtualIndex [indexes peek-state]
  db/Index
  (-seek-values [this k]
    (reset! peek-state (vec (for [idx indexes]
                              (db/-seek-values idx k))))
    (db/-next-values this))

  db/OrderedIndex
  (-next-values [this]
    (let [[n value] (->> (map-indexed vector @peek-state)
                         (remove (comp nil? second))
                         (sort-by (comp first second) bu/bytes-comparator)
                         (first))]
      (when n
        (swap! peek-state assoc n (db/-next-values (get indexes n))))
      value)))

(defn new-or-virtual-index [indexes]
  (->OrVirtualIndex indexes (atom nil)))

(defn- new-unary-join-iterator-state [idx [value results]]
  {:idx idx
   :key (or value idx/nil-id-bytes)
   :result-name (:name idx (gensym "result-name"))
   :results (set results)})

(defrecord UnaryJoinVirtualIndex [indexes iterators-state]
  db/Index
  (-seek-values [this k]
    (let [iterators (->> (for [idx indexes]
                           (new-unary-join-iterator-state idx (db/-seek-values idx k)))
                         (sort-by :key bu/bytes-comparator)
                         (vec))]
      (reset! iterators-state {:iterators iterators :index 0})
      (db/-next-values this)))

  db/OrderedIndex
  (-next-values [this]
    (when-let [{:keys [iterators ^long index]} @iterators-state]
      (let [{:keys [key result-name idx]} (get iterators index)
            max-index (mod (dec index) (count iterators))
            max-k (:key (get iterators max-index))
            match? (bu/bytes=? key max-k)
            next-value+results (if match?
                                 (do (log/debug :next result-name)
                                     (db/-next-values idx))
                                 (do (log/debug :seek result-name (bu/bytes->hex max-k))
                                     (db/-seek-values idx (reify
                                                            idx/ValueToBytes
                                                            (value->bytes [_]
                                                              max-k)

                                                            idx/IdToBytes
                                                            (id->bytes [_]
                                                              max-k)))))]
        (reset! iterators-state
                (when next-value+results
                  {:iterators (assoc iterators index (new-unary-join-iterator-state idx next-value+results))
                   :index (mod (inc index) (count iterators))}))
        (if match?
          (let [names (map :result-name iterators)]
            (log/debug :match names (bu/bytes->hex max-k))
            [max-k (zipmap names (map :results iterators))])
          (recur))))))

(defn new-unary-join-virtual-index [indexes]
  (->UnaryJoinVirtualIndex indexes (atom nil)))

(defn constrain-join-result-by-empty-names [max-ks join-results]
  (when (every? not-empty (vals join-results))
    join-results))

(defrecord NAryJoinVirtualIndex [unary-join-indexes constrain-result-fn join-state]
  db/Index
  (-seek-values [this k]
    (reset! join-state {:needs-seek? true
                        :min-vs (vec k)
                        :result-stack []})
    (db/-next-values this))

  db/OrderedIndex
  (-next-values [this]
    (let [{:keys [needs-seek? min-vs result-stack]} @join-state
          depth (count result-stack)
          max-depth (dec (count unary-join-indexes))
          idx (get unary-join-indexes depth)
          values (if needs-seek?
                   (do (swap! join-state assoc :needs-seek? false)
                       (db/-seek-values idx (get min-vs depth)))
                   (db/-next-values idx))
          [max-k new-values] values
          max-ks (conj (mapv first result-stack) max-k)
          [_ parent-result] (last result-stack)
          result (some->> new-values
                          (merge-with set/intersection parent-result)
                          (constrain-result-fn max-ks)
                          (not-empty))]
      (cond
        (and values (= depth max-depth))
        (do (log/debug :leaf-match-candidate (mapv bu/bytes->hex max-ks) result)
            (if result
              (do (log/debug :leaf-match (mapv bu/bytes->hex max-ks))
                  [max-ks result])
              (do (log/debug :leaf-match-constrained (mapv bu/bytes->hex max-ks))
                  (recur))))

        values
        (do (if result
              (do (log/debug :open-level depth)
                  (swap! join-state #(-> %
                                         (assoc :needs-seek? true)
                                         (update :result-stack conj [max-k result]))))
              (log/debug :open-level-constrained (bu/bytes->hex max-k)))
            (recur))

        (and (nil? values) (pos? depth))
        (do (log/debug :close-level depth)
            (swap! join-state update :result-stack pop)
            (recur))))))

(defn new-n-ary-join-virtual-index [unary-index-groups constrain-result-fn]
  (->NAryJoinVirtualIndex (mapv new-unary-join-virtual-index unary-index-groups) constrain-result-fn (atom nil)))

(defn- values+unary-join-results->value+entities [content-hash-entity-idx entity-as-of-idx content-hash+unary-join-results]
  (when-let [[content-hash] content-hash+unary-join-results]
    (let [value+content-hashes [content-hash [(idx/new-id content-hash)]]]
      (when-let [[_ [entity-tx]] (value+content-hashes->value+entities content-hash-entity-idx entity-as-of-idx value+content-hashes)]
        [(idx/value->bytes (:eid entity-tx)) [entity-tx]]))))

(defrecord SharedEntityLiteralAttributeValuesVirtualIndex [unary-join-literal-doc-idx content-hash-entity-idx entity-as-of-idx]
  db/Index
  (-seek-values [this k]
    (->> (db/-seek-values unary-join-literal-doc-idx k)
         (values+unary-join-results->value+entities content-hash-entity-idx entity-as-of-idx)))

  db/OrderedIndex
  (-next-values [this]
    (when-let [values+unary-join-results (seq (db/-next-values unary-join-literal-doc-idx))]
      (or (values+unary-join-results->value+entities content-hash-entity-idx entity-as-of-idx values+unary-join-results)
          (recur)))))

(defn new-shared-literal-attribute-entities-virtual-index [snapshot attr+values business-time transact-time]
  (let [entity-as-of-idx (->EntityAsOfIndex (ks/new-iterator snapshot) business-time transact-time)
        content-hash-entity-idx (->ContentHashEntityIndex (ks/new-iterator snapshot))
        unary-join-literal-doc-idx (->> (for [[attr value] attr+values]
                                          (->DocLiteralAttributeValueIndex (ks/new-iterator snapshot) attr value))
                                        (new-unary-join-virtual-index))]
    (->> (->SharedEntityLiteralAttributeValuesVirtualIndex
          unary-join-literal-doc-idx
          content-hash-entity-idx
          entity-as-of-idx)
         (new-sorted-virtual-index))))

(defn new-entity-attribute-value-virtual-index [snapshot attr range-constraints business-time transact-time]
  (let [entity-as-of-idx (->EntityAsOfIndex (ks/new-iterator snapshot) business-time transact-time)
        content-hash-entity-idx (->ContentHashEntityIndex (ks/new-iterator snapshot))
        doc-idx (-> (new-doc-attribute-value-index (ks/new-iterator snapshot) attr)
                    (wrap-with-range-constraints range-constraints))]
    (->EntityAttributeValueVirtualIndex doc-idx content-hash-entity-idx entity-as-of-idx)))

;; Caching

(defrecord CachedObjectStore [cache object-store]
  db/ObjectStore
  (get-objects [this ks]
    (->> (for [k ks]
           [k (lru/compute-if-absent
               cache
               k
               #(get (db/get-objects object-store [%]) %))])
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

(defn- named-cache [state cache-name cache-size]
  (get (swap! state
              update
              cache-name
              (fn [cache]
                (or cache (lru/new-cache cache-size))))
       cache-name))

(defn new-cached-object-store
  ([kv]
   (new-cached-object-store kv default-doc-cache-size))
  ([kv cache-size]
   (->CachedObjectStore (named-cache (:state kv)::doc-cache cache-size)
                        (->DocObjectStore kv))))

(defn- ensure-iterator-open [closed-state]
  (when @closed-state
    (throw (IllegalStateException. "Iterator closed."))))

(defrecord CachedIterator [i closed-state]
  ks/KvIterator
  (-seek [_ k]
    (locking i
      (ensure-iterator-open closed-state)
      (ks/-seek i k)))

  (-next [_]
    (locking i
      (ensure-iterator-open closed-state)
      (ks/-next i)))

  (-value [_]
    (locking i
      (ensure-iterator-open closed-state)
      (ks/-value i)))

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

(defn ^crux.doc.CachedSnapshot new-cached-snapshot [snapshot close-snapshot?]
  (->CachedSnapshot snapshot close-snapshot? (atom #{})))
