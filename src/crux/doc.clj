(ns crux.doc
  (:require [clojure.tools.logging :as log]
            [clojure.set :as set]
            [clojure.spec.alpha :as s]
            [crux.byte-utils :as bu]
            [crux.index :as idx]
            [crux.kv-store :as ks]
            [crux.db :as db]
            [crux.lru :as lru]
            [crux.query :as q]
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
    (let [seek-k (idx/encode-attribute+value+content-hash-key attr value (or k idx/empty-byte-array))
          prefix-size (- (alength seek-k) idx/id-size)]
      (when-let [k (ks/-seek i seek-k)]
        (when (bu/bytes=? k seek-k prefix-size)
          (let [[_ content-hash] (idx/decode-attribute+value+content-hash-key->value+content-hash k)]
            [(idx/value->bytes content-hash) [content-hash]])))))

  db/OrderedIndex
  (-next-values [this]
    (when-let [k (ks/-next i)]
      (let [seek-k (idx/encode-attribute+value-prefix-key attr value)
            prefix-size (alength seek-k)]
        (when (bu/bytes=? k seek-k prefix-size)
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

(defn new-equal-virtual-index [idx v]
  (let [pred (value-comparsion-predicate zero? v)]
    (->PredicateVirtualIndex idx pred (constantly v))))

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

(defn- new-doc-attribute-value-index [i attr]
  (->DocAttributeValueIndex i attr (atom nil)))

(defn- wrap-with-range-constraints [idx range-constraints]
  (if range-constraints
    (range-constraints idx)
    idx))

(defn- normalize-value [v]
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

(defn- idx->seq [idx]
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

(defn- new-literal-entity-attribute-values-virtual-index [object-store entity-as-of-idx entity attr]
  (->LiteralEntityAttributeValuesVirtualIndex object-store entity-as-of-idx entity attr (atom nil)))

(defn- literal-entity-values-internal [object-store snapshot entity attr range-constraints business-time transact-time]
  (let [entity-as-of-idx (->EntityAsOfIndex (ks/new-iterator snapshot) business-time transact-time)]
    (-> (new-literal-entity-attribute-values-virtual-index object-store entity-as-of-idx entity attr)
        (wrap-with-range-constraints range-constraints))))

(defn literal-entity-values [object-store snapshot entity attr range-constraints business-time transact-time]
  (with-open [snapshot (new-cached-snapshot snapshot false)]
    (->> (literal-entity-values-internal object-store snapshot entity attr range-constraints business-time transact-time)
         (idx->seq)
         (vec))))

;; Join

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

(defn- new-sorted-virtual-index [idx]
  (->SortedVirtualIndex
   (->> (idx->seq idx)
        (sort-by first bu/bytes-comparator)
        (vec))
   (atom nil)))

(defn- new-leapfrog-iterator-state [idx [value results]]
  {:idx idx
   :key (or value idx/nil-id-bytes)
   :result-name (:name idx (gensym "result-name"))
   :results (set results)})

(defrecord UnaryJoinVirtualIndex [indexes iterators-state]
  db/Index
  (-seek-values [this k]
    (let [iterators (->> (for [idx indexes]
                           (new-leapfrog-iterator-state idx (db/-seek-values idx k)))
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
                  {:iterators (assoc iterators index (new-leapfrog-iterator-state idx next-value+results))
                   :index (mod (inc index) (count iterators))}))
        (if match?
          (let [names (map :result-name iterators)]
            (log/debug :match names (bu/bytes->hex max-k))
            [max-k (zipmap names (map :results iterators))])
          (recur))))))

(defn- new-unary-join-virtual-index [indexes]
  (->UnaryJoinVirtualIndex indexes (atom nil)))

(defn unary-leapfrog-join [indexes]
  (->> (new-unary-join-virtual-index indexes)
       (idx->seq)
       (vec)))

(defn- constrain-triejoin-result [shared-names result]
  (->> shared-names
       (reduce
        (fn [result result-names]
          (if (and result (every? result result-names))
            (some->> (map result result-names)
                     (apply set/intersection)
                     (not-empty)
                     (repeat)
                     (zipmap result-names)
                     (merge result))
            result))
        result)))

(defrecord TriejoinVirtualIndex [unary-join-indexes shared-names trie-state]
  db/Index
  (-seek-values [this k]
    (reset! trie-state {:needs-seek? true
                        :min-vs (vec k)
                        :result-stack []})
    (db/-next-values this))

  db/OrderedIndex
  (-next-values [this]
    (let [{:keys [needs-seek? min-vs result-stack]} @trie-state
          depth (count result-stack)
          max-depth (dec (count unary-join-indexes))
          idx (get unary-join-indexes depth)
          values (if needs-seek?
                   (do (swap! trie-state assoc :needs-seek? false)
                       (db/-seek-values idx (get min-vs depth)))
                   (db/-next-values idx))
          [max-k new-values] values
          [_ parent-result] (last result-stack)
          result (some->> new-values
                          (merge parent-result)
                          (constrain-triejoin-result shared-names)
                          (not-empty))]
      (cond
        (and values (= depth max-depth))
        (let [max-ks (conj (mapv first result-stack) max-k)]
          (log/debug :leaf-match-candidate (mapv bu/bytes->hex max-ks) result)
          (if result
            (do (log/debug :leaf-match (mapv bu/bytes->hex max-ks))
                [max-ks result])
            (do (log/debug :leaf-match-constrained (mapv bu/bytes->hex max-ks))
                (recur))))

        values
        (do (if result
              (do (log/debug :open-level depth)
                  (swap! trie-state #(-> %
                                         (assoc :needs-seek? true)
                                         (update :result-stack conj [max-k result]))))
              (log/debug :open-level-constrained (bu/bytes->hex max-k)))
            (recur))

        (and (nil? values) (pos? depth))
        (do (log/debug :close-level depth)
            (swap! trie-state update :result-stack pop)
            (recur))))))

(defn- new-triejoin-virtual-index [unary-join-indexes shared-names]
  (->TriejoinVirtualIndex (vec unary-join-indexes) shared-names (atom nil)))

(defn- leapfrog-triejoin-internal [unary-indexes shared-names]
  (new-triejoin-virtual-index (mapv new-unary-join-virtual-index unary-indexes) shared-names))

(defn leapfrog-triejoin [unary-indexes shared-names]
  (->> (leapfrog-triejoin-internal unary-indexes shared-names)
       (idx->seq)
       (vec)))

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

(defn- shared-literal-attribute-entities-join-internal [snapshot attr+values business-time transact-time]
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

(defn shared-literal-attribute-entities-join [snapshot attr+values business-time transact-time]
  (with-open [snapshot (new-cached-snapshot snapshot false)]
    (->> (shared-literal-attribute-entities-join-internal snapshot attr+values business-time transact-time)
         (idx->seq)
         (vec))))

(defn entity-attribute-value-join-internal [snapshot attr range-constraints business-time transact-time]
  (let [entity-as-of-idx (->EntityAsOfIndex (ks/new-iterator snapshot) business-time transact-time)
        content-hash-entity-idx (->ContentHashEntityIndex (ks/new-iterator snapshot))
        doc-idx (-> (new-doc-attribute-value-index (ks/new-iterator snapshot) attr)
                    (wrap-with-range-constraints range-constraints))]
    (-> (->EntityAttributeValueVirtualIndex doc-idx content-hash-entity-idx entity-as-of-idx)
        (assoc :name attr))))

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

;; Query

(defrecord DocEntity [object-store eid content-hash bt]
  db/Entity
  (attr-val [this ident]
    (get (db/->map this) ident))

  (->id [this]
    ;; TODO: we want to get rid of the need for :crux.db/id
    (or (db/attr-val this :crux.db/id) eid))

  (->map [this]
    (get (db/get-objects object-store [content-hash]) content-hash))

  (->business-time [this]
    bt)

  (eq? [this that]
    (= eid (:eid that))))

;; TODO: old range constraints format was a map, still used by
;; crux.query, this will be replaced by wrapper fns.
(defn- range-constraints-map->fn [{:keys [min-v inclusive-min-v? max-v inclusive-max-v?]
                                   :as range-constraints}]
  (fn [idx]
    (-> idx
        ((if inclusive-max-v?
           new-less-than-equal-virtual-index
           new-less-than-virtual-index) max-v)
        ((if inclusive-min-v?
           new-greater-than-equal-virtual-index
           new-greater-than-virtual-index) min-v))))

(defrecord DocDatasource [kv object-store business-time transact-time]
  db/Datasource
  (new-query-context [this]
    (new-cached-snapshot (ks/new-snapshot kv) true))

  (entities [this query-context]
    (for [entity-tx (all-entities query-context business-time transact-time)]
      (map->DocEntity (assoc entity-tx :object-store object-store))))

  (entities-for-attribute-value [this query-context ident range-constraints]
    (for [entity-tx (entities-by-attribute-value-at query-context ident (range-constraints-map->fn range-constraints) business-time transact-time)]
      (map->DocEntity (assoc entity-tx :object-store object-store))))

  (entity-join [this query-context attrs range-constraints]
    (let [indexes (for [attr attrs]
                    (entity-attribute-value-join-internal query-context attr (range-constraints-map->fn range-constraints) transact-time transact-time))]
      (distinct (for [[v join-results] (unary-leapfrog-join indexes)
                      [k entities] join-results
                      entity-tx entities]
                  (map->DocEntity (assoc entity-tx :object-store object-store))))))

  (entity [this query-context eid]
    (when-let [entity-tx (first (entities-at query-context [eid] business-time transact-time))]
      (map->DocEntity (assoc entity-tx :object-store object-store))))

  (entity-history [this query-context eid]
    (for [entity-tx (entity-history query-context eid)]
      (map->DocEntity (assoc entity-tx :object-store object-store)))))

(def ^:const default-await-tx-timeout 10000)

(defn- await-tx-time [kv transact-time ^long timeout]
  (let [timeout-at (+ timeout (System/currentTimeMillis))]
    (while (pos? (compare transact-time (read-meta kv :crux.tx-log/tx-time)))
      (Thread/sleep 100)
      (when (>= (System/currentTimeMillis) timeout-at)
        (throw (IllegalStateException. (str "Timed out waiting for: " transact-time
                                            " index has:" (read-meta kv :crux.tx-log/tx-time))))))))

(defn db
  ([kv]
   (db kv (Date.)))
  ([kv business-time]
   (->DocDatasource kv
                    (new-cached-object-store kv)
                    business-time
                    (Date.)))
  ([kv business-time transact-time]
   (await-tx-time kv transact-time default-await-tx-timeout)
   (->DocDatasource kv
                    (new-cached-object-store kv)
                    business-time
                    transact-time)))

;; Index-based Query

(defn- logic-var? [x]
  (symbol? x))

(defn q
  ([{:keys [kv] :as db} q]
   (with-open [snapshot (new-cached-snapshot (ks/new-snapshot kv) true)]
     (set (crux.doc/q snapshot db q))))
  ([snapshot {:keys [kv object-store business-time transact-time] :as db} q]
   (let [{:keys [find where] :as q} (s/conform :crux.query/query q)]
     (when (= :clojure.spec.alpha/invalid q)
       (throw (throw (IllegalArgumentException.
                      (str "Invalid input: " (s/explain-str :crux.query/query q))))))
     (let [{bgp-clauses :bgp
            range-clauses :range
            pred-clauses :pred} (->> (for [[type clause] where]
                                       (if (contains? #{:bgp :range :pred} type)
                                         {type [clause]}
                                         (throw (IllegalArgumentException.
                                                 (str "Unsupported clause: "
                                                      type " "
                                                      (pr-str clause))))))
                                     (apply merge-with into))
           e-vars (set (for [clause bgp-clauses
                             :when (logic-var? (:e clause))]
                         (:e clause)))
           v-var->range-clauses (->> (for [clause range-clauses]
                                       (if (contains? e-vars (:sym clause))
                                         (throw (IllegalArgumentException.
                                                 (str "Cannot add range constraints on entity variable: "
                                                      (pr-str clause))))
                                         clause))
                                     (group-by :sym))
           v-var->range-constrants (->> (for [[_ clauses] v-var->range-clauses]
                                          (->> (for [{:keys [op val]} clauses]
                                                 (case op
                                                   < #(new-less-than-virtual-index val)
                                                   <= #(new-less-than-equal-virtual-index val)
                                                   > #(new-greater-than-virtual-index val)
                                                   >= #(new-greater-than-equal-virtual-index val)))
                                               (apply comp)))
                                        {})
           e-var->v-var-clauses (->> (for [clause bgp-clauses
                                           :when (and (logic-var? (:e clause))
                                                      (logic-var? (:v clause)))]
                                       clause)
                                     (group-by :e))
           var->names (->> (for [[_ clauses] e-var->v-var-clauses
                                 clause clauses
                                 :let [var (:v clause)]]
                             {var [(gensym var)]})
                           (apply merge-with into))
           v-var->e-var (->> (for [[e clauses] e-var->v-var-clauses
                                   clause clauses
                                   :when (not (contains? e-vars (:v clause)))]
                               [(:v clause) (:e clause)])
                             (into {}))
           var->joins {}
           e-var->literal-v-clauses (->> (for [clause bgp-clauses
                                               :when (and (logic-var? (:e clause))
                                                          (not (logic-var? (:v clause))))]
                                           clause)
                                         (group-by :e))
           [var->joins var->names] (reduce
                                    (fn [[var->joins var->names] [var clauses]]
                                      (let [var-name (gensym var)
                                            idx (shared-literal-attribute-entities-join-internal
                                                 snapshot
                                                 (vec (for [{:keys [a v]} clauses]
                                                        [a v]))
                                                 business-time transact-time)]
                                        [(merge-with into {var [(assoc idx :name var-name)]} var->joins)
                                         (merge-with into {var [var-name]} var->names)]))
                                    [var->joins var->names]
                                    e-var->literal-v-clauses)
           e-var+v-var->join-clauses (->> (for [clause bgp-clauses
                                                :when (and (logic-var? (:e clause))
                                                           (logic-var? (:v clause))
                                                           (contains? e-vars (:v clause)))]
                                            clause)
                                          (group-by (juxt :e :v)))
           [var->joins var->names] (reduce
                                    (fn [[var->joins var->names] [[e-var v-var] clauses]]
                                      (let [indexes (for [clause clauses
                                                          :let [var-name (gensym e-var)]]
                                                      (assoc (entity-attribute-value-join-internal snapshot
                                                                                                   (:a clause)
                                                                                                   (get v-var->range-constrants v-var)
                                                                                                   business-time
                                                                                                   transact-time)
                                                             :name var-name))]
                                        [(merge-with into {v-var (vec indexes)} var->joins)
                                         (merge-with into {e-var (mapv :name indexes)} var->names)]))
                                    [var->joins var->names]
                                    e-var+v-var->join-clauses)
           v-var->literal-e-clauses (->> (for [clause bgp-clauses
                                               :when (and (not (logic-var? (:e clause)))
                                                          (logic-var? (:v clause)))]
                                           clause)
                                         (group-by :v))
           [var->joins var->names] (reduce
                                    (fn [[var->joins var->names] [v-var clauses]]
                                      (let [indexes (for [{:keys [e a]} clauses
                                                          :let [var-name (gensym v-var)]]
                                                      (assoc (literal-entity-values-internal
                                                              object-store
                                                              snapshot
                                                              e
                                                              a
                                                              (get v-var->range-constrants v-var)
                                                              business-time transact-time)
                                                             :name var-name))]
                                        [(merge-with into {v-var (vec indexes)} var->joins)
                                         (merge {v-var (mapv :name indexes)} var->names)]))
                                    [var->joins var->names]
                                    v-var->literal-e-clauses)
           v-var-name->attr (->> (for [{:keys [a v]} bgp-clauses
                                       :when (logic-var? v)
                                       var-name (get var->names v)]
                                   [var-name a])
                                 (into {}))
           e-var-name->attr (zipmap (mapcat var->names e-vars)
                                    (repeat :crux.db/id))
           var-names->attr (merge v-var-name->attr e-var-name->attr)
           preds (some->> (for [clause pred-clauses
                                :let [{:keys [pred-fn args]} clause]]
                            (fn [var->result]
                              (->> (for [arg args]
                                     (get var->result arg arg))
                                   (apply pred-fn))))
                          (not-empty)
                          (apply every-pred))
           cartesian (fn cartesian [[x & xs]]
                       (if-not (seq x)
                         [[]]
                         (for [a x
                               bs (cartesian xs)]
                           (cons a bs))))]
       (doseq [var find
               :when (not (contains? var->names var))]
         (throw (IllegalArgumentException. (str "Find clause references unbound variable: " var))))
       (for [[v join-results] (idx->seq (leapfrog-triejoin-internal (vec (vals var->joins))
                                                                    (mapv var->names e-vars)))
             result (cartesian
                     (for [var find
                           :let [var-name (first (get var->names var))
                                 e-var-name (if-let [e-var (get v-var->e-var var)]
                                              (first (get var->names e-var))
                                              var-name)]]
                       (for [{:keys [content-hash]} (get join-results e-var-name)
                             :let [doc (get (db/get-objects object-store [content-hash]) content-hash)]
                             value (normalize-value (get doc (get var-names->attr var-name)))]
                         value)))
             :when (or (nil? preds) (preds (zipmap find result)))]
         (vec result))))))
