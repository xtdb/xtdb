(ns crux.doc
  (:require [clojure.tools.logging :as log]
            [clojure.set :as set]
            [clojure.spec.alpha :as s]
            [crux.byte-utils :as bu]
            [crux.index :as idx]
            [crux.kv-store :as ks]
            [crux.db :as db]
            [crux.io :as cio]
            [crux.lru :as lru]
            [taoensso.nippy :as nippy])
  (:import [java.io Closeable File RandomAccessFile]
           [java.util Arrays Collections Comparator Date]
           [crux.index EntityTx]))

(set! *unchecked-math* :warn-on-boxed)

;; Docs

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
  (let [[value] (idx/decode-attribute+value+entity+content-hash-key->value+entity+content-hash k)]
    (reset! peek-state {:last-k k :value value})
    [value :crux.doc.binary-placeholder/value]))

(defrecord DocAttributeValueEntityValueIndex [i attr peek-state]
  db/Index
  (seek-values [this k]
    (when-let [k (->> (or k idx/empty-byte-array)
                      (idx/encode-attribute+value+entity+content-hash-key (idx/id->bytes attr))
                      (ks/seek i))]
      (attribute-value+placeholder k peek-state)))

  db/OrderedIndex
  (next-values [this]
    (let [{:keys [^bytes last-k]} @peek-state
          prefix-size (- (alength last-k) idx/id-size idx/id-size)]
      (when-let [k (some->> (bu/inc-unsigned-bytes! (Arrays/copyOf last-k prefix-size))
                            (ks/seek i))]
        (attribute-value+placeholder k peek-state)))))

(defn new-doc-attribute-value-entity-value-index [snapshot attr]
  (let [prefix (idx/encode-attribute+value+entity+content-hash-key (idx/id->bytes attr))]
    (->DocAttributeValueEntityValueIndex (new-prefix-kv-iterator (ks/new-iterator snapshot) prefix) attr (atom nil))))

(defn- attribute-value-entity-entity+value [i ^bytes current-k attr value entity-as-of-idx peek-state]
  (loop [k current-k]
    (reset! peek-state (bu/inc-unsigned-bytes! (Arrays/copyOf k (- (alength k) idx/id-size))))
    (or (let [[_ entity] (idx/decode-attribute+value+entity+content-hash-key->value+entity+content-hash k)
              [_ ^EntityTx entity-tx] (db/seek-values entity-as-of-idx entity)]
          (when entity-tx
            (let [version-k (idx/encode-attribute+value+entity+content-hash-key
                             (idx/id->bytes attr)
                             value
                             (idx/id->bytes entity)
                             (idx/id->bytes (.content-hash entity-tx)))]
              (when-let [found-k (ks/seek i version-k)]
                (when (bu/bytes=? version-k found-k)
                  [(idx/id->bytes entity) entity-tx])))))
        (when-let [k (some->> @peek-state (ks/seek i))]
          (recur k)))))

(defn- attribute-value-value+prefix-iterator [i value-entity-value-idx attr]
  (let [{:keys [value]} @(:peek-state value-entity-value-idx)
        prefix (idx/encode-attribute+value+entity+content-hash-key (idx/id->bytes attr) value)]
    [value (new-prefix-kv-iterator i prefix)]))

(defrecord DocAttributeValueEntityEntityIndex [i attr value-entity-value-idx entity-as-of-idx peek-state]
  db/Index
  (seek-values [this k]
    (let [[value i] (attribute-value-value+prefix-iterator i value-entity-value-idx attr)]
      (when-let [k (->> (idx/encode-attribute+value+entity+content-hash-key
                         (idx/id->bytes attr)
                         value
                         (if k
                           (idx/id->bytes k)
                           idx/empty-byte-array))
                        (ks/seek i))]
        (attribute-value-entity-entity+value i k attr value entity-as-of-idx peek-state))))

  db/OrderedIndex
  (next-values [this]
    (let [[value i] (attribute-value-value+prefix-iterator i value-entity-value-idx attr)]
      (when-let [k (some->> @peek-state (ks/seek i))]
        (attribute-value-entity-entity+value i k attr value entity-as-of-idx peek-state)))))

(defn new-doc-attribute-value-entity-entity-index [snapshot attr value-entity-value-idx entity-as-of-idx]
  (->DocAttributeValueEntityEntityIndex (ks/new-iterator snapshot) attr value-entity-value-idx entity-as-of-idx (atom nil)))

;; AEV

(defn- attribute-entity+placeholder [k attr entity-as-of-idx peek-state]
  (let [[e] (idx/decode-attribute+entity+value+content-hash-key->entity+value+content-hash k)
        [_ entity-tx] (db/seek-values entity-as-of-idx e)]
    (reset! peek-state {:last-k k :entity-tx entity-tx})
    (if entity-tx
      [(idx/id->bytes e) :crux.doc.binary-placeholder/entity]
      ::deleted-entity)))

(defrecord DocAttributeEntityValueEntityIndex [i attr entity-as-of-idx peek-state]
  db/Index
  (seek-values [this k]
    (when-let [k (->> (if k
                        (idx/id->bytes k)
                        idx/empty-byte-array)
                      (idx/encode-attribute+entity+value+content-hash-key (idx/id->bytes attr))
                      (ks/seek i))]
      (let [placeholder (attribute-entity+placeholder k attr entity-as-of-idx peek-state)]
        (if (= ::deleted-entity placeholder)
          (db/next-values this)
          placeholder))))

  db/OrderedIndex
  (next-values [this]
    (let [{:keys [^bytes last-k]} @peek-state
          prefix-size (+ idx/index-id-size idx/id-size idx/id-size)]
      (when-let [k (some->> (bu/inc-unsigned-bytes! (Arrays/copyOf last-k prefix-size))
                            (ks/seek i))]
        (let [placeholder (attribute-entity+placeholder k attr entity-as-of-idx peek-state)]
          (if (= ::deleted-entity placeholder)
            (db/next-values this)
            placeholder))))))

(defn new-doc-attribute-entity-value-entity-index [snapshot attr entity-as-of-idx]
  (let [prefix (idx/encode-attribute+entity+value+content-hash-key (idx/id->bytes attr))]
    (->DocAttributeEntityValueEntityIndex (new-prefix-kv-iterator (ks/new-iterator snapshot) prefix) attr entity-as-of-idx (atom nil))))

(defn- attribute-entity-value-value+entity [i ^bytes current-k attr ^EntityTx entity-tx peek-state]
  (when entity-tx
    (loop [k current-k]
      (reset! peek-state (bu/inc-unsigned-bytes! (Arrays/copyOf k (- (alength k) idx/id-size))))
      (or (let [[_ value] (idx/decode-attribute+entity+value+content-hash-key->entity+value+content-hash k)]
            (let [version-k (idx/encode-attribute+entity+value+content-hash-key
                             (idx/id->bytes attr)
                             (idx/id->bytes (.eid entity-tx))
                             value
                             (idx/id->bytes (.content-hash entity-tx)))]
              (when-let [found-k (ks/seek i version-k)]
                (when (bu/bytes=? version-k found-k)
                  [value entity-tx]))))
          (when-let [k (some->> @peek-state (ks/seek i))]
            (recur k))))))

(defn- attribute-value-entity-tx+prefix-iterator [i entity-value-entity-idx attr]
  (let [{:keys [^EntityTx entity-tx]} @(:peek-state entity-value-entity-idx)
        prefix (idx/encode-attribute+entity+value+content-hash-key (idx/id->bytes attr) (idx/id->bytes (.eid entity-tx)))]
    [entity-tx (new-prefix-kv-iterator i prefix)]))

(defrecord DocAttributeEntityValueValueIndex [i attr entity-value-entity-idx peek-state]
  db/Index
  (seek-values [this k]
    (let [[^EntityTx entity-tx i] (attribute-value-entity-tx+prefix-iterator i entity-value-entity-idx attr)]
      (when-let [k (->> (idx/encode-attribute+entity+value+content-hash-key
                         (idx/id->bytes attr)
                         (idx/id->bytes (.eid entity-tx))
                         (or k idx/empty-byte-array))
                        (ks/seek i))]
        (attribute-entity-value-value+entity i k attr entity-tx peek-state))))

  db/OrderedIndex
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

  db/OrderedIndex
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
  (let [pred (value-comparsion-predicate (comp not pos?) (idx/value->bytes max-v))]
    (->PredicateVirtualIndex idx pred identity)))

(defn new-less-than-virtual-index [idx max-v]
  (let [pred (value-comparsion-predicate neg? (idx/value->bytes max-v))]
    (->PredicateVirtualIndex idx pred identity)))

(defn new-greater-than-equal-virtual-index [idx min-v]
  (let [min-v (idx/value->bytes min-v)
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

  db/OrderedIndex
  (next-values [this]
    (db/next-values idx)))

(defn new-greater-than-virtual-index [idx min-v]
  (let [min-v (idx/value->bytes min-v)
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
  (let [id (idx/id->bytes (:crux.db/id doc))
        content-hash (idx/id->bytes content-hash)]
    (ks/store kv (->> (for [[k v] doc
                            v (normalize-value v)
                            :let [k (idx/id->bytes k)
                                  v (idx/value->bytes v)]
                            :when (seq v)]
                        [[(idx/encode-attribute+value+entity+content-hash-key k v id content-hash)
                          idx/empty-byte-array]
                         [(idx/encode-attribute+entity+value+content-hash-key k id v content-hash)
                          idx/empty-byte-array]])
                      (reduce into [])))))

(defn delete-doc-from-index [kv content-hash doc]
  (let [id (idx/id->bytes (:crux.db/id doc))
        content-hash (idx/id->bytes content-hash)]
    (ks/delete kv (->> (for [[k v] doc
                             v (normalize-value v)
                             :let [k (idx/id->bytes k)
                                   v (idx/value->bytes v)]
                             :when (seq v)]
                         [(idx/encode-attribute+value+entity+content-hash-key k v id content-hash)
                          (idx/encode-attribute+entity+value+content-hash-key k id v content-hash)])
                       (reduce into [])))))

(defrecord DocObjectStore [kv]
  db/ObjectStore
  (get-objects [this snapshot ks]
    (with-open [i (ks/new-iterator snapshot)]
      (->> (for [seek-k (->> (map (comp idx/encode-doc-key idx/id->bytes) ks)
                             (sort bu/bytes-comparator))
                 :let [k (ks/seek i seek-k)]
                 :when (and k (bu/bytes=? seek-k k))]
             [(idx/decode-doc-key k)
              (nippy/fast-thaw (ks/value i))])
           (into {}))))

  (put-objects [this kvs]
    (ks/store kv (for [[k v] kvs]
                   [(idx/encode-doc-key (idx/id->bytes k))
                    (nippy/fast-freeze v)])))

  (delete-objects [this ks]
    (ks/delete kv (map (comp idx/encode-doc-key idx/id->bytes) ks)))

  Closeable
  (close [_]))

;; Meta

(defn store-meta [kv k v]
  (ks/store kv [[(idx/encode-meta-key (idx/id->bytes k))
                 (nippy/fast-freeze v)]]))

(defn read-meta [kv k]
  (with-open [snapshot (ks/new-snapshot kv)
              i (ks/new-iterator snapshot)]
    (when-let [k (ks/seek i (idx/encode-meta-key (idx/id->bytes k)))]
      (nippy/fast-thaw (ks/value i)))))

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

;; Entities

(declare ^{:tag 'java.io.Closeable} new-cached-snapshot)

(defn- ^EntityTx enrich-entity-tx [entity-tx content-hash]
  (assoc entity-tx :content-hash (some-> content-hash not-empty idx/new-id)))

(defrecord EntityAsOfIndex [i business-time transact-time]
  db/Index
  (db/seek-values [this k]
    (let [prefix-size (+ idx/index-id-size idx/id-size)
          seek-k (idx/encode-entity+bt+tt+tx-id-key
                  (idx/id->bytes k)
                  business-time
                  transact-time)]
      (loop [k (ks/seek i seek-k)]
        (when (and k (bu/bytes=? seek-k k prefix-size))
          (let [v (ks/value i)
                entity-tx (-> (idx/decode-entity+bt+tt+tx-id-key k)
                              (enrich-entity-tx v))]
            (if (<= (compare (.tt entity-tx) transact-time) 0)
              (when-not (bu/bytes=? idx/nil-id-bytes v)
                [(idx/id->bytes (.eid entity-tx)) entity-tx])
              (recur (ks/next i)))))))))

(defn new-entity-as-of-index [snapshot business-time transact-time]
  (->EntityAsOfIndex (ks/new-iterator snapshot) business-time transact-time))

(defn entities-at [snapshot entities business-time transact-time]
  (with-open [i (ks/new-iterator snapshot)]
    (let [entity-as-of-idx (->EntityAsOfIndex i business-time transact-time)]
      (some->> (for [entity entities
                     :let [[_ entity-tx] (db/seek-values entity-as-of-idx entity)]
                     :when entity-tx]
                 entity-tx)
               (not-empty)
               (vec)))))

(defn all-entities [snapshot business-time transact-time]
  (with-open [i (ks/new-iterator snapshot)]
    (let [eids (->> (all-keys-in-prefix i (idx/encode-entity+bt+tt+tx-id-key))
                    (map (comp :eid idx/decode-entity+bt+tt+tx-id-key))
                    (distinct))]
      (entities-at snapshot eids business-time transact-time))))

(defn entity-history [snapshot entity]
  (with-open [i (ks/new-iterator snapshot)]
    (let [seek-k (idx/encode-entity+bt+tt+tx-id-key (idx/id->bytes entity))]
      (vec (for [[k v] (all-keys-in-prefix i seek-k true)]
             (-> (idx/decode-entity+bt+tt+tx-id-key k)
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
      (bu/compare-bytes (or a idx/nil-id-bytes)
                        (or b idx/nil-id-bytes)))))

(defrecord SortedVirtualIndex [values ^RandomAccessFile raf ^File file seq-state]
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
        (if file
          (read-external-sort-value raf fst)
          fst)
        (reset! seq-state nil))))

  db/OrderedIndex
  (next-values [this]
    (let [{:keys [fst]} (swap! seq-state (fn [{[x & xs] :rest
                                               :as seq-state}]
                                           (assoc seq-state :fst x :rest xs)))]
      (when fst
        (if file
          (read-external-sort-value raf fst)
          fst))))

  Closeable
  (close [this]
    (when raf
      (.close raf))
    (when file
      (.delete file))))

(defn new-sorted-virtual-index [idx-or-seq]
  (let [idx-as-seq (if (satisfies? db/OrderedIndex idx-or-seq)
                     (idx->seq idx-or-seq)
                     idx-or-seq)]
    (->SortedVirtualIndex
     (->> idx-as-seq
          (sort-by first bu/bytes-comparator)
          (vec))
     nil
     nil
     (atom nil))))

(defrecord OrVirtualIndex [indexes peek-state]
  db/Index
  (seek-values [this k]
    (reset! peek-state (vec (for [idx indexes]
                              (db/seek-values idx k))))
    (db/next-values this))

  db/OrderedIndex
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
    (let [version-k (idx/encode-attribute+entity+value+content-hash-key
                     (idx/id->bytes a)
                     (idx/id->bytes (.eid entity-tx))
                     (idx/value->bytes v)
                     (idx/id->bytes (.content-hash entity-tx)))]
      (with-open [i (new-prefix-kv-iterator (ks/new-iterator snapshot) version-k)]
        (when (ks/seek i version-k)
          entity-tx)))))

(defn- new-unary-join-iterator-state [idx [value results]]
  (let [result-name (:name idx)]
    {:idx idx
     :key (or value idx/nil-id-bytes)
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

  db/LayeredIndex
  (open-level [this]
    (doseq [idx indexes]
      (db/open-level idx)))

  (close-level [this]
    (doseq [idx indexes]
      (db/close-level idx)))

  (max-depth [this]
    1)

  db/OrderedIndex
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
            (recur)))))))

(defn new-unary-join-virtual-index [indexes]
  (->UnaryJoinVirtualIndex indexes (atom nil)))

(defn constrain-join-result-by-empty-names [join-keys join-results]
  (when (not-any? nil? (vals join-results))
    join-results))

(defrecord NAryJoinLayeredVirtualIndex [unary-join-indexes depth-state]
  db/Index
  (seek-values [this k]
    (db/seek-values (get unary-join-indexes @depth-state) k))

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
    (count unary-join-indexes))

  db/OrderedIndex
  (next-values [this]
    (db/next-values (get unary-join-indexes @depth-state))))

(defn new-n-ary-join-layered-virtual-index [indexes]
  (->NAryJoinLayeredVirtualIndex indexes (atom 0)))

(defrecord BinaryJoinLayeredVirtualIndex [index-and-depth-state]
  db/Index
  (seek-values [this k]
    (let [{:keys [indexes depth]} @index-and-depth-state]
      (db/seek-values (get indexes depth) k)))

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
    (count (:indexes @index-and-depth-state)))

  db/OrderedIndex
  (next-values [this]
    (let [{:keys [indexes depth]} @index-and-depth-state]
      (db/next-values (get indexes depth)))))

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
    (db/max-depth n-ary-index))

  db/OrderedIndex
  (next-values [this]
    (when-let [values (db/next-values n-ary-index)]
      (if-let [result (build-constrained-result constrain-result-fn (:result-stack @walk-state) values)]
        (do (swap! walk-state assoc :last result)
            [(first values) (second (last result))])
        (recur)))))

(defn new-n-ary-constraining-layered-virtual-index [idx constrain-result-fn]
  (->NAryConstrainingLayeredVirtualIndex idx constrain-result-fn (atom {:result-stack [] :last nil})))

(defn layered-idx->seq [idx]
  (let [max-depth (long (db/max-depth idx))
        build-result (fn [max-ks [max-k new-values]]
                       (when new-values
                         (conj max-ks max-k)))
        build-leaf-results (fn [max-ks idx]
                             (vec (for [result (idx->seq idx)
                                        :let [leaf-key (build-result max-ks result)]
                                        :when leaf-key]
                                    [leaf-key (last result)])))
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
                 (lazy-seq
                  (if (= depth (dec max-depth))
                    (concat (build-leaf-results max-ks idx)
                            (close-level))
                    (if-let [result (if needs-seek?
                                      (db/seek-values idx nil)
                                      (db/next-values idx))]
                      (open-level result)
                      (close-level))))))]
    (when (pos? max-depth)
      (step [] 0 true))))

(defn- relation-virtual-index-depth ^long [iterators-state]
  (dec (count (:indexes @iterators-state))))

(defrecord RelationVirtualIndex [relation-name max-depth layered-range-constraints iterators-state]
  db/OrderedIndex
  (seek-values [this k]
    (let [{:keys [indexes]} @iterators-state]
      (when-let [idx (last indexes)]
        (let [[k {:keys [value child-idx]}] (db/seek-values idx k)]
          (swap! iterators-state merge {:child-idx child-idx
                                        :needs-seek? false})
          (when k
            [k value])))))

  db/Index
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
         [(idx/value->bytes value)
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

;; TODO: this is quite complicated and not currently used, could be
;; deleted.
(defrecord NAryOrLayeredVirtualIndex [lhs rhs or-state]
  db/Index
  (seek-values [this k]
    (let [lhs-depth (long (get-in @or-state [:lhs :depth]))
          rhs-depth (long (get-in @or-state [:rhs :depth]))
          depth (max lhs-depth rhs-depth)]
      (when (= lhs-depth depth)
        (swap! or-state assoc-in [:lhs :peek] #(db/seek-values lhs k)))
      (when (= rhs-depth depth)
        (swap! or-state assoc-in [:rhs :peek] #(db/seek-values rhs k))))
    (db/next-values this))

  db/LayeredIndex
  (open-level [this]
    (let [lhs-depth (long (get-in @or-state [:lhs :depth]))
          rhs-depth (long (get-in @or-state [:rhs :depth]))
          depth (max lhs-depth rhs-depth)
          lhs-value (when (= lhs-depth depth)
                      (get-in @or-state [:lhs :last]))
          rhs-value (when (= rhs-depth depth)
                      (get-in @or-state [:rhs :last]))
          diff (long (cond
                       (nil? (first lhs-value))
                       1

                       (nil? (first rhs-value))
                       -1

                       :else
                       (bu/compare-bytes (first lhs-value) (first rhs-value))))]
      (cond
        (zero? diff)
        (do (swap! or-state #(-> %
                                 (update-in [:lhs :depth] inc)
                                 (assoc-in [:lhs :last] nil)
                                 (update-in [:lhs :parent-peek] conj (get-in % [:lhs :peek]))
                                 (update-in [:rhs :depth] inc)
                                 (assoc-in [:rhs :last] nil)
                                 (update-in [:rhs :parent-peek] conj (get-in % [:rhs :peek]))))
            (db/open-level lhs)
            (db/open-level rhs))

        (and (= lhs-depth depth) lhs-value (neg? diff))
        (do (swap! or-state #(-> %
                                 (update-in [:lhs :depth] inc)
                                 (assoc-in [:lhs :last] nil)
                                 (update-in [:lhs :parent-peek] conj (get-in % [:lhs :peek]))))
            (db/open-level lhs))

        (and (= rhs-depth depth) rhs-value (pos? diff))
        (do (swap! or-state #(-> %
                                 (update-in [:rhs :depth] inc)
                                 (assoc-in [:rhs :last] nil)
                                 (update-in [:rhs :parent-peek] conj (get-in % [:rhs :peek]))))
            (db/open-level rhs))))
    nil)

  (close-level [this]
    (let [lhs-depth (long (get-in @or-state [:lhs :depth]))
          rhs-depth (long (get-in @or-state [:rhs :depth]))
          depth (max lhs-depth rhs-depth)]
      (when (= lhs-depth depth)
        (swap! or-state #(-> %
                             (update-in [:lhs :depth] dec)
                             (assoc-in [:lhs :last] nil)
                             (assoc-in [:lhs :peek] (last (get-in % [:lhs :parent-peek])))
                             (update-in [:lhs :parent-peek] pop)))
        (db/close-level lhs))

      (when (= rhs-depth depth)
        (swap! or-state #(-> %
                             (update-in [:rhs :depth] dec)
                             (assoc-in [:rhs :last] nil)
                             (assoc-in [:rhs :peek] (last (get-in % [:rhs :parent-peek])))
                             (update-in [:rhs :parent-peek] pop)))
        (db/close-level rhs)))
    nil)

  (max-depth [this]
    (db/max-depth lhs))

  db/OrderedIndex
  (next-values [this]
    (let [lhs-depth (long (get-in @or-state [:lhs :depth]))
          rhs-depth (long (get-in @or-state [:rhs :depth]))
          depth (max lhs-depth rhs-depth)
          lhs-value (when (= lhs-depth depth)
                      ((get-in @or-state [:lhs :peek])))
          rhs-value (when (= rhs-depth depth)
                      ((get-in @or-state [:rhs :peek])))]
      (swap! or-state #(merge-with merge % {:lhs (when (= lhs-depth depth)
                                                   {:last lhs-value})
                                            :rhs (when (= rhs-depth depth)
                                                   {:last rhs-value})}))
      (cond
        (and (= lhs-depth depth)
             (= rhs-depth depth))
        (if (or (nil? lhs-value)
                (and (not (nil? rhs-value))
                     (pos? (bu/compare-bytes (first lhs-value)
                                             (first rhs-value)))))
          (do (swap! or-state (fn [state]
                                (-> state
                                    (assoc-in [:rhs :peek] #(db/next-values rhs))
                                    (assoc-in [:lhs :peek] (constantly lhs-value)))))
              rhs-value)
          (do (swap! or-state (fn [state]
                                (-> state
                                    (assoc-in [:lhs :peek] #(db/next-values lhs))
                                    (assoc-in [:rhs :peek] (constantly rhs-value)))))
              lhs-value))

        lhs-value
        (do (swap! or-state assoc-in [:lhs :peek] #(db/next-values lhs))
            lhs-value)

        rhs-value
        (do (swap! or-state assoc-in [:rhs :peek] #(db/next-values rhs))
            rhs-value)))))

(defn new-n-ary-or-layered-virtual-index [lhs rhs]
  (->NAryOrLayeredVirtualIndex lhs rhs (atom {:lhs {:depth 0 :last nil :peek nil :parent-peek []}
                                              :rhs {:depth 0 :last nil :peek nil :parent-peek []}})))

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

;; TODO: this should be changed to something more sensible, currently
;; the KV has a state atom where the cache lives, this is to simplify
;; API usage, and the kv instance is the main object. The main blocker
;; to remove this is the crux.query/db call hiding the cache.
;; NOTE: now also used for query caching in crux.query/build-sub-query.
(defn get-or-create-named-cache [{:keys [state] :as kv} cache-name cache-size]
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
   (->CachedObjectStore (get-or-create-named-cache kv ::doc-cache cache-size)
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

(defn ^crux.doc.CachedSnapshot new-cached-snapshot [snapshot close-snapshot?]
  (->CachedSnapshot snapshot close-snapshot? (atom #{})))
