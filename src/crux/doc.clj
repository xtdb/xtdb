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
           [java.util Collections Comparator Date]
           [crux.index EntityTx]))

(set! *unchecked-math* :warn-on-boxed)

;; Docs

;; AVE

(defn- attribute-value+placeholder [k attr peek-state]
  (let [seek-k (idx/encode-attribute+value-entity-prefix-key attr idx/empty-byte-array)]
    (when (bu/bytes=? k seek-k (alength seek-k))
      (let [[v] (idx/decode-attribute+value+entity+content-hash-key->value+entity+content-hash k)]
        (reset! peek-state {:last k})
        [(idx/value->bytes v) {:crux.doc.binary-placeholder/value #{true}}]))))

(defrecord DocAttributeValueEntityValueIndex [i attr peek-state]
  db/Index
  (seek-values [this k]
    (when-let [k (->> (or k idx/empty-byte-array)
                      (idx/encode-attribute+value-entity-prefix-key attr)
                      (ks/seek i))]
      (attribute-value+placeholder k attr peek-state)))

  db/OrderedIndex
  (next-values [this]
    (when-let [k (ks/next i)]
      (let [prefix-size (- (alength ^bytes k) idx/id-size idx/id-size)
            k (if (bu/bytes=? k (:last @peek-state) prefix-size)
                (ks/seek i (bu/inc-unsigned-bytes k prefix-size))
                k)]
        (attribute-value+placeholder k attr peek-state)))))

(defn- attribute-value+entity+content-hashes-for-current-key [i ^bytes current-k attr v-bytes peek-state]
  (let [prefix-size (- (alength current-k) idx/id-size)
        seek-k (idx/encode-attribute+value-entity-prefix-key attr v-bytes)]
    (when (bu/bytes=? seek-k current-k (alength seek-k))
      (loop [acc []
             k current-k]
        (if-let [value+entity+content-hash (when (and k (bu/bytes=? current-k k prefix-size))
                                             (idx/decode-attribute+value+entity+content-hash-key->value+entity+content-hash k))]
          (recur (conj acc value+entity+content-hash)
                 (ks/next i))
          (do (reset! peek-state {:peek k :last current-k})
              (when (seq acc)
                [(idx/value->bytes (second (first acc))) (mapv #(nth % 2) acc)])))))))

(defrecord DocAttributeValueEntityEntityIndex [i value-entity-value-idx peek-state]
  db/Index
  (seek-values [this k]
    (when-let [[v e] (some->> (:peek-state value-entity-value-idx)
                              (deref)
                              :last
                              (idx/decode-attribute+value+entity+content-hash-key->value+entity+content-hash))]
      (let [v (reify
                idx/ValueToBytes
                (value->bytes [_]
                  v))]
        (when-let [k (->> (idx/encode-attribute+value+entity+content-hash-key
                           (:attr value-entity-value-idx)
                           v
                           (or k e)
                           idx/empty-byte-array)
                          (ks/seek i))]
          (attribute-value+entity+content-hashes-for-current-key i k (:attr value-entity-value-idx) v peek-state)))))

  db/OrderedIndex
  (next-values [this]
    (when-let [v (try
                   (some->> (:peek-state value-entity-value-idx)
                            (deref)
                            :last
                            (idx/decode-attribute+value+entity+content-hash-key->value+entity+content-hash)
                            (first))
                   (catch AssertionError ignore))]
      (let [v (reify
                idx/ValueToBytes
                (value->bytes [_]
                  v))]
        (when-let [k (or (:peek @peek-state) (ks/next i))]
          (attribute-value+entity+content-hashes-for-current-key i k (:attr value-entity-value-idx) v peek-state))))))

(defn new-doc-attribute-value-entity-value-index [i attr]
  (->DocAttributeValueEntityValueIndex i attr (atom nil)))

(defn new-doc-attribute-value-entity-entity-index [i value-entity-value-idx]
  (->DocAttributeValueEntityEntityIndex i value-entity-value-idx (atom nil)))

;; AEV

(defn- attribute-entity+placeholder [k attr peek-state]
  (let [seek-k (idx/encode-attribute+entity-value-prefix-key attr idx/empty-byte-array)]
    (when (bu/bytes=? k seek-k (alength seek-k))
      (let [[_ e] (idx/decode-attribute+value+entity+content-hash-key->value+entity+content-hash k)]
        (reset! peek-state {:last k})
        [(idx/value->bytes e) {:crux.doc.binary-placeholder/entity #{true}}]))))

(defrecord DocAttributeEntityValueEntityIndex [i attr peek-state]
  db/Index
  (seek-values [this k]
    (when-let [k (->> (or k idx/empty-byte-array)
                      (idx/id->bytes)
                      (idx/encode-attribute+entity-value-prefix-key attr)
                      (ks/seek i))]
      (attribute-entity+placeholder k attr peek-state)))

  db/OrderedIndex
  (next-values [this]
    (when-let [k (ks/next i)]
      (let [prefix-size (+ Short/SIZE idx/id-size idx/id-size)
            k (if (bu/bytes=? k (:last @peek-state) prefix-size)
                (ks/seek i (bu/inc-unsigned-bytes k prefix-size))
                k)]
        (attribute-entity+placeholder k attr peek-state)))))

(defn- attribute-entity+value+content-hashes-for-current-key [i ^bytes current-k attr entity-bytes peek-state]
  (let [prefix-size (- (alength current-k) idx/id-size)
        seek-k (idx/encode-attribute+entity-value-prefix-key attr entity-bytes)]
    (when (bu/bytes=? seek-k current-k (alength seek-k))
      (loop [acc []
             k current-k]
        (if-let [entity+value+content-hash (when (and k (bu/bytes=? current-k k prefix-size))
                                             (idx/decode-attribute+entity+value+content-hash-key->entity+value+content-hash k))]
          (recur (conj acc entity+value+content-hash)
                 (ks/next i))
          (do (reset! peek-state {:peek k :last current-k})
              (when (seq acc)
                [(idx/value->bytes (second (first acc))) (mapv #(nth % 2) acc)])))))))

(defrecord DocAttributeEntityValueValueIndex [i entity-value-entity-idx peek-state]
  db/Index
  (seek-values [this k]
    (when-let [[e v] (try
                       (some->> (:peek-state entity-value-entity-idx)
                                (deref)
                                :last
                                (idx/decode-attribute+entity+value+content-hash-key->entity+value+content-hash))
                       (catch AssertionError ignore))]
      (let [e (idx/id->bytes e)]
        (when-let [k (->> (idx/encode-attribute+entity+value+content-hash-key
                           (:attr entity-value-entity-idx)
                           e
                           (or k idx/empty-byte-array)
                           idx/empty-byte-array)
                          (ks/seek i))]
          (attribute-entity+value+content-hashes-for-current-key i k (:attr entity-value-entity-idx) e peek-state)))))

  db/OrderedIndex
  (next-values [this]
    (when-let [e (some->> (:peek-state entity-value-entity-idx)
                          (deref)
                          :last
                          (idx/decode-attribute+entity+value+content-hash-key->entity+value+content-hash)
                          (first))]
      (let [e (idx/id->bytes e)]
        (when-let [k (or (:peek @peek-state) (ks/next i))]
          (attribute-entity+value+content-hashes-for-current-key i k (:attr entity-value-entity-idx) e peek-state))))))

(defn new-doc-attribute-entity-value-entity-index [i attr]
  (->DocAttributeEntityValueEntityIndex i attr (atom nil)))

(defn new-doc-attribute-entity-value-value-index [i entity-value-entity-idx]
  (->DocAttributeEntityValueValueIndex i entity-value-entity-idx (atom nil)))

;; Regular Indexes

(defrecord DocLiteralAttributeValueIndex [i attr value]
  db/Index
  (seek-values [this k]
    (let [seek-k (idx/encode-attribute+value+content-hash-key attr value (or k idx/empty-byte-array))]
      (when-let [^bytes k (ks/seek i seek-k)]
        (when (bu/bytes=? k seek-k (- (alength k) idx/id-size))
          (let [[_ content-hash] (idx/decode-attribute+value+content-hash-key->value+content-hash k)]
            [(idx/value->bytes content-hash) [content-hash]])))))

  db/OrderedIndex
  (next-values [this]
    (when-let [^bytes k (ks/next i)]
      (let [seek-k (idx/encode-attribute+value-prefix-key attr value)]
        (when (bu/bytes=? k seek-k (- (alength k) idx/id-size))
          (let [[_ content-hash] (idx/decode-attribute+value+content-hash-key->value+content-hash k)]
            [(idx/value->bytes content-hash) [content-hash]]))))))

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

(defrecord GreaterThanVirtualIndex [idx]
  db/Index
  (seek-values [this k]
    (or (db/seek-values idx k)
        (db/next-values idx)))

  db/OrderedIndex
  (next-values [this]
    (db/next-values idx)))

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

(defn wrap-with-range-constraints [idx range-constraints]
  (if range-constraints
    (range-constraints idx)
    idx))

(defn normalize-value [v]
  (cond-> v
    (not (or (vector? v)
             (set? v))) (vector)))

(defn index-doc [kv content-hash doc]
  (let [id (:crux.db/id doc)]
    (ks/store kv (->> (for [[k v] doc
                            v (normalize-value v)
                            :when (seq (idx/value->bytes v))]
                        [[(idx/encode-attribute+value+content-hash-key k v content-hash)
                          idx/empty-byte-array]
                         [(idx/encode-attribute+value+entity+content-hash-key k v id content-hash)
                          idx/empty-byte-array]
                         [(idx/encode-attribute+entity+value+content-hash-key k id v content-hash)
                          idx/empty-byte-array]])
                      (reduce into [])))))

(defn delete-doc-from-index [kv content-hash doc]
  (let [id (:crux.db/id doc)]
    (ks/delete kv (->> (for [[k v] doc
                             v (normalize-value v)
                             :when (seq (idx/value->bytes v))]
                         [(idx/encode-attribute+value+content-hash-key k v content-hash)
                          (idx/encode-attribute+value+entity+content-hash-key k v id content-hash)
                          (idx/encode-attribute+entity+value+content-hash-key k id v content-hash)])
                       (reduce into [])))))

(defrecord DocObjectStore [kv]
  db/ObjectStore
  (get-objects [this ks]
    (with-open [snapshot (ks/new-snapshot kv)
                i (ks/new-iterator snapshot)]
      (->> (for [seek-k (->> (map idx/encode-doc-key ks)
                             (sort bu/bytes-comparator))
                 :let [k (ks/seek i seek-k)]
                 :when (and k (bu/bytes=? seek-k k))]
             [(idx/decode-doc-key k)
              (nippy/fast-thaw (ks/value i))])
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
    (when-let [k (ks/seek i (idx/encode-meta-key k))]
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
    (let [prefix-size (+ Short/BYTES idx/id-size)
          seek-k (idx/encode-entity+bt+tt-prefix-key
                  k
                  business-time
                  transact-time)]
      (loop [k (ks/seek i seek-k)]
        (when (and k (bu/bytes=? seek-k k prefix-size))
          (let [v (ks/value i)
                entity-tx (-> (idx/decode-entity+bt+tt+tx-id-key k)
                              (enrich-entity-tx v))]
            (if (<= (compare (.tt entity-tx) transact-time) 0)
              (when-not (bu/bytes=? idx/nil-id-bytes v)
                [(idx/id->bytes (.eid entity-tx)) [entity-tx]])
              (recur (ks/next i)))))))))

(defn entities-at [snapshot entities business-time transact-time]
  (with-open [i (ks/new-iterator snapshot)]
    (let [entity-as-of-idx (->EntityAsOfIndex i business-time transact-time)]
      (some->> (for [entity entities
                     :let [[_ [entity-tx]] (db/seek-values entity-as-of-idx entity)]
                     :when entity-tx]
                 entity-tx)
               (not-empty)
               (vec)))))

(defrecord ContentHashEntityIndex [i]
  db/Index
  (db/seek-values [this k]
    (when-let [value+results (->> (idx/encode-content-hash-prefix-key k)
                                  (all-keys-in-prefix i)
                                  (map idx/decode-content-hash+entity-key->entity)
                                  (seq))]
      [(ffirst value+results) (mapv second value+results)])))

(defn- value+content-hashes->value+entities [content-hash-entity-idx entity-as-of-idx value+content-hashes]
  (when-let [[v content-hashes] value+content-hashes]
    (when-let [entity-txs (->> (for [content-hash content-hashes
                                     :let [[_ entities] (db/seek-values content-hash-entity-idx content-hash)]
                                     entity entities
                                     :let [[_ [entity-tx]] (db/seek-values entity-as-of-idx entity)]
                                     :when (and entity-tx (= content-hash (.content-hash ^EntityTx entity-tx)))]
                                 entity-tx)
                               (seq))]
      [v (vec entity-txs)])))

(defrecord EntityAttributeValueVirtualIndex [doc-idx content-hash-entity-idx entity-as-of-idx]
  db/Index
  (seek-values [this k]
    (when-let [values (db/seek-values doc-idx k)]
      (if-let [result (value+content-hashes->value+entities content-hash-entity-idx entity-as-of-idx values)]
        result
        (db/next-values this))))

  db/OrderedIndex
  (next-values [this]
    (when-let [values (db/next-values doc-idx)]
      (if-let [result (value+content-hashes->value+entities content-hash-entity-idx entity-as-of-idx values)]
        result
        (recur)))))

(defn entities-by-attribute-value-at [snapshot attr range-constraints business-time transact-time]
  (with-open [di (ks/new-iterator snapshot)
              ci (ks/new-iterator snapshot)
              ei (ks/new-iterator snapshot)]
    (let [doc-idx (-> (new-doc-attribute-value-entity-value-index di attr)
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

(extend-protocol db/LayeredIndex
  Object
  (open-level [_])
  (close-level [_])
  (max-depth [_] 1))

(defrecord LiteralEntityAttributeValuesVirtualIndex [object-store entity-as-of-idx entity attr attr-state]
  db/Index
  (seek-values [this k]
    (if-let [entity-tx (get @attr-state :entity-tx (let [[_ [entity-tx]] (db/seek-values entity-as-of-idx entity)]
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
  (next-values [this]
    (let [{:keys [first entity-tx]} (swap! attr-state (fn [{[x & xs] :rest
                                                            :as attr-state}]
                                                        (assoc attr-state :first x :rest xs)))]
      (when first
        [first [entity-tx]]))))

(defn new-literal-entity-attribute-values-virtual-index [object-store snapshot entity attr range-constraints business-time transact-time]
  (let [entity-as-of-idx (->EntityAsOfIndex (ks/new-iterator snapshot) business-time transact-time)]
    (-> (->LiteralEntityAttributeValuesVirtualIndex object-store entity-as-of-idx entity attr (atom nil))
        (wrap-with-range-constraints range-constraints))))

(def ^:private ^:dynamic *external-sort-limit* (* 1024 1024))

(defn- read-external-sort-value [^RandomAccessFile raf [k offset length]]
  (let [bs (byte-array length)]
    (.seek raf offset)
    (.readFully raf bs)
    [k (nippy/fast-thaw bs)]))

(defrecord SortedVirtualIndex [values ^RandomAccessFile raf ^File file seq-state]
  db/Index
  (seek-values [this k]
    (let [idx (Collections/binarySearch values
                                        [(idx/value->bytes k)]
                                        (reify Comparator
                                          (compare [_ [a] [b]]
                                            (bu/compare-bytes (or a idx/nil-id-bytes)
                                                              (or b idx/nil-id-bytes)))))
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

(defn new-sorted-external-virtual-index [idx-or-seq]
  (let [idx-as-seq (if (satisfies? db/OrderedIndex idx-or-seq)
                     (idx->seq idx-or-seq)
                     idx-or-seq)
        file (doto (File/createTempFile "crux-external-sort" ".nippy")
               (.deleteOnExit))
        path (.getAbsolutePath file)
        [acc] (with-open [raf (RandomAccessFile. file "rw")]
                (->> idx-as-seq
                     (reduce
                      (fn [[acc ^long offset] [k v]]
                        (let [bs ^bytes (nippy/fast-freeze v)]
                          (.write raf bs)
                          [(conj acc [k offset (count bs)])
                           (+ offset (count bs))]))
                      [[] 0])))
        raf (RandomAccessFile. file "r")]
    (cio/register-cleaner file (fn []
                                 (.close raf)
                                 (.delete (File. path))))
    (->SortedVirtualIndex
     (vec (sort-by first bu/bytes-comparator acc))
     raf
     file
     (atom nil))))

(defn new-sorted-in-memory-virtual-index [idx-or-seq]
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

(defn new-sorted-virtual-index [idx-or-seq]
  (let [idx-as-seq (if (satisfies? db/OrderedIndex idx-or-seq)
                     (idx->seq idx-or-seq)
                     idx-or-seq)]
    (if (and *external-sort-limit* (seq (drop *external-sort-limit* idx-as-seq)))
      (new-sorted-external-virtual-index idx-as-seq)
      (new-sorted-in-memory-virtual-index idx-as-seq))))

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

(defn- new-unary-join-iterator-state [idx [value results]]
  (let [result-name (:name idx (gensym "result-name"))]
    {:idx idx
     :key (or value idx/nil-id-bytes)
     :result-name result-name
     :results (cond
                (nil? results)
                nil
                (map? results)
                results
                :else
                {result-name (set results)})}))

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
                                               (db/seek-values idx (reify
                                                                     idx/ValueToBytes
                                                                     (value->bytes [_]
                                                                       max-k)

                                                                     idx/IdToBytes
                                                                     (id->bytes [_]
                                                                       max-k)))))]
                  (when next-value+results
                    {:iterators (assoc iterators index (new-unary-join-iterator-state idx next-value+results))
                     :index (mod (inc index) (count iterators))}))
               (reset! iterators-thunk-state))
          (if match?
            (let [names (map :result-name iterators)]
              (log/debug :match names (bu/bytes->hex max-k))
              (when-let [result (->> (map :results iterators)
                                     (apply merge-with set/intersection)
                                     (not-empty))]
                [max-k result]))
            (recur)))))))

(defn new-unary-join-virtual-index [indexes]
  (->UnaryJoinVirtualIndex indexes (atom nil)))

(defn constrain-join-result-by-empty-names [join-keys join-results]
  (when (every? not-empty (vals join-results))
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


(defn- build-constrained-result [constrain-result-fn result-stack [max-k new-values]]
  (let [[max-ks parent-result] (last result-stack)
        join-keys (conj (or max-ks []) max-k)]
    (when-let [join-results (->> (merge-with set/intersection parent-result new-values)
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

(defn- values+unary-join-results->value+entities [content-hash-entity-idx entity-as-of-idx content-hash+unary-join-results]
  (when-let [[content-hash] content-hash+unary-join-results]
    (let [value+content-hashes [content-hash [(idx/new-id content-hash)]]]
      (when-let [[_ [entity-tx]] (value+content-hashes->value+entities content-hash-entity-idx entity-as-of-idx value+content-hashes)]
        [(idx/value->bytes (:eid entity-tx)) [entity-tx]]))))

(defrecord SharedEntityLiteralAttributeValuesVirtualIndex [unary-join-literal-doc-idx content-hash-entity-idx entity-as-of-idx]
  db/Index
  (seek-values [this k]
    (->> (db/seek-values unary-join-literal-doc-idx k)
         (values+unary-join-results->value+entities content-hash-entity-idx entity-as-of-idx)))

  db/OrderedIndex
  (next-values [this]
    (when-let [values+unary-join-results (seq (db/next-values unary-join-literal-doc-idx))]
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

(defn new-shared-literal-attribute-for-known-entities-virtual-index [object-store snapshot entities attr+values business-time transact-time]
  (let [entities (entities-at snapshot entities business-time transact-time)
        content-hash->doc (db/get-objects object-store (map :content-hash entities))]
    (->> (for [{:keys [eid content-hash] :as entity} entities
               :let [doc (get content-hash->doc content-hash)]
               :when (->> (for [[attr value] attr+values]
                            (contains? (set (normalize-value (get doc attr))) value))
                          (every? true?))]
           [(idx/value->bytes eid) [entity]])
         (new-sorted-virtual-index))))

(defn new-entity-attribute-value-virtual-index [snapshot attr-or-idx range-constraints business-time transact-time]
  (let [entity-as-of-idx (->EntityAsOfIndex (ks/new-iterator snapshot) business-time transact-time)
        content-hash-entity-idx (->ContentHashEntityIndex (ks/new-iterator snapshot))
        doc-idx (if (satisfies? db/OrderedIndex attr-or-idx)
                  attr-or-idx
                  (-> (new-doc-attribute-value-entity-value-index (ks/new-iterator snapshot) attr-or-idx)
                      (wrap-with-range-constraints range-constraints)))]
    (->EntityAttributeValueVirtualIndex doc-idx content-hash-entity-idx entity-as-of-idx)))

(defn new-entity-attribute-value-known-entities-virtual-index [object-store snapshot entities attr range-constraints business-time transact-time]
  (let [entities (entities-at snapshot entities business-time transact-time)
        content-hash->doc (db/get-objects object-store (map :content-hash entities))
        value->entities (->> (for [{:keys [eid content-hash] :as entity} entities
                                   :let [doc (get content-hash->doc content-hash)]
                                   value (normalize-value (get doc attr))]
                               {value #{entity}})
                             (apply merge-with into))]
    (-> (for [[value entities] value->entities]
          [(idx/value->bytes value) (vec entities)])
        (new-sorted-virtual-index)
        (wrap-with-range-constraints range-constraints))))

(defn- build-nested-index [tuples [range-constraints & next-range-constraints]]
  (-> (new-sorted-in-memory-virtual-index
       (for [prefix (partition-by first tuples)
             :let [value (ffirst prefix)]]
         [(idx/value->bytes value)
          {:value value
           :child-idx (when (seq (next (first prefix)))
                        (build-nested-index (map next prefix) next-range-constraints))}]))
      (wrap-with-range-constraints range-constraints)))

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
            [k [value]])))))

  db/Index
  (next-values [this]
    (let [{:keys [needs-seek? indexes]} @iterators-state]
      (if needs-seek?
        (db/seek-values this nil)
        (when-let [idx (last indexes)]
          (let [[k {:keys [value child-idx]}] (db/next-values idx)]
            (swap! iterators-state assoc :child-idx child-idx)
            (when k
              [k [value]]))))))

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

(defn update-relation-virtual-index!
  ([relation tuples]
   (update-relation-virtual-index! relation tuples (:layered-range-constraints relation)))
  ([relation tuples layered-range-constraints]
   (reset! (:iterators-state relation)
           {:indexes [(binding [nippy/*freeze-fallback* :write-unfreezable]
                        (build-nested-index (sort tuples) layered-range-constraints))]
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
