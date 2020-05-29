(ns ^:no-doc crux.index
  (:require [crux.codec :as c]
            [crux.db :as db]
            [crux.io :as cio]
            [crux.kv :as kv]
            [crux.memory :as mem]
            [crux.morton :as morton]
            [taoensso.nippy :as nippy])
  (:import [clojure.lang IReduceInit MapEntry Seqable Sequential]
           crux.api.IndexVersionOutOfSyncException
           [crux.index BinaryJoinLayeredVirtualIndexPeekState BinaryJoinLayeredVirtualIndexState DocAttributeValueEntityEntityIndexState EntityHistoryRangeState EntityValueEntityPeekState NAryJoinLayeredVirtualIndexState NAryWalkState RelationIteratorsState RelationNestedIndexState SortedVirtualIndexState UnaryJoinIteratorState UnaryJoinIteratorsThunkFnState UnaryJoinIteratorsThunkState ValueEntityValuePeekState]
           [java.io Closeable DataInputStream]
           [java.util Collections Comparator Date]
           java.util.function.Supplier
           [org.agrona DirectBuffer ExpandableDirectByteBuffer]
           org.agrona.io.DirectBufferInputStream))

(set! *unchecked-math* :warn-on-boxed)

;; NOTE: A buffer returned from an kv/KvIterator can only be assumed
;; to be valid until the next call on the same iterator. In practice
;; this limitation is only for RocksJNRKv.
;; TODO: It would be nice to make this explicit somehow.

;; Indexes

(def ^ThreadLocal seek-buffer-tl
  (ThreadLocal/withInitial
   (reify Supplier
     (get [_]
       (ExpandableDirectByteBuffer.)))))

;; AVE

(defrecord DocAttributeValueEntityValueIndex [index-store ^DirectBuffer attr entity-resolver ^BinaryJoinLayeredVirtualIndexPeekState peek-state]
  db/Index
  (seek-values [this k]
    (let [[v & vs] (db/av index-store attr k entity-resolver)]
      (set! (.-seq peek-state) vs)
      (set! (.-key peek-state) (first v))
      v))

  (next-values [this]
    (when-let [[v & vs] (.-seq peek-state)]
      (set! (.-seq peek-state) vs)
      (set! (.-key peek-state) (first v))
      v)))

(defn new-doc-attribute-value-entity-value-index [index-store attr entity-resolver]
  (->DocAttributeValueEntityValueIndex index-store (c/->id-buffer attr) entity-resolver (BinaryJoinLayeredVirtualIndexPeekState. nil nil)))

(defrecord DocAttributeValueEntityEntityIndex [index-store ^DirectBuffer attr ^DocAttributeValueEntityValueIndex value-entity-value-idx entity-resolver-fn ^BinaryJoinLayeredVirtualIndexPeekState peek-state]
  db/Index
  (seek-values [this k]
    (when (c/valid-id? k)
      (let [value-buffer (.-key ^BinaryJoinLayeredVirtualIndexPeekState (.peek-state value-entity-value-idx))
            [v & vs] (db/ave index-store attr value-buffer k entity-resolver-fn)]
        (set! (.-seq peek-state) vs)
        (set! (.-key peek-state) (first v))
        v)))

  (next-values [this]
    (when-let [[v & vs] (.-seq peek-state)]
      (set! (.-seq peek-state) vs)
      (set! (.-key peek-state) (first v))
      v)))

(defn new-doc-attribute-value-entity-entity-index [index-store attr value-entity-value-idx entity-resolver-fn]
  (->DocAttributeValueEntityEntityIndex index-store (c/->id-buffer attr) value-entity-value-idx entity-resolver-fn (BinaryJoinLayeredVirtualIndexPeekState. nil nil)))

;; AEV

(defrecord DocAttributeEntityValueEntityIndex [index-store ^DirectBuffer attr entity-resolver ^BinaryJoinLayeredVirtualIndexPeekState peek-state]
  db/Index
  (seek-values [this k]
    (when (c/valid-id? k)
      (let [[v & vs] (db/ae index-store attr k entity-resolver)]
        (set! (.-seq peek-state) vs)
        (set! (.-key peek-state) (first v))
        v)))

  (next-values [this]
    (when-let [[v & vs] (.-seq peek-state)]
      (set! (.-seq peek-state) vs)
      (set! (.-key peek-state) (first v))
      v)))

(defn new-doc-attribute-entity-value-entity-index [index-store attr entity-resolver-fn]
  (->DocAttributeEntityValueEntityIndex index-store (c/->id-buffer attr) entity-resolver-fn (BinaryJoinLayeredVirtualIndexPeekState. nil nil)))

(defrecord DocAttributeEntityValueValueIndex [index-store ^DirectBuffer attr ^DocAttributeEntityValueEntityIndex entity-value-entity-idx entity-resolver-fn ^BinaryJoinLayeredVirtualIndexPeekState peek-state]
  db/Index
  (seek-values [this k]
    (let [eid-buffer (.-key ^BinaryJoinLayeredVirtualIndexPeekState (.peek-state entity-value-entity-idx))
          [v & vs] (db/aev index-store attr eid-buffer k entity-resolver-fn)]
      (set! (.-seq peek-state) vs)
      (set! (.-key peek-state) (first v))
      v))

  (next-values [this]
    (when-let [[v & vs] (.-seq peek-state)]
      (set! (.-seq peek-state) vs)
      (set! (.-key peek-state) (first v))
      v)))

(defn new-doc-attribute-entity-value-value-index [index-store attr entity-value-entity-idx entity-resolver-fn]
  (->DocAttributeEntityValueValueIndex index-store (c/->id-buffer attr) entity-value-entity-idx entity-resolver-fn (BinaryJoinLayeredVirtualIndexPeekState. nil nil)))

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

(defn new-prefix-equal-virtual-index [idx ^DirectBuffer prefix-v]
  (let [seek-k-pred (value-comparsion-predicate (comp not neg?) prefix-v (.capacity prefix-v))
        pred (value-comparsion-predicate zero? prefix-v (.capacity prefix-v))]
    (->PredicateVirtualIndex idx pred (fn [k]
                                        (if (seek-k-pred k)
                                          k
                                          prefix-v)))))

(defn new-less-than-equal-virtual-index [idx ^DirectBuffer max-v]
  (let [pred (value-comparsion-predicate (comp not pos?) max-v)]
    (->PredicateVirtualIndex idx pred identity)))

(defn new-less-than-virtual-index [idx ^DirectBuffer max-v]
  (let [pred (value-comparsion-predicate neg? max-v)]
    (->PredicateVirtualIndex idx pred identity)))

(defn new-greater-than-equal-virtual-index [idx ^DirectBuffer min-v]
  (let [pred (value-comparsion-predicate (comp not neg?) min-v)]
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

(defn new-greater-than-virtual-index [idx ^DirectBuffer min-v]
  (let [pred (value-comparsion-predicate pos? min-v)
        idx (->PredicateVirtualIndex idx pred (fn [k]
                                                (if (pred k)
                                                  k
                                                  min-v)))]
    (->GreaterThanVirtualIndex idx)))

(defn new-equals-virtual-index [idx ^DirectBuffer v]
  (let [pred (value-comparsion-predicate zero? v)]
    (->PredicateVirtualIndex idx pred (fn [k]
                                        (if (pred k)
                                          k
                                          v)))))

(defn wrap-with-range-constraints [idx range-constraints]
  (if range-constraints
    (range-constraints idx)
    idx))

;; Meta

(defn meta-kv [k v]
  [(c/encode-meta-key-to (.get seek-buffer-tl) (c/->id-buffer k))
   (mem/->off-heap (nippy/fast-freeze v))])

(defn store-meta [kv k v]
  (kv/store kv [(meta-kv k v)]))

(defn read-meta [kv k]
  (let [seek-k (c/encode-meta-key-to (.get seek-buffer-tl) (c/->id-buffer k))]
    (with-open [snapshot (kv/new-snapshot kv)]
      (some->> (kv/get-value snapshot seek-k)
               (DirectBufferInputStream.)
               (DataInputStream.)
               (nippy/thaw-from-in!)))))

;; Object Store

(defn <-nippy-buffer [buf]
  (nippy/thaw-from-in! (DataInputStream. (DirectBufferInputStream. buf))))

(defn ->nippy-buffer [v]
  (mem/->off-heap (nippy/fast-freeze v)))

(defn evicted-doc?
  [{:crux.db/keys [id evicted?] :as doc}]
  (boolean (or (= :crux.db/evicted id) evicted?)))

(defn keep-non-evicted-doc [doc]
  (when-not (evicted-doc? doc)
    doc))

(defn multiple-values? [v]
  (or (vector? v) (set? v)))

(defn vectorize-value [v]
  (cond-> v
    (not (multiple-values? v))
    (vector)))

(defn doc-predicate-stats [doc]
  (->> (for [[k v] doc]
         [k (count (vectorize-value v))])
       (into {})))

;; Utils

(defn current-index-version [kv]
  (with-open [snapshot (kv/new-snapshot kv)]
    (some->> (kv/get-value snapshot (c/encode-index-version-key-to (.get seek-buffer-tl)))
             (c/decode-index-version-value-from))))

(defn check-and-store-index-version [kv]
  (if-let [index-version (current-index-version kv)]
    (when (not= c/index-version index-version)
      (throw (IndexVersionOutOfSyncException.
              (str "Index version on disk: " index-version " does not match index version of code: " c/index-version))))
    (doto kv
      (kv/store [[(c/encode-index-version-key-to nil)
                  (c/encode-index-version-value-to nil c/index-version)]])
      (kv/fsync)))
  kv)

;; NOTE: We need to copy the keys and values here, as the originals
;; returned by the iterator will (may) get invalidated by the next
;; iterator call.

(defn idx->series
  [idx]
  (reify
    IReduceInit
    (reduce [_ rf init]
      (loop [ret (rf init (db/seek-values idx nil))]
        (if-let [x (db/next-values idx)]
          (let [ret (rf ret x)]
            (if (reduced? ret)
              @ret
              (recur ret)))
          ret)))
    Seqable
    (seq [_]
      (when-let [result (db/seek-values idx nil)]
        (->> (repeatedly #(db/next-values idx))
             (take-while identity)
             (cons result))))
    Sequential))

(defn idx->seq
  [idx]
  (seq (idx->series idx)))

;; Join

(extend-protocol db/LayeredIndex
  Object
  (open-level [_])
  (close-level [_])
  (max-depth [_] 1))

(def ^:private sorted-virtual-index-key-comparator
  (reify Comparator
    (compare [_ [a] [b]]
      (mem/compare-buffers (or a c/empty-buffer)
                           (or b c/empty-buffer)))))

(defrecord SortedVirtualIndex [values ^SortedVirtualIndexState state]
  db/Index
  (seek-values [this k]
    (let [idx (Collections/binarySearch values
                                        [k]
                                        sorted-virtual-index-key-comparator)
          [x & xs] (subvec values (if (neg? idx)
                                    (dec (- idx))
                                    idx))]
      (set! (.seq state) (seq xs))
      x))

  (next-values [this]
    (when-let [[x & xs] (.seq state)]
      (set! (.seq state) (seq xs))
      x)))

(defn new-sorted-virtual-index [idx-or-seq]
  (let [idx-as-seq (if (satisfies? db/Index idx-or-seq)
                     (idx->seq idx-or-seq)
                     idx-or-seq)]
    (->SortedVirtualIndex
     (->> idx-as-seq
          (sort-by first mem/buffer-comparator)
          (distinct)
          (vec))
     (SortedVirtualIndexState. nil))))

;; NOTE: Not used by production code, kept for reference.
(defrecord OrVirtualIndex [indexes ^objects peek-state]
  db/Index
  (seek-values [this k]
    (loop [[idx & indexes] indexes
           i 0]
      (when idx
        (aset peek-state i (db/seek-values idx k))
        (recur indexes (inc i))))
    (db/next-values this))

  (next-values [this]
    (let [[n value] (->> (map-indexed vector peek-state)
                         (remove (comp nil? second))
                         (sort-by (comp first second) mem/buffer-comparator)
                         (first))]
      (when n
        (aset peek-state n (db/next-values (get indexes n))))
      value)))

(defn new-or-virtual-index [indexes]
  (->OrVirtualIndex indexes (object-array (count indexes))))

(defn- new-unary-join-iterator-state [idx [value results]]
  (let [result-name (:name idx)]
    (UnaryJoinIteratorState.
     idx
     (or value c/empty-buffer)
     (when (and result-name value)
       {result-name results}))))

(defrecord UnaryJoinVirtualIndex [indexes ^UnaryJoinIteratorsThunkFnState state]
  db/Index
  (seek-values [this k]
    (->> #(let [iterators (->> (for [idx indexes]
                                 (new-unary-join-iterator-state idx (db/seek-values idx k)))
                               (sort-by (fn [x] (.key ^UnaryJoinIteratorState x)) mem/buffer-comparator)
                               (vec))]
            (UnaryJoinIteratorsThunkState. iterators 0))
         (set! (.thunk state)))
    (db/next-values this))

  (next-values [this]
    (when-let [iterators-thunk (.thunk state)]
      (when-let [iterators-thunk ^UnaryJoinIteratorsThunkState (iterators-thunk)]
        (let [iterators (.iterators iterators-thunk)
              index (.index iterators-thunk)
              iterator-state ^UnaryJoinIteratorState (nth iterators index nil)
              max-index (mod (dec index) (count iterators))
              max-k (.key ^UnaryJoinIteratorState (nth iterators max-index nil))
              match? (mem/buffers=? (.key iterator-state) max-k)
              idx (.idx iterator-state)]
          (->> #(let [next-value+results (if match?
                                           (db/next-values idx)
                                           (db/seek-values idx max-k))]
                  (when next-value+results
                    (set! (.iterators iterators-thunk)
                          (assoc iterators index (new-unary-join-iterator-state idx next-value+results)))
                    (set! (.index iterators-thunk) (mod (inc index) (count iterators)))
                    iterators-thunk))
               (set! (.thunk state)))
          (if match?
            (let [new-results (map (fn [x] (.results ^UnaryJoinIteratorState x)) iterators)]
              (when-let [result (->> new-results
                                     (apply merge-with
                                            (fn [x y]
                                              (if (map? y)
                                                y
                                                x))))]
                (MapEntry/create max-k result)))
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

(defrecord SingletonUnaryJoinVirtualIndex [idx]
  db/Index
  (seek-values [this k]
    (let [[k result] (db/seek-values idx k)]
      (when k
        (MapEntry/create k {(:name idx) result}))))

  (next-values [this]
    (let [[k result] (db/next-values idx)]
      (when k
        (MapEntry/create k {(:name idx) result}))))

  db/LayeredIndex
  (open-level [this]
    (db/open-level idx))

  (close-level [this]
    (db/close-level idx))

  (max-depth [this]
    1))

(defn new-unary-join-virtual-index [indexes]
  (if (= 1 (count indexes))
    (->SingletonUnaryJoinVirtualIndex (first indexes))
    (->UnaryJoinVirtualIndex indexes (UnaryJoinIteratorsThunkFnState. nil))))

(defn constrain-join-result-by-empty-names [join-keys join-results]
  (when (not-any? nil? (vals join-results))
    join-results))

(defrecord NAryJoinLayeredVirtualIndex [unary-join-indexes ^NAryJoinLayeredVirtualIndexState state]
  db/Index
  (seek-values [this k]
    (db/seek-values (nth unary-join-indexes (.depth state) nil) k))

  (next-values [this]
    (db/next-values (nth unary-join-indexes (.depth state) nil)))

  db/LayeredIndex
  (open-level [this]
    (db/open-level (nth unary-join-indexes (.depth state) nil))
    (set! (.depth state) (inc (.depth state)))
    nil)

  (close-level [this]
    (db/close-level (nth unary-join-indexes (dec (long (.depth state))) nil))
    (set! (.depth state) (dec (.depth state)))
    nil)

  (max-depth [this]
    (count unary-join-indexes)))

(defn new-n-ary-join-layered-virtual-index [indexes]
  (->NAryJoinLayeredVirtualIndex indexes (NAryJoinLayeredVirtualIndexState. 0)))

(defrecord BinaryJoinLayeredVirtualIndex [^BinaryJoinLayeredVirtualIndexState state]
  db/Index
  (seek-values [this k]
    (db/seek-values (nth (.indexes state) (.depth state) nil) k))

  (next-values [this]
    (db/next-values (nth (.indexes state) (.depth state) nil)))

  db/LayeredIndex
  (open-level [this]
    (db/open-level (nth (.indexes state) (.depth state) nil))
    (set! (.depth state) (inc (.depth state)))
    nil)

  (close-level [this]
    (db/close-level (nth (.indexes state) (dec (long (.depth state))) nil))
    (set! (.depth state) (dec (.depth state)))
    nil)

  (max-depth [this]
    2))

(defn new-binary-join-virtual-index
  ([]
   (new-binary-join-virtual-index nil nil))
  ([lhs-index rhs-index]
   (->BinaryJoinLayeredVirtualIndex (BinaryJoinLayeredVirtualIndexState.
                                     [lhs-index rhs-index]
                                     0))))

(defn update-binary-join-order! [^BinaryJoinLayeredVirtualIndex binary-join-index lhs-index rhs-index]
  (set! (.indexes ^BinaryJoinLayeredVirtualIndexState (.state binary-join-index)) [lhs-index rhs-index])
  binary-join-index)

(defn- build-constrained-result [constrain-result-fn result-stack [max-k new-values]]
  (let [[max-ks parent-result] (last result-stack)
        join-keys (conj (or max-ks []) max-k)]
    (when-let [join-results (->> (merge parent-result new-values)
                                 (constrain-result-fn join-keys)
                                 (not-empty))]
      (conj result-stack [join-keys join-results]))))

(defrecord NAryConstrainingLayeredVirtualIndex [n-ary-index constrain-result-fn ^NAryWalkState state]
  db/Index
  (seek-values [this k]
    (when-let [[value :as values] (db/seek-values n-ary-index k)]
      (if-let [result (build-constrained-result constrain-result-fn (.result-stack state) values)]
        (do (set! (.last state) result)
            (MapEntry/create value (second (last result))))
        (db/next-values this))))

  (next-values [this]
    (when-let [[value :as values] (db/next-values n-ary-index)]
      (if-let [result (build-constrained-result constrain-result-fn (.result-stack state) values)]
        (do (set! (.last state) result)
            (MapEntry/create value (second (last result))))
        (recur))))

  db/LayeredIndex
  (open-level [this]
    (db/open-level n-ary-index)
    (set! (.result-stack state) (.last state))
    nil)

  (close-level [this]
    (db/close-level n-ary-index)
    (set! (.result-stack state) (pop (.result-stack state)))
    nil)

  (max-depth [this]
    (db/max-depth n-ary-index)))

(defn new-n-ary-constraining-layered-virtual-index [idx constrain-result-fn]
  (->NAryConstrainingLayeredVirtualIndex idx constrain-result-fn (NAryWalkState. [] nil)))

(defn layered-idx->seq [idx]
  (when idx
    (let [max-depth (long (db/max-depth idx))
          build-result (fn [max-ks [max-k new-values]]
                         (when new-values
                           (conj max-ks max-k)))
          build-leaf-results (fn [max-ks idx]
                               (for [result (idx->seq idx)
                                     :let [leaf-key (build-result max-ks result)]
                                     :when leaf-key]
                                 (MapEntry/create leaf-key (last result))))
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
        (step [] 0 true)))))

(defn- relation-virtual-index-depth ^long [^RelationIteratorsState iterators-state]
  (dec (count (.indexes iterators-state))))

(defrecord RelationVirtualIndex [relation-name max-depth layered-range-constraints encode-value-fn ^RelationIteratorsState state]
  db/Index
  (seek-values [this k]
    (when-let [idx (last (.indexes state))]
      (let [[k ^RelationNestedIndexState nested-index-state] (db/seek-values idx k)]
        (set! (.child-idx state) (some-> nested-index-state (.child-idx)))
        (set! (.needs-seek? state) false)
        (when k
          (MapEntry/create k (.value nested-index-state))))))

  (next-values [this]
    (if (.needs-seek? state)
      (db/seek-values this nil)
      (when-let [idx (last (.indexes state))]
        (let [[k ^RelationNestedIndexState nested-index-state] (db/next-values idx)]
          (set! (.child-idx state) (some-> nested-index-state (.child-idx)))
          (when k
            (MapEntry/create k (.value nested-index-state)))))))

  db/LayeredIndex
  (open-level [this]
    (when (= max-depth (relation-virtual-index-depth state))
      (throw (IllegalStateException. (str "Cannot open level at max depth: " max-depth))))
    (set! (.indexes state) (conj (.indexes state) (.child-idx state)))
    (set! (.child-idx state) nil)
    (set! (.needs-seek? state) true)
    nil)

  (close-level [this]
    (when (zero? (relation-virtual-index-depth state))
      (throw (IllegalStateException. "Cannot close level at root.")))
    (set! (.indexes state) (pop (.indexes state)))
    (set! (.child-idx state) nil)
    (set! (.needs-seek? state) false)
    nil)

  (max-depth [this]
    max-depth))

(defn- build-nested-index [encode-value-fn tuples [range-constraints & next-range-constraints]]
  (-> (new-sorted-virtual-index
       (for [prefix (vals (group-by first tuples))
             :let [value (ffirst prefix)]]
         (MapEntry/create (encode-value-fn value)
                          (RelationNestedIndexState.
                           value
                           (when (seq (next (first prefix)))
                             (build-nested-index encode-value-fn (map next prefix) next-range-constraints))))))
      (wrap-with-range-constraints range-constraints)))

(defn update-relation-virtual-index!
  ([^RelationVirtualIndex relation tuples]
   (update-relation-virtual-index! relation tuples (.layered-range-constraints relation)))
  ([^RelationVirtualIndex relation tuples layered-range-constraints]
   (let [state ^RelationIteratorsState (.state relation)]
     (set! (.indexes state) [(binding [nippy/*freeze-fallback* :write-unfreezable]
                               (build-nested-index (.encode-value-fn relation) tuples layered-range-constraints))])
     (set! (.child-idx state) nil)
     (set! (.needs-seek? state) false))
   relation))

(defn new-relation-virtual-index
  ([relation-name tuples max-depth encode-value-fn]
   (new-relation-virtual-index relation-name tuples max-depth encode-value-fn nil))
  ([relation-name tuples max-depth encode-value-fn layered-range-constraints]
   (let [iterators-state (RelationIteratorsState. nil nil false)]
     (update-relation-virtual-index! (->RelationVirtualIndex relation-name max-depth layered-range-constraints encode-value-fn iterators-state) tuples))))
