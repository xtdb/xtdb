(ns ^:no-doc crux.index
  (:require [crux.db :as db]
            [crux.memory :as mem]
            [taoensso.nippy :as nippy])
  (:import [crux.index BinaryJoinLayeredVirtualIndexPeekState BinaryJoinLayeredVirtualIndexState NAryJoinLayeredVirtualIndexState NAryWalkState
            RelationVirtualIndexState SortedVirtualIndexState UnaryJoinIteratorState UnaryJoinIteratorsThunkFnState UnaryJoinIteratorsThunkState]
           java.util.Comparator
           org.agrona.DirectBuffer))

(set! *unchecked-math* :warn-on-boxed)

;; AVE

(defrecord DocAttributeValueEntityValueIndex [index-store ^DirectBuffer attr entity-resolver ^BinaryJoinLayeredVirtualIndexPeekState peek-state]
  db/Index
  (seek-values [this k]
    (let [[v & vs] (db/av index-store attr k entity-resolver)]
      (set! (.-seq peek-state) vs)
      (set! (.-key peek-state) v)
      v))

  (next-values [this]
    (when-let [[v & vs] (.-seq peek-state)]
      (set! (.-seq peek-state) vs)
      (set! (.-key peek-state) v)
      v)))

(defn new-doc-attribute-value-entity-value-index [index-store attr entity-resolver]
  (->DocAttributeValueEntityValueIndex index-store attr entity-resolver (BinaryJoinLayeredVirtualIndexPeekState. nil nil)))

(defrecord DocAttributeValueEntityEntityIndex [index-store ^DirectBuffer attr ^DocAttributeValueEntityValueIndex value-entity-value-idx entity-resolver-fn ^BinaryJoinLayeredVirtualIndexPeekState peek-state]
  db/Index
  (seek-values [this k]
    (let [value-buffer (.-key ^BinaryJoinLayeredVirtualIndexPeekState (.peek-state value-entity-value-idx))
          [v & vs] (db/ave index-store attr value-buffer k entity-resolver-fn)]
      (set! (.-seq peek-state) vs)
      (set! (.-key peek-state) v)
      v))

  (next-values [this]
    (when-let [[v & vs] (.-seq peek-state)]
      (set! (.-seq peek-state) vs)
      (set! (.-key peek-state) v)
      v)))

(defn new-doc-attribute-value-entity-entity-index [index-store attr value-entity-value-idx entity-resolver-fn]
  (->DocAttributeValueEntityEntityIndex index-store attr value-entity-value-idx entity-resolver-fn (BinaryJoinLayeredVirtualIndexPeekState. nil nil)))

;; AEV

(defrecord DocAttributeEntityValueEntityIndex [index-store ^DirectBuffer attr entity-resolver ^BinaryJoinLayeredVirtualIndexPeekState peek-state]
  db/Index
  (seek-values [this k]
    (let [[v & vs] (db/ae index-store attr k entity-resolver)]
      (set! (.-seq peek-state) vs)
      (set! (.-key peek-state) v)
      v))

  (next-values [this]
    (when-let [[v & vs] (.-seq peek-state)]
      (set! (.-seq peek-state) vs)
      (set! (.-key peek-state) v)
      v)))

(defn new-doc-attribute-entity-value-entity-index [index-store attr entity-resolver-fn]
  (->DocAttributeEntityValueEntityIndex index-store attr entity-resolver-fn (BinaryJoinLayeredVirtualIndexPeekState. nil nil)))

(defrecord DocAttributeEntityValueValueIndex [index-store ^DirectBuffer attr ^DocAttributeEntityValueEntityIndex entity-value-entity-idx entity-resolver-fn ^BinaryJoinLayeredVirtualIndexPeekState peek-state]
  db/Index
  (seek-values [this k]
    (let [eid-buffer (.-key ^BinaryJoinLayeredVirtualIndexPeekState (.peek-state entity-value-entity-idx))
          [v & vs] (db/aev index-store attr eid-buffer k entity-resolver-fn)]
      (set! (.-seq peek-state) vs)
      (set! (.-key peek-state) v)
      v))

  (next-values [this]
    (when-let [[v & vs] (.-seq peek-state)]
      (set! (.-seq peek-state) vs)
      (set! (.-key peek-state) v)
      v)))

(defn new-doc-attribute-entity-value-value-index [index-store attr entity-value-entity-idx entity-resolver-fn]
  (->DocAttributeEntityValueValueIndex index-store attr entity-value-entity-idx entity-resolver-fn (BinaryJoinLayeredVirtualIndexPeekState. nil nil)))

;; Range Constraints

(defrecord PredicateVirtualIndex [idx pred seek-k-fn]
  db/Index
  (seek-values [this k]
    (when-let [v (db/seek-values idx (seek-k-fn k))]
      (when (pred v)
        v)))

  (next-values [this]
    (when-let [v (db/next-values idx)]
      (when (pred v)
        v))))

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

;; Utils

(defn idx->seq
  [idx]
  (when-let [result (db/seek-values idx nil)]
    (->> (repeatedly #(db/next-values idx))
         (take-while identity)
         (cons result))))

;; Join

(extend-protocol db/LayeredIndex
  Object
  (open-level [_])
  (close-level [_])
  (max-depth [_] 1))

(defn- new-unary-join-iterator-state [idx value]
  (UnaryJoinIteratorState. idx (or value mem/empty-buffer)))

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
          (->> #(let [v (if match?
                          (db/next-values idx)
                          (db/seek-values idx max-k))]
                  (when v
                    (set! (.iterators iterators-thunk)
                          (assoc iterators index (new-unary-join-iterator-state idx v)))
                    (set! (.index iterators-thunk) (mod (inc index) (count iterators)))
                    iterators-thunk))
               (set! (.thunk state)))
          (if match?
            max-k
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
  (if (= 1 (count indexes))
    (first indexes)
    (->UnaryJoinVirtualIndex indexes (UnaryJoinIteratorsThunkFnState. nil))))

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

(defn new-binary-join-virtual-index [lhs-index rhs-index]
   (->BinaryJoinLayeredVirtualIndex (BinaryJoinLayeredVirtualIndexState. [lhs-index rhs-index] 0)))

(defn- build-constrained-result [constrain-result-fn result-stack max-k]
  (let [max-ks (last result-stack)
        join-keys (conj (or max-ks []) max-k)]
    (when (constrain-result-fn join-keys)
      (conj result-stack join-keys))))

(defrecord NAryConstrainingLayeredVirtualIndex [n-ary-index constrain-result-fn ^NAryWalkState state]
  db/Index
  (seek-values [this k]
    (when-let [v (db/seek-values n-ary-index k)]
      (if-let [result (build-constrained-result constrain-result-fn (.result-stack state) v)]
        (do (set! (.last state) result)
            v)
        (db/next-values this))))

  (next-values [this]
    (when-let [v (db/next-values n-ary-index)]
      (if-let [result (build-constrained-result constrain-result-fn (.result-stack state) v)]
        (do (set! (.last state) result)
            v)
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
          step (fn step [max-ks ^long depth needs-seek?]
                 (when (Thread/interrupted)
                   (throw (InterruptedException.)))
                 (let [close-level (fn []
                                     (when (pos? depth)
                                       (lazy-seq
                                        (db/close-level idx)
                                        (step (pop max-ks) (dec depth) false))))
                       open-level (fn [v]
                                    (db/open-level idx)
                                    (if-let [max-ks (conj max-ks v)]
                                      (step max-ks (inc depth) true)
                                      (do (db/close-level idx)
                                          (step max-ks depth false))))]
                   (if (= depth (dec max-depth))
                     (concat (for [v (idx->seq idx)]
                               (conj max-ks v))
                             (close-level))
                     (if-let [v (if needs-seek?
                                  (db/seek-values idx nil)
                                  (db/next-values idx))]
                       (open-level v)
                       (close-level)))))]
      (when (pos? max-depth)
        (step [] 0 true)))))

(defrecord SortedVirtualIndex [sc ^SortedVirtualIndexState state]
  db/Index
  (seek-values [this k]
    (set! (.seq state) (subseq sc >= (or k mem/empty-buffer)))
    (db/next-values this))

  (next-values [this]
    (when-let [[x & xs] (.seq state)]
      (set! (.seq state) (seq xs))
      (some-> x (key)))))

(defn- new-sorted-virtual-index [sc]
  (->SortedVirtualIndex sc (SortedVirtualIndexState. nil)))

(defrecord RelationVirtualIndex [max-depth ^RelationVirtualIndexState state layered-range-constraints encode-value-fn]
  db/Index
  (seek-values [this k]
    (when-let [k (db/seek-values (last (.indexes state)) (or k mem/empty-buffer))]
      (set! (.key state) k)
      k))

  (next-values [this]
    (when-let [k (db/next-values (last (.indexes state)))]
      (set! (.key state) k)
      k))

  db/LayeredIndex
  (open-level [this]
    (when (= max-depth (count (.path state)))
      (throw (IllegalStateException. (str "Cannot open level at max depth: " max-depth))))
    (let [path (conj (.path state) (.key state))
          level (count path)]
      (set! (.path state) path)
      (set! (.indexes state) (conj (.indexes state)
                                   (wrap-with-range-constraints
                                    (new-sorted-virtual-index (get-in (.tree state) path))
                                    (get layered-range-constraints level))))
      (set! (.key state) nil))
    nil)

  (close-level [this]
    (when (zero? (count (.path state)))
      (throw (IllegalStateException. "Cannot close level at root.")))
    (set! (.path state) (pop (.path state)))
    (set! (.indexes state) (pop (.indexes state)))
    (set! (.key state) nil)
    nil)

  (max-depth [_]
    max-depth))

(defn- sorted-map-assoc-in [m [k & ks] v]
  (if ks
    (assoc m k (sorted-map-assoc-in (or (get m k) (sorted-map-by mem/buffer-comparator)) ks v))
    (assoc m k v)))

(defn update-relation-virtual-index!
  ([^RelationVirtualIndex relation tuples]
   (update-relation-virtual-index! relation tuples (.layered-range-constraints relation)))
  ([^RelationVirtualIndex relation tuples layered-range-constraints]
   (let [encode-value-fn (.encode_value_fn relation)
         tree (binding [nippy/*freeze-fallback* :write-unfreezable]
                (reduce
                 (fn [acc tuple]
                   (sorted-map-assoc-in acc (mapv encode-value-fn tuple) nil))
                 (sorted-map-by mem/buffer-comparator)
                 tuples))
         root-level (wrap-with-range-constraints
                     (new-sorted-virtual-index tree)
                     (get layered-range-constraints 0))
         state ^RelationVirtualIndexState (.state relation)]
     (set! (.tree state) tree)
     (set! (.path state) [])
     (set! (.indexes state) [root-level])
     (set! (.key state) nil)
     relation)))

(defn new-relation-virtual-index
  ([relation-name tuples max-depth encode-value-fn]
   (new-relation-virtual-index relation-name tuples max-depth encode-value-fn nil))
  ([relation-name tuples max-depth encode-value-fn layered-range-constraints]
   (update-relation-virtual-index! (->RelationVirtualIndex max-depth
                                                           (RelationVirtualIndexState. nil nil nil nil)
                                                           layered-range-constraints
                                                           encode-value-fn)
                                   tuples)))
