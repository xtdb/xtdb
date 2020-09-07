(ns ^:no-doc crux.index
  (:require [crux.db :as db]
            [crux.memory :as mem])
  (:import [crux.index IndexStoreIndexState NAryJoinLayeredVirtualIndexState NAryWalkState
            RelationVirtualIndexState SortedVirtualIndexState UnaryJoinIteratorState UnaryJoinIteratorsThunkFnState UnaryJoinIteratorsThunkState]
           clojure.lang.Box
           java.util.function.Function
           [java.util Comparator Iterator NavigableMap TreeMap]
           org.agrona.DirectBuffer))

(set! *unchecked-math* :warn-on-boxed)

;; Index Store

(defrecord IndexStoreIndex [seek-fn ^IndexStoreIndexState state]
  db/Index
  (seek-values [this k]
    (let [[v & vs] (seek-fn k)]
      (set! (.-seq state) vs)
      (set! (.-key state) v)
      v))

  (next-values [this]
    (when-let [[v & vs] (.-seq state)]
      (set! (.-seq state) vs)
      (set! (.-key state) v)
      v)))

(defn new-index-store-index ^crux.index.IndexStoreIndex [seek-fn]
  (->IndexStoreIndex seek-fn (IndexStoreIndexState. nil nil)))

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
        v)))

  db/LayeredIndex
  (open-level [_]
    (db/open-level idx))

  (close-level [_]
    (db/close-level idx))

  (max-depth [_]
    (db/max-depth idx)))

(defn- value-comparsion-predicate
  ([compare-pred compare-v]
   (value-comparsion-predicate compare-pred compare-v Integer/MAX_VALUE))
  ([compare-pred ^Box compare-v ^long max-length]
   (if (.val compare-v)
     (fn [value]
       (and value (compare-pred (mem/compare-buffers value (.val compare-v) max-length))))
     (constantly true))))

(defn new-prefix-equal-virtual-index [idx ^Box prefix-v ^long prefix-size]
  (let [seek-k-pred (value-comparsion-predicate (comp not neg?) prefix-v prefix-size)
        pred (value-comparsion-predicate zero? prefix-v prefix-size)]
    (->PredicateVirtualIndex idx pred (fn [k]
                                        (if (seek-k-pred k)
                                          k
                                          (mem/limit-buffer (.val prefix-v) prefix-size))))))

(defn new-less-than-equal-virtual-index [idx max-v]
  (let [pred (value-comparsion-predicate (comp not pos?) max-v)]
    (->PredicateVirtualIndex idx pred identity)))

(defn new-less-than-virtual-index [idx max-v]
  (let [pred (value-comparsion-predicate neg? max-v)]
    (->PredicateVirtualIndex idx pred identity)))

(defn new-greater-than-equal-virtual-index [idx ^Box min-v]
  (let [pred (value-comparsion-predicate (comp not neg?) min-v)]
    (->PredicateVirtualIndex idx pred (fn [k]
                                        (if (pred k)
                                          k
                                          (.val min-v))))))

(defrecord GreaterThanVirtualIndex [idx]
  db/Index
  (seek-values [this k]
    (or (db/seek-values idx k)
        (db/next-values idx)))

  (next-values [this]
    (db/next-values idx))

  db/LayeredIndex
  (open-level [_]
    (db/open-level idx))

  (close-level [_]
    (db/close-level idx))

  (max-depth [_]
    (db/max-depth idx)))

(defn new-greater-than-virtual-index [idx ^Box min-v]
  (let [pred (value-comparsion-predicate pos? min-v)
        idx (->PredicateVirtualIndex idx pred (fn [k]
                                                (if (pred k)
                                                  k
                                                  (.val min-v))))]
    (->GreaterThanVirtualIndex idx)))

(defn new-equals-virtual-index [idx ^Box v]
  (let [pred (value-comparsion-predicate zero? v)]
    (->PredicateVirtualIndex idx pred (fn [k]
                                        (if (pred k)
                                          k
                                          (.val v))))))

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

(defn- long-mod ^long [^long num ^long div]
  (let [m (rem num div)]
    (if (or (zero? m) (= (pos? num) (pos? div)))
      m
      (+ m div))))

(defrecord UnaryJoinVirtualIndex [indexes ^UnaryJoinIteratorsThunkFnState state]
  db/Index
  (seek-values [this k]
    (->> #(let [iterators (->> (for [idx indexes]
                                 (new-unary-join-iterator-state idx (db/seek-values idx k)))
                               (sort-by (fn [x] (.key ^UnaryJoinIteratorState x)) mem/buffer-comparator)
                               (to-array))]
            (UnaryJoinIteratorsThunkState. iterators 0))
         (set! (.thunk state)))
    (db/next-values this))

  (next-values [this]
    (when-let [iterators-thunk (.thunk state)]
      (when-let [iterators-thunk ^UnaryJoinIteratorsThunkState (iterators-thunk)]
        (let [iterators ^objects (.iterators iterators-thunk)
              index (.index iterators-thunk)
              iterator-state ^UnaryJoinIteratorState (aget iterators index)
              max-index (long-mod (dec index) (alength iterators))
              max-k (.key ^UnaryJoinIteratorState (aget iterators max-index))
              match? (mem/buffers=? (.key iterator-state) max-k)
              idx (.idx iterator-state)]
          (->> #(let [v (if match?
                          (db/next-values idx)
                          (db/seek-values idx max-k))]
                  (when v
                    (set! (.-key iterator-state) v)
                    (set! (.index iterators-thunk) (long-mod (inc index) (alength iterators)))
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

(defrecord SortedVirtualIndex [^NavigableMap m ^SortedVirtualIndexState state]
  db/Index
  (seek-values [this k]
    (set! (.iterator state) (.iterator (.tailSet (.navigableKeySet m) (or k mem/empty-buffer))))
    (db/next-values this))

  (next-values [this]
    (when-let [iterator ^Iterator (.iterator state)]
      (when (.hasNext iterator)
        (.next iterator)))))

(defn- new-sorted-virtual-index [m]
  (->SortedVirtualIndex m (SortedVirtualIndexState. nil)))

(defrecord RelationVirtualIndex [max-depth ^RelationVirtualIndexState state encode-value-fn]
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
                                   (new-sorted-virtual-index (get-in (.tree state) path))))
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

(defn- tree-map-put-in [^TreeMap m [k & ks] v]
  (if ks
    (doto m
      (-> (.computeIfAbsent k
                            (reify Function
                              (apply [_ k]
                                (TreeMap. (.comparator m)))))
          (tree-map-put-in ks v)))
    (doto m
      (.put k v))))

(defn update-relation-virtual-index!
  ([^RelationVirtualIndex relation tuples]
   (update-relation-virtual-index! relation tuples (.encode_value_fn relation) false))
  ([^RelationVirtualIndex relation tuples encode-value-fn single-values?]
   (let [tree (if single-values?
                (reduce
                 (fn [^TreeMap acc v]
                   (doto acc
                     (.put (encode-value-fn v) nil)))
                 (TreeMap. mem/buffer-comparator)
                 tuples)
                (reduce
                 (fn [acc tuple]
                   (tree-map-put-in acc (mapv encode-value-fn tuple) nil))
                 (TreeMap. mem/buffer-comparator)
                 tuples))
         root-level (new-sorted-virtual-index tree)
         state ^RelationVirtualIndexState (.state relation)]
     (set! (.tree state) tree)
     (set! (.path state) [])
     (set! (.indexes state) [root-level])
     (set! (.key state) nil)
     relation)))

(defn new-relation-virtual-index [tuples max-depth encode-value-fn]
  (update-relation-virtual-index! (->RelationVirtualIndex max-depth
                                                          (RelationVirtualIndexState. nil nil nil nil)
                                                          encode-value-fn)
                                  tuples))

(defrecord SingletonVirtualIndex [v]
  db/Index
  (seek-values [_ k]
    (when-not (pos? (mem/compare-buffers (or k mem/empty-buffer) v))
      v))

  (next-values [_]))

(defn new-singleton-virtual-index [v encode-value-fn]
  (->SingletonVirtualIndex (encode-value-fn v)))
