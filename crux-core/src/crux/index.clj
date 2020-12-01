(ns ^:no-doc crux.index
  (:require [crux.db :as db]
            [crux.memory :as mem])
  (:import [clojure.lang Box IDeref]
           java.util.function.Function
           [java.util ArrayList Arrays Collection Comparator Iterator List NavigableSet NavigableMap TreeMap TreeSet]
           org.agrona.DirectBuffer))

(set! *unchecked-math* :warn-on-boxed)

(deftype DerefIndex [idx ^:unsynchronized-mutable x]
  db/Index
  (seek-values [this k]
    (let [v (db/seek-values idx k)]
      (set! x v)
      v))

  (next-values [this]
    (let [v (db/next-values idx)]
      (set! x v)
      v))

  db/LayeredIndex
  (open-level [_]
    (db/open-level idx))

  (close-level [_]
    (db/close-level idx))

  (max-depth [_]
    (db/max-depth idx))

  IDeref
  (deref [_]
    x))

(defn new-deref-index ^crux.index.DerefIndex [idx]
  (if (instance? DerefIndex idx)
    idx
    (->DerefIndex idx nil)))

(deftype SeekFnIndex [seek-fn ^:unsynchronized-mutable xs]
  db/Index
  (seek-values [this k]
    (let [[v & vs] (seq (seek-fn k))]
      (set! xs vs)
      v))

  (next-values [this]
    (when-let [[v & vs] xs]
      (set! xs vs)
      v)))

(defn new-seek-fn-index ^crux.index.SeekFnIndex [seek-fn]
  (->SeekFnIndex seek-fn nil))

;; Range Constraints

(deftype PredicateVirtualIndex [idx pred seek-k-fn]
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

(deftype GreaterThanVirtualIndex [idx]
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

(def ^:private chunk-size 8)

(defn idx->seq
  [idx]
  (when-let [result (db/seek-values idx nil)]
    ((fn step [cb v]
       (if v
         (do (chunk-append cb v)
             (if (= (count cb) chunk-size)
               (chunk-cons (chunk cb)
                           (lazy-seq
                            (step (chunk-buffer chunk-size)
                                  (db/next-values idx))))
               (recur cb (db/next-values idx))))
         (chunk-cons (chunk cb) nil)))
     (chunk-buffer chunk-size) result)))

;; Join

(extend-protocol db/LayeredIndex
  Object
  (open-level [_])
  (close-level [_])
  (max-depth [_] 1))

(def ^:private ^Comparator unary-join-iterator-state-comparator
  (reify Comparator
    (compare [_ x y]
      (mem/compare-buffers (.deref ^IDeref x)
                           (.deref ^IDeref y)))))

(def ^:private init-state ::init)
(def ^:private next-state ::next)

(deftype UnaryJoinVirtualIndex [^objects indexes
                                ^:unsynchronized-mutable ^long index
                                ^:unsynchronized-mutable state]
  db/Index
  (seek-values [this k]
    (if-let [indexes (loop [n 0
                            acc indexes]
                       (if (= n (alength acc))
                         acc
                         (let [idx ^DerefIndex (aget indexes n)]
                           (when-let [v (db/seek-values idx k)]
                             (recur (inc n) acc)))))]
      (do (doto indexes
            (Arrays/sort unary-join-iterator-state-comparator))
          (set! index 0)
          (set! state init-state)
          (db/next-values this))
      (set! state nil)))

  (next-values [this]
    (when (and state (not (identical? init-state state)))
      (let [idx (aget indexes index)]
        (if (if (identical? next-state state)
              (db/next-values idx)
              (db/seek-values idx state))
          (let [index (inc index)
                index (if (= index (alength indexes))
                        0
                        index)]
            (set! (.index this) index))
          (set! state nil))))
    (when state
      (let [idx ^DerefIndex (aget indexes index)
            max-index (if (zero? index)
                        (dec (alength indexes))
                        (dec index))
            max-k (.deref ^DerefIndex (aget indexes max-index))
            match? (mem/buffers=? (.deref idx) max-k)]
        (set! state (if match?
                      next-state
                      max-k))
        (if match?
          max-k
          (recur)))))

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
    (->UnaryJoinVirtualIndex
     (object-array (for [idx indexes]
                     (new-deref-index idx)))
     0
     nil)))

(deftype NAryJoinLayeredVirtualIndex [^objects indexes
                                      ^:unsynchronized-mutable ^long depth
                                      constrain-result-fn
                                      ^:unsynchronized-mutable ^List join-keys]
  db/Index
  (seek-values [this k]
    (when-let [v (db/seek-values (aget indexes depth) k)]
      (if (constrain-result-fn (doto join-keys
                                 (.set depth v)) (inc depth))
        v
        (db/next-values this))))

  (next-values [this]
    (when-let [v (db/next-values (aget indexes depth))]
      (if (constrain-result-fn (doto join-keys
                                 (.set depth v)) (inc depth))
        v
        (recur))))

  db/LayeredIndex
  (open-level [this]
    (db/open-level (aget indexes depth))
    (set! depth (inc depth))
    nil)

  (close-level [this]
    (db/close-level (aget indexes (dec depth)))
    (set! depth (dec depth))
    nil)

  (max-depth [this]
    (alength indexes)))

(defn new-n-ary-join-layered-virtual-index
  ([indexes]
   (new-n-ary-join-layered-virtual-index indexes (constantly true)))
  ([indexes constrain-result-fn]
   (->NAryJoinLayeredVirtualIndex (object-array indexes)
                                  0
                                  constrain-result-fn
                                  (doto (ArrayList.)
                                    (.addAll (repeat (count indexes) nil))))))

(defn layered-idx->seq [idx]
  (when idx
    (let [max-depth (long (db/max-depth idx))
          step (fn step [max-ks ^long depth needs-seek?]
                 (when (Thread/interrupted)
                   (throw (InterruptedException.)))
                 (if (= depth (dec max-depth))
                   (concat (for [v (idx->seq idx)]
                             (conj max-ks v))
                           (when (pos? depth)
                             (lazy-seq
                              (db/close-level idx)
                              (step (pop max-ks) (dec depth) false))))
                   (if-let [v (if needs-seek?
                                (db/seek-values idx nil)
                                (db/next-values idx))]
                     (do (db/open-level idx)
                         (recur (conj max-ks v) (inc depth) true))
                     (when (pos? depth)
                       (db/close-level idx)
                       (recur (pop max-ks) (dec depth) false)))))]
      (when (pos? max-depth)
        (step [] 0 true)))))

(deftype SortedVirtualIndex [^NavigableSet s ^:unsynchronized-mutable ^Iterator iterator]
  db/Index
  (seek-values [this k]
    (set! iterator (.iterator (.tailSet s (or k mem/empty-buffer))))
    (db/next-values this))

  (next-values [this]
    (when (and iterator (.hasNext iterator))
      (.next iterator))))

(defn- new-sorted-virtual-index [s]
  (->SortedVirtualIndex s nil))

(definterface IRelationVirtualIndexUpdate
  (^void updateIndex [tree indexes]))

(deftype RelationVirtualIndex [max-depth
                               encode-value-fn
                               ^:unsynchronized-mutable tree
                               ^:unsynchronized-mutable path
                               ^:unsynchronized-mutable indexes
                               ^:unsynchronized-mutable key]
  db/Index
  (seek-values [this k]
    (when-let [k (db/seek-values (last indexes) (or k mem/empty-buffer))]
      (set! key k)
      k))

  (next-values [this]
    (when-let [k (db/next-values (last indexes))]
      (set! key k)
      k))

  db/LayeredIndex
  (open-level [this]
    (when (= max-depth (count path))
      (throw (IllegalStateException. (str "Cannot open level at max depth: " max-depth))))
    (let [new-path (conj path key)
          level (count new-path)]
      (set! path new-path)
      (set! indexes (conj indexes
                          (some->  ^NavigableMap (get-in tree new-path)
                                   (.navigableKeySet)
                                   (new-sorted-virtual-index))))
      (set! key nil))
    nil)

  (close-level [this]
    (when (zero? (count path))
      (throw (IllegalStateException. "Cannot close level at root.")))
    (set! path (pop path))
    (set! indexes (pop indexes))
    (set! key nil)
    nil)

  (max-depth [_]
    max-depth)

  IRelationVirtualIndexUpdate
  (updateIndex [_ new-tree new-indexes]
    (set! tree new-tree)
    (set! path [])
    (set! indexes new-indexes)
    (set! key nil)))

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
   (let [tree (when-not single-values?
                (reduce
                 (fn [acc tuple]
                   (tree-map-put-in acc (mapv encode-value-fn tuple) nil))
                 (TreeMap. mem/buffer-comparator)
                 tuples))
         root-level (if single-values?
                      (new-sorted-virtual-index (if (instance? NavigableSet tuples)
                                                  tuples
                                                  (doto (TreeSet. mem/buffer-comparator)
                                                    (.addAll (mapv encode-value-fn tuples)))))
                      (new-sorted-virtual-index (.navigableKeySet ^NavigableMap tree)))]
     (.updateIndex relation tree [root-level])
     relation)))

(defn new-relation-virtual-index [tuples max-depth encode-value-fn]
  (update-relation-virtual-index! (->RelationVirtualIndex max-depth
                                                          encode-value-fn
                                                          nil nil nil nil)
                                  tuples))

(deftype SingletonVirtualIndex [v]
  db/Index
  (seek-values [_ k]
    (when-not (pos? (mem/compare-buffers (or k mem/empty-buffer) v))
      v))

  (next-values [_]))

(defn new-singleton-virtual-index [v encode-value-fn]
  (->SingletonVirtualIndex (encode-value-fn v)))
