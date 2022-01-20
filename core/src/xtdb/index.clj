(ns ^:no-doc xtdb.index
  (:require [xtdb.db :as db]
            [xtdb.memory :as mem])
  (:import (clojure.lang Box IDeref IPersistentVector)
           (java.util ArrayList Arrays Comparator Iterator List NavigableMap NavigableSet TreeMap TreeSet)
           (java.util.function Function)))

(set! *unchecked-math* :warn-on-boxed)

;; NOTE: you might be tempted to try to include instances of these in the query cache to avoid re-creating them, as I was...
;; while instances could possibly be re-used so long as they were only used from one thread at any one time,
;; including them in the query cache may mean they would be used from _multiple_ threads at the same time.

(deftype DerefIndex [idx ^:unsynchronized-mutable x]
  db/Index
  (seek-values [_ k]
    (let [v (db/seek-values idx k)]
      (set! x v)
      v))

  (next-values [_]
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

(defn new-deref-index ^xtdb.index.DerefIndex [idx]
  (if (instance? DerefIndex idx)
    idx
    (->DerefIndex idx nil)))

(deftype SeekFnIndex [seek-fn ^:unsynchronized-mutable xs]
  db/Index
  (seek-values [_ k]
    (let [[v & vs] (seq (seek-fn k))]
      (set! xs vs)
      v))

  (next-values [_]
    (when-let [[v & vs] xs]
      (set! xs vs)
      v)))

(defn new-seek-fn-index ^xtdb.index.SeekFnIndex [seek-fn]
  (->SeekFnIndex seek-fn nil))

;; Range Constraints

(deftype PredicateVirtualIndex [idx pred seek-k-fn]
  db/Index
  (seek-values [_ k]
    (when-let [v (db/seek-values idx (seek-k-fn k))]
      (when (pred v)
        v)))

  (next-values [_]
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
  (seek-values [_ k]
    (or (db/seek-values idx k)
        (db/next-values idx)))

  (next-values [_]
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

(def ^:const ^:private chunk-size 8)

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
                           (when (db/seek-values idx k)
                             (recur (inc n) acc)))))]
      (do (doto indexes
            (Arrays/sort unary-join-iterator-state-comparator))
          (set! index 0)
          (set! state init-state)
          (db/next-values this))
      (set! state nil)))

  (next-values [this]
    (loop []
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
            (recur))))))

  db/LayeredIndex
  (open-level [_]
    (doseq [idx indexes]
      (db/open-level idx)))

  (close-level [_]
    (doseq [idx indexes]
      (db/close-level idx)))

  (max-depth [_]
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

  (next-values [_]
    (loop []
      (when-let [v (db/next-values (aget indexes depth))]
        (if (constrain-result-fn (doto join-keys
                                   (.set depth v)) (inc depth))
          v
          (recur)))))

  db/LayeredIndex
  (open-level [_]
    (db/open-level (aget indexes depth))
    (set! depth (inc depth))
    nil)

  (close-level [_]
    (db/close-level (aget indexes (dec depth)))
    (set! depth (dec depth))
    nil)

  (max-depth [_]
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
          step (fn step [^IPersistentVector max-ks ^long depth needs-seek?]
                 (when (Thread/interrupted)
                   (throw (InterruptedException.)))
                 (if (= depth (dec max-depth))
                   (concat
                    (for [v (idx->seq idx)]
                      (.assocN max-ks depth v))
                    (when (pos? depth)
                      (lazy-seq
                       (db/close-level idx)
                       (step max-ks (dec depth) false))))
                   (if-let [v (if needs-seek?
                                (db/seek-values idx nil)
                                (db/next-values idx))]
                     (do (db/open-level idx)
                         (recur (.assocN max-ks depth v) (inc depth) true))
                     (when (pos? depth)
                       (db/close-level idx)
                       (recur max-ks (dec depth) false)))))]
      (when (pos? max-depth)
        (step (vec (repeat max-depth nil)) 0 true)))))

(deftype CollectionVirtualIndex [^NavigableSet s ^:unsynchronized-mutable ^Iterator iterator]
  db/Index
  (seek-values [this k]
    (set! iterator (.iterator (.tailSet s (or k mem/empty-buffer))))
    (db/next-values this))

  (next-values [_]
    (when (and iterator (.hasNext iterator))
      (.next iterator))))

(defn new-collection-virtual-index [s]
  (->CollectionVirtualIndex (if (instance? NavigableSet s)
                              s
                              (doto (TreeSet. mem/buffer-comparator)
                                (.addAll s)))
                            nil))

(deftype ScalarVirtualIndex [v]
  db/Index
  (seek-values [_ k]
    (when-not (pos? (mem/compare-buffers (or k mem/empty-buffer) v))
      v))

  (next-values [_]))

(defn new-scalar-virtual-index [v]
  (->ScalarVirtualIndex v))

(definterface IRelationVirtualIndexUpdate
  (^void updateIndex [tree rootIndex]))

(deftype RelationVirtualIndex [^long max-depth
                               ^:unsynchronized-mutable tree
                               ^:unsynchronized-mutable ^long depth
                               ^:unsynchronized-mutable ^objects path
                               ^:unsynchronized-mutable ^objects indexes]
  db/Index
  (seek-values [_ k]
    (when-let [k (db/seek-values (aget indexes depth) (or k mem/empty-buffer))]
      (aset path depth k)
      k))

  (next-values [_]
    (when-let [k (db/next-values (aget indexes depth))]
      (aset path depth k)
      k))

  db/LayeredIndex
  (open-level [_]
    (when (= max-depth depth)
      (throw (IllegalStateException. (str "Cannot open level at max depth: " max-depth))))
    (set! depth (inc depth))
    (loop [n 0
           ^NavigableMap tree tree]
      (when tree
        (if (= depth n)
          (aset indexes depth (new-collection-virtual-index (.navigableKeySet tree)))
          (recur (inc n) (.get tree (aget path n))))))
    nil)

  (close-level [_]
    (when (zero? depth)
      (throw (IllegalStateException. "Cannot close level at root.")))
    (set! depth (dec depth))
    nil)

  (max-depth [_]
    max-depth)

  IRelationVirtualIndexUpdate
  (updateIndex [_ new-tree root-index]
    (set! tree new-tree)
    (set! depth 0)
    (aset indexes 0 root-index)))

(defn- tree-map-put-in [^TreeMap m [k & ks] v]
  (if ks
    (doto m
      (-> (.computeIfAbsent k
                            (reify Function
                              (apply [_ _k]
                                (TreeMap. (.comparator m)))))
          (tree-map-put-in ks v)))
    (doto m
      (.put k v))))

(defn update-relation-virtual-index!
  ([^RelationVirtualIndex relation tuples]
   (update-relation-virtual-index! relation tuples false))
  ([^RelationVirtualIndex relation tuples single-var?]
   (if single-var?
     (.updateIndex relation nil (new-collection-virtual-index tuples))

     (let [tree (->> (reduce (fn [acc tuple]
                               (tree-map-put-in acc tuple nil))
                             (TreeMap. mem/buffer-comparator)
                             tuples))
           root-idx (new-collection-virtual-index (.navigableKeySet ^NavigableMap tree))]
       (.updateIndex relation tree root-idx)))

   relation))

(defn new-relation-virtual-index [tuples max-depth]
  (doto (->RelationVirtualIndex max-depth
                                nil
                                0
                                (object-array (long max-depth))
                                (object-array (long max-depth)))
    (update-relation-virtual-index! tuples)))
