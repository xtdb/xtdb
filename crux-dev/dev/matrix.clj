(ns matrix
  (:require [clojure.java.io :as io]
            [clojure.set :as set]
            [clojure.walk :as w]
            [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            [crux.hash :as hash]
            [crux.io :as cio]
            [crux.kv :as kv]
            [crux.lru :as lru]
            [crux.memory :as mem]
            [crux.rdf :as rdf]
            [crux.query :as q]
            [taoensso.nippy :as nippy])
  (:import [java.util ArrayList Date HashMap HashSet IdentityHashMap List Map]
           [java.util.function Function IntFunction ToIntFunction]
           [clojure.lang Box ILookup Indexed]
           crux.ByteUtils
           [java.io DataInputStream DataOutput DataOutputStream IOException]
           java.nio.ByteOrder
           [org.agrona.collections Int2ObjectHashMap Object2IntHashMap]
           org.agrona.concurrent.UnsafeBuffer
           org.agrona.ExpandableDirectByteBuffer
           [org.agrona DirectBuffer MutableDirectBuffer]
           org.agrona.io.DirectBufferInputStream
           [org.roaringbitmap IntConsumer RoaringBitmap]))

;; Matrix / GraphBLAS style breath first search
;; https://redislabs.com/redis-enterprise/technology/redisgraph/
;; https://www.slideshare.net/RoiLipman/graph-algebra
;; https://www.youtube.com/embed/xnez6tloNSQ
;; MAGiQ http://www.vldb.org/pvldb/vol11/p1978-jamour.pdf
;; gSMat https://arxiv.org/pdf/1807.07691.pdf
;; GraphBLAS https://github.com/tgmattso/GraphBLAS/blob/master/graph_blas-HPEC18.pdf

(set! *unchecked-math* :warn-on-boxed)

;; Bitmap-per-Row version.

;; NOTE: toString on Int2ObjectHashMap seems to be broken. entrySet
;; also seems to be behaving unexpectedly (returns duplicated keys),
;; at least when using RoaringBitmaps as values.
(defn- new-matrix
  (^Int2ObjectHashMap []
   (Int2ObjectHashMap.))
  (^Int2ObjectHashMap [^long size]
   (Int2ObjectHashMap. (Math/ceil (/ size 0.9)) 0.9)))

(defn- new-bitmap ^org.roaringbitmap.RoaringBitmap []
  (RoaringBitmap.))

(defn- matrix-and
  ([a] a)
  ([^Int2ObjectHashMap a ^Int2ObjectHashMap b]
   (if (< (.size b) (.size a))
     (recur b a)
     (let [i (.iterator (.entrySet a))
           m ^Int2ObjectHashMap (new-matrix (.size a))]
       (while (.hasNext i)
         (.next i)
         (let [row (.getIntKey i)]
           (when-let [b-cols ^RoaringBitmap (.get b row)]
             (.put m row (RoaringBitmap/and ^RoaringBitmap (.getValue i) b-cols)))))
       m))))

(defn- bitmap-and
  ([a] a)
  ([^RoaringBitmap a ^RoaringBitmap b]
   (RoaringBitmap/and a b)))

(defn- matrix-cardinality ^long [^Int2ObjectHashMap a]
  (->> (.values a)
       (map #(.getCardinality ^RoaringBitmap %))
       (reduce +)))

(defn- predicate-cardinality [{:keys [p->so ^Map predicate-cardinality-cache]} attr]
  (.computeIfAbsent predicate-cardinality-cache
                    attr
                    (reify Function
                      (apply [_ k]
                        (matrix-cardinality (get p->so k))))))

(defn- new-diagonal-matrix [^RoaringBitmap diag]
  (let [diag (.toArray diag)
        m ^Int2ObjectHashMap (new-matrix (alength diag))]
    (dotimes [idx (alength diag)]
      (let [d (aget diag idx)]
        (.put m d (doto (new-bitmap)
                    (.add d)))))
    m))

(defn- transpose [^Int2ObjectHashMap a]
  (let [i (.iterator (.entrySet a))
        m ^Int2ObjectHashMap (new-matrix (.size a))]
    (while (.hasNext i)
      (.next i)
      (let [row (.getIntKey i)
            cols ^RoaringBitmap (.getValue i)]
        (.forEach cols
                  (reify IntConsumer
                    (accept [_ col]
                      (.add ^RoaringBitmap (.computeIfAbsent m col
                                                             (reify IntFunction
                                                               (apply [_ _]
                                                                 (new-bitmap))))
                            row))))))
    m))

(defn- matlab-any
  "Determine if any array elements are nonzero. Test each column for
  nonzero elements."
  ^org.roaringbitmap.RoaringBitmap [^Int2ObjectHashMap a]
  (if (.isEmpty a)
    (new-bitmap)
    (RoaringBitmap/or (.iterator (.values a)))))

(defn- matlab-any-of-transpose
  "Determine if any array elements are nonzero. Test each row for
  nonzero elements."
  ^org.roaringbitmap.RoaringBitmap [^Int2ObjectHashMap a]
  (let [i (.iterator (.keySet a))
        b (new-bitmap)]
    (while (.hasNext i)
      (.add b (.nextInt i)))
    b))

(defn- mult-diag [^RoaringBitmap diag ^Int2ObjectHashMap a]
  (cond (.isEmpty a)
        (new-matrix)

        (nil? diag)
        a

        :else
        (let [m ^Int2ObjectHashMap (new-matrix (.getCardinality diag))
              diag (.toArray diag)]
          (dotimes [idx (alength diag)]
            (let [d (aget diag idx)]
              (when-let [b (.get a d)]
                (.put m d b))))
          m)))

(defn- new-literal-e-mask ^org.roaringbitmap.RoaringBitmap [{:keys [^ToIntFunction value->id p->so]} a e]
  (let [id (.applyAsInt value->id e)]
    (or (.get ^Int2ObjectHashMap (get p->so a) id)
        (new-bitmap))))

(defn- new-literal-v-mask ^org.roaringbitmap.RoaringBitmap [{:keys [^ToIntFunction value->id p->os]} a v]
  (let [id (.applyAsInt value->id v)]
    (or (.get ^Int2ObjectHashMap (get p->os a) id)
        (new-bitmap))))

(defn- join [{:keys [p->os p->so] :as graph} a mask-e mask-v]
  (let [result (mult-diag
                mask-e
                (if mask-v
                  (transpose
                   (mult-diag
                    mask-v
                    (get p->os a)))
                  (get p->so a)))]
    [result
     (matlab-any-of-transpose result)
     (matlab-any result)]))

(def ^:private logic-var? symbol?)
(def ^:private literal? (complement logic-var?))

(defn- compile-query [graph q]
  (let [{:keys [find where]} (s/conform :crux.query/query q)
        triple-clauses (->> where
                            (filter (comp #{:triple} first))
                            (map second))
        literal-clauses (for [{:keys [e v] :as clause} triple-clauses
                              :when (or (literal? e)
                                        (literal? v))]
                          clause)
        literal-vars (->> (mapcat (juxt :e :v) literal-clauses)
                          (filter logic-var?)
                          (set))
        clauses-in-cardinality-order (->> triple-clauses
                                          (remove (set literal-clauses))
                                          (sort-by (fn [{:keys [a]}]
                                                     (predicate-cardinality graph a)))
                                          (vec))
        clauses-in-join-order (loop [[{:keys [e v] :as clause} & clauses] clauses-in-cardinality-order
                                     order []
                                     vars #{}]
                                (if-not clause
                                  order
                                  (if (or (empty? vars)
                                          (seq (set/intersection vars (set [e v]))))
                                    (recur clauses (conj order clause) (into vars [e v]))
                                    (recur (concat clauses [clause]) order vars))))
        var-access-order (->> (for [{:keys [e v]} clauses-in-join-order]
                                [e v])
                              (apply concat literal-vars)
                              (distinct)
                              (vec))
        _ (when-not (set/subset? (set find) (set var-access-order))
            (throw (IllegalArgumentException.
                    "Cannot calculate var access order, does the query form a single connected graph?")))
        var->mask (->> (for [{:keys [e a v]} literal-clauses]
                         (merge
                          (when (literal? e)
                            {v (new-literal-e-mask graph a e)})
                          (when (literal? v)
                            {e (new-literal-v-mask graph a v)})))
                       (apply merge-with bitmap-and))
        initial-result (->> (for [[v bs] var->mask]
                              {v {v (new-diagonal-matrix bs)}})
                            (apply merge-with merge))
        var-result-order (mapv (zipmap var-access-order (range)) find)]
    [var->mask initial-result clauses-in-join-order var-access-order var-result-order]))

(def ^:const ^:private unknown-id -1)

(deftype ParentVarIndexAndPlan [idx plan])

(defn- build-result-set [{:keys [^IntFunction id->value]} var-result-order tuples]
  (let [var-result-order (int-array var-result-order)
        vars (alength var-result-order)]
    (->> tuples
         (map (fn [^ints tuple]
                (loop [idx 0
                       acc (transient [])]
                  (if (= idx vars)
                    (persistent! acc)
                    (recur (inc idx)
                           (conj! acc (.apply id->value (aget tuple (aget var-result-order idx)))))))))
         (into #{}))))

(defn- build-result [graph var-access-order var-result-order result]
  (let [[root-var] var-access-order
        seed (->> (concat
                   (for [[v bs] (get result root-var)]
                     (matlab-any-of-transpose bs))
                   (for [[e vs] result
                         [v bs] vs
                         :when (= v root-var)]
                     (matlab-any bs)))
                  (reduce bitmap-and))]
    (->> ((fn step [^RoaringBitmap xs [var & var-access-order] parent-vars ^ints ctx ^Map plan-cache ^List acc]
            (when (and xs (not (.isEmpty xs)))
              (let [parent-var-idx+plans (when var
                                           (.computeIfAbsent
                                            plan-cache
                                            var
                                            (reify Function
                                              (apply [_ _]
                                                (reduce
                                                 (fn [^List acc [p-idx p]]
                                                   (let [a (get-in result [p var])
                                                         b (when-let [b (get-in result [var p])]
                                                             (cond-> (transpose b)
                                                               a (matrix-and a)))
                                                         plan (or b a)]
                                                     (cond-> acc
                                                       plan (.add (ParentVarIndexAndPlan. p-idx plan)))
                                                     acc))
                                                 (ArrayList.)
                                                 (map-indexed vector parent-vars))))))
                    parent-var (last parent-vars)
                    new-parent-vars (conj parent-vars var)
                    depth (dec (count parent-vars))]
                (.forEach xs
                          (reify IntConsumer
                            (accept [_ x]
                              (let [ctx (doto ctx
                                          (aset depth x))]
                                (if-not var
                                  (.add acc (aclone ctx))
                                  (when-let [ys (reduce
                                                 (fn [^RoaringBitmap acc ^ParentVarIndexAndPlan p+plan]
                                                   (let [^Int2ObjectHashMap plan (.plan p+plan)
                                                         ys (let [row-id (aget ctx (.idx p+plan))]
                                                              (when-not (= unknown-id row-id)
                                                                (.get plan row-id)))]
                                                     (if (and acc ys)
                                                       (bitmap-and acc ys)
                                                       ys)))
                                                 nil
                                                 parent-var-idx+plans)]
                                    (step ys
                                          var-access-order
                                          new-parent-vars
                                          ctx
                                          plan-cache
                                          acc)))))))
                acc)))
          seed
          (next var-access-order)
          [root-var]
          (int-array (count var-access-order))
          (HashMap.)
          (ArrayList.))
         (build-result-set graph var-result-order))))

(defn query [{:keys [^Map query-cache] :as graph} q]
  (let [[var->mask
         initial-result
         clauses-in-join-order
         var-access-order
         var-result-order] (.computeIfAbsent query-cache
                                             q
                                             (reify Function
                                               (apply [_ k]
                                                 (compile-query graph q))))]
    (loop [idx 0
           var->mask var->mask
           result initial-result]
      (if-let [{:keys [e a v] :as clause} (get clauses-in-join-order idx)]
        (let [[join-result mask-e mask-v] (join
                                           graph
                                           a
                                           (get var->mask e)
                                           (get var->mask v))]
          (if (empty? join-result)
            #{}
            (recur (inc idx)
                   (assoc var->mask e mask-e v mask-v)
                   (update-in result [e v] (fn [bs]
                                             (if bs
                                               (matrix-and bs join-result)
                                               join-result))))))
        (build-result graph var-access-order var-result-order result)))))

;; Persistence Spike, this is not how it will eventually look / work,
;; but can server queries out of memory mapped buffers in LMDB.

(def ^:private id->value-idx-id 0)
(def ^:private value->id-idx-id 1)
(def ^:private p->so-idx-id 2)
(def ^:private p->os-idx-id 3)
(def ^:private node-idx-id 4)
(def ^:private latest-id-idx-id 5)

(defn- date->reverse-time-ms ^long [^Date date]
  (bit-xor (bit-not (.getTime date)) Long/MIN_VALUE))

(defn- reverse-time-ms->time-ms ^long [^long reverse-time-ms]
  (bit-xor (bit-not reverse-time-ms) Long/MIN_VALUE))

(defn within-prefix?
  ([^DirectBuffer prefix ^DirectBuffer k]
   (within-prefix? prefix k (.capacity prefix)))
  ([^DirectBuffer prefix ^DirectBuffer k ^long length]
   (and k (mem/buffers=? k prefix length))))

(declare rebuild-matrix-from-hash-tree node-pad branches)

(defn- hash-tree-key->transaction-time-ms ^long [^DirectBuffer k]
  (reverse-time-ms->time-ms (.getLong k (+ Integer/BYTES Integer/BYTES Long/BYTES) ByteOrder/BIG_ENDIAN)))

(defn- node-by-hash [node-cache i ^MutableDirectBuffer content-hash-b ^DirectBuffer node-hash]
  (or (get node-cache node-hash)
      (let [c-seek-k (doto content-hash-b
                       (.putBytes Integer/BYTES node-hash 0 (.capacity node-hash)))
            c-k ^DirectBuffer (kv/seek i c-seek-k)]
        (when (within-prefix? c-seek-k c-k)
          (let [v ^DirectBuffer (kv/value i)]
            (lru/compute-if-absent
             node-cache
             (mem/as-buffer (mem/->on-heap node-hash))
             identity
             (fn [_]
               (if (<= (.getInt v 0 ByteOrder/BIG_ENDIAN) (long branches))
                 (mem/copy-to-unpooled-buffer v)
                 (with-open [in (DataInputStream. (DirectBufferInputStream. v))]
                   (doto (new-bitmap)
                     (.deserialize in)))))))))))

(defn- root-hash-for-matrix [snapshot ^Date valid-time ^Date transaction-time idx-id p-id seek-b]
  (let [valid-time-ms (.getTime valid-time)
        transaction-time-ms (.getTime transaction-time)
        reverse-valid-time-ms (date->reverse-time-ms valid-time)
        idx-id (int idx-id)
        seek-k (mem/with-buffer-out seek-b
                 (fn [^DataOutput out]
                   (.writeInt out idx-id)
                   (.writeInt out p-id)
                   (.writeLong out reverse-valid-time-ms)))]
    (with-open [i (kv/new-iterator snapshot)]
      (let [k ^DirectBuffer (kv/seek i seek-k)]
        (if (within-prefix? seek-k k (* 2 Integer/BYTES))
          (loop [k k]
            (if (<= (hash-tree-key->transaction-time-ms k) transaction-time-ms)
              (kv/value i)
              (let [next-k (kv/next i)]
                (when (within-prefix? seek-k k)
                  (recur next-k))))))))))

(defn- new-snapshot-matrix [snapshot content-hash-b node-cache matrix-cache root-hash]
  (if root-hash
    (let [f (fn [k]
              (with-open [i (kv/new-iterator snapshot)]
                (rebuild-matrix-from-hash-tree
                 k
                 (fn [^DirectBuffer node-hash]
                   (node-by-hash node-cache i content-hash-b node-hash)))))]
      (if-not matrix-cache
        (f root-hash)
        (or (get matrix-cache root-hash)
            (lru/compute-if-absent
             matrix-cache
             (mem/as-buffer (mem/->on-heap root-hash))
             identity
             f))))
    (new-matrix)))

(deftype SnapshotValueToId [snapshot ^Object2IntHashMap cache seek-b]
  ToIntFunction
  (applyAsInt [_ k]
    (.computeIfAbsent cache
                      k
                      (reify ToIntFunction
                        (applyAsInt [_ k]
                          (with-open [i (kv/new-iterator snapshot)]
                            (let [seek-k (mem/with-buffer-out seek-b
                                           (fn [^DataOutput out]
                                             (nippy/freeze-to-out! out k))
                                           false
                                           Integer/BYTES)
                                  k (kv/seek i seek-k)]
                              (if (within-prefix? seek-k k)
                                (.readInt (DataInputStream. (DirectBufferInputStream. (kv/value i))))
                                unknown-id))))))))

(deftype SnapshotIdToValue [snapshot ^Int2ObjectHashMap cache ^MutableDirectBuffer seek-b]
  IntFunction
  (apply [_ k]
    (.computeIfAbsent cache
                      k
                      (reify IntFunction
                        (apply [_ k]
                          (with-open [i (kv/new-iterator snapshot)]
                            (let [seek-k (doto seek-b
                                           (.putInt Integer/BYTES k ByteOrder/BIG_ENDIAN))
                                  k (kv/seek i seek-k)]
                              (when (within-prefix? seek-k k)
                                (nippy/thaw-from-in! (DataInputStream. (DirectBufferInputStream. (kv/value i))))))))))))

;; NOTE: Using buffers between transactions isn't safe, see:
;; https://javadoc.lwjgl.org/index.html?org/lwjgl/util/lmdb/LMDB.html
;; "Values returned from the database are valid only until a subsequent update operation, or the end of the transaction."
;; This cache is currently purely on-heap, both keys and values.
(def ^:private node-cache (lru/new-cache (* 2 1024 1024)))
(def ^:private matrix-cache (lru/new-cache 128))

(defn kv->graph
  ([snapshot]
   (let [now (cio/next-monotonic-date)]
     (kv->graph snapshot now now node-cache matrix-cache)))
  ([snapshot valid-time transaction-time node-cache matrix-cache]
   (let [seek-b (ExpandableDirectByteBuffer.)
         value->id-seek-b (doto (ExpandableDirectByteBuffer.)
                            (.putInt 0 value->id-idx-id ByteOrder/BIG_ENDIAN))
         id->value-seek-b (doto ^MutableDirectBuffer (mem/allocate-buffer (* Integer/BYTES 2))
                            (.putInt 0 id->value-idx-id ByteOrder/BIG_ENDIAN))
         content-hash-b (doto ^MutableDirectBuffer (mem/allocate-buffer (+ hash/id-hash-size Integer/BYTES))
                          (.putInt 0 node-idx-id ByteOrder/BIG_ENDIAN))
         p->so-cache (HashMap.)
         p->os-cache (HashMap.)
         value->id (->SnapshotValueToId snapshot (Object2IntHashMap. unknown-id) value->id-seek-b)]
     {:value->id value->id
      :id->value (->SnapshotIdToValue snapshot (Int2ObjectHashMap.) id->value-seek-b)
      :p->so (reify ILookup
               (valAt [this k]
                 (.computeIfAbsent p->so-cache
                                   k
                                   (reify Function
                                     (apply [_ _]
                                       (let [p-id (.applyAsInt ^ToIntFunction value->id k)]
                                         (->> (root-hash-for-matrix snapshot valid-time transaction-time p->so-idx-id p-id seek-b)
                                              (new-snapshot-matrix snapshot content-hash-b node-cache matrix-cache)))))))

               (valAt [this k default]
                 (throw (UnsupportedOperationException.))))
      :p->os (reify ILookup
               (valAt [this k]
                 (.computeIfAbsent p->os-cache
                                   k
                                   (reify Function
                                     (apply [_ _]
                                       (let [p-id (.applyAsInt ^ToIntFunction value->id k)]
                                         (->> (root-hash-for-matrix snapshot valid-time transaction-time p->os-idx-id p-id seek-b)
                                              (new-snapshot-matrix snapshot content-hash-b node-cache matrix-cache)))))))

               (valAt [this k default]
                 (throw (UnsupportedOperationException.))))
      :predicate-cardinality-cache (HashMap.)
      :query-cache (HashMap.)})))

(defn- get-known-id ^long [^ToIntFunction value->id ^Object2IntHashMap pending-id-state value]
  (let [pending-id (.getValue pending-id-state value)]
    (if (not= unknown-id pending-id)
      pending-id
      (.applyAsInt value->id value))))

(defn- maybe-create-id [snapshot ^ToIntFunction value->id value ^Box last-id-state ^Object2IntHashMap pending-id-state seek-b]
  (let [id (get-known-id value->id pending-id-state value)]
    (if-not (= unknown-id id)
      []
      (let [ ;; TODO: This is obviously a slow way to find the latest id.
            id (.computeIfAbsent pending-id-state
                                 value
                                 (reify ToIntFunction
                                   (applyAsInt [_ k]
                                     (when-not (.val last-id-state)
                                       (with-open [i (kv/new-iterator snapshot)]
                                         (let [prefix-k (mem/with-buffer-out seek-b
                                                          (fn [^DataOutput out]
                                                            (.writeInt out latest-id-idx-id)))
                                               k ^DirectBuffer (kv/seek i prefix-k)]
                                           (if (within-prefix? prefix-k k)
                                             (set! (.val last-id-state) (.getInt ^DirectBuffer (kv/value i) 0 ByteOrder/BIG_ENDIAN))
                                             (set! (.val last-id-state) 0)))))
                                     (unchecked-int (set! (.val last-id-state)
                                                          (unchecked-inc-int (or (.val last-id-state) 0)))))))
            b (ExpandableDirectByteBuffer.)]
        (when (= id unknown-id)
          (throw (IllegalStateException.
                  (str "Out of ids, maximum id is: " (dec (Integer/toUnsignedLong -1))))))
        [[(mem/with-buffer-out b
            (fn [^DataOutput out]
              (.writeInt out id->value-idx-id)
              (.writeInt out id)))
          (mem/with-buffer-out b
            (fn [^DataOutput out]
              (nippy/freeze-to-out! out value)))]
         [(mem/with-buffer-out b
            (fn [^DataOutput out]
              (.writeInt out value->id-idx-id)
              (nippy/freeze-to-out! out value)))
          (mem/with-buffer-out b
            (fn [^DataOutput out]
              (.writeInt out id)))]]))))

(deftype HashTree [^DirectBuffer root-hash ^Map nodes])

;; TODO: Early spike to split up matrix.
(def ^:private ^{:tag 'long} branches 2048)
(def ^:private ^{:tag 'long} bits-per-branch (Long/numberOfTrailingZeros branches))

(defn build-hash-tree ^matrix.HashTree [^Int2ObjectHashMap m]
  (when-not (empty? m)
    (let [max-known (long (reduce max (map #(Integer/toUnsignedLong %) (sort (.keySet m)))))
          b (ExpandableDirectByteBuffer.)
          nodes (HashMap.)
          leaf-cache (HashMap.)
          build (fn build [^long start ^long end ^long depth]
                  (let [node (if (<= (- end start) 1)
                               (let [start-row (unchecked-int start)]
                                 (when-let [x (.get m start-row)]
                                   (conj (.computeIfAbsent leaf-cache
                                                           x
                                                           (reify Function
                                                             (apply [_ _]
                                                               (let [x (doto ^RoaringBitmap x
                                                                         (.runOptimize))
                                                                     x-serialized (mem/with-buffer-out b
                                                                                    (fn [^DataOutput out]
                                                                                      (.serialize x out)))
                                                                     x-hash (hash/id-hash
                                                                             (mem/allocate-buffer hash/id-hash-size)
                                                                             x-serialized)]
                                                                 (.put nodes x-hash x-serialized)
                                                                 [x-hash x-serialized]))))
                                         start-row)))
                               (let [new-depth (inc depth)
                                     step (max (unsigned-bit-shift-right (inc (Integer/toUnsignedLong -1))
                                                                         (* new-depth bits-per-branch))
                                               1)
                                     step (if (>= step (- end start))
                                            1
                                            step)
                                     children (loop [n start
                                                     acc []]
                                                (if (and (< n end) (<= n max-known))
                                                  (let [next-end (+ n step)]
                                                    (recur next-end
                                                           (if-let [child (build n next-end new-depth)]
                                                             (conj acc child)
                                                             acc)))
                                                  acc))]
                                 (when (seq children)
                                   (if (and (= 1 (count children)) (pos? depth))
                                     (first children)
                                     (let [inner-node (mem/copy-buffer
                                                       (do (.putInt b 0 (count children) ByteOrder/BIG_ENDIAN)
                                                           (loop [offset Integer/BYTES
                                                                  [[_ _ idx] & children] children]
                                                             (when idx
                                                               (.putInt b offset (int idx) ByteOrder/BIG_ENDIAN)
                                                               (recur (+ offset Integer/BYTES) children)))
                                                           (loop [offset (+ Integer/BYTES (* (count children) Integer/BYTES))
                                                                  [[h] & children] children]
                                                             (when h
                                                               (.putBytes b offset ^DirectBuffer h 0 hash/id-hash-size)
                                                               (recur (+ offset hash/id-hash-size) children)))
                                                           b)
                                                       (+ Integer/BYTES
                                                          (* (count children) Integer/BYTES)
                                                          (* (count children) hash/id-hash-size)))
                                           combine-hash (hash/id-hash
                                                         (mem/allocate-buffer hash/id-hash-size)
                                                         inner-node)]
                                       (.put nodes combine-hash inner-node)
                                       [combine-hash inner-node start])))))]
                    node))
          [root-hash] (build 0 (inc (Integer/toUnsignedLong (int unknown-id))) 0)]
      (HashTree. root-hash nodes))))

(defn rebuild-matrix-from-hash-tree [root node->buffer-fn]
  (let [m (Int2ObjectHashMap.)
        build (fn build [node ^long idx]
                (if-let [^DirectBuffer v (node->buffer-fn node)]
                  (if (instance? RoaringBitmap v)
                    (.put m (int idx) (.clone ^RoaringBitmap v))
                    (let [number-of-children (.getInt v 0 ByteOrder/BIG_ENDIAN)]
                      (if (<= number-of-children branches)
                        (dotimes [n number-of-children]
                          (let [idx (.getInt v (+ Integer/BYTES (* Integer/BYTES (long n))) ByteOrder/BIG_ENDIAN)]
                            (build (mem/slice-buffer v (+ Integer/BYTES
                                                          (* number-of-children Integer/BYTES)
                                                          (* (long n) hash/id-hash-size)) hash/id-hash-size) idx)))
                        (with-open [in (DataInputStream. (DirectBufferInputStream. v))]
                          (.put m (int idx) (doto (new-bitmap)
                                              (.deserialize in)))))))
                  (throw (IllegalStateException. (str "Could not find hash: " (mem/buffer->hex node))))))]
    (build root unknown-id)
    m))

(def ^:const default-chunk-size 1000000)

(defn transact-rdf-triples
  ([kv triples]
   (transact-rdf-triples kv triples nil))
  ([kv triples valid-time]
   (with-open [snapshot (kv/new-snapshot kv)]
     (let [transaction-time (cio/next-monotonic-date)
           valid-time (or valid-time transaction-time)
           tx-start-time (.getTime transaction-time)
           b (ExpandableDirectByteBuffer.)
           content-hash-b (doto ^MutableDirectBuffer (mem/allocate-buffer (+ hash/id-hash-size Integer/BYTES))
                            (.putInt 0 node-idx-id ByteOrder/BIG_ENDIAN))
           last-id-state (Box. nil)
           matrix-cache nil
           {:keys [value->id] :as g} (kv->graph snapshot valid-time transaction-time node-cache matrix-cache)
           seek-b (ExpandableDirectByteBuffer.)
           pending-id-state (Object2IntHashMap. unknown-id)
           id-kvs (->> (for [[s p o] triples
                             :let [s-id-kvs (maybe-create-id snapshot value->id s last-id-state pending-id-state seek-b)
                                   p-id-kvs (maybe-create-id snapshot value->id p last-id-state pending-id-state seek-b)
                                   o-id-kvs (maybe-create-id snapshot value->id o last-id-state pending-id-state seek-b)]]
                         (concat s-id-kvs p-id-kvs o-id-kvs))
                       (reduce into [])
                       (into {}))
           id-kvs (if-let [last-id (.val last-id-state)]
                    (assoc id-kvs
                           (mem/with-buffer-out b
                             (fn [^DataOutput out]
                               (.writeInt out latest-id-idx-id)))
                           (mem/with-buffer-out b
                             (fn [^DataOutput out]
                               (.writeInt out last-id))))
                    id-kvs)
           tree-nodes (HashMap.)
           tx->root-kvs (->> (for [[p triples] (group-by second triples)
                                   :let [p->so ^Int2ObjectHashMap (get-in g [:p->so p])
                                         p->os ^Int2ObjectHashMap (get-in g [:p->os p])
                                         p-id (get-known-id value->id pending-id-state p)]]
                               (do (doseq [[s triples] (group-by first triples)
                                           :let [s-id (get-known-id value->id pending-id-state s)
                                                 row ^RoaringBitmap (.computeIfAbsent p->so
                                                                                      s-id
                                                                                      (reify IntFunction
                                                                                        (apply [_ _]
                                                                                          (new-bitmap))))]
                                           [_ _ o] triples
                                           :let [o-id (get-known-id value->id pending-id-state o)]]
                                     (.checkedAdd row o-id))
                                   (doseq [[o triples] (group-by last triples)
                                           :let [o-id (get-known-id value->id pending-id-state o)
                                                 row ^RoaringBitmap (.computeIfAbsent p->os
                                                                                      o-id
                                                                                      (reify IntFunction
                                                                                        (apply [_ _]
                                                                                          (new-bitmap))))]
                                           [s _ _] triples
                                           :let [s-id (get-known-id value->id pending-id-state s)]]
                                     (.checkedAdd row s-id))
                                   (concat
                                    [(let [hash-tree ^HashTree (build-hash-tree p->so)
                                           nodes ^Map (.nodes hash-tree)]
                                       (.putAll tree-nodes nodes)
                                       [(mem/with-buffer-out b
                                          (fn [^DataOutput out]
                                            (.writeInt out p->so-idx-id)
                                            (.writeInt out p-id)
                                            (.writeLong out (date->reverse-time-ms valid-time))
                                            (.writeLong out (date->reverse-time-ms transaction-time))))
                                        (.root-hash hash-tree)])]
                                    [(let [hash-tree ^HashTree (build-hash-tree p->os)
                                           nodes ^Map (.nodes hash-tree)]
                                       (.putAll tree-nodes nodes)
                                       [(mem/with-buffer-out b
                                          (fn [^DataOutput out]
                                            (.writeInt out p->os-idx-id)
                                            (.writeInt out p-id)
                                            (.writeLong out (date->reverse-time-ms valid-time))
                                            (.writeLong out (date->reverse-time-ms transaction-time))))
                                        (.root-hash hash-tree)])])))
                             (reduce into [])
                             (into {}))
           node-kvs (with-open [i (kv/new-iterator snapshot)]
                      (->> tree-nodes
                           (reduce
                            (fn [acc [^DirectBuffer k v]]
                              (if (get node-cache k)
                                acc
                                (assoc acc
                                       (mem/copy-buffer
                                        (doto content-hash-b
                                          (.putBytes Integer/BYTES k 0 (.capacity k)))
                                        (+ Integer/BYTES hash/id-hash-size))
                                       v)))
                            {})))
           kvs (merge id-kvs tx->root-kvs node-kvs)]
       (kv/store kv kvs)
       (log/info :triples (count triples) :kvs (count kvs) :time (- (System/currentTimeMillis) tx-start-time))
       (doseq [[k v] node-kvs]
         (lru/compute-if-absent
          node-cache
          (mem/as-buffer (mem/->on-heap (mem/slice-buffer k Integer/BYTES hash/id-hash-size)))
          identity
          (fn [_]
            (mem/copy-to-unpooled-buffer v))))))))

(defn load-rdf-into-kv
  ([kv resource]
   (load-rdf-into-kv kv resource {}))
  ([kv resource {:keys [chunk-size drop-chunks take-chunks] :as opts
                 :or {drop-chunks 0
                      take-chunks Long/MAX_VALUE
                      chunk-size default-chunk-size}}]
   (with-open [in (io/input-stream (io/resource resource))]
     (doseq [chunk (->> (rdf/ntriples-seq in)
                        (map rdf/rdf->clj)
                        (partition-all chunk-size)
                        (drop drop-chunks)
                        (take (or take-chunks Long/MAX_VALUE)))]
       (transact-rdf-triples kv chunk)))))

(def ^:const lubm-triples-resource "lubm/University0_0.ntriples")
(def ^:const watdiv-triples-resource "watdiv/data/watdiv.10M.nt")

;; LUBM:
(comment
  ;; Create an Kv store for the LUBM data.
  (def lm-lubm (kv/open (kv/new-kv-store "crux.kv.lmdb.LMDBKv")
                        {:db-dir "dev-storage/matrix-lmdb-lubm"}))
  ;; Populate with LUBM data.
  (set-log-level! 'matrix :info)
  (matrix/load-rdf-into-kv lm-lubm matrix/lubm-triples-resource)

  ;; Try LUBM query 9:
  (with-open [snapshot (kv/new-snapshot lm-lubm)]
    (= (matrix/query
        (matrix/kv->graph snapshot)
        (rdf/with-prefix {:ub "http://swat.cse.lehigh.edu/onto/univ-bench.owl#"}
          '{:find [x y z]
            :where [[x :rdf/type :ub/GraduateStudent]
                    [y :rdf/type :ub/AssistantProfessor]
                    [z :rdf/type :ub/GraduateCourse]
                    [x :ub/advisor y]
                    [y :ub/teacherOf z]
                    [x :ub/takesCourse z]]}))
       #{[:http://www.Department0.University0.edu/GraduateStudent76
          :http://www.Department0.University0.edu/AssistantProfessor5
          :http://www.Department0.University0.edu/GraduateCourse46]
         [:http://www.Department0.University0.edu/GraduateStudent143
          :http://www.Department0.University0.edu/AssistantProfessor8
          :http://www.Department0.University0.edu/GraduateCourse53]
         [:http://www.Department0.University0.edu/GraduateStudent60
          :http://www.Department0.University0.edu/AssistantProfessor8
          :http://www.Department0.University0.edu/GraduateCourse52]}))

  ;; Close when done.
  (.close lm-lubm))

;; WatDiv:
(comment
  ;; Create an Kv store for our matrices.
  (def lm-watdiv (kv/open (kv/new-kv-store "crux.kv.lmdb.LMDBKv")
                          {:db-dir "dev-storage/matrix-lmdb-watdiv"}))
  ;; This only needs to be done once and then whenever the data
  ;; storage code changes. You need this file:
  ;; https://dsg.uwaterloo.ca/watdiv/watdiv.10M.tar.bz2 Place it at
  ;; test/watdiv/data/watdiv.10M.nt
  (set-log-level! 'matrix :info)
  (matrix/load-rdf-into-kv lm-watdiv matrix/watdiv-triples-resource)

  ;; Run the 240 reference queries against the Matrix spike, skipping
  ;; errors and two queries that currently blocks.
  (defn run-watdiv-reference
    ([lm]
     (run-watdiv-reference lm (io/resource "watdiv/watdiv_crux.edn") #{35 67}))
    ([lm resource skip?]
     (with-open [snapshot (crux.kv/new-snapshot lm)]
       (let [wg (matrix/kv->graph snapshot)
             times (mapv
                    (fn [{:keys [idx query crux-results]}]
                      (let [start (System/currentTimeMillis)]
                        (try
                          (let [result (count (matrix/query wg (crux.sparql/sparql->datalog query)))]
                            (assert (= crux-results result)
                                    (pr-str [idx crux-results result])))
                          (catch Throwable t
                            (prn idx t)))
                        (- (System/currentTimeMillis) start)))
                    (->> (read-string (slurp resource))
                         (remove :crux-error)
                         (remove (comp skip? :idx))))
             total (reduce + times)]
         {:total total :average (/ total (double (count times)))}))))

  (run-watdiv-reference lm-watdiv)

  ;; Close when done.
  (.close lm-watdiv))
