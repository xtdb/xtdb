(ns matrix
  (:require [clojure.java.io :as io]
            [clojure.set :as set]
            [clojure.walk :as w]
            [clojure.spec.alpha :as s]
            [crux.io :as cio]
            [crux.kv :as kv]
            [crux.lru :as lru]
            [crux.memory :as mem]
            [crux.rdf :as rdf]
            [crux.query :as q]
            [taoensso.nippy :as nippy])
  (:import [java.util ArrayList Date HashMap HashSet IdentityHashMap Map]
           [java.util.function Function IntFunction ToIntFunction]
           [clojure.lang Box ILookup Indexed]
           crux.ByteUtils
           [java.io DataInputStream DataOutput DataOutputStream]
           java.nio.ByteOrder
           [org.agrona.collections Int2ObjectHashMap Object2IntHashMap]
           org.agrona.concurrent.UnsafeBuffer
           org.agrona.ExpandableDirectByteBuffer
           [org.agrona DirectBuffer MutableDirectBuffer]
           org.agrona.io.DirectBufferInputStream
           [org.roaringbitmap IntConsumer]
           [org.roaringbitmap.buffer ImmutableRoaringBitmap MutableRoaringBitmap]))

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

(defn- new-bitmap ^org.roaringbitmap.buffer.MutableRoaringBitmap []
  (MutableRoaringBitmap.))

(defn- matrix-and
  ([a] a)
  ([^Int2ObjectHashMap a ^Int2ObjectHashMap b]
   (let [ks (doto (HashSet. (.keySet a))
              (.retainAll (.keySet b)))]
     (reduce (fn [^Int2ObjectHashMap m k]
               (let [k (int k)]
                 (doto m
                   (.put k (ImmutableRoaringBitmap/and (.get a k) (.get b k))))))
             (new-matrix (count ks))
             ks))))

(defn- bitmap-and
  ([a] a)
  ([^ImmutableRoaringBitmap a ^ImmutableRoaringBitmap b]
   (ImmutableRoaringBitmap/and a b)))

(defn- matrix-cardinality ^long [^Int2ObjectHashMap a]
  (->> (.values a)
       (map #(.getCardinality ^ImmutableRoaringBitmap %))
       (reduce +)))

(defn- predicate-cardinality [{:keys [p->so ^Map predicate-cardinality-cache]} attr]
  (.computeIfAbsent predicate-cardinality-cache
                    attr
                    (reify Function
                      (apply [_ k]
                        (matrix-cardinality (get p->so k))))))

(defn- new-diagonal-matrix [^ImmutableRoaringBitmap diag]
  (let [diag (.toArray diag)]
    (reduce
     (fn [^Int2ObjectHashMap m d]
       (let [d (int d)]
         (doto m
           (.put d (doto (new-bitmap)
                     (.add d))))))
     (new-matrix (alength diag))
     diag)))

(defn- transpose [^Int2ObjectHashMap a]
  (loop [i (.iterator (.keySet a))
         m ^Int2ObjectHashMap (new-matrix (.size a))]
    (if-not (.hasNext i)
      m
      (let [row (.nextInt i)
            cols ^ImmutableRoaringBitmap (.get a row)]
        (.forEach cols
                  (reify IntConsumer
                    (accept [_ col]
                      (let [^MutableRoaringBitmap x (.computeIfAbsent m col
                                                                      (reify IntFunction
                                                                        (apply [_ _]
                                                                          (new-bitmap))))]
                        (.add x row)))))
        (recur i m)))))

(defn- matlab-any
  "Determine if any array elements are nonzero. Test each column for
  nonzero elements."
  ^org.roaringbitmap.buffer.ImmutableRoaringBitmap [^Int2ObjectHashMap a]
  (if (.isEmpty a)
    (new-bitmap)
    (ImmutableRoaringBitmap/or (.iterator (.values a)))))

(defn- matlab-any-of-transpose
  "Determine if any array elements are nonzero. Test each row for
  nonzero elements."
  ^org.roaringbitmap.buffer.ImmutableRoaringBitmap [^Int2ObjectHashMap a]
  (loop [i (.iterator (.keySet a))
         acc (new-bitmap)]
    (if-not (.hasNext i)
      acc
      (recur i (doto acc
                 (.add (.nextInt i)))))))

(defn- mult-diag [^ImmutableRoaringBitmap diag ^Int2ObjectHashMap a]
  (cond (.isEmpty a)
        (new-matrix)

        (nil? diag)
        a

        :else
        (doto ^MutableRoaringBitmap
            (reduce
             (fn [^Int2ObjectHashMap m d]
               (let [d (int d)]
                 (if-let [b (.get a d)]
                   (doto m
                     (.put d b))
                   m)))
             (new-matrix (.getCardinality diag))
             (.toArray diag)))))

(defn- new-literal-e-mask ^org.roaringbitmap.buffer.ImmutableRoaringBitmap [{:keys [^ToIntFunction value->id p->so]} a e]
  (let [id (.applyAsInt value->id e)]
    (or (.get ^Int2ObjectHashMap (get p->so a) id)
        (new-bitmap))))

(defn- new-literal-v-mask ^org.roaringbitmap.buffer.ImmutableRoaringBitmap [{:keys [^ToIntFunction value->id p->os]} a v]
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

(deftype ParentVarAndPlan [parent-var plan])

(defn query [{:keys [^IntFunction id->value ^Map query-cache] :as graph} q]
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
        (let [[root-var] var-access-order
              seed (->> (concat
                         (for [[v bs] (get result root-var)]
                           (matlab-any-of-transpose bs))
                         (for [[e vs] result
                               [v bs] vs
                               :when (= v root-var)]
                           (matlab-any bs)))
                        (reduce bitmap-and))
              transpose-cache (IdentityHashMap.)]
          (->> ((fn step [^ImmutableRoaringBitmap xs [var & var-access-order] parent-vars ^Object2IntHashMap ctx]
                  (when (and xs (not (.isEmpty xs)))
                    (let [acc (ArrayList.)
                          parent-var+plan (when var
                                            (reduce
                                             (fn [^ArrayList acc p]
                                               (let [a (get-in result [p var])
                                                     b (when-let [b (get-in result [var p])]
                                                         (.computeIfAbsent transpose-cache
                                                                           b
                                                                           (reify Function
                                                                             (apply [_ b]
                                                                               (cond-> (transpose b)
                                                                                 a (matrix-and a))))))
                                                     plan (or b a)]
                                                 (cond-> acc
                                                   plan (.add (ParentVarAndPlan. p plan)))
                                                 acc))
                                             (ArrayList.)
                                             parent-vars))
                          parent-var (last parent-vars)]
                      (.forEach xs
                                (reify IntConsumer
                                  (accept [_ x]
                                    (if-not var
                                      (.add acc [(.apply id->value x)])
                                      (when-let [xs (reduce
                                                     (fn [^ImmutableRoaringBitmap acc ^ParentVarAndPlan p+plan]
                                                       (let [p (.parent-var p+plan)
                                                             ^Int2ObjectHashMap plan (.plan p+plan)
                                                             xs (if (= parent-var p)
                                                                  (.get plan x)
                                                                  (let [idx (.get ctx p)]
                                                                    (when-not (= unknown-id idx)
                                                                      (.get plan idx))))]
                                                         (if (and acc xs)
                                                           (bitmap-and acc xs)
                                                           xs)))
                                                     nil
                                                     parent-var+plan)]
                                        (doseq [y (step xs
                                                        var-access-order
                                                        (conj parent-vars var)
                                                        (doto ctx
                                                          (.put (last parent-vars) x)))]
                                          (.add acc (cons (.apply id->value x) y))))))))
                      (seq acc))))
                seed
                (next var-access-order)
                [root-var]
                (Object2IntHashMap. unknown-id))
               (map #(mapv (vec %) var-result-order))
               (into #{})))))))

;; Persistence Spike, this is not how it will eventually look / work,
;; but can server queries out of memory mapped buffers in LMDB.

(def ^:private id->value-idx-id 0)
(def ^:private value->id-idx-id 1)
(def ^:private p->so-idx-id 2)
(def ^:private p->os-idx-id 3)
(def ^:private row-content-idx-id 4)

(defn- date->reverse-time-ms ^long [^Date date]
  (bit-xor (bit-not (.getTime date)) Long/MIN_VALUE))

(defn- reverse-time-ms->time-ms ^long [^long reverse-time-ms]
  (bit-xor (bit-not reverse-time-ms) Long/MIN_VALUE))

(defn- key->business-time-ms ^long [^DirectBuffer k]
  (reverse-time-ms->time-ms (.getLong k (- (.capacity k) Long/BYTES Long/BYTES) ByteOrder/BIG_ENDIAN)))

(defn- key->transaction-time-ms ^long [^DirectBuffer k]
  (reverse-time-ms->time-ms (.getLong k (- (.capacity k) Long/BYTES) ByteOrder/BIG_ENDIAN)))

(defn- key->row-id ^long [^DirectBuffer k]
  (.getInt k (- (.capacity k) Integer/BYTES Long/BYTES Long/BYTES) ByteOrder/BIG_ENDIAN))

(deftype RowIdAndKey [^int row-id ^DirectBuffer key])

(def ^:private ^:const no-matches-for-row -1)
(def ^:private ^:const sha1-size 20)
(def ^:private ^:const bitmap-in-memory-threshold 2)

(defn within-prefix? [^DirectBuffer prefix ^DirectBuffer k]
  (and k (mem/buffers=? k prefix (.capacity prefix))))

(defn- new-snapshot-matrix [snapshot ^Date business-time ^Date transaction-time idx-id p seek-b ^MutableDirectBuffer content-hash-b bitmap-buffer-cache]
  (let [business-time-ms (.getTime business-time)
        transaction-time-ms (.getTime transaction-time)
        reverse-business-time-ms (date->reverse-time-ms business-time)
        idx-id (int idx-id)
        seek-k (mem/with-buffer-out seek-b
                 (fn [^DataOutput out]
                   (.writeInt out idx-id)
                   (nippy/freeze-to-out! out p)))]
    (with-open [i (kv/new-iterator snapshot)]
      (loop [id->row ^Int2ObjectHashMap (new-matrix)
             k ^DirectBuffer (kv/seek i seek-k)]
        (if (within-prefix? seek-k k)
          (let [row-id+k (loop [k k]
                           (let [row-id (int (key->row-id k))]
                             (if (<= (key->business-time-ms k) business-time-ms)
                               (RowIdAndKey. row-id k)
                               (let [found-k ^DirectBuffer (kv/seek i (mem/with-buffer-out seek-b
                                                                        (fn [^DataOutput out]
                                                                          (.writeInt out row-id)
                                                                          (.writeLong out reverse-business-time-ms))
                                                                        false
                                                                        (.capacity seek-k)))]
                                 (when (within-prefix? seek-k found-k)
                                   (let [found-row-id (int (key->row-id found-k))]
                                     (if (= row-id found-row-id)
                                       (RowIdAndKey. row-id k)
                                       (recur found-k))))))))
                row-id+k (when row-id+k
                           (let [row-id (.row-id ^RowIdAndKey row-id+k)]
                             (loop [k (.key ^RowIdAndKey row-id+k)]
                               (if (<= (key->transaction-time-ms k) transaction-time-ms)
                                 (RowIdAndKey. row-id k)
                                 (let [next-k (kv/next i)]
                                   (when (within-prefix? seek-k k)
                                     (if (= row-id (int (key->row-id next-k)))
                                       (recur next-k)
                                       (RowIdAndKey. no-matches-for-row next-k))))))))]
            (if-not row-id+k
              id->row
              (let [row-id (.row-id ^RowIdAndKey row-id+k)]
                (if (not= no-matches-for-row row-id)
                  (let [buffer-hash ^DirectBuffer (kv/value i)
                        row (lru/compute-if-absent bitmap-buffer-cache
                                                   buffer-hash
                                                   (fn [_]
                                                     (let [c-seek-k (doto content-hash-b
                                                                      (.putBytes Integer/BYTES buffer-hash 0 (.capacity buffer-hash)))
                                                           c-k ^DirectBuffer (kv/seek i c-seek-k)]
                                                       (assert (and c-k (= row-content-idx-id (.getInt c-k 0 ByteOrder/BIG_ENDIAN))))
                                                       (let [b (ImmutableRoaringBitmap. (.byteBuffer ^DirectBuffer (kv/value i)))]
                                                         (if (<= (.getCardinality b) bitmap-in-memory-threshold)
                                                           (.toMutableRoaringBitmap b)
                                                           b)))))]
                    (recur (doto id->row
                             (.put row-id row))
                           (kv/seek i (mem/with-buffer-out seek-b
                                        (fn [^DataOutput out]
                                          (.writeInt out (inc row-id)))
                                        false
                                        (.capacity seek-k)))))
                  (recur id->row (.key ^RowIdAndKey row-id+k))))))
          id->row)))))

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
                                             (.writeInt out value->id-idx-id)
                                             (nippy/freeze-to-out! out k))
                                           false)
                                  k (kv/seek i seek-k)]
                              (if (within-prefix? seek-k k)
                                (.readInt (DataInputStream. (DirectBufferInputStream. (kv/value i))))
                                unknown-id))))))))

(deftype SnapshotIdToValue [snapshot ^Int2ObjectHashMap cache seek-b]
  IntFunction
  (apply [_ k]
    (.computeIfAbsent cache
                      k
                      (reify IntFunction
                        (apply [_ k]
                          (with-open [i (kv/new-iterator snapshot)]
                            (let [seek-k (mem/with-buffer-out seek-b
                                           (fn [^DataOutput out]
                                             (.writeInt out id->value-idx-id)
                                             (.writeInt out k))
                                           false)
                                  k (kv/seek i seek-k)]
                              (when (within-prefix? seek-k k)
                                (nippy/thaw-from-in! (DataInputStream. (DirectBufferInputStream. (kv/value i))))))))))))

;; TODO: This isn't safe across snapshots if the address inside LMDB
;; changes.
(def ^:private bitmap-buffer-cache (lru/new-cache (* 2 1024 1024)))

(defn lmdb->graph
  ([snapshot]
   (let [now (cio/next-monotonic-date)]
     (lmdb->graph snapshot now now bitmap-buffer-cache)))
  ([snapshot business-time transaction-time bitmap-buffer-cache]
   (let [seek-b (ExpandableDirectByteBuffer.)
         content-hash-b (doto ^MutableDirectBuffer (mem/allocate-buffer (+ sha1-size Integer/BYTES))
                          (.putInt 0 row-content-idx-id ByteOrder/BIG_ENDIAN))
         matrix-cache (HashMap.)]
     {:value->id (->SnapshotValueToId snapshot (Object2IntHashMap. unknown-id) seek-b)
      :id->value (->SnapshotIdToValue snapshot (Int2ObjectHashMap.) seek-b)
      :p->so (reify ILookup
               (valAt [this k]
                 (.computeIfAbsent matrix-cache
                                   [p->so-idx-id k]
                                   (reify Function
                                     (apply [_ _]
                                       (new-snapshot-matrix snapshot business-time transaction-time p->so-idx-id k seek-b content-hash-b bitmap-buffer-cache)))))

               (valAt [this k default]
                 (throw (UnsupportedOperationException.))))
      :p->os (reify ILookup
               (valAt [this k]
                 (.computeIfAbsent matrix-cache
                                   [p->os-idx-id k]
                                   (reify Function
                                     (apply [_ _]
                                       (new-snapshot-matrix snapshot business-time transaction-time p->os-idx-id k seek-b content-hash-b bitmap-buffer-cache)))))

               (valAt [this k default]
                 (throw (UnsupportedOperationException.))))
      :predicate-cardinality-cache (HashMap.)
      :query-cache (HashMap.)})))

(defn- get-or-create-id [kv value ^Box last-id-state ^Object2IntHashMap pending-id-state]
  (with-open [snapshot (kv/new-snapshot kv)]
    (let [seek-b (ExpandableDirectByteBuffer.)
          value->id ^ToIntFunction (->SnapshotValueToId snapshot (Object2IntHashMap. unknown-id) seek-b)
          id (.applyAsInt value->id value)]
      (if (not= unknown-id id)
        [id []]
        (let [b (ExpandableDirectByteBuffer.)
              prefix-k (mem/with-buffer-out seek-b
                         (fn [^DataOutput out]
                           (.writeInt out id->value-idx-id)))
              ;; TODO: This is obviously a slow way to find the latest id.
              id (.computeIfAbsent pending-id-state
                                   value
                                   (reify ToIntFunction
                                     (applyAsInt [_ k]
                                       (when (= unknown-id (.val last-id-state))
                                         (with-open [i (kv/new-iterator snapshot)]
                                           (loop [last-id (int 0)
                                                  k ^DirectBuffer (kv/seek i prefix-k)]
                                             (if (within-prefix? prefix-k k)
                                               (recur (.getInt k Integer/BYTES ByteOrder/BIG_ENDIAN) (kv/next i))
                                               (set! (.val last-id-state) last-id)))))
                                       (unchecked-int (set! (.val last-id-state)
                                                            (unchecked-inc-int (or (.val last-id-state) 0)))))))]
          (when (= id unknown-id)
            (throw (IllegalStateException.
                    (str "Out of ids, maximum id is: " (dec (Integer/toUnsignedLong -1))))))
          [id [[(mem/with-buffer-out b
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
                    (.writeInt out id)))]]])))))

(defn load-rdf-into-lmdb
  ([kv resource]
   (load-rdf-into-lmdb kv resource 100000))
  ([kv resource chunk-size]
   (with-open [in (io/input-stream (io/resource resource))]
     (let [now (cio/next-monotonic-date)
           business-time now
           transaction-time now
           b (ExpandableDirectByteBuffer.)
           content-hash-b (doto ^MutableDirectBuffer (mem/allocate-buffer (+ sha1-size Integer/BYTES))
                            (.putInt 0 row-content-idx-id ByteOrder/BIG_ENDIAN))
           last-id-state (Box. nil)]
       (doseq [chunk (->> (rdf/ntriples-seq in)
                          (map rdf/rdf->clj)
                          (partition-all chunk-size))]
         (with-open [snapshot (kv/new-snapshot kv)]
           (let [bitmap-buffer-cache (lru/new-cache (* 1024 1024))
                 g (lmdb->graph snapshot business-time transaction-time bitmap-buffer-cache)
                 row-cache (HashMap.)
                 get-current-row (fn [idx-id idx p ^long id]
                                   (let [id (int id)]
                                     (.computeIfAbsent row-cache
                                                       [idx-id idx p id]
                                                       (reify Function
                                                         (apply [_ _]
                                                           [(mem/with-buffer-out b
                                                              (fn [^DataOutput out]
                                                                (.writeInt out idx-id)
                                                                (nippy/freeze-to-out! out p)
                                                                (.writeInt out id)
                                                                (.writeLong out (date->reverse-time-ms business-time))
                                                                (.writeLong out (date->reverse-time-ms transaction-time))))
                                                            (let [^Int2ObjectHashMap m (get-in g [idx p])
                                                                  b ^ImmutableRoaringBitmap (or (.get m id) (new-bitmap))]
                                                              (.toMutableRoaringBitmap ^ImmutableRoaringBitmap b))])))))

                 pending-id-state (Object2IntHashMap. unknown-id)
                 kvs (->> (for [[s p o] chunk
                                :let [[^long s-id s-id-kvs] (get-or-create-id kv s last-id-state pending-id-state)
                                      [^long o-id o-id-kvs] (get-or-create-id kv o last-id-state pending-id-state)]]
                            (concat s-id-kvs
                                    o-id-kvs
                                    [(let [[k ^MutableRoaringBitmap row] (get-current-row p->so-idx-id :p->so p s-id)]
                                       [k (doto row
                                            (.add o-id))])]
                                    [(let [[k ^MutableRoaringBitmap row] (get-current-row p->os-idx-id :p->os p o-id)]
                                       [k (doto row
                                            (.add s-id))])]))
                          (reduce into [])
                          (into (sorted-map-by mem/buffer-comparator)))
                 bitmap->hash (IdentityHashMap.)
                 bitmap-kvs (with-open [i (kv/new-iterator snapshot)]
                              (->> (for [[_ v] kvs
                                         :when (instance? MutableRoaringBitmap v)
                                         :let [v (doto ^MutableRoaringBitmap v
                                                   (.runOptimize))
                                               v-serialized (mem/with-buffer-out b
                                                              (fn [^DataOutput out]
                                                                (.serialize v out))
                                                              false)
                                               k (crux.ByteUtils/sha1
                                                  (mem/allocate-buffer sha1-size)
                                                  v-serialized)
                                               c-k (doto content-hash-b
                                                     (.putBytes Integer/BYTES k 0 (.capacity k)))]]
                                     (do (.put bitmap->hash v k)
                                         (when-not (within-prefix? c-k (kv/seek i c-k))
                                           [(mem/copy-buffer c-k)
                                            (mem/copy-buffer v-serialized)])))
                                   (remove nil?)
                                   (into (sorted-map-by mem/buffer-comparator))))
                 kvs (->> (for [[k v] kvs]
                            [k (.getOrDefault bitmap->hash v v)])
                          (into bitmap-kvs))]
             (prn (count chunk))
             (kv/store kv kvs))))))))

(def ^:const lubm-triples-resource "lubm/University0_0.ntriples")
(def ^:const watdiv-triples-resource "watdiv/data/watdiv.10M.nt")

;; LUBM:
(comment
  ;; Create an LMDBKv store for the LUBM data.
  (def lm-lubm (kv/open (kv/new-kv-store "crux.kv.lmdb.LMDBKv")
                        {:db-dir "dev-storage/matrix-lmdb-lubm"}))
  ;; Populate with LUBM data.
  (matrix/load-rdf-into-lmdb lm-lubm matrix/lubm-triples-resource)

  ;; Try LUBM query 9:
  (with-open [snapshot (kv/new-snapshot lm-lubm)]
    (= (matrix/query
        (matrix/lmdb->graph snapshot)
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
  (.close lm-lmdb))

;; WatDiv:
(comment
  ;; Create an LMDBKv store for our matrices.
  (def lm-watdiv (kv/open (kv/new-kv-store "crux.kv.lmdb.LMDBKv")
                          {:db-dir "dev-storage/matrix-lmdb-watdiv"}))
  ;; This only needs to be done once and then whenever the data
  ;; storage code changes. You need this file:
  ;; https://dsg.uwaterloo.ca/watdiv/watdiv.10M.tar.bz2 Place it at
  ;; test/watdiv/data/watdiv.10M.nt
  (matrix/load-rdf-into-lmdb lm-watdiv matrix/watdiv-triples-resource)

  ;; Run the 240 reference queries against the Matrix spike, skipping
  ;; errors and two queries that currently blocks.
  (defn run-watdiv-reference
    ([lm]
     (run-watdiv-reference lm (io/resource "watdiv/watdiv_crux.edn") #{35 67}))
    ([lm resource skip?]
     (with-open [snapshot (crux.kv/new-snapshot lm)]
       (let [wg (matrix/lmdb->graph snapshot)
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
