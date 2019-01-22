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
  (:import [java.util ArrayList Date HashMap HashSet IdentityHashMap List Map]
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
(def ^:private row-content-idx-id 4)

(defn- date->reverse-time-ms ^long [^Date date]
  (bit-xor (bit-not (.getTime date)) Long/MIN_VALUE))

(defn- reverse-time-ms->time-ms ^long [^long reverse-time-ms]
  (bit-xor (bit-not reverse-time-ms) Long/MIN_VALUE))

(defn- key->row-id ^long [^DirectBuffer k]
  (.getInt k (+ Integer/BYTES Integer/BYTES) ByteOrder/BIG_ENDIAN))

(defn- key->business-time-ms ^long [^DirectBuffer k]
  (reverse-time-ms->time-ms (.getLong k (+ Integer/BYTES Integer/BYTES Integer/BYTES) ByteOrder/BIG_ENDIAN)))

(defn- key->transaction-time-ms ^long [^DirectBuffer k]
  (reverse-time-ms->time-ms (.getLong k (+ Integer/BYTES Integer/BYTES Integer/BYTES Long/BYTES) ByteOrder/BIG_ENDIAN)))

(deftype RowIdAndKey [^int row-id ^DirectBuffer key])

(def ^:private ^:const no-matches-for-row -1)
(def ^:private ^:const sha1-size 20)

(defn within-prefix? [^DirectBuffer prefix ^DirectBuffer k]
  (and k (mem/buffers=? k prefix (.capacity prefix))))

(defn- new-snapshot-matrix [snapshot ^Date business-time ^Date transaction-time idx-id p-id seek-b ^MutableDirectBuffer content-hash-b bitmap-buffer-cache]
  (let [business-time-ms (.getTime business-time)
        transaction-time-ms (.getTime transaction-time)
        reverse-business-time-ms (date->reverse-time-ms business-time)
        idx-id (int idx-id)
        seek-k (mem/with-buffer-out seek-b
                 (fn [^DataOutput out]
                   (.writeInt out idx-id)
                   (.writeInt out p-id)))]
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
                        row (or (get bitmap-buffer-cache buffer-hash)
                                (let [buffer-hash (mem/as-buffer (mem/->on-heap buffer-hash))]
                                  (lru/compute-if-absent bitmap-buffer-cache
                                                         buffer-hash
                                                         (fn [_]
                                                           (let [c-seek-k (doto content-hash-b
                                                                            (.putBytes Integer/BYTES buffer-hash 0 (.capacity buffer-hash)))
                                                                 c-k ^DirectBuffer (kv/seek i c-seek-k)]
                                                             (assert (and c-k (= row-content-idx-id (.getInt c-k 0 ByteOrder/BIG_ENDIAN))))
                                                             (doto (new-bitmap)
                                                               (.deserialize (DataInputStream. (DirectBufferInputStream. (kv/value i))))))))))]
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
(def ^:private bitmap-buffer-cache (lru/new-cache (* 2 1024 1024)))

(defn lmdb->graph
  ([snapshot]
   (let [now (cio/next-monotonic-date)]
     (lmdb->graph snapshot now now bitmap-buffer-cache)))
  ([snapshot business-time transaction-time bitmap-buffer-cache]
   (let [seek-b (ExpandableDirectByteBuffer.)
         value->id-seek-b (doto (ExpandableDirectByteBuffer.)
                            (.putInt 0 value->id-idx-id ByteOrder/BIG_ENDIAN))
         id->value-seek-b (doto ^MutableDirectBuffer (mem/allocate-buffer (* Integer/BYTES 2))
                            (.putInt 0 id->value-idx-id ByteOrder/BIG_ENDIAN))
         content-hash-b (doto ^MutableDirectBuffer (mem/allocate-buffer (+ sha1-size Integer/BYTES))
                          (.putInt 0 row-content-idx-id ByteOrder/BIG_ENDIAN))
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
                                         (new-snapshot-matrix snapshot business-time transaction-time p->so-idx-id p-id seek-b content-hash-b bitmap-buffer-cache))))))

               (valAt [this k default]
                 (throw (UnsupportedOperationException.))))
      :p->os (reify ILookup
               (valAt [this k]
                 (.computeIfAbsent p->os-cache
                                   k
                                   (reify Function
                                     (apply [_ _]
                                       (let [p-id (.applyAsInt ^ToIntFunction value->id k)]
                                         (new-snapshot-matrix snapshot business-time transaction-time p->os-idx-id p-id seek-b content-hash-b bitmap-buffer-cache))))))

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
                                     (when (= unknown-id (.val last-id-state))
                                       (with-open [i (kv/new-iterator snapshot)]
                                         (let [prefix-k (mem/with-buffer-out seek-b
                                                          (fn [^DataOutput out]
                                                            (.writeInt out id->value-idx-id)))]
                                           (loop [last-id (int 0)
                                                  k ^DirectBuffer (kv/seek i prefix-k)]
                                             (if (within-prefix? prefix-k k)
                                               (recur (.getInt k Integer/BYTES ByteOrder/BIG_ENDIAN) (kv/next i))
                                               (set! (.val last-id-state) last-id))))))
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
           (let [{:keys [value->id] :as g} (lmdb->graph snapshot business-time transaction-time bitmap-buffer-cache)
                 seek-b (ExpandableDirectByteBuffer.)
                 pending-id-state (Object2IntHashMap. unknown-id)
                 id-kvs (->> (for [[s p o] chunk
                                   :let [s-id-kvs (maybe-create-id snapshot value->id s last-id-state pending-id-state seek-b)
                                         p-id-kvs (maybe-create-id snapshot value->id p last-id-state pending-id-state seek-b)
                                         o-id-kvs (maybe-create-id snapshot value->id o last-id-state pending-id-state seek-b)]]
                               (concat s-id-kvs p-id-kvs o-id-kvs))
                             (reduce into [])
                             (into {}))
                 kvs (->> (for [[p triples] (group-by second chunk)
                                :let [p->so-row-cache (Int2ObjectHashMap.)
                                      p->os-row-cache (Int2ObjectHashMap.)
                                      p-id (get-known-id value->id pending-id-state p)
                                      row-key (fn [idx-id id]
                                                (mem/with-buffer-out b
                                                  (fn [^DataOutput out]
                                                    (.writeInt out idx-id)
                                                    (.writeInt out p-id)
                                                    (.writeInt out id)
                                                    (.writeLong out (date->reverse-time-ms business-time))
                                                    (.writeLong out (date->reverse-time-ms transaction-time)))))]
                                [s _ o] triples
                                :let [s-id (get-known-id value->id pending-id-state s)
                                      o-id (get-known-id value->id pending-id-state o)]]
                            (concat
                             (let [[k ^RoaringBitmap row :as kv]
                                   (.computeIfAbsent p->so-row-cache
                                                     s-id
                                                     (reify IntFunction
                                                       (apply [_ _]
                                                         [(row-key p->so-idx-id s-id)
                                                          (let [^Int2ObjectHashMap m (get-in g [:p->so p])]
                                                            (if-let [b (.get m s-id)]
                                                              (.clone ^RoaringBitmap b)
                                                              (new-bitmap)))])))]
                               (when (.checkedAdd row o-id)
                                 [kv]))
                             (let [[k ^RoaringBitmap row :as kv]
                                   (.computeIfAbsent p->os-row-cache
                                                     o-id
                                                     (reify IntFunction
                                                       (apply [_ _]
                                                         [(row-key p->os-idx-id o-id)
                                                          (let [^Int2ObjectHashMap m (get-in g [:p->os p])]
                                                            (if-let [b (.get m o-id)]
                                                              (.clone ^RoaringBitmap b)
                                                              (new-bitmap)))])))]
                               (when (.checkedAdd row s-id)
                                 [kv]))))
                          (reduce into [])
                          (into id-kvs))
                 bitmap->hash (IdentityHashMap.)
                 known-hashes (HashSet.)
                 bitmap-kvs (with-open [i (kv/new-iterator snapshot)]
                              (->> (vals kvs)
                                   (filter #(instance? RoaringBitmap %))
                                   (reduce
                                    (fn [acc v]
                                      (or (when-not (.containsKey bitmap->hash v)
                                            (let [v (doto ^RoaringBitmap v
                                                      (.runOptimize))
                                                  v-serialized (mem/with-buffer-out b
                                                                 (fn [^DataOutput out]
                                                                   (.serialize v out))
                                                                 false)
                                                  k (crux.ByteUtils/sha1
                                                     (mem/allocate-buffer sha1-size)
                                                     v-serialized)]
                                              (.put bitmap->hash v k)
                                              (when-not (.contains known-hashes k)
                                                (.add known-hashes k)
                                                (let [c-k (doto content-hash-b
                                                            (.putBytes Integer/BYTES k 0 (.capacity k)))]
                                                  (when-not (within-prefix? c-k (kv/seek i c-k))
                                                    (assoc acc
                                                           (mem/copy-buffer c-k)
                                                           (mem/copy-buffer v-serialized)))))))
                                          acc))
                                    {})))
                 kvs (->> (for [[k v] kvs]
                            [k (.getOrDefault bitmap->hash v v)])
                          (into bitmap-kvs))]
             (prn (count chunk))
             (kv/store kv kvs))))))))

;; TODO: Early spike to split up matrix.
(defn build-merkle-tree
  ([^Int2ObjectHashMap m]
   (let [max-known (long (reduce max (map #(Integer/toUnsignedLong %) (sort (.keySet m)))))
         b (ExpandableDirectByteBuffer.)
         empty-hash (crux.ByteUtils/sha1
                     (mem/allocate-buffer sha1-size)
                     (mem/allocate-buffer 0))
         nil-hashes (object-array
                     (reduce
                      (fn [acc ^long x]
                        (conj acc (crux.ByteUtils/sha1
                                   (mem/allocate-buffer sha1-size)
                                   (get acc (dec x)))))
                      [empty-hash]
                      (range 1 (inc Integer/SIZE))))
         acc (HashMap.)
         build (fn build [^long start ^long end ^long depth]
                 (let [[k v :as node]
                       (if (= (- end start) 1)
                         (let [start-row (unchecked-int start)]
                           (if-let [x (.get m start-row)]
                             (let [x (doto ^RoaringBitmap x
                                       (.runOptimize))
                                   x-serialized (mem/with-buffer-out b
                                                  (fn [^DataOutput out]
                                                    (.serialize x out))
                                                  false)
                                   x-hash (crux.ByteUtils/sha1
                                           (mem/allocate-buffer sha1-size)
                                           x-serialized)]
                               [x-hash x-serialized])
                             [(aget nil-hashes depth) nil]))
                         (let [half (unsigned-bit-shift-right (+ start end) 1)
                               new-depth (inc depth)
                               [^DirectBuffer left-hash] (if (<= start max-known)
                                                           (build start half new-depth)
                                                           [(aget nil-hashes new-depth) nil])
                               [^DirectBuffer right-hash] (if (<= half max-known)
                                                            (build half end new-depth)
                                                            [(aget nil-hashes new-depth) nil])
                               concat-hash (mem/copy-buffer
                                            (doto b
                                              (.putBytes 0 left-hash 0 sha1-size)
                                              (.putBytes sha1-size right-hash 0 sha1-size))
                                            (* sha1-size 2))
                               combine-hash (crux.ByteUtils/sha1
                                             (mem/allocate-buffer sha1-size)
                                             concat-hash)]
                           [combine-hash concat-hash]))]
                   (when v
                     (.put acc k v))
                   node))]
     (build 0 (Integer/toUnsignedLong (int unknown-id)) 0)
     acc)))

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
