(ns matrix
  (:require [clojure.java.io :as io]
            [clojure.set :as set]
            [clojure.walk :as w]
            [clojure.spec.alpha :as s]
            [crux.rdf :as rdf]
            [crux.query :as q])
  (:import [java.util Arrays ArrayList BitSet HashMap HashSet IdentityHashMap Map]
           [java.util.function Function IntFunction]
           clojure.lang.Indexed
           java.io.DataOutputStream
           [org.agrona.collections Hashing Int2ObjectHashMap]
           org.agrona.ExpandableDirectByteBuffer
           org.agrona.io.ExpandableDirectBufferOutputStream
           [org.roaringbitmap BitmapDataProviderSupplier FastRankRoaringBitmap ImmutableBitmapDataProvider IntConsumer RoaringBitmap]
           [org.roaringbitmap.buffer ImmutableRoaringBitmap MutableRoaringBitmap]
           [org.roaringbitmap.longlong LongConsumer Roaring64NavigableMap]
           [org.ejml.data DMatrix DMatrixRMaj
            DMatrixSparse DMatrixSparseCSC]
           org.ejml.dense.row.CommonOps_DDRM
           [org.ejml.sparse.csc CommonOps_DSCC MatrixFeatures_DSCC]
           org.ejml.generic.GenericMatrixOps_F64))

(set! *unchecked-math* :warn-on-boxed)

;; Matrix / GraphBLAS style breath first search
;; https://redislabs.com/redis-enterprise/technology/redisgraph/
;; MAGiQ http://www.vldb.org/pvldb/vol11/p1978-jamour.pdf
;; gSMat https://arxiv.org/pdf/1807.07691.pdf

(defn square-matrix ^DMatrixSparseCSC [size]
  (DMatrixSparseCSC. size size))

(defn row-vector ^DMatrixSparseCSC [size]
  (DMatrixSparseCSC. 1 size))

(defn col-vector ^DMatrixSparseCSC [size]
  (DMatrixSparseCSC. size 1))

(defn equals-matrix [^DMatrix a ^DMatrix b]
  (GenericMatrixOps_F64/isEquivalent a b 0.0))

(defn resize-matrix [^DMatrixSparse m new-size]
  (let [grown (square-matrix new-size)]
    (CommonOps_DSCC/extract m
                            0
                            (.getNumCols m)
                            0
                            (.getNumRows m)
                            grown
                            0
                            0)
    grown))

(defn ensure-matrix-capacity ^DMatrixSparseCSC [^DMatrixSparseCSC m size factor]
  (if (> (long size) (.getNumRows m))
    (resize-matrix m (* (long factor) (long size)))
    m))

(defn round-to-power-of-two [^long x ^long stride]
  (bit-and (bit-not (dec stride))
           (dec (+ stride x))))

(defn load-rdf-into-matrix-graph [resource]
  (with-open [in (io/input-stream (io/resource resource))]
    (let [{:keys [value->id
                  eid->matrix]}
          (->> (rdf/ntriples-seq in)
               (map rdf/rdf->clj)
               (reduce (fn [{:keys [value->id eid->matrix]} [s p o]]
                         (let [value->id (reduce (fn [value->id v]
                                                   (update value->id v (fn [x]
                                                                         (or x (count value->id)))))
                                                 value->id
                                                 [s p o])]
                           {:eid->matrix (update eid->matrix
                                                 p
                                                 (fn [x]
                                                   (let [s-id (long (get value->id s))
                                                         o-id (long (get value->id o))
                                                         size (count value->id)
                                                         m (if x
                                                             (ensure-matrix-capacity x size 2)
                                                             (square-matrix size))]
                                                     (doto m
                                                       (.unsafe_set s-id o-id 1.0)))))
                            :value->id value->id}))))
          max-size (round-to-power-of-two (count value->id) 64)]
      {:eid->matrix (->> (for [[k ^DMatrixSparseCSC v] eid->matrix]
                           [k (ensure-matrix-capacity v max-size 1)])
                         (into {}))
       :value->id value->id
       :id->value (->> (set/map-invert value->id)
                       (into (sorted-map)))
       :max-size max-size})))

(defn print-assigned-values [{:keys [id->value] :as graph} ^DMatrix m]
  (if (instance? DMatrixSparse m)
    (.printNonZero ^DMatrixSparse m)
    (.print m))
  (doseq [r (range (.getNumRows m))
          c (range (.getNumCols m))
          :when (= 1.0 (.unsafe_get m r c))]
    (if (= 1 (.getNumRows m))
      (prn (get id->value c))
      (prn (get id->value r) (get id->value c))))
  (prn))

;; TODO: Couldn't this be a vector in most cases?
(defn new-constant-matix ^DMatrixSparseCSC [{:keys [value->id max-size] :as graph} & vs]
  (let [m (square-matrix max-size)]
    (doseq [v vs
            :let [id (get value->id v)]
            :when id]
      (.unsafe_set m id id 1.0))
    m))

(defn transpose-matrix ^DMatrixSparseCSC [^DMatrixSparseCSC m]
  (CommonOps_DSCC/transpose m nil nil))

(defn boolean-matrix ^DMatrixSparseCSC [^DMatrixSparseCSC m]
  (dotimes [n (alength (.nz_values m))]
    (when (pos? (aget (.nz_values m) n))
      (aset (.nz_values m) n 1.0)))
  m)

(defn ^DMatrixSparseCSC assign-mask
  ([^DMatrixSparseCSC mask ^DMatrixSparseCSC w u]
   (assign-mask mask w u pos?))
  ([^DMatrixSparseCSC mask ^DMatrixSparseCSC w u pred]
   (assert (= 1 (.getNumCols mask) (.getNumCols w)))
   (dotimes [n (min (.getNumRows mask) (.getNumRows w))]
     (when (pred (.get mask n 0))
       (.set w n 0 (double u))))
   w))

(defn mask ^DMatrixSparseCSC [^DMatrixSparseCSC mask ^DMatrixSparseCSC w]
  (assign-mask mask w 0.0 zero?))

(defn inverse-mask ^DMatrixSparseCSC [^DMatrixSparseCSC mask ^DMatrixSparseCSC w]
  (assign-mask mask w 0.0 pos?))

(defn multiply-matrix ^DMatrixSparseCSC [^DMatrixSparseCSC a ^DMatrixSparseCSC b]
  (doto (DMatrixSparseCSC. (.getNumRows a) (.getNumCols b))
    (->> (CommonOps_DSCC/mult a b))))

(defn or-matrix ^DMatrixSparseCSC [^DMatrixSparseCSC a ^DMatrixSparseCSC b]
  (->> (multiply-matrix a b)
       (boolean-matrix)))

(defn multiply-elements-matrix ^DMatrixSparseCSC [^DMatrixSparseCSC a ^DMatrixSparseCSC b]
  (let [c (DMatrixSparseCSC. (max (.getNumRows a) (.getNumRows b))
                             (max (.getNumCols a) (.getNumCols b)))]
    (CommonOps_DSCC/elementMult a b c nil nil)
    c))

(defn add-elements-matrix ^DMatrixSparseCSC [^DMatrixSparseCSC a ^DMatrixSparseCSC b]
  (let [c (DMatrixSparseCSC. (max (.getNumRows a) (.getNumRows b))
                             (max (.getNumCols a) (.getNumCols b)))]
    (CommonOps_DSCC/add 1.0 a 1.0 b c nil nil)
    c))

(defn or-elements-matrix ^DMatrixSparseCSC [^DMatrixSparseCSC a ^DMatrixSparseCSC b]
  (->> (add-elements-matrix a b)
       (boolean-matrix)))

;; NOTE: these return row vectors, which they potentially shouldn't?
;; Though the result of this is always fed straight into a diagonal.
(defn matlab-any-matrix ^DMatrixRMaj [^DMatrixSparseCSC m]
  (CommonOps_DDRM/transpose (CommonOps_DSCC/maxCols m nil) nil))

(defn matlab-any-matrix-sparse ^DMatrixSparseCSC [^DMatrixSparseCSC m]
  (let [v (col-vector (.getNumRows m))]
    (doseq [^long x (->> (.col_idx m)
                         (map-indexed vector)
                         (remove (comp zero? second))
                         (partition-by second)
                         (map ffirst))]
      (.unsafe_set v (dec x) 0 1.0))
    v))

(defn diagonal-matrix ^DMatrixSparseCSC [^DMatrix v]
  (let [target-size (.getNumRows v)
        m (square-matrix target-size)]
    (doseq [i (range target-size)
            :let [x (.unsafe_get v i 0)]
            :when (not (zero? x))]
      (.unsafe_set m i i x))
    m))

;; GraphBLAS Tutorial https://github.com/tgmattso/GraphBLAS

(defn graph-blas-tutorial []
  (let [num-nodes 7
        graph (doto (square-matrix num-nodes)
                (.set 0 1 1.0)
                (.set 0 3 1.0)
                (.set 1 4 1.0)
                (.set 1 6 1.0)
                (.set 2 5 1.0)
                (.set 3 0 1.0)
                (.set 3 2 1.0)
                (.set 4 5 1.0)
                (.set 5 2 1.0)
                (.set 6 2 1.0)
                (.set 6 3 1.0)
                (.set 6 4 1.0))
        vec (doto (col-vector num-nodes)
              (.set 2 0 1.0))]
    (println "Exercise 3: Adjacency matrix")
    (println "Matrix: Graoh =")
    (.print graph)

    (println "Exercise 4: Matrix Vector Multiplication")
    (println "Vector: Target node =")
    (.print vec)
    (println "Vector: sources =")
    (.print (multiply-matrix graph vec))

    (println "Exercise 5: Matrix Vector Multiplication")
    (let [vec (doto (col-vector num-nodes)
                (.set 6 0 1.0))]
      (println "Vector: source node =")
      (.print vec)
      (println "Vector: neighbours =")
      (.print (multiply-matrix (transpose-matrix graph) vec)))

    (println "Exercise 7: Traverse the graph")
    (let [w (doto (col-vector num-nodes)
              (.set 0 0 1.0))]
      (println "Vector: wavefront(src) =")
      (.print w)
      (loop [w w
             n 0]
        (when (< n num-nodes)
          ;; TODO: This should really use or and not accumulate.
          (let [w (or-matrix (transpose-matrix graph) w)]
            (println "Vector: wavefront =")
            (.print w)
            (recur w (inc n))))))

    (println "Exercise 9: Avoid revisiting")
    (let [w (doto (col-vector num-nodes)
              (.set 0 0 1.0))
          v (col-vector num-nodes)]
      (println "Vector: wavefront(src) =")
      (.print w)
      (loop [v v
             w w
             n 0]
        (when (< n num-nodes)
          (let [v (or-elements-matrix v w)]
            (println "Vector: visited =")
            (.print v)
            (let [w (inverse-mask v (or-matrix (transpose-matrix graph) w))]
              (println "Vector: wavefront =")
              (.print w)
              (when-not (MatrixFeatures_DSCC/isZeros w 0)
                (recur v w (inc n))))))))

    (println "Exercise 10: level BFS")
    (let [w (doto (col-vector num-nodes)
              (.set 0 0 1.0))
          levels (col-vector num-nodes)]
      (println "Vector: wavefront(src) =")
      (.print w)
      (loop [levels levels
             w w
             lvl 1]
        (let [levels (assign-mask w levels lvl)]
          (println "Vector: levels =")
          (.print levels)
          (let [w (inverse-mask levels (or-matrix (transpose-matrix graph) w))]
            (println "Vector: wavefront =")
            (.print w)
            (when-not (MatrixFeatures_DSCC/isZeros w 0)
              (recur levels w (inc lvl)))))))))

(def ^:const example-data-artists-resource "crux/example-data-artists.nt")

(defn example-data-artists-with-matrix [{:keys [eid->matrix id->value value->id max-size] :as graph}]
  (println "== Data")
  (doseq [[k ^DMatrixSparseCSC v] eid->matrix]
    (prn k)
    (print-assigned-values graph v))

  (println "== Query")
  (let [ ;; ?x :rdf/label "The Potato Eaters" -- backwards, so
        ;; transposing adjacency matrix.
        potato-eaters-label (multiply-matrix
                             (new-constant-matix graph "The Potato Eaters" "Guernica")
                             (transpose-matrix (:http://www.w3.org/2000/01/rdf-schema#label eid->matrix)))
        ;; Create mask for subjects. A mask is a diagonal, like the
        ;; constant matrix. Done to "lift" the new left hand side into
        ;; "focus".
        potato-eaters-mask (->> potato-eaters-label ;; TODO: Why transpose here in MAGiQ paper?
                                (matlab-any-matrix)
                                (diagonal-matrix))
        ;; ?y :example/creatorOf ?x -- backwards, so transposing adjacency
        ;; matrix.
        creator-of (multiply-matrix
                    potato-eaters-mask
                    (transpose-matrix (:http://example.org/creatorOf eid->matrix)))
        ;; ?y :foaf/firstName ?z -- forwards, so no transpose of adjacency matrix.
        creator-of-fname (multiply-matrix
                          (->> creator-of
                               (matlab-any-matrix)
                               (diagonal-matrix))
                          (:http://xmlns.com/foaf/0.1/firstName eid->matrix))]
    (print-assigned-values graph potato-eaters-label)
    (print-assigned-values graph potato-eaters-mask)
    (print-assigned-values graph creator-of)
    (print-assigned-values graph creator-of-fname)))

(def ^:const lubm-triples-resource "lubm/University0_0.ntriples")

;; TODO: Doesn't work. Redo based on example-data-artists-with-matrix
;; example.
#_(defn lubm-query-2-with-matrix [{:keys [eid->matrix value->id id->value max-size]}]
    (let [m_01 (DMatrixSparseCSC. max-size max-size)
          _ (CommonOps_DSCC/mult
             (doto (DMatrixSparseCSC. max-size max-size)
               (.set (get value->id
                          (rdf/with-prefix {:ub "http://swat.cse.lehigh.edu/onto/univ-bench.owl#"}
                            :ub/GraduateStudent))
                     (get value->id
                          (rdf/with-prefix {:ub "http://swat.cse.lehigh.edu/onto/univ-bench.owl#"}
                            :ub/GraduateStudent))
                     1.0))
             (CommonOps_DSCC/transpose ^DMatrixSparseCSC (get eid->matrix (crux.rdf/with-prefix :rdf/type))
                                       nil
                                       nil)
             m_01)
          _ (doto m_01
              (.shrinkArrays)
              (.sortIndices nil))
          m_12 (DMatrixSparseCSC. max-size max-size)
          _ (CommonOps_DSCC/mult
             (let [any (CommonOps_DSCC/maxCols (CommonOps_DSCC/transpose m_01
                                                                         nil
                                                                         nil)

                                               nil)]
               (CommonOps_DSCC/diag (.data any)))
             (CommonOps_DSCC/transpose ^DMatrixSparseCSC (get eid->matrix (rdf/with-prefix {:ub "http://swat.cse.lehigh.edu/onto/univ-bench.owl#"}
                                                                            :ub/undergraduateDegreeFrom))
                                       nil
                                       nil)
             m_12)
          _ (doto m_12
              (.shrinkArrays)
              (.sortIndices nil))]

      ;; These are the undergraduateDegreeFrom triples for the
      ;; GarduateStudent. The filter doesn't work properly, lists everyone.
      (doseq [r (range (.getNumRows m_12))
              c (range (.getNumCols m_12))
              :when (.isAssigned m_12 r c)]
        (prn (get id->value r) (get id->value c)))

      ;; These are all the graduate students.
      #_(doseq [r (range (.getNumRows m_01))
                c (range (.getNumCols m_01))
                :when (.isAssigned m_01 r c)]
          (prn (get id->value r) (get id->value c)))
      m_12))

(def ^:const watdiv-triples-resource "watdiv/watdiv.10M.nt")

;; TODO: org.roaringbitmap.longlong.Roaring64NavigableMap don't seem
;; to be possible to back by buffers. Getting smaller ranges requires
;; some form of tree of id space compression.  Max possible is
;; 4294967294 but the larger the size, the slower the operations.
(def ^:const roaring-size (bit-shift-right Integer/MAX_VALUE 4))

(def ^org.roaringbitmap.BitmapDataProviderSupplier fast-rank-supplier
  (reify BitmapDataProviderSupplier
    (newEmpty [_]
      (FastRankRoaringBitmap.))))

(defn new-roaring-bitmap ^org.roaringbitmap.longlong.Roaring64NavigableMap []
  (Roaring64NavigableMap. fast-rank-supplier))

(defn roaring-bit->row ^long [^long b]
  (Long/divideUnsigned b roaring-size))

(defn roaring-bit->col ^long [^long b]
  (Long/remainderUnsigned b roaring-size))

(defn roaring-bit->coord [^long b]
  [(roaring-bit->row b)
   (roaring-bit->col b)])

(defn roaring-row->bit ^long [^long b]
  (unchecked-multiply roaring-size b))

(defn roaring-coord->bit
  (^long [[r ^long c]]
   (roaring-coord->bit r c))
  (^long [r ^long c]
   (unchecked-add c (roaring-row->bit r))))

(defn roaring-set-cell ^org.roaringbitmap.longlong.Roaring64NavigableMap [^Roaring64NavigableMap bs cell]
  (doto bs
    (.addLong (roaring-coord->bit cell))))

(defn roaring-set-row ^org.roaringbitmap.longlong.Roaring64NavigableMap [^Roaring64NavigableMap bs ^long r]
  (let [start-bit (roaring-row->bit r)
        end-bit (unchecked-add start-bit roaring-size)]
    (doto bs
      ;; NOTE: Seems to be a bug if there are only ranges when invoking and.
      (.addLong start-bit)
      (.addLong (unchecked-dec end-bit))
      (.add start-bit end-bit)
      (.runOptimize))))

(defn roaring-and
  (^org.roaringbitmap.longlong.Roaring64NavigableMap [a] a)
  (^org.roaringbitmap.longlong.Roaring64NavigableMap [^Roaring64NavigableMap a ^Roaring64NavigableMap b]
   (doto a
     (.and b))))

(defn roaring-or
  (^org.roaringbitmap.longlong.Roaring64NavigableMap [a] a)
  (^org.roaringbitmap.longlong.Roaring64NavigableMap [^Roaring64NavigableMap a ^Roaring64NavigableMap b]
   (doto a
     (.or b))))

(defn roaring-cardinality ^long [^Roaring64NavigableMap a]
  (if a
    (.getLongCardinality a)
    0))

(defn roaring-sparse-matrix ^org.roaringbitmap.longlong.Roaring64NavigableMap [cells]
  (reduce
   (fn [^Roaring64NavigableMap acc cell]
     (doto acc
       (.addLong (roaring-coord->bit cell))))
   (new-roaring-bitmap)
   cells))

(defn roaring-diagonal-matrix ^org.roaringbitmap.longlong.Roaring64NavigableMap [diag]
  (roaring-sparse-matrix (for [d diag]
                           [d d])))

(defn roaring-column-vector ^org.roaringbitmap.longlong.Roaring64NavigableMap [vs]
  (roaring-sparse-matrix (for [v vs]
                           [0 v])))

(defn roaring-seq [^Roaring64NavigableMap bs]
  (some->> bs
           (.iterator)
           (iterator-seq)))

(defn roaring-all-cells [^Roaring64NavigableMap bs]
  (let [acc (ArrayList. (roaring-cardinality bs))]
    (.forEach bs
              (reify LongConsumer
                (accept [_ v]
                  (.add acc (roaring-bit->coord v)))))
    (seq acc)))

(defn roaring-can-use-array? [^Roaring64NavigableMap bs]
  (let [cardinality (roaring-cardinality bs)]
    (and (pos? cardinality)
         (<= cardinality Integer/MAX_VALUE))))

(defn roaring-array-map ^org.roaringbitmap.longlong.Roaring64NavigableMap [^Roaring64NavigableMap bs ^clojure.lang.IFn$LL f]
  (if (roaring-can-use-array? bs)
    (let [ls (.toArray bs)]
      (loop [idx (unchecked-dec-int (alength ls))]
        (aset ls idx (.invokePrim f (aget ls idx)))
        (if (zero? idx)
          (Roaring64NavigableMap/bitmapOf (doto ls
                                            (Arrays/sort)))
          (recur (unchecked-dec-int idx)))))
    (let [acc (new-roaring-bitmap)]
      (.forEach bs
                (reify LongConsumer
                  (accept [_ v]
                    (.addLong acc (f v)))))
      acc)))

(defn roaring-transpose ^org.roaringbitmap.longlong.Roaring64NavigableMap [^Roaring64NavigableMap bs]
  (roaring-array-map bs (fn ^long [^long bit]
                          (roaring-coord->bit (roaring-bit->col bit)
                                              (roaring-bit->row bit)))))

(defn roaring-matlab-any ^org.roaringbitmap.longlong.Roaring64NavigableMap [^Roaring64NavigableMap bs]
  (roaring-array-map bs roaring-bit->col))

(defn roaring-matlab-any-transpose ^org.roaringbitmap.longlong.Roaring64NavigableMap [^Roaring64NavigableMap bs]
  (roaring-array-map bs roaring-bit->row))

(defn roaring-row
  (^org.roaringbitmap.longlong.Roaring64NavigableMap [^Roaring64NavigableMap bs ^long row]
   (roaring-row (new-roaring-bitmap) bs row))
  (^org.roaringbitmap.longlong.Roaring64NavigableMap [^Roaring64NavigableMap to ^Roaring64NavigableMap bs ^long row]
   (let [start-bit (roaring-row->bit row)
         end-bit (unchecked-add start-bit roaring-size)
         first (max (unchecked-dec (.rankLong bs start-bit)) 0)
         cardinality (roaring-cardinality bs)]
     (loop [i (int first)]
       (when (< i cardinality)
         (let [s (.select bs i)]
           (cond
             (< s start-bit)
             (recur (unchecked-inc-int i))

             (< s end-bit)
             (do (.addLong to s)
                 (recur (unchecked-inc-int i)))))))
     to)))

(defn roaring-mult-diag ^org.roaringbitmap.longlong.Roaring64NavigableMap [^Roaring64NavigableMap diag ^Roaring64NavigableMap bs]
  (cond (or (nil? bs)
            (.isEmpty bs))
        (new-roaring-bitmap)

        (nil? diag)
        (doto (new-roaring-bitmap)
          (.or bs))

        :else
        (let [acc (new-roaring-bitmap)]
          (.forEach diag
                    (reify LongConsumer
                      (accept [_ d]
                        (roaring-row acc bs d))))
          (doto acc
            (.runOptimize)))))

(defn load-rdf-into-roaring-graph [resource]
  (with-open [in (io/input-stream (io/resource resource))]
    (let [{:keys [value->id
                  p->so]}
          (->> (rdf/ntriples-seq in)
               (map rdf/rdf->clj)
               (reduce (fn [{:keys [value->id p->so]} [s p o]]
                         (let [value->id (reduce (fn [value->id v]
                                                   (update value->id v (fn [x]
                                                                         (or x (count value->id)))))
                                                 value->id
                                                 [s p o])]
                           (assert (< (count value->id) roaring-size))
                           {:p->so (update p->so
                                           p
                                           (fn [x]
                                             (let [s-id (long (get value->id s))
                                                   o-id (long (get value->id o))
                                                   ^Roaring64NavigableMap bs (or x (new-roaring-bitmap))
                                                   bit (roaring-coord->bit s-id o-id)]
                                               (doto bs
                                                 (.addLong bit)))))
                            :value->id value->id}))))
          max-size (round-to-power-of-two (count value->id) 64)
          p->so (->> (for [[k v] p->so]
                       [k (doto ^Roaring64NavigableMap v
                            (.runOptimize))])
                     (into {}))]
      {:p->so p->so
       :p->os (->> (for [[k v] p->so]
                     [k (roaring-transpose v)])
                   (into {}))
       :value->id value->id
       :id->value (set/map-invert value->id)
       :max-size max-size})))

(defn roaring-literal-e [{:keys [value->id p->so]} a e]
  (-> (roaring-column-vector (keep value->id [e]))
      (roaring-mult-diag (p->so a))
      (roaring-matlab-any)))

(defn roaring-literal-v [{:keys [value->id p->os]} a v]
  (-> (roaring-column-vector (keep value->id [v]))
      (roaring-mult-diag (p->os a))
      (roaring-matlab-any)))

(defn roaring-join [{:keys [p->os p->so] :as graph} a ^Roaring64NavigableMap mask-e ^Roaring64NavigableMap mask-v]
  (let [result (roaring-mult-diag
                mask-e
                (roaring-transpose
                 (roaring-mult-diag
                  mask-v
                  (p->os a))))]
    [result
     (roaring-matlab-any-transpose result)
     (roaring-matlab-any result)]))

(def logic-var? symbol?)
(def literal? (complement logic-var?))

(defn roaring-query [{:keys [value->id id->value p->so p->os] :as graph} q]
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
                                          (sort-by (comp roaring-cardinality p->so :a))
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
                            {v (roaring-literal-e graph a e)})
                          (when (literal? v)
                            {e (roaring-literal-v graph a v)})))
                       (apply merge-with roaring-and))
        initial-result (->> (for [[v bs] var->mask]
                              {v {v (roaring-diagonal-matrix (roaring-seq bs))}})
                            (apply merge-with merge))]
    (loop [idx 0
           var->mask var->mask
           result initial-result]
      (if-let [{:keys [e a v] :as clause} (get clauses-in-join-order idx)]
        (let [[join-result mask-e mask-v] (roaring-join
                                           graph
                                           a
                                           (get var->mask e)
                                           (get var->mask v))
              join-result (if (= e v)
                            (roaring-and join-result (roaring-transpose join-result))
                            join-result)]
          (recur (inc idx)
                 (assoc var->mask e mask-e v mask-v)
                 (update-in result [e v] (fn [bs]
                                           (if bs
                                             (roaring-and bs join-result)
                                             join-result)))))
        (let [root-var (first var-access-order)
              seed (->> (concat
                         (for [[v bs] (get result root-var)]
                           (roaring-matlab-any-transpose bs))
                         (for [[e vs] result
                               [v bs] vs
                               :when (= v root-var)]
                           (roaring-matlab-any bs)))
                        (reduce roaring-and))
              var-result-order (mapv (zipmap var-access-order (range)) find)
              transpose-memo (memoize roaring-transpose)]
          (->> ((fn step [^Roaring64NavigableMap xs [var & var-access-order] parent-vars ctx]
                  (when (pos? (roaring-cardinality xs))
                    (let [acc (ArrayList.)
                          parent-var->plan (when var
                                             (->> (for [p parent-vars
                                                        :let [a (get-in result [p var])
                                                              b (when-let [b (get-in result [var p])]
                                                                  (transpose-memo b))
                                                              plan (if (and a b)
                                                                     (roaring-and a b)
                                                                     (or a b))]
                                                        :when plan]
                                                    [p plan])
                                                  (into {})))
                          parent-var (last parent-vars)]
                      (.forEach xs
                                (reify LongConsumer
                                  (accept [_ x]
                                    (if-not var
                                      (.add acc [x])
                                      (when-let [access (seq (for [[p plan] parent-var->plan]
                                                               (if (= parent-var p)
                                                                 (roaring-row plan x)
                                                                 (some->> (get ctx p) (roaring-row plan)))))]
                                        (doseq [y (step (->> access
                                                             (remove nil?)
                                                             (map roaring-matlab-any)
                                                             (reduce roaring-and))
                                                        var-access-order
                                                        (conj parent-vars var)
                                                        (assoc ctx (last parent-vars) x))]
                                          (.add acc (cons x y))))))))
                      (seq acc))))
                seed
                (next var-access-order)
                [root-var]
                {})
               (map #(->> var-result-order
                          (mapv (comp
                                 id->value
                                 (vec %)))))
               (into #{})))))))

(comment
  (matrix/roaring-query
   lg
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
     :http://www.Department0.University0.edu/GraduateCourse52]})

;; Bitmap-per-Row version, about 10x faster than single large
;; Roaring64NavigableMap except for sparse-matrix and transpose, which
;; are up to 10x slower.

(comment

  (def c (read-string (slurp "test/watdiv/watdiv_crux.edn")))
  (def wg-r (matrix/load-rdf-into-r-graph matrix/watdiv-triples-resource))

  (let [total (atom 0)
        qs (remove :crux-error c)]
    (doseq [{:keys [idx query crux-results crux-time]} qs
            :when (not (contains? #{35 67} idx))]
      (let [start (System/currentTimeMillis)
            result (count (matrix/r-query wg-r (crux.sparql/sparql->datalog query)))]
        (try
          (assert (= crux-results result) (pr-str [crux-results result]))
          (catch Throwable t
            (prn t)))
        (let [t (- (System/currentTimeMillis) start)]
          (swap! total + t))))
    (prn @total (/ @total (double (count qs))))))

;; NOTE: toString on Int2ObjectHashMap seems to be broken. entrySet
;; also seems to be behaving unexpectedly (returns duplicated keys),
;; at least when using RoaringBitmaps as values.
(defn new-r-bitmap
  (^Int2ObjectHashMap []
   (Int2ObjectHashMap.))
  (^Int2ObjectHashMap [^long size]
   (Int2ObjectHashMap. (Math/ceil (/ size 0.9)) 0.9)))

(defn new-r-roaring-bitmap ^org.roaringbitmap.buffer.MutableRoaringBitmap []
  (MutableRoaringBitmap.))

(defn r-and
  ([a] a)
  ([^Int2ObjectHashMap a ^Int2ObjectHashMap b]
   (let [ks (doto (HashSet. (.keySet a))
              (.retainAll (.keySet b)))]
     (reduce (fn [^Int2ObjectHashMap m k]
               (let [k (int k)]
                 (doto m
                   (.put k (ImmutableRoaringBitmap/and (.get a k) (.get b k))))))
             (new-r-bitmap (count ks))
             ks))))

(defn r-or
  ([a] a)
  ([^Int2ObjectHashMap a ^Int2ObjectHashMap b]
   (let [ks (doto (HashSet. (.keySet a))
              (.retainAll (.keySet b)))]
     (reduce (fn [^Int2ObjectHashMap m k]
               (let [k (int k)]
                 (doto m
                   (.put k (ImmutableRoaringBitmap/or (.get a k) (.get b k))))))
             (new-r-bitmap (count ks))
             ks))))

(defn r-roaring-and
  ([a] a)
  ([^ImmutableRoaringBitmap a ^ImmutableRoaringBitmap b]
   (ImmutableRoaringBitmap/and a b)))

(defn r-cardinality ^long [^Int2ObjectHashMap a]
  (->> (.values a)
       (map #(.getCardinality ^ImmutableBitmapDataProvider %))
       (reduce +)))

(def ^Map pred-cardinality-cache (HashMap.))

(defn r-pred-cardinality [a attr]
  (.computeIfAbsent pred-cardinality-cache
                    attr
                    (reify Function
                      (apply [_ k]
                        (r-cardinality a)))))

(declare r-column-vector)

(defn r-sparse-matrix [cells]
  (let [rows (->> (sort cells)
                  (partition-by (fn [[row]]
                                  row)))]
    (reduce
     (fn [^Int2ObjectHashMap m [[[row]] :as cells]]
       (reduce
        (fn [^MutableRoaringBitmap x [_ col]]
          (doto x
            (.add (int col))))
        (.computeIfAbsent m
                          (int row)
                          (reify IntFunction
                            (apply [_ k]
                              (new-r-roaring-bitmap))))
        cells)
       m)
     (new-r-bitmap (count rows))
     rows)))

(defn r-diagonal-matrix [diag]
  (reduce (fn [^Int2ObjectHashMap m d]
            (doto m
              (.put (int d) (doto (new-r-roaring-bitmap)
                              (.add (int d))))))
          (new-r-bitmap (count diag))
          diag))

(defn r-column-vector ^org.roaringbitmap.buffer.ImmutableRoaringBitmap [vs]
  (reduce
   (fn [^MutableRoaringBitmap acc v]
     (doto acc
       (.add (int v))))
   (new-r-roaring-bitmap)
   vs))

(defn r-all-cells [^Int2ObjectHashMap a]
  (reduce (fn [acc row]
            (into acc (->> (.toArray ^ImmutableRoaringBitmap (.get a (int row)))
                           (mapv #(vector row %)))))
          [] (.keySet a)))

(defn r-transpose [^Int2ObjectHashMap a]
  (reduce
   (fn [^Int2ObjectHashMap m row]
     (let [row (int row)
           cells ^ImmutableRoaringBitmap (.get a row)]
       (.forEach cells
                 (reify IntConsumer
                   (accept [_ v]
                     (let [^MutableRoaringBitmap x (.computeIfAbsent m
                                                                     v
                                                                     (reify IntFunction
                                                                       (apply [_ k]
                                                                         (new-r-roaring-bitmap))))]
                       (.add x row)))))
       m))
   (new-r-bitmap (count a))
   (.keySet a)))

(defn r-matlab-any ^org.roaringbitmap.buffer.ImmutableRoaringBitmap [^Int2ObjectHashMap a]
  (if (.isEmpty a)
    (new-r-roaring-bitmap)
    (ImmutableRoaringBitmap/or (.iterator (.values a)))))

(defn r-matlab-any-transpose ^org.roaringbitmap.buffer.ImmutableRoaringBitmap [^Int2ObjectHashMap a]
  (if (.isEmpty a)
    (new-r-roaring-bitmap)
    (r-column-vector (.keySet a))))

(defn r-row ^org.roaringbitmap.buffer.ImmutableRoaringBitmap [^Int2ObjectHashMap a ^long row]
  (if-let [b (.get a (int row))]
    b
    (new-r-roaring-bitmap)))

(defn r-mult-diag [^ImmutableRoaringBitmap diag ^Int2ObjectHashMap a]
  (cond (.isEmpty a)
        (new-r-bitmap)

        (nil? diag)
        a

        :else
        (reduce
         (fn [^Int2ObjectHashMap m d]
           (let [d (int d)]
             (if-let [b (.get a d)]
               (doto m
                 (.put d b))
               m)))
         (new-r-bitmap (.getCardinality diag))
         (.toArray diag))))

(defn r-literal-e [{:keys [value->id p->so]} a e]
  (-> (r-column-vector (keep value->id [e]))
      (r-mult-diag (p->so a))
      (r-matlab-any)))

(defn r-literal-v [{:keys [value->id p->os]} a v]
  (-> (r-column-vector (keep value->id [v]))
      (r-mult-diag (p->os a))
      (r-matlab-any)))

(defn r-join [{:keys [p->os p->so] :as graph} a ^ImmutableRoaringBitmap mask-e ^ImmutableRoaringBitmap mask-v]
  (let [result (r-mult-diag
                mask-e
                (p->so a) ;; TODO: is the transpose worth it?
                #_(r-transpose
                   (r-mult-diag
                    mask-v
                    (p->os a))))]
    [result
     (r-matlab-any-transpose result)
     (r-matlab-any result)]))

(def ^Map r-query-cache (HashMap.))

(defn r-compile-query [{:keys [value->id p->so] :as graph} q]
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
                                                     (r-pred-cardinality (get p->so a) a)))
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
                            {v (r-literal-e graph a e)})
                          (when (literal? v)
                            {e (r-literal-v graph a v)})))
                       (apply merge-with r-and))
        initial-result (->> (for [[v bs] var->mask]
                              {v {v (r-diagonal-matrix (.toArray ^ImmutableRoaringBitmap bs))}})
                            (apply merge-with merge))
        var-result-order (mapv (zipmap var-access-order (range)) find)]
    [var->mask initial-result clauses-in-join-order var-access-order var-result-order]))

(defn r-query [{:keys [^Int2ObjectHashMap id->value] :as graph} q]
  (let [[var->mask
         initial-result
         clauses-in-join-order
         var-access-order
         var-result-order] (.computeIfAbsent r-query-cache
                                             [(System/identityHashCode graph) q]
                                             (reify Function
                                               (apply [_ k]
                                                 (r-compile-query graph q))))]
    (loop [idx 0
           var->mask var->mask
           result initial-result]
      (if-let [{:keys [e a v] :as clause} (get clauses-in-join-order idx)]
        (let [[join-result mask-e mask-v] (r-join
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
                                               (r-and bs join-result)
                                               join-result))))))
        (let [[root-var] var-access-order
              seed (->> (concat
                         (for [[v bs] (get result root-var)]
                           (r-matlab-any-transpose bs))
                         (for [[e vs] result
                               [v bs] vs
                               :when (= v root-var)]
                           (r-matlab-any bs)))
                        (reduce r-roaring-and))
              transpose-cache (IdentityHashMap.)]
          (->> ((fn step [^ImmutableRoaringBitmap xs [var & var-access-order] parent-vars ^Map ctx]
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
                                                                               (cond-> (r-transpose b)
                                                                                 a (r-and a))))))
                                                     plan (or b a)]
                                                 (cond-> acc
                                                   plan (.add [p plan]))
                                                 acc))
                                             (ArrayList.)
                                             parent-vars))
                          parent-var (last parent-vars)]
                      (.forEach xs
                                (reify IntConsumer
                                  (accept [_ x]
                                    (if-not var
                                      (.add acc [(.get id->value x)])
                                      (when-let [xs (reduce
                                                     (fn [^ImmutableRoaringBitmap acc [p plan]]
                                                       (let [xs (if (= parent-var p)
                                                                  (r-row plan x)
                                                                  (some->> (.get ctx p) (r-row plan)))]
                                                         (if acc
                                                           (r-roaring-and acc xs)
                                                           xs)))
                                                     nil
                                                     parent-var+plan)]
                                        (doseq [y (step xs
                                                        var-access-order
                                                        (conj parent-vars var)
                                                        (doto ctx
                                                          (.put (last parent-vars) x)))]
                                          (.add acc (cons (.get id->value x) y))))))))
                      (seq acc))))
                seed
                (next var-access-order)
                [root-var]
                (IdentityHashMap.))
               (map #(mapv (vec %) var-result-order))
               (into #{})))))))

(defn load-rdf-into-r-graph [resource]
  (with-open [in (io/input-stream (io/resource resource))]
    (let [{:keys [value->id
                  p->so]}
          (->> (rdf/ntriples-seq in)
               (map rdf/rdf->clj)
               (reduce (fn [{:keys [value->id p->so]} [s p o]]
                         (let [value->id (reduce (fn [value->id v]
                                                   (update value->id v (fn [x]
                                                                         (or x (count value->id)))))
                                                 value->id
                                                 [s p o])]
                           (assert (< (count value->id) roaring-size))
                           {:p->so (update p->so
                                           p
                                           (fn [^Int2ObjectHashMap m]
                                             (let [m (or m (new-r-bitmap))
                                                   s-id (int (get value->id s))
                                                   o-id (int (get value->id o))
                                                   ^MutableRoaringBitmap bs (.computeIfAbsent m s-id
                                                                                              (reify IntFunction
                                                                                                (apply [_ k]
                                                                                                  (new-r-roaring-bitmap))))]
                                               (doto bs
                                                 (.add o-id))
                                               m)))
                            :value->id value->id}))))
          max-size (round-to-power-of-two (count value->id) 64)
          ->immutable-bitmap (fn [^MutableRoaringBitmap x]
                               (let [eb (ExpandableDirectByteBuffer. (.serializedSizeInBytes x))]
                                 (with-open [out (DataOutputStream. (ExpandableDirectBufferOutputStream. eb))]
                                   (.serialize (doto x
                                                 (.runOptimize)) out))
                                 (ImmutableRoaringBitmap. (.byteBuffer eb))))
          ->immutable-matrix (fn [^Int2ObjectHashMap m]
                               (doseq [k (.keySet m)]
                                 (.put m (int k) (->immutable-bitmap (.get m (int k)))))
                               m)]
      {:p->os (->> (for [[k v] p->so]
                     [k (->immutable-matrix (r-transpose v))])
                   (into {}))
       :p->so (->> (for [[k v] p->so]
                     [k (->immutable-matrix v)])
                   (into {}))
       :value->id value->id
       :id->value (doto (Int2ObjectHashMap.)
                    (.putAll (set/map-invert value->id)))
       :max-size max-size})))

;; Other Matrix-format related spikes:

(defn mat-mul [^doubles a ^doubles b]
  (assert (= (alength a) (alength b)))
  (let [size (alength a)
        n (bit-shift-right size 1)
        c (double-array size)]
    (dotimes [i n]
      (dotimes [j n]
        (let [row-idx (* n i)
              c-idx (+ row-idx j)]
          (dotimes [k n]
            (aset c
                  c-idx
                  (+ (aget c c-idx)
                     (* (aget a (+ row-idx k))
                        (aget b (+ (* n k) j)))))))))
    c))

(defn vec-mul [^doubles a ^doubles x]
  (let [size (alength a)
        n (bit-shift-right size 1)
        y (double-array n)]
    (assert (= n (alength x)))
    (dotimes [i n]
      (dotimes [j n]
        (aset y
              i
              (+ (aget y i)
                 (* (aget a (+ (* n i) j))
                    (aget x j))))))
    y))

;; CSR
;; https://people.eecs.berkeley.edu/~aydin/GALLA-sparse.pdf

{:nrows 6
 :row-ptr (int-array [0 2 5 6 9 12 16])
 :col-ind (int-array [0 1
                      1 3 5
                      2
                      2 4 5
                      0 3 4
                      0 2 3 5])
 :values (double-array [5.4 1.1
                        6.3 7.7 8.8
                        1.1
                        2.9 3.7 2.9
                        9.0 1.1 4.5
                        1.1 2.9 3.7 1.1])}

;; DCSC Example
{:nrows 8
 :jc (int-array [1 7 8])
 :col-ptr (int-array [1 3 4 5])
 :row-ind (int-array [6 8 4 5])
 :values (double-array [0.1 0.2 0.3 0.4])}

(defn vec-csr-mul [{:keys [^long nrows ^ints row-ptr ^ints col-ind ^doubles values] :as a} ^doubles x]
  (let [n nrows
        y (double-array n)]
    (assert (= n (alength x)))
    (dotimes [i n]
      (loop [j (aget row-ptr i)
             yr 0.0]
        (if (< j (aget row-ptr (inc i)))
          (recur (inc i)
                 (+ yr
                    (* (aget values j)
                       (aget x (aget col-ind j)))))
          (aset y i yr))))
    y))
