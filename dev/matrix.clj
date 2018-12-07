(ns matrix
  (:require [clojure.java.io :as io]
            [clojure.set :as set]
            [clojure.walk :as w]
            [crux.rdf :as rdf])
  (:import java.util.BitSet
           [org.ejml.data DMatrix DMatrixRMaj
            DMatrixSparse DMatrixSparseCSC]
           org.ejml.dense.row.CommonOps_DDRM
           [org.ejml.sparse.csc CommonOps_DSCC MatrixFeatures_DSCC]
           org.ejml.generic.GenericMatrixOps_F64))

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

;; Based on the example-data-artists-with-matrix, but using BitSet and
;; boolean operations. The idea is to treat the diagonal matrix
;; multiplications as or over ranges. Not necessarily efficient,
;; should use something like
;; https://github.com/RoaringBitmap/RoaringBitmap
(defn bitset-spike []
  (let [size 64
        bit->coord (fn [b]
                     [(quot b size)
                      (mod b size)])
        row->bit (fn ^long [b]
                   (* size b))
        coord->bit (fn ^long [[r c]]
                     (+ c (row->bit r)))
        set-cell (fn [^BitSet bs cell]
                   (doto bs
                     (.set (long (coord->bit cell)))))
        set-row (fn [^BitSet bs ^long r]
                  (doto bs
                    (.set (long (row->bit r))
                          (long (row->bit (inc r))))))
        sparse-matrix (fn [cells]
                        (reduce set-cell (BitSet.) cells))
        all-cells (fn [^BitSet bs]
                    ((fn step [i]
                       (let [i (.nextSetBit bs i)]
                         (when (not= -1 i)
                           (cons (bit->coord i)
                                 (lazy-seq (step (inc i))))))) 0))
        transpose (fn [bs]
                    (->> (all-cells bs)
                         (map reverse)
                         (sparse-matrix)))
        matlab-any (fn [bs]
                     (->> (all-cells bs)
                          (map second)
                          (distinct)))
        mult-diag (fn [diag ^BitSet bs-b]
                    (doto ^BitSet
                        (reduce (fn [bs-a v]
                                  (set-row bs-a v))
                                (BitSet.)
                                diag)
                        (.and bs-b)))

        value->id {:http://example.org/Picasso 0
                   "Pablo" 2
                   :http://example.org/guernica 6
                   :http://example.org/VanGogh 21
                   "Vincent" 23
                   :http://example.org/potatoEaters 27}
        id->value (set/map-invert value->id)

        ->ids #(w/postwalk-replace value->id %)
        <-ids #(w/postwalk-replace id->value %)

        p->so {:http://example.org/creatorOf
               (-> [[:http://example.org/Picasso :http://example.org/guernica]
                    [:http://example.org/VanGogh :http://example.org/potatoEaters]]
                   (->ids)
                   (sparse-matrix))
               :http://xmlns.com/foaf/0.1/firstName
               (->  [[:http://example.org/Picasso "Pablo"]
                     [:http://example.org/VanGogh "Vincent"]]
                    (->ids)
                    (sparse-matrix))}
        p->os (->> (for [[k v] p->so]
                     [k (transpose v)])
                   (into {}))]

    (-> (->ids #{:http://example.org/guernica
                 :http://example.org/potatoEaters})
        (mult-diag (p->os :http://example.org/creatorOf))
        (matlab-any)
        (mult-diag (p->so :http://xmlns.com/foaf/0.1/firstName))
        (all-cells)
        (<-ids)
        (->> (into {})))))
