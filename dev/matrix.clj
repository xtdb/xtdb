(ns matrix
  (:require [clojure.java.io :as io]
            [clojure.core.matrix :as m]
            [clojure.set :as set]
            [crux.rdf :as rdf])
  (:import [org.ejml.data DMatrix DMatrixRMaj
            DMatrixSparse DMatrixSparseCSC]
           org.ejml.sparse.csc.CommonOps_DSCC
           org.ejml.generic.GenericMatrixOps_F64))

;; Matrix / GraphBLAS style breath first search
;; https://redislabs.com/redis-enterprise/technology/redisgraph/
;; MAGiQ http://www.vldb.org/pvldb/vol11/p1978-jamour.pdf
;; gSMat https://arxiv.org/pdf/1807.07691.pdf

(defn square-matrix ^DMatrixSparseCSC [size]
  (DMatrixSparseCSC. size size))

(defn sparse-vector ^DMatrixSparseCSC [size]
  (DMatrixSparseCSC. 1 size))

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
               (map rdf/statement->clj)
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

(defn multiply-matrix ^DMatrixSparseCSC [^DMatrixSparseCSC a ^DMatrixSparseCSC b]
  (let [target-size (max (.getNumRows a) (.getNumRows b))]
    (doto (square-matrix target-size)
      (->> (CommonOps_DSCC/mult a b)))))

(defn multiply-elements-matrix ^DMatrixSparseCSC [^DMatrixSparseCSC a ^DMatrixSparseCSC b]
  (let [target-size (max (.getNumRows a) (.getNumRows b))
        c (square-matrix target-size)]
    (CommonOps_DSCC/elementMult a b c nil nil)
    c))

(defn matlab-any-matrix ^DMatrixRMaj [^DMatrixSparseCSC m]
  (CommonOps_DSCC/maxCols m nil))

(defn matlab-any-matrix-sparse ^DMatrixSparseCSC [^DMatrixSparseCSC m]
  (let [v (sparse-vector (.getNumRows m))]
    (doseq [^long x (->> (.col_idx m)
                         (map-indexed vector)
                         (remove (comp zero? second))
                         (partition-by second)
                         (map ffirst))]
      (.unsafe_set v 0 (dec x) 1.0))
    v))

(defn diagonal-matrix ^DMatrixSparseCSC [^DMatrix v]
  (let [target-size (.getNumCols v)
        m (square-matrix target-size)]
    (doseq [i (range target-size)
            :let [x (.unsafe_get v 0 i)]
            :when (not (zero? x))]
      (.unsafe_set m i i x))
    m))

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

;; core.matrix stuff, will be deleted.

(def adjacency-matrix
  (m/matrix [[0 0 0 1 0 0 0]
             [1 0 0 0 0 0 0]
             [0 0 0 1 0 1 1]
             [1 0 0 0 0 0 1]
             [0 1 0 0 0 0 1]
             [0 0 1 0 1 0 0]
             [0 1 0 0 0 0 0]]))

;; Breath first search, looking for neighbours of elements 1 and 3 (0
;; and 2 in zero-based indexing).
(def bfs-mask (m/matrix [1 0 1 0 0 0 0]))

;; One hop.
(assert (= (m/matrix [0.0 1.0 0.0 1.0 0.0 1.0 0.0])
           (m/mmul adjacency-matrix bfs-mask)))

;; Two hops, value is number of in-edges.
(assert (= (m/matrix [1.0 0.0 2.0 0.0 1.0 0.0 1.0])
           (m/mmul adjacency-matrix adjacency-matrix bfs-mask)))

;; Breath first search, looking for neighbours of elements 1, 3 and 4
;; with individual results.
(def multiple-source-bfs-mask
  (m/matrix [[0 1 0]
             [0 0 0]
             [0 0 1]
             [1 0 0]
             [0 0 0]
             [0 0 0]
             [0 0 0]]))

(assert (= (m/matrix [[1.0 0.0 0.0]
                      [0.0 1.0 0.0]
                      [1.0 0.0 0.0]
                      [0.0 1.0 0.0]
                      [0.0 0.0 0.0]
                      [0.0 0.0 1.0]
                      [0.0 0.0 0.0]])
           (m/mmul adjacency-matrix multiple-source-bfs-mask)))

;; MAGiQ http://www.vldb.org/pvldb/vol11/p1978-jamour.pdf

(def magiq (m/matrix [[0 2 1 0 1]
                      [0 0 0 2 2]
                      [0 0 0 3 0]
                      [0 0 0 0 0]
                      [0 0 5 5 0]]))
;; a b c e = 1 2 3 5

;; SELECT ?x ?y ?z ?w WHERE {
;; ?x <a> ?y .
;; ?y <c> ?z .
;; ?x <b> ?w .
;; }

;; Mxy = I ∗ a ⊗ A
;; Myz = diag(any(M'xy)) ∗ c ⊗ A
;; Mxy = Mxy × diag(any(Myz))
;; Mxw = diag(any(Mxy)) ∗ b ⊗ A

;; "The first line selects the valid bindings of variables x and y
;; using predicate a from the RDF matrix A, and stores the results in
;; matrix Mxy. The second line uses the bindings of y and predicate c
;; to select the bindings of z.  The third line updates the bindings
;; of x and y to eliminate bindings invalidated by predicate
;; c. Finally, the fourth line uses the bindings of x in Mxy with
;; predicate b to select the valid bindings of w."

;; "The undirected version of the query graph is traversed in a
;; depth-first fashion"

;; "Forward edges are translated to RDF selection operations that
;; produce the binding matrix for the variables of the query
;; edge. Backward edges are translated to selection operations that
;; filter out invalid variable bindings"

;; Determine if any array elements are nonzero.
(defn matlab-any [m]
  (->> (m/columns m)
       (mapv #(if (some pos? %) 1.0 0.0))
       (m/matrix)))

(assert (= (m/matrix [0.0 0.0 1.0])
           (matlab-any [[0 0 3]
                        [0 0 3]
                        [0 0 3]])))

;; Creates a sparse matrix with matlab syntax, cols, rows, vals, rows,
;; cols
(defn matlab-sparse [i j v m n]
  (-> (m/zero-matrix m n)
      (m/set-indices (map vector i j)
                     (map double v))
      (m/sparse)))

(assert (= (m/matrix [[0.0 0.0 0.0]
                      [0.0 0.0 0.0]
                      [0.0 1.0 0.0]])
           (matlab-sparse [2] [1] [1] 3 3)))

;; This example is LUBM query 2 written with clause order.

;; SELECT ?x, ?y, ?z
;; WHERE
;; { ?z ub:subOrganizationOf ?y .
;;   ?z rdf:type ub:Department .
;;   ?x ub:memberOf ?z .
;;   ?x rdf:type ub:GraduateStudent .
;;   ?x ub:undergraduateDegreeFrom ?y
;;   ?y rdf:type ub:University .
;; }

;; This is a GUESS based on the Matlab translation in the
;; paper. Should be possible to play with in the REPL if one loads
;; some real LUBM data into matrix form. This is taken from the
;; screenshot on page 4. This also includes the graph form of the
;; query above and the order its walked.

;; G is a list of matrixes, one per predicate. N is dimension (I think).
;; [G, N] = load_rdf_graph('data/lubm2560.nt');

;; This creates a matrix with a single cell set, GradStud, and selects
;; the entries in G{type} with this value into M_01 (GradStud- > ?x).
;; GradStud and type are ids of predicates I think.
;; M_01 = sparse([GradStud], [GradStud], [1], N, N) * G{type}';

;; (def m_01 (m/mmul (matlab-sparse [GradStud] [GradStud] [1.0] n n)
;;                   (m/transpose (get g type))))

;; Transposes M_01 (') any returns one for every row with a column set
;; and creates a diagonal matrix to select elements from G{uGradFrom}
;; based on ?x into M_12 (?x -> ?y).
;; M_12 = diag(any(M_01')) * G{uGradFrom}';

;; (def m_12 (m/mmul (m/diagonal-matrix (matlab-any (m/transpose m_01)))
;;                   (m/transpose (get g uGradFrom))))

;; Transposes M_12 and selects like above, navigates from ?y to its types.
;; M_24 = diag(any(M_12')) * G{type};

;; (def m_24 (m/mmul (m/diagonal-matrix (matlab-any (m/transpose m_12)))
;;                   (get g type)))

;; Select the types with Univ. M_24 (?y -> Univ)
;; M_24 = M_24 * sparse([Univ], [Univ], [1], N, N);

;; (def m_24 (m/mmul m24 (matlab-sparse [Univ] [Univ] [1.0] n n)))

;; Navigates from ?y to subOrgOf to M_25 (?z -> ?y) as the navigation is
;; backwards there's no transpose (I think).
;; M_25 = diag(any(M_24)) * G{subOrgOf}';

;; (def m_25 (m/mmul (m/diagonal-matrix (matlab-any m_24))
;;                   (m/transpose (get g subOrgOf))))

;; Updates ?x -> ?y, I think to ensure it contains the same links to
;; ?y as M_25 (?z -> ?y).
;; M_12 = M_12 * diag(any(M_25));

;; (def m_12 (m/mmul m_12 (m/diagonal-matrix (matlab-any m_25))))

;; Navigate to M_13 (?x -> ?z).
;; M_13 = diag(any(M_12)) * G{memberOf};

;; (def m_13 (m/mmul (m/diagonal-matrix (matlab-any m_12))
;;                   (get g memberOf)))

;; Selects the ?z with type Dept into M_36 (?z -> Dept)
;; M_36 = diag(any(M_13')) * G{type};

;; (def m_36 (m/mmul (m/diagonal-matrix (matlab-any (m/transpose m_13)))
;;                   (get g type)))

;; M_36 = M_36 * sparse([Dept], [Dept], [1], N, N);

;; (def m_36 (m/mmul m36 (matlab-sparse [Dept] [Dept] [1.0] n n)))

;; Updates ?x -> ?z based on Dept to ensure they contain the same ?z.
;; M_13 = M_13 * diag(any(M_36));

;; (def m_13 (m/mmul m_13 (m/diagonal-matrix (matlab-any m_36))))

;; Updates GradStud -> ?x based on ?x -> ?z to ensure they contain the
;; same ?x.
;; M_01 = M_01 * diag(any(M_13));

;; (def m_01 (m/mmul m_01 (m/diagonal-matrix (matlab-any m_13))))

;; print_results({M_01, M_12, M_24, M_25, M_13, M_36});

;; (m/pm m_01)
;; (m/pm m_12)
;; (m/pm m_24)
;; (m/pm m_25)
;; (m/pm m_13)
;; (m/pm m_36)

;; Using EJML instead of core.matrix

(defn core-matrix->ejml-sparse-matrix ^DMatrixSparseCSC [vm]
  (let [[r c] (m/shape vm)
        m (DMatrixSparseCSC. r (or c 1))]
    (m/emap-indexed (fn [[r c] x]
                      (.unsafe_set m r (or c 0) x))
                    vm)
    m))
