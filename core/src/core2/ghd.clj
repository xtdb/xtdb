(ns core2.ghd
  (:require [clojure.set :as set]
            [clojure.tools.logging :as log]))

;; "A Backtracking-Based Algorithm for Computing Hypertree-Decompositions"
;; https://arxiv.org/abs/cs/0701083

;; C++ version: https://github.com/daajoe/detkdecomp

;; The usage of this can eventually be either to augment or replace
;; the current planner in Crux classic, and/or to be the base for a
;; new core2 planner.

;; In Crux classic, one additional motivation for this work is to be
;; able to implement sub-tree caching in our current engine, which
;; doesn't strictly require replacing its current planner.


;;;; TODO:

;; Sanity check, adder-15 doesn't return correct width (should be 2).

;; Calculating, but then not returning some widths is likely hiding a
;; bug.

;; Expand is assumed to be possible to replace with a cache, is this
;; really the case?

;; We probably want Fractional Hypertree Decompositions, see
;; EmptyHeaded for example (decomposition is based on the AJAR paper):
;; https://github.com/HazyResearch/EmptyHeaded
;; http://arxiv.org/abs/1503.02368

;; Investigate https://github.com/cem-okulmus/BalancedGo which a later
;; heuristic from the same group, and is based on CSP, we could use
;; core.async. Also contains a Go implementation of det-k-decomp,
;; which is what's implemented here.


;; Lambda: edges, "edge cover" (relations)
;; Chi: vertices, "bag" (variables).
(defrecord HTree [lambda chi subtrees])

(defrecord HGraph [edge->vertices vertice->edges])

(defn- invert-edges->vertices [edge->vertices]
  (reduce
   (fn [acc [k vs]]
     (reduce
      (fn [acc v]
        (update acc v (fnil conj (sorted-set)) k))
      acc
      vs))
   (sorted-map)
   edge->vertices))

(defn separate [{:keys [edge->vertices] :as ^HGraph h} edges separator]
  (let [separator-vertices (mapcat edge->vertices separator)
        edge->vertices (select-keys edge->vertices edges)
        vertice->edges (apply dissoc
                              (invert-edges->vertices edge->vertices)
                              separator-vertices)
        edges (set/difference edges separator)]
    (if-let [vertice->edges (not-empty vertice->edges)]
      (let [component (loop [acc nil]
                        (let [new-acc (reduce
                                       (fn [acc new-edges]
                                         (if acc
                                           (if (not-empty (set/intersection acc new-edges))
                                             (set/union acc new-edges)
                                             acc)
                                           new-edges))
                                       acc
                                       (vals vertice->edges))]
                          (if (= new-acc acc)
                            acc
                            (recur new-acc))))]
        (cons component (separate h edges component)))
      (when (not-empty edges)
        (list edges)))))

(defn cover [{:keys [vertice->edges edge->vertices] :as ^HGraph h} vertices]
  (let [bound-edges (mapcat val (select-keys vertice->edges vertices))
        edge-weights (frequencies bound-edges)
        edge-order (mapv first (sort-by second > (sort-by first edge-weights)))
        separators (for [n (range (count edge-order))
                         :let [initial-edge (nth edge-order n)
                               initial-cover (set/intersection vertices (set (get edge->vertices initial-edge)))]
                         :when (not-empty initial-cover)
                         separator (last
                                    (reduce
                                     (fn [[current-cover current-edges acc] m]
                                       (let [edge (nth edge-order m)
                                             new-vertices (set (get edge->vertices edge))
                                             new-cover (set/union current-cover (set/intersection vertices new-vertices))
                                             new-edges (cond-> current-edges
                                                         (not= new-cover current-cover)
                                                         (conj edge))]
                                         (if (= new-cover vertices)
                                           [initial-cover (sorted-set initial-edge) (conj acc new-edges)]
                                           [new-cover new-edges acc])))
                                     [initial-cover (sorted-set initial-edge) []]
                                     (range (inc n) (count edge-order))))]
                     separator)]
    (for [separator (distinct separators)
          :when (not (some #(set/superset? separator %) (remove #{separator} separators)))]
      separator)))

;; NOTE: this is supposed to expand pruned sub-trees, see paper, idea
;; is that we avoid this by making succ-seps a cache and not just a
;; marker.
(defn- expand [^HTree ht]
  ht)

(defn htree->tree-seq [^HTree ht]
  (tree-seq (partial instance? HTree) :subtrees ht))

(defn htree-decomp-width [^HTree ht]
  (->> (htree->tree-seq ht)
       (map (comp count :lambda))
       (reduce max 0)))

(defn htree-decomp-depth [^HTree ht]
  (->> (map htree-decomp-depth (:subtrees ht))
       (reduce max 0)
       (inc)))

(defn htree-join-order [^HTree ht]
  (vec (distinct (mapcat :chi (htree->tree-seq ht)))))

(defn htree-complete? [^HTree ht]
  (= (set (keys (:edge->vertices (meta ht))))
     (set (mapcat :lambda (htree->tree-seq ht)))))

(defn htree-normalize [^HTree ht]
  (let [removed-edges (atom #{})]
    (-> ht
        (update :subtrees (fn [subtrees]
                            (->> (for [subtree subtrees
                                       :let [subtree (htree-normalize subtree)]]
                                   (if (set/subset? (:chi subtree) (:chi ht))
                                     (do (swap! removed-edges set/union (:lambda subtree))
                                         (:subtrees subtree))
                                     [subtree]))
                                 (reduce into []))))
        (update :lambda set/union @removed-edges))))

(defn- constraints->edge-vertices [constraints]
  (->> (for [[relation & vars] constraints]
         [relation (vec vars)])
       (into (sorted-map))))

(defn ->hgraph [constraints]
  (let [edge->vertices (constraints->edge-vertices constraints)]
    (->HGraph edge->vertices (invert-edges->vertices edge->vertices))))

(defn- all-vertices [{:keys [edge->vertices] :as ^HGraph h} edges]
  (into (sorted-set) (mapcat edge->vertices edges)))

(def ^:private ^:dynamic *backtrack-context*)

(declare decomp-cov)

(defn- decomp-sub [{:keys [edge->vertices] :as ^HGraph h} k components separator]
  (log/debug :decomp-sub components separator)
  (reduce
   (fn [acc component]
     (let [child-conn (set/intersection (all-vertices h component) (all-vertices h separator))]
       (if-let [ht (get (:succ-seps @*backtrack-context*) [separator component])]
         (conj acc ht)
         (if-let [ht (decomp-cov h k component child-conn)]
           (do (swap! *backtrack-context* update :succ-seps assoc [separator component] ht)
               (conj acc ht))
           (do (swap! *backtrack-context* update :fail-seps conj [separator component])
               (reduced nil))))))
   []
   components))

(defn- decomp-add [{:keys [edge->vertices] :as ^HGraph h} k edges conn cov-sep]
  (let [in-cov-sep (set/intersection cov-sep edges)]
    (when (or (not-empty in-cov-sep) (< (count cov-sep) k))
      (log/debug :decomp-add edges conn cov-sep)
      (reduce
       (fn [acc add-sep]
         (let [separator (set/union cov-sep add-sep)
               components (separate h edges separator)]
           (when (empty? (set/intersection
                          (:fail-seps @*backtrack-context*)
                          (set (for [component components]
                                 [separator component]))))
             (when-let [subtrees (not-empty (decomp-sub h k components separator))]
               (let [chi (set/union conn (all-vertices h (set/union in-cov-sep add-sep)))]
                 (reduced (with-meta (->HTree separator chi subtrees) h)))))))
       nil
       (if (empty? in-cov-sep)
         (map sorted-set edges)
         [#{}])))))

(defn- decomp-cov [{:keys [edge->vertices] :as ^HGraph h} k edges conn]
  (log/debug :decomp-cov edges conn)
  (if (<= (count edges) k)
    (with-meta (->HTree edges (all-vertices h edges) []) h)
    (reduce
     (fn [acc cov-sep]
       (when-let [ht (decomp-add h k edges conn cov-sep)]
         (reduced ht)))
     nil
     (or (not-empty (cover h conn))
         [#{}]))))

(defn det-k-decomp [{:keys [edge->vertices
                            vertice->edges] :as ^HGraph h} k]
  (binding [*backtrack-context* (atom {:fail-seps #{}
                                       :succ-seps {}})]
    (let [edges (into (sorted-set) (keys edge->vertices))
          ht (expand (decomp-cov h k edges (sorted-set)))]
      ;; TODO: should not be necessary, likely hides a bug?
      (when (<= (htree-decomp-width ht) k)
        ht))))
