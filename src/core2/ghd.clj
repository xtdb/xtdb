(ns core2.ghd
  (:require [clojure.set :as set])
  (:import [java.util Random]))

;; "A Backtracking-Based Algorithm for Computing Hypertree-Decompositions"
;; https://arxiv.org/abs/cs/0701083

;; Does not implement the backtracking part.

;; Lambda are the edges (relations) and chi the vertices (variables).
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
  (let [edge->vertices (select-keys edge->vertices edges)
        vertice->edges (apply dissoc
                              (invert-edges->vertices edge->vertices)
                              (mapcat edge->vertices separator))
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
        edge-order (mapv first (sort-by second > (sort-by first edge-weights)))]
    (for [n (range (count edge-order))
          :let [initial-edge (nth edge-order n)
                initial-cover (set/intersection vertices (set (get edge->vertices initial-edge)))]
          separator (last (reduce
                           (fn [[current-cover current-edges acc] m]
                             (let [edge (nth edge-order m)
                                   new-vertices (set (get edge->vertices edge))
                                   new-cover (set/union current-cover (set/intersection vertices new-vertices))
                                   new-edges (cond-> current-edges
                                               (not= new-cover current-cover)
                                               (conj edge))]
                               (if (= new-cover vertices)
                                 [initial-cover #{initial-edge} (conj acc new-edges)]
                                 [new-cover new-edges acc])))
                           [initial-cover #{initial-edge} []]
                           (range (inc n) (count edge-order))))]
      separator)))

(defn- guess-separator [{:keys [edge->vertices] :as ^HGraph h} k ^Random rng]
  (let [edges (vec (keys edge->vertices))]
    (repeatedly (fn []
                  (->> (repeatedly (inc (.nextInt rng k))
                                   #(nth edges (.nextInt rng (count edges))))
                       (into (sorted-set)))))))

(defn htree->tree-seq [^HTree ht]
  (tree-seq (partial instance? HTree) :subtrees ht))

(defn htree-decomp-width [^HTree ht]
  (->> (htree->tree-seq ht)
       (map (comp count :lambda))
       (reduce max 0)))

(defn htree-width [hts]
  (reduce min (map htree-decomp-width hts)))

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

(defn k-decomposable
  ([^HGraph h k]
   (k-decomposable h k (Random. 0)))
  ([{:keys [edge->vertices] :as ^HGraph h} k ^Random rng]
   (let [edges (into (sorted-set) (keys edge->vertices))]
     (k-decomposable h k rng edges (sorted-set))))
  ([{:keys [edge->vertices] :as ^HGraph h} k ^Random rng edges old-separator]
   (assert (and (pos? k) (<= k (inc (count edge->vertices)))))
   (let [connecting-vertices (set/intersection (all-vertices h edges)
                                               (all-vertices h old-separator))]
     (for [separator (guess-separator h k rng)
           :let [separator-edges-intersection (set/intersection separator edges)]
           :when (and (set/subset? connecting-vertices (all-vertices h separator))
                      (not-empty separator-edges-intersection))
           :let [subtrees (reduce
                           (fn [subtrees component]
                             (if-let [h-tree (first (k-decomposable h k rng component separator))]
                               (conj subtrees h-tree)
                               (reduced nil)))
                           []
                           (separate h edges separator))
                 chi (set/union connecting-vertices (all-vertices h separator-edges-intersection))]]
       (with-meta (->HTree separator chi subtrees) h)))))

(def ^:private ^:dynamic *backtrack-context*)

(declare decomp-cov)

(defn- decomp-sub [{:keys [edge->vertices] :as ^HGraph h} k components separator]
  (let [subtrees []]
    (reduce
     (fn [acc component]
       (let [child-conn (set/intersection (all-vertices h component) (all-vertices h separator))]
         (if (contains? (:succ-seps @*backtrack-context*) [separator child-conn])
           (conj acc (with-meta (->HTree component child-conn []) h))
           (if-let [ht (decomp-cov h k component child-conn)]
             (do (swap! *backtrack-context* update :succ-seps conj [separator child-conn])
                 (conj acc ht))
             (do (swap! *backtrack-context* update :fail-seps conj [separator child-conn])
                 (reduced nil))))))
     subtrees
     components)))

(defn- decomp-add [{:keys [edge->vertices] :as ^HGraph h} k edges conn cov-sep]
  (let [in-cov-sep (set/intersection cov-sep edges)]
    (when (or (not-empty in-cov-sep) (< (count cov-sep) k))
      (let [add-size (if (empty? in-cov-sep)
                       1
                       0)]
        (reduce
         (fn [acc add-sep]
           (let [separator (set/union cov-sep add-sep)
                 components (separate h edges separator)]
             (when (not-empty (set/intersection
                               (:fail-seps @*backtrack-context*)
                               (set (for [component components]
                                      [separator component]))))
               (when-let [subtrees (not-empty (decomp-sub h k components))]
                 (let [chi (set/union conn (all-vertices h (set/union in-cov-sep add-sep)))]
                   (reduced (with-meta (->HTree separator chi subtrees) h)))))))
         nil
         (if (= add-size 1)
           (map set edges)
           [#{}]))))))

(defn- decomp-cov [{:keys [edge->vertices] :as ^HGraph h} k edges conn]
  (if (<= (count edges) k)
    (with-meta (->HTree edges (all-vertices h edges) []) h)
    (reduce
     (fn [acc cov-sep]
       (when-let [ht (decomp-add h k edges conn cov-sep)]
         (reduced ht)))
     nil
     (cover h conn))))

(defn det-k-decomp [{:keys [edge->vertices] :as ^HGraph h} k]
  (let [edges (into (sorted-set) (keys edge->vertices))]
    (binding [*backtrack-context* (atom {:fail-seps #{}
                                         :succ-seps #{}})]
      (decomp-cov h k edges (sorted-set)))))
