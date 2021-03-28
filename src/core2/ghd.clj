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

(defn k-decomposable
  ([^HGraph h k]
   (k-decomposable h k (Random. 0)))
  ([{:keys [edge->vertices] :as ^HGraph h} k ^Random rng]
   (let [edges (into (sorted-set) (keys edge->vertices))]
     (k-decomposable h k rng edges (sorted-set))))
  ([{:keys [edge->vertices] :as ^HGraph h} k ^Random rng edges old-sep]
   (assert (and (pos? k) (<= k (count edge->vertices))))
   (for [separator (guess-separator h k rng)
         :when (and (set/subset? (set/intersection edges old-sep) separator)
                    (not-empty (set/intersection separator edges)))
         :let [subtrees (reduce
                         (fn [subtrees component]
                           (if-let [h-tree (first (k-decomposable h k rng component separator))]
                             (conj subtrees h-tree)
                             (reduced nil)))
                         []
                         (separate h edges separator))
               chi (->> (set/union (set/intersection edges old-sep)
                                   (set/intersection separator edges))
                        (mapcat edge->vertices)
                        (into (sorted-set)))]]
     (with-meta (->HTree separator chi subtrees) h))))
