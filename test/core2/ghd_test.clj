(ns core2.ghd-test
  (:require [clojure.test :as t]
            [clojure.set :as set])
  (:import [java.util Random]))

;; "A Backtracking-Based Algorithm for Computing Hypertree-Decompositions"
;; https://arxiv.org/abs/cs/0701083

;; Does not implement the backtracking part.

;; Lambda are the edges (relations) and chi the vertices (variables).
(defrecord HTree [lambda chi sub-trees])

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

(defn- separate [{:keys [edge->vertices] :as ^HGraph h} edges separator]
  (let [edge->vertices (select-keys edge->vertices edges)
        vertice->edges (apply dissoc
                              (invert-edges->vertices edge->vertices)
                              (mapcat edge->vertices separator))
        edges (set/difference edges separator)]
    (if-let [vertice->edges (not-empty vertice->edges)]
      (let [comp (loop [acc nil]
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
        (cons comp (separate h edges comp)))
      (when (not-empty edges)
        (list edges)))))

(defn- guess-separator [{:keys [edge->vertices] :as ^HGraph h} k ^Random rng]
  (let [edges (vec (keys edge->vertices))]
    (repeatedly (fn []
                  (->> (repeatedly #(nth edges (.nextInt rng (count edges))))
                       (distinct)
                       (take (inc (.nextInt rng k)))
                       (into (sorted-set)))))))

(defn htree->tree-seq [^HTree ht]
  (tree-seq map? :sub-trees ht))

(defn htree-decomp-width [^HTree ht]
  (->> (htree->tree-seq ht)
       (map (comp count :lambda))
       (reduce max 0)))

(defn htree-width [hts]
  (reduce min (map htree-decomp-width hts)))

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
         :let [sub-trees (reduce
                          (fn [sub-trees comp]
                            (if-let [h-tree (first (k-decomposable h k rng comp separator))]
                              (conj sub-trees h-tree)
                              (reduced nil)))
                          #{}
                          (separate h edges separator))
               chi (->> (set/union (set/intersection edges old-sep)
                                   (set/intersection separator edges))
                        (map edge->vertices)
                        (reduce into (sorted-set)))]]
     (with-meta (->HTree separator chi sub-trees) h))))


;; NOTE: this is not necessarily correct, but need some test as its a
;; test ns.
(t/deftest can-separate-components
  (let [h (->hgraph '[[:A a b c]
                      [:B d e f]
                      [:C c d g]
                      [:D a f i]
                      [:E g i]
                      [:F b e h]
                      [:G e j]
                      [:H a h j]])]
    (t/is (= [#{:B :C :D :E :F :G :H}]
             (separate h #{:A :B :C :D :E :F :G :H} #{:A})))

    (t/is (= [#{:C :D :E} #{:F :G :H}]
             (separate h #{:A :B :C :D :E :F :G :H} #{:A :B})))

    (t/is (= [#{:E}]
             (separate h #{:C :D :E} #{:C :D})))

    (t/is (= [#{:F :H}]
             (separate h #{:F :G :H} #{:G})))))
