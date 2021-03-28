(ns core2.ghd-test
  (:require [clojure.test :as t]
            [clojure.set :as set])
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

(defn- separate [{:keys [edge->vertices] :as ^HGraph h} edges separator]
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

(def backtracking-paper-graph '[[:A a b c]
                                [:B d e f]
                                [:C c d g]
                                [:D a f i]
                                [:E g i]
                                [:F b e h]
                                [:G e j]
                                [:H a h j]])

(t/deftest can-separate-components
  (let [h (->hgraph backtracking-paper-graph)]
    (t/is (= [#{:B :C :D :E :F :G :H}]
             (separate h #{:A :B :C :D :E :F :G :H} #{:A})))

    (t/is (= [#{:C :D :E} #{:F :G :H}]
             (separate h #{:A :B :C :D :E :F :G :H} #{:A :B})))

    (t/is (= [#{:E}]
             (separate h #{:C :D :E} #{:C :D})))

    (t/is (= [#{:F :H}]
             (separate h #{:F :G :H} #{:G})))))

(t/deftest can-compute-join-order
  (let [h (->hgraph backtracking-paper-graph)]
    (t/is (= '[d e f j b c g h a i]
             (htree-join-order
              (first (k-decomposable h 2)))))))

(def adder-15 '[[:and15 TempHa15 I29 I30]
                [:xorA13 TempG13 S13 C12]
                [:or14 C14 TempHb14 TempHa14]
                [:init C0]
                [:xorA3 S3 C2 TempG3]
                [:or12 TempHb12 TempHa12 C12]
                [:andA4 C3 TempHb4 TempG4]
                [:xorA14 TempG14 S14 C13]
                [:and13 I26 TempHa13 I25]
                [:and9 TempHa9 I17 I18]
                [:or3 C3 TempHa3 TempHb3]
                [:xorA12 S12 C11 TempG12]
                [:xor11 I21 TempG11 I22]
                [:or7 TempHb7 C7 TempHa7]
                [:andA9 C8 TempHb9 TempG9]
                [:or5 TempHb5 TempHa5 C5]
                [:or15 C15 TempHb15 TempHa15]
                [:xorA5 TempG5 C4 S5]
                [:xor14 I27 I28 TempG14]
                [:xor3 I5 I6 TempG3]
                [:or11 C11 TempHa11 TempHb11]
                [:xorA9 TempG9 C8 S9]
                [:and6 TempHa6 I12 I11]
                [:andA6 TempG6 TempHb6 C5]
                [:xorA8 S8 C7 TempG8]
                [:and10 I19 I20 TempHa10]
                [:or13 TempHb13 TempHa13 C13]
                [:andA3 C2 TempHb3 TempG3]
                [:and3 TempHa3 I5 I6]
                [:andA7 TempHb7 C6 TempG7]
                [:xorA7 TempG7 C6 S7]
                [:and8 I16 TempHa8 I15]
                [:xorA15 C14 S15 TempG15]
                [:and5 I9 I10 TempHa5]
                [:or1 TempHa1 TempHb1 C1]
                [:or8 TempHa8 TempHb8 C8]
                [:xor12 I23 I24 TempG12]
                [:xor8 TempG8 I16 I15]
                [:andA13 TempG13 TempHb13 C12]
                [:or6 TempHb6 C6 TempHa6]
                [:xor6 TempG6 I12 I11]
                [:xorA10 TempG10 S10 C9]
                [:or4 C4 TempHa4 TempHb4]
                [:xorA1 C0 S1 TempG1]
                [:xorA4 C3 S4 TempG4]
                [:and12 I23 I24 TempHa12]
                [:andA5 TempG5 TempHb5 C4]
                [:and7 I14 I13 TempHa7]
                [:andA1 C0 TempG1 TempHb1]
                [:xorA6 S6 TempG6 C5]
                [:andA8 TempHb8 TempG8 C7]
                [:xor4 TempG4 I8 I7]
                [:and4 I8 I7 TempHa4]
                [:and11 I22 TempHa11 I21]
                [:andA11 TempHb11 C10 TempG11]
                [:xor1 TempG1 I2 I1]
                [:andA15 TempG15 TempHb15 C14]
                [:or9 TempHb9 TempHa9 C9]
                [:and1 TempHa1 I1 I2]
                [:xor2 I3 I4 TempG2]
                [:xor7 TempG7 I14 I13]
                [:and14 I28 TempHa14 I27]
                [:xor5 I10 I9 TempG5]
                [:xor9 I18 I17 TempG9]
                [:andA2 TempHb2 C1 TempG2]
                [:or10 C10 TempHb10 TempHa10]
                [:andA12 TempG12 C11 TempHb12]
                [:andA10 TempHb10 C9 TempG10]
                [:andA14 TempG14 C13 TempHb14]
                [:xorA11 C10 TempG11 S11]
                [:xor15 TempG15 I29 I30]
                [:xor10 I20 TempG10 I19]
                [:or2 TempHb2 TempHa2 C2]
                [:xorA2 C1 TempG2 S2]
                [:xor13 TempG13 I26 I25]
                [:and2 I4 I3 TempHa2]])

(t/deftest can-decompose-adder-15
  (t/is (= 106 (count (htree-join-order (first (k-decomposable (->hgraph adder-15) 2)))))))
