(ns core2.rewrite
  (:require [clojure.string :as str]
            [clojure.walk :as w]
            [clojure.zip :as z]))

(set! *unchecked-math* :warn-on-boxed)

;; Zipper pattern matching

(defn- zip-next-skip-subtree [loc]
  (or (z/right loc)
      (loop [p loc]
        (when-let [p (z/up p)]
          (or (z/right p)
              (recur p))))))

(defn zip-match
  ([pattern-loc loc]
   (zip-match pattern-loc loc {}))
  ([pattern-loc loc acc]
   (loop [pattern-loc pattern-loc
          loc loc
          acc acc]
     (cond
       (or (nil? pattern-loc)
           (z/end? pattern-loc))
       acc

       (or (nil? loc)
           (z/end? loc))
       nil

       (and (z/branch? pattern-loc)
            (z/branch? loc))
       (when (= (count (z/children pattern-loc))
                (count (z/children loc)))
         (recur (z/down pattern-loc) (z/down loc) acc))

       :else
       (let [pattern-node (z/node pattern-loc)
             node (z/node loc)]
         (cond
           (= pattern-node node)
           (recur (zip-next-skip-subtree pattern-loc) (zip-next-skip-subtree loc) acc)

           (and (symbol? pattern-node)
                (= node (get acc pattern-node node)))
           (recur (z/next pattern-loc)
                  (zip-next-skip-subtree loc)
                  (cond-> acc
                    (not= '_ pattern-node) (assoc pattern-node
                                                  (if (:z (meta pattern-node))
                                                    loc
                                                    node))))

           :else
           nil))))))

(defmacro zmatch {:style/indent 1} [loc & [pattern expr & clauses]]
  (when pattern
    (if expr
      (let [vars (->> (flatten pattern)
                      (filter symbol?)
                      (remove '#{_}))]
        `(let [loc# ~loc
               loc# (if (:zip/make-node (meta loc#))
                      loc#
                      (z/vector-zip loc#))]
           (if-let [{:syms [~@vars] :as acc#} (zip-match (z/vector-zip '~pattern) loc#)]
             ~expr
             (zmatch loc# ~@clauses))))
      pattern)))

;; Attribute Grammar spike.

;; See:
;; https://inkytonik.github.io/kiama/Attribution
;; https://arxiv.org/pdf/2110.07902.pdf
;; https://haslab.uminho.pt/prmartins/files/phd.pdf
;; https://github.com/christoff-buerger/racr

(defn ctor [ag]
  (when ag
    (let [node (z/node ag)]
      (when (vector? node)
        (first node)))))

(defn- z-nth [ag ^long n]
  (reduce
   (fn [ag f]
     (f ag))
   (z/down ag)
   (repeat (if (neg? n)
             (+ (count (z/children ag)) n)
             n)
           z/right)))

(def parent z/up)
(def $ z-nth)
(def child-idx (comp count z/lefts))

(defn lexeme [ag ^long n]
  (some-> ($ ag n) (z/node)))

(defn first-child? [ag]
  (= 1 (count (z/lefts ag))))

(defn single-child? [loc]
  (= 1 (count (rest (z/children loc)))))

(defn left-or-parent [ag]
  (if (first-child? ag)
    (parent ag)
    (z/left ag)))

(defmacro inherit [ag]
  `(some-> (parent ~ag) (recur)))

(defmacro zcase {:style/indent 1} [ag & body]
  `(case (ctor ~ag) ~@body))

(defn with-memoized-attributes [attr-vars f]
  (let [attrs (zipmap attr-vars (map (comp memoize deref) attr-vars))]
    (with-redefs-fn attrs f)))

;; Strategic Zippers based on Ztrategic

;; https://arxiv.org/pdf/2110.07902.pdf
;; https://www.di.uminho.pt/~joost/publications/SBLP2004LectureNotes.pdf

;; Strafunski:
;; https://www.di.uminho.pt/~joost/publications/AStrafunskiApplicationLetter.pdf
;; https://arxiv.org/pdf/cs/0212048.pdf
;; https://arxiv.org/pdf/cs/0204015.pdf
;; https://arxiv.org/pdf/cs/0205018.pdf

;; Type Preserving

(defn seq-tp [& xs]
  (fn [z]
    (reduce
     (fn [acc x]
       (if-some [acc (x acc)]
         acc
         (reduced nil)))
     z
     xs)))

(defn choice-tp [& xs]
  (fn [z]
    (reduce
     (fn [_ x]
       (when-some [z (x z)]
         (reduced z)))
     nil
     xs)))

(defn all-tp [f]
  (fn [z]
    (if-some [d (z/down z)]
      (loop [z d]
        (when-some [z (f z)]
          (if-some [r (z/right z)]
            (recur r)
            (z/up z))))
      z)))

(defn one-tp [f]
  (fn [z]
    (when-some [d (z/down z)]
      (loop [z d]
        (if-some [z (f z)]
          (z/up z)
          (when-some [r (z/right z)]
            (recur r)))))))

(defn full-td-tp
  ([f]
   (fn self [z]
     ((seq-tp f (all-tp self)) z)))
  ([f z]
   ((full-td-tp f) z)))

(defn full-bu-tp
  ([f]
   (fn self [z]
     ((seq-tp (all-tp self) f) z)))
  ([f z]
   ((full-bu-tp f) z)))

(defn once-td-tp
  ([f]
   (fn self [z]
     ((choice-tp f (one-tp self)) z)))
  ([f z]
   ((once-td-tp) f) z))

(defn once-bu-tp
  ([f]
   (fn self [z]
     ((choice-tp (one-tp self) f) z)))
  ([f z]
   ((once-bu-tp f) z)))

(defn stop-td-tp
  ([f]
   (fn self [z]
     ((choice-tp f (all-tp self)) z)))
  ([f z]
   ((stop-td-tp f) z)))

(defn z-try-apply-m [f]
  (fn [z]
    (some->> (f z)
             (z/replace z))))

(defn adhoc-tp [f g]
  (choice-tp (z-try-apply-m g) f))

(defn id-tp [x] x)

(defn fail-tp [_])

(def mono-tp (partial adhoc-tp fail-tp))

(defn try-tp [f]
  (choice-tp f id-tp))

(defn repeat-tp
  ([f]
   (fn [z]
     (if-some [z (f z)]
       (recur z)
       z)))
  ([f z]
   ((repeat-tp f) z)))

(defn innermost
  ([f]
   (fn self [z]
     ((seq-tp (all-tp self) (try-tp (seq-tp f self))) z)))
  ([f z]
   ((innermost f) z)))

(defn outermost
  ([f]
   (repeat-tp (once-td-tp f)))
  ([f z]
   ((outermost f) z)))

(def topdown full-td-tp)

;; Type Unifying

(defn- monoid [z]
  (get (meta z) :zip/monoid into))

;; TODO: should this short-circuit properly? Ztrategic doesn't seem
;; to.
(defn seq-tu [& xs]
  (fn [z]
    (transduce (map (fn [x] (x z)))
               (monoid z)
               xs)))

(def choice-tu choice-tp)

(defn all-tu [f]
  (fn [z]
    (let [m (monoid z)]
      (if-some [d (z/down z)]
        (loop [z d
               acc (m)]
          (when-some [x (f z)]
            (let [acc (m acc x)]
              (if-some [r (z/right z)]
                (recur r acc)
                (m acc)))))
        (m)))))

(defn one-tu [f]
  (fn [z]
    (let [m (monoid z)]
      (when-some [d (z/down z)]
        (loop [z d
               acc (m)]
          (if-some [x (f z)]
            (m acc x)
            (when-some [r (z/right z)]
              (recur r acc))))))))

(defn full-td-tu
  ([f]
   (fn self [z]
     ((seq-tu f (all-tu self)) z)))
  ([f z]
   ((full-td-tu f) z)))

(defn full-bu-tu
  ([f]
   (fn self [z]
     ((seq-tu (all-tu self) f) z)))
  ([f z]
   ((full-bu-tu f) z)))

(defn once-td-tu
  ([f]
   (fn self [z]
     ((choice-tu f (one-tu self)) z)))
  ([f z]
   ((once-td-tu f) z)))

(defn once-bu-tu
  ([f]
   (fn self [z]
     ((choice-tu (one-tu self) f) z)))
  ([f z]
   ((once-bu-tu f) z)))

(defn stop-td-tu
  ([f]
   (fn self [z]
     ((choice-tu f (all-tu self)) z)))
  ([f z]
   ((stop-td-tu f) z)))

(defn adhoc-tu [f g]
  (choice-tu g f))

(defn fail-tu [_])

(defn const-tu [x]
  (constantly x))

(def mono-tu (partial adhoc-tu fail-tu))

(defn with-tu-monoid [z f]
  (vary-meta z assoc :zip/monoid f))

(def collect full-td-tu)

(def collect-stop stop-td-tu)

(def select once-td-tu)

(comment

  (defn use-attributes
    ([attr-fn loc]
     (use-attributes attr-fn conj loc))
    ([attr-fn f loc]
     (loop [loc (z/right (z/down loc))
            acc (f)]
       (if loc
         (recur (z/right loc)
                (if (z/branch? loc)
                  (if-some [v (attr-fn loc)]
                    (f acc v)
                    acc)
                  acc))
         (f acc)))))

  (declare repmin globmin locmin)

  (defn repmin [loc]
    (zmatch loc
      [:fork _ _] (use-attributes repmin
                                  (completing
                                   (fn
                                     ([] [:fork])
                                     ([x y] (conj x y))))
                                  loc)
      [:leaf _] [:leaf (globmin loc)]))

  (defn locmin [loc]
    (zmatch loc
      [:fork _ _] (use-attributes locmin
                                  (completing
                                   (fn
                                     ([] Long/MAX_VALUE)
                                     ([x y] (min x y))))
                                  loc)
      [:leaf n] n))

  (defn globmin [loc]
    (if-let [p (z/up loc)]
      (globmin p)
      (locmin loc)))

  ;; https://web.fe.up.pt/~jacome/downloads/CEFP15.pdf
  ;; "Watch out for that tree! A Tutorial on Shortcut Deforestation"

  (defn repm [t]
    (case (first t)
      :leaf [(fn [z]
               [:leaf z]) (second t)]
      :fork (let [[t1 m1] (repm (second t))
                  [t2 m2] (repm (last t))]
              [(fn [z]
                 [:fork (t1 z) (t2 z)]) (min m1 m2)])))

  (let [[t m] (repm [:fork [:fork [:leaf 1] [:leaf 2]] [:fork [:leaf 3] [:leaf 4]]])]
    (t m))

  ;; http://hackage.haskell.org/package/ZipperAG
  ;; https://www.sciencedirect.com/science/article/pii/S0167642316000812
  ;; "Embedding attribute grammars and their extensions using functional zippers"

  ;; Chapter 3 & 4:

  (defn dcli [ag]
    (case (ctor ag)
      :root {}
      :let (case (ctor (parent ag))
             :root (dcli (parent ag))
             :cons-let (env (parent ag)))
      (case (ctor (parent ag))
        (:cons :cons-let) (assoc (dcli (parent ag))
                                 (lexeme (parent ag) 1)
                                 (parent ag))
        (dcli (parent ag)))))

  (defn dclo [ag]
    (case (ctor ag)
      :root (dclo ($ ag 1))
      :let (dclo ($ ag 1))
      (:cons :cons-let) (dclo ($ ag 3))
      :empty (dcli ag)))

  (defn env [ag]
    (case (ctor ag)
      :root (dclo ag)
      :let (case (ctor (parent ag))
             :cons-let (dclo ag)
             (env (parent ag)))
      (env (parent ag))))

  (defn lev [ag]
    (case (ctor ag)
      :root 0
      :let (case (ctor (parent ag))
             :cons-let (inc (lev (parent ag)))
             (lev (parent ag)))
      (lev (parent ag))))

  (defn must-be-in [m n]
    (if (contains? m n)
      []
      [n]))

  (defn must-not-be-in [m n ag]
    (let [r (get m n)]
      (if (and r (= (lev ag) (lev r)))
        [n]
        [])))

  (defn errs [ag]
    (case (ctor ag)
      (:cons :cons-let) (->> [(must-not-be-in (dcli ag) (lexeme ag 1) ag)
                              (errs ($ ag 2))
                              (errs ($ ag 3))]
                             (reduce into))
      :variable (must-be-in (env ag) (lexeme ag 1))
      (use-attributes errs into ag)))

  (errs
   (z/vector-zip
    [:root
     [:let
      [:cons "a" [:plus [:variable "b"] [:constant 3]]
       [:cons "c" [:constant 8]
        [:cons-let "w" [:let
                        [:cons "c" [:times [:variable "a"] [:variable "b"]]
                         [:empty]]
                        [:times [:variable "c"] [:variable "b"]]]
         [:cons "b" [:minus [:times [:variable "c"] [:constant 3]] [:variable "c"]]
          [:empty]]]]]
      [:minus [:times [:variable "c"] [:variable "w"]] [:variable "a"]]]]))

  ;; let a = z + 3
  ;;     c = 8
  ;;     a = (c ∗ 3) − c
  ;; in (a + 7) ∗ c

  (errs
   (z/vector-zip
    [:root
     [:let
      [:cons "a" [:plus [:variable "z"] [:constant 3]]
       [:cons "c" [:constant 8]
        [:cons "a" [:minus [:times [:variable "c"] [:constant 3]] [:variable "c"]]
         [:empty]]]]
      [:times [:plus [:variable "a"] [:constant 7]] [:variable "c"]]]]))

  ;; https://github.com/christoff-buerger/racr
  ;; https://dl.acm.org/doi/pdf/10.1145/2814251.2814257
  ;; "Reference Attribute Grammar Controlled Graph Rewriting: Motivation and Overview"

  (defn find-l-decl [n name]
    (when n
      (if (and (= :Decl (ctor n))
               (= (lexeme n 2) name))
        n
        (recur (z/left n) name))))

  (declare l-decl)

  (defn g-decl [n name]
    (case (ctor (parent n))
      :Block (or (find-l-decl n name)
                 (some-> n
                         #_(parent)
                         (parent)
                         (g-decl name)))
      :Prog (or (find-l-decl n name)
                (z/vector-zip [:DErr]))))

  (defn l-decl [n name]
    (case (ctor n)
      :Decl (when (= (lexeme n 1) name)
              n)))

  (defn type' [n]
    (case (ctor n)
      :Use (type' (g-decl n (lexeme n 1)))
      :Decl (lexeme n 1)
      :DErr "ErrorType"
      (:Prog :Block) (type' (z/rightmost (z/down n)))))

  (defn well-formed? [n]
    (case (ctor n)
      :Use (not= (type' n) "ErrorType")
      :Decl (= (g-decl n (lexeme n 2)) n)
      :DErr false
      (:Prog :Block) (use-attributes well-formed?
                                     (completing
                                      (fn
                                        ([] true)
                                        ([x y] (and x y))))
                                     n)))

  (well-formed?
   (z/vector-zip
    [:Prog
     [:Decl "Integer" "a"]
     [:Block [:Use "b"] [:Use  "a"] [:Decl "Real" "a"] [:Use "a"]]
     [:Use "a"]
     [:Decl "Real" "a"]]))

  ;; Based number example

  (defn base [n]
    (case (ctor n)
      :basechar (case (lexeme n 1)
                  "o" 8
                  "d" 10)
      :based-num (base ($ n 2))
      (:num :digit) (base (parent n))))

  (defn value [n]
    (case (ctor n)
      :digit (let [v (Double/parseDouble (lexeme n 1))]
               (if (> v (base n))
                 Double/NaN
                 v))
      :num (if (= 2 (count (z/children n)))
             (value ($ n 1))
             (+ (* (base n)
                   (value ($ n 1)))
                (value ($ n 2))))
      :based-num (value ($ n 1))))

  (time
   (= 229.0
      (with-memoized-attributes
        [#'base #'value]
        #(value
          (z/vector-zip
           [:based-num
            [:num
             [:num
              [:num
               [:num
                [:digit "3"]]
               [:digit "4"]]
              [:digit "5"]]]
            [:basechar "o"]]))))))

(comment
  (defn ->attributed-tree [tree attr-vars]
    (with-memoized-attributes attr-vars
      #(loop [loc (z/vector-zip tree)]
         (if (z/end? loc)
           (z/node loc)
           (recur (z/next (if (z/branch? loc)
                            (if-let [acc (some->> (for [k attr-vars
                                                        :let [v (k loc)]
                                                        :when (some? v)]
                                                    [(:name (meta k)) v])
                                                  (not-empty)
                                                  (into {}))]
                              (z/edit loc vary-meta merge acc)
                              loc)
                            loc)))))))

  (let [tree [:fork [:fork [:leaf 1] [:leaf 2]] [:fork [:leaf 3] [:leaf 4]]]
        tree (->attributed-tree tree [#'repmin #'locmin #'globmin])]
    (keep meta (tree-seq vector? seq tree)))

  (zip-match (z/vector-zip '[:leaf n])
             (z/vector-zip [:leaf 2]))

  ((full-td-tp (adhoc-tp id-tp (fn [x] (prn x) (when (number? x) (str x)))))
   (z/vector-zip [:fork [:fork [:leaf 1] [:leaf 2]] [:fork [:leaf 3] [:leaf 4]]]))

  ((full-bu-tu (mono-tu (fn [x] (prn x) (when (number? x) [x]))))
   (z/vector-zip [:fork [:fork [:leaf 1] [:leaf 2]] [:fork [:leaf 3] [:leaf 4]]]))

  (= ["a" "c" "b" "c"]
     ((full-td-tu (mono-tu
                   #(zmatch %
                      [:assign s _ _] [s]
                      [:nested-let s _ _] [s]
                      _ [])))
      (z/vector-zip
       [:let
        [:assign "a"
         [:add [:var "b"] [:const 0]]
         [:assign "c" [:const 2]
          [:nested-let "b" [:let [:assign "c" [:const 3] [:empty-list]]
                            [:add [:var "c"] [:var "c"]]]
           [:empty-list]]]]
        [:sub [:add [:var "a"] [:const 7]] [:var "c"]]]))))

;; Experimental comprehension rewrites based on rules in
;; https://okmij.org/ftp/meta-programming/quel.pdf

;; See also:
;; https://lists.w3.org/Archives/Public/public-rif-wg/2008Oct/att-0054/p457-fegaras.pdf
;; https://db.inf.uni-tuebingen.de/staticfiles/publications/Comprehensions.pdf
;; https://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.88.9148&rep=rep1&type=pdf
;; https://www.researchgate.net/publication/2724181_Improving_List_Comprehension_Database_Queries
;; https://homepages.inf.ed.ac.uk/jcheney/publications/cheney13icfp.pdf
;; http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.365.3145&rep=rep1&type=pdf

(defn- normalize-comprehension [comprehension]
  (letfn [(stage-1-symbolic-reduction [z]
            (zmatch z
              [:get l [:record attrs]]
              ;;=> RecordBeta
              (letfn [(step [z]
                        (case (ctor z)
                          :attr (when (= l (lexeme z 1))
                                  (lexeme z 2))
                          nil))]
                ((once-td-tu (mono-tu step)) ($ z -1)))

              [:for x [:yield M] N]
              ;;=> ForYield
              (letfn [(step [[n :as z]]
                        (case (ctor z)
                          :for (when (= x (lexeme z 1))
                                 n)
                          (when (= x n)
                            M)))]
                (z/node ((stop-td-tp (mono-tp step)) ($ z -1))))

              [:for x [:for y L M] N]
              ;;=> ForFor
              [:for y L [:for x M N]] ;; if y not free in N

              [:for x [:where L M] N]
              ;;=> ForWhere1
              [:where L [:for x M N]]

              [:for x [] N]
              ;;=> ForEmpty1
              []

              [:for x [:union-all L M] N]
              ;;=>ForUnionAll1
              [:union-all [:for x L N] [:for x M N]]

              [:where true M]
              ;;=> WhereTrue
              M

              [:where false M]
              ;;=> WhereFalse
              []))

          (stage-2-adhoc-rules [z]
            (zmatch z
              [:for x L [:union-all M N]]
              ;;=> ForUnionAll2
              [:union-all [:for x L M] [:for x L N]]

              [:for x M []]
              ;;=> ForEmpty2
              []

              [:where L [:union-all M N]]
              ;;=> WhereUnion
              [:union-all [:where L M] [:where L N]]

              [:where L []]
              ;;=> WhereEmpty
              []

              [:where L [:where M N]]
              ;;=> WhereWhere
              [:where [:and L M] N]

              [:where L [:for x M N]]
              ;;=> WhereFor
              [:for x M [:where L N]]))]

    (->> (z/vector-zip comprehension)
         (innermost (mono-tp stage-1-symbolic-reduction))
         (innermost (mono-tp stage-2-adhoc-rules))
         (z/node))))

(comment
  (time
   (= '[:for e [:table employee]
        [:for d [:table department]
         [:where [:and [:> [:get :wage e] 20] [:= [:get :deptID d] [:get :deptID e]]]
          [:yield [:record
                   [:attrs
                    [:attr :name [:get :name e]]
                    [:attr :dep [:get :name d]]
                    [:attr :wage [:get :wage e]]]]]]]]
      (doto (normalize-comprehension
             '[:for e [:for e [:table employee]
                       [:where [:> [:get :wage e] 20]
                        [:yield e]]]
               [:for d [:table department]
                [:where [:= [:get :deptID d] [:get :deptID e]]
                 [:yield [:record
                          [:attrs
                           [:attr :name [:get :name e]]
                           [:attr :dep [:get :name d]]
                           [:attr :wage [:get :wage e]]]]]]]])
        (clojure.pprint/pprint))))

  (time
   (= '[:for o [:table orders]
        [:for p [:table products]
         [:where
          [:and [:eq [:get :oid o] oid] [:eq [:get :pid p] [:get :pid o]]]
          [:yield
           [:record
            [:attrs
             [:attr :pid [:get :pid p]]
             [:attr :name [:get :name p]]
             [:attr :sale [:* [:get :price p] [:get :qty o]]]]]]]]]
      (doto (normalize-comprehension
             '[:for y [:for o [:table orders]
                       [:where [:eq [:get :oid o] oid]
                        [:yield [:record
                                 [:attrs
                                  [:attr :pid [:get :pid o]]
                                  [:attr :qty [:get :qty o]]]]]]]
               [:for p [:table products]
                [:where [:eq [:get :pid p] [:get :pid y]]
                 [:yield [:record
                          [:attrs
                           [:attr :pid [:get :pid p]]
                           [:attr :name [:get :name p]]
                           [:attr :sale [:* [:get :price p] [:get :qty y]]]]]]]]])
        (clojure.pprint/pprint)))))
