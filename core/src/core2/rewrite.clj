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
(def lexme (comp z/node $))
(def child-idx (comp count z/lefts))

(defn first-child? [ag]
  (= 1 (count (z/lefts ag))))

(defn single-child? [loc]
  (= 1 (count (rest (z/children loc)))))

(defn prev [ag]
  (if (first-child? ag)
    (parent ag)
    (z/left ag)))

(defn with-memoized-attributes [attr-vars f]
  (let [attrs (zipmap attr-vars (map (comp memoize deref) attr-vars))]
    (with-redefs-fn attrs f)))

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

;; Strategic Zippers based on Ztrategic

;; https://arxiv.org/pdf/2110.07902.pdf
;; https://www.di.uminho.pt/~joost/publications/SBLP2004LectureNotes.pdf

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

(declare all-tp one-tp)

(defn full-td-tp [f]
  (fn self [z]
    ((seq-tp f (all-tp self)) z)))

(defn full-bu-tp [f]
  (fn self [z]
    ((seq-tp (all-tp self) f) z)))

(defn once-td-tp [f]
  (fn self [z]
    ((choice-tp f (one-tp self)) z)))

(defn once-bu-tp [f]
  (fn self [z]
    ((choice-tp (one-tp self) f) z)))

(defn stop-td-tp [f]
  (fn self [z]
    ((choice-tp f (all-tp self)) z)))

(defn stop-bu-tp [f]
  (fn self [z]
    ((choice-tp (all-tp self) f) z)))

(declare z-try-apply-m z-try-apply-mz)

(defn- maybe-keep [x y]
  (fn [z]
    (if-some [r (y z)]
      (if-some [k (x r)]
        k
        r)
      (x z))))

(defn adhoc-tp [f g]
  (maybe-keep f (z-try-apply-m g)))

(defn adhoc-tpz [f g]
  (maybe-keep f (z-try-apply-mz g)))

(defn id-tp [x] x)

(defn fail-tp [_])

(defn- all-tp-right [f]
  (fn [z]
    (if-some [r (z/right z)]
      (z/left (f r))
      z)))

(defn- all-tp-down [f]
  (fn [z]
    (if-some [d (z/down z)]
      (z/up (f d))
      z)))

(defn all-tp [f]
  (seq-tp (all-tp-down f) (all-tp-right f)))

(defn one-tp-right [f]
  (fn [z]
    (some->> (z/right z)
             (f)
             (z/left))))

(defn one-tp-down [f]
  (fn [z]
    (some->> (z/down z)
             (f)
             (z/up))))

(defn one-tp [f]
  (choice-tp (one-tp-down f) (one-tp-right f)))

(def mono-tp (partial adhoc-tp fail-tp))

(def mono-tpz (partial adhoc-tpz fail-tp))

(defn try-tp [f]
  (choice-tp f id-tp))

(defn repeat-tp [f]
  (fn [z]
    (if-some [z (f z)]
      (recur z)
      z)))

(defn innermost [f]
  (repeat-tp (once-bu-tp f)))

(defn outermost [f]
  (repeat-tp (once-td-tp f)))

;; Matching

;; match (?), build (!), scope ({}), where
;; dsig : p1 -> p2 (where s)? = {x... : ?p1; where(s); !p2 } // x... free vars in p1, s, p2
;; guard = s1 < s2 + s3
;; not(s) = s < fail + id
;; where = {x: ?x; s; !x}
;; if = where(s1) < s2 + s3
;; <s>p = !p; s
;; s => p = s; ?p
;; <add>(i, j) => k = !(i, j); add; ?k

(defn match [pattern]
  (let [pattern-loc (z/vector-zip pattern)]
    (fn [z]
      (when-let [env (zip-match pattern-loc z (::env (meta z)))]
        (vary-meta z assoc ::env env)))))

(defn build [pattern]
  (fn [z]
    (when-let [env (::env (meta z))]
      (z/replace z (w/postwalk-replace env pattern)))))

(defn scope [free-vars f]
  (fn [z]
    (let [parent-env (::env (meta z))
          z (vary-meta z assoc ::env (apply dissoc parent-env free-vars))]
      (when-let [z (f z)]
        (vary-meta z update ::env merge (select-keys parent-env free-vars))))))

(defn where [f]
  (fn [z]
    (when-let [new-env (::env (meta (f z)))]
      (vary-meta z assoc ::env new-env))))

(defn guard [test then else]
  (fn [z]
    (if-let [z (test z)]
      (then z)
      (else z))))

(defn not' [f]
  (guard f fail-tp id-tp))

(defn if' [test then else]
  (guard (where test) then else))

(defn rule
  ([p1 p2]
   (rule p1 p2 {:where id-tp}))
  ([p1 p2 {f :where}]
   ;; NOTE: this should also capture vars inside where, but this would
   ;; require more plumbing as this will be a function, can manually
   ;; wrap the rule in a scope.
   (let [free-vars (filterv symbol? (flatten (concat p1 p2)))]
     (scope free-vars (seq-tp (match p1) (where f) (build p2))))))

;; Type Unifying

(defn- mempty [z]
  ((get (meta z) :zip/mempty vector)))

(defn- mappend [z]
  (get (meta z) :zip/mappend into))

(defn seq-tu [& xs]
  (fn [z]
    (transduce (map (fn [x] (x z)))
               (mappend z)
               (mempty z)
               (reverse xs))))

(def choice-tu choice-tp)

(declare all-tu one-tu)

(defn full-td-tu [f]
  (fn self [z]
    ((seq-tu (all-tu self) f) z)))

(defn full-bu-tu [f]
  (fn self [z]
    ((seq-tu f (all-tu self)) z)))

(defn once-td-tu [f]
  (fn self [z]
    ((choice-tu f (one-tu self)) z)))

(defn once-bu-tu [f]
  (fn self [z]
    ((choice-tu (one-tu self) f) z)))

(defn stop-td-tu [f]
  (fn self [z]
    ((choice-tu f (all-tu self)) z)))

(defn stop-bu-tu [f]
  (fn self [z]
    ((choice-tu (all-tu self) f) z)))

(defn- all-tu-down [f]
  (fn [z]
    (when-some [d (z/down z)]
      (f d))))

(defn- all-tu-right [f]
  (fn [z]
    (when-some [r (z/right z)]
      (f r))))

(defn all-tu [f]
  (fn [z]
    ((seq-tu (all-tu-down f) (all-tu-right f)) z)))

(defn one-tu [f]
  (fn [z]
    ((choice-tu (all-tu-down f) (all-tu-right f)) z)))

(declare z-try-reduce-m z-try-reduce-mz)

(defn adhoc-tu [f g]
  (choice-tu (z-try-reduce-m g) f))

(defn adhoc-tuz [f g]
  (choice-tu (z-try-reduce-mz g) f))

(defn fail-tu [_])

(defn const-tu [x]
  (constantly x))

(def mono-tu (partial adhoc-tu fail-tu))

(def mono-tuz (partial adhoc-tuz fail-tu))

(defn z-try-reduce-mz [f]
  (fn [z]
    (some-> (z/node z) (f z))))

(defn z-try-reduce-m [f]
  (fn [z]
    (some-> (z/node z) (f))))

(defn z-try-apply-mz [f]
  (fn [z]
    (some->> (f (z/node z) z)
             (z/replace z))))

(defn z-try-apply-m [f]
  (fn [z]
    (some->> (f (z/node z))
             (z/replace z))))

(defn with-tu-monoid [z mempty mappend]
  (vary-meta z assoc :zip/mempty mempty :zip/mappend mappend))

(comment

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
                                 (lexme (parent ag) 1)
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
      (:cons :cons-let) (->> [(must-not-be-in (dcli ag) (lexme ag 1) ag)
                              (errs ($ ag 2))
                              (errs ($ ag 3))]
                             (reduce into))
      :variable (must-be-in (env ag) (lexme ag 1))
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
               (= (lexme n 2) name))
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
      :Decl (when (= (lexme n 1) name)
              n)))

  (defn type' [n]
    (case (ctor n)
      :Use (type' (g-decl n (lexme n 1)))
      :Decl (lexme n 1)
      :DErr "ErrorType"
      (:Prog :Block) (type' (z/rightmost (z/down n)))))

  (defn well-formed? [n]
    (case (ctor n)
      :Use (not= (type' n) "ErrorType")
      :Decl (= (g-decl n (lexme n 2)) n)
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
      :basechar (case (lexme n 1)
                  "o" 8
                  "d" 10)
      :based-num (base ($ n 2))
      (:num :digit) (base (parent n))))

  (defn value [n]
    (case (ctor n)
      :digit (let [v (Double/parseDouble (lexme n 1))]
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
