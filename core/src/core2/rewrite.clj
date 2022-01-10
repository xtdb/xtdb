(ns core2.rewrite
  (:require [clojure.string :as str]
            [clojure.walk :as w]
            [clojure.zip :as zip])
  (:import [clojure.lang IDeref Indexed IRecord]))

(set! *unchecked-math* :warn-on-boxed)

;; Rewrite engine.

(defprotocol Rule
  (--> [_ loc])
  (<-- [_ loc]))

(defn- zip-dispatch [loc direction-fn]
  (let [tree (zip/node loc)]
    (if-let [rule (and (vector? tree)
                       (get-in (meta loc) [::ctx 0 :rules (first tree)]))]
      (direction-fn rule loc)
      loc)))

(defn ->ctx [loc]
  (first (::ctx (meta loc))))

(defn- init-ctx [loc ctx]
  (vary-meta loc assoc ::ctx [ctx]))

(defn ->ctx-stack [loc]
  (::ctx (meta loc)))

(defn vary-ctx [loc f & args]
  (vary-meta loc update-in [::ctx 0] (fn [ctx]
                                       (apply f ctx args))))

(defn conj-ctx [loc k v]
  (vary-ctx loc update k conj v))

(defn into-ctx [loc k v]
  (vary-ctx loc update k into v))

(defn assoc-ctx [loc k v]
  (vary-ctx loc assoc k v))

(defn assoc-in-ctx [loc ks v]
  (vary-ctx loc assoc-in ks v))

(defn- pop-ctx [loc]
  (vary-meta loc update ::ctx (comp vec rest)))

(defn- push-ctx [loc ctx]
  (vary-meta loc update ::ctx (partial into [ctx])))

(defn text-nodes [loc]
  (filterv string? (flatten (zip/node loc))))

(defn single-child? [loc]
  (= 1 (count (rest (zip/children loc)))))

(defn ->before [before-fn]
  (reify Rule
    (--> [_ loc]
      (before-fn loc (->ctx loc)))

    (<-- [_ loc] loc)))

(defn ->after [after-fn]
  (reify Rule
    (--> [_ loc] loc)

    (<-- [_ loc]
      (after-fn loc (->ctx loc)))))

(defn ->around [before-fn after-fn]
  (reify Rule
    (--> [_ loc]
      (before-fn loc (->ctx loc)))

    (<-- [_ loc]
      (after-fn loc (->ctx loc)))))

(defn ->text [kw]
  (->before
   (fn [loc _]
     (assoc-ctx loc kw (str/join (text-nodes loc))))))

(defn ->scoped [{:keys [rule-overrides init exit]
                 :or {rule-overrides {}
                      init ->ctx
                      exit (fn [loc _]
                             loc)}}]
  (reify Rule
    (--> [_ loc]
      (push-ctx loc (update (init loc) :rules merge rule-overrides)))

    (<-- [_ loc]
      (-> (pop-ctx loc)
          (exit (->ctx loc))))))

(defn rewrite-tree [tree ctx]
  (loop [loc (init-ctx (zip/vector-zip tree) ctx)]
    (if (zip/end? loc)
      (zip/root loc)
      (let [loc (zip-dispatch loc -->)]
        (recur (cond
                 (zip/branch? loc)
                 (zip/down loc)

                 (seq (zip/rights loc))
                 (zip/right (zip-dispatch loc <--))

                 :else
                 (loop [p loc]
                   (let [p (zip-dispatch p <--)]
                     (if-let [parent (zip/up p)]
                       (if (seq (zip/rights parent))
                         (zip/right (zip-dispatch parent <--))
                         (recur parent))
                       [(zip/node p) :end])))))))))

;; Zipper pattern matching

(defn- zip-next-skip-subtree [loc]
  (or (zip/right loc)
      (loop [p loc]
        (when-let [p (zip/up p)]
          (or (zip/right p)
              (recur p))))))

(defn- zip-match
  ([pattern-loc loc]
   (zip-match pattern-loc loc {}))
  ([pattern-loc loc acc]
   (loop [pattern-loc pattern-loc
          loc loc
          acc acc]
     (cond
       (or (nil? pattern-loc)
           (zip/end? pattern-loc))
       acc

       (or (nil? loc)
           (zip/end? loc))
       nil

       (and (zip/branch? pattern-loc)
            (zip/branch? loc))
       (when (= (count (zip/children pattern-loc))
                (count (zip/children loc)))
         (recur (zip/down pattern-loc) (zip/down loc) acc))

       :else
       (let [pattern-node (zip/node pattern-loc)
             node (zip/node loc)]
         (cond
           (= pattern-node node)
           (recur (zip-next-skip-subtree pattern-loc) (zip-next-skip-subtree loc) acc)

           (and (symbol? pattern-node)
                (= node (get acc pattern-node node)))
           (recur (zip/next pattern-loc)
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
                      (zip/vector-zip loc#))]
           (if-let [{:syms [~@vars] :as acc#} (zip-match (zip/vector-zip '~pattern) loc#)]
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
   (loop [loc (zip/right (zip/down loc))
          acc (f)]
     (if loc
       (recur (zip/right loc)
              (if (zip/branch? loc)
                (if-some [v (attr-fn loc)]
                  (f acc v)
                  acc)
                acc))
       (f acc)))))

(defn ctor [ag]
  (when ag
    (let [node (zip/node ag)]
      (when (vector? node)
        (first node)))))

(defn- z-nth [ag n]
  (reduce
   (fn [ag f]
     (f ag))
   (zip/down ag)
   (repeat (if (neg? n)
             (+ (count (zip/children ag)) n)
             n)
           zip/right)))

(defn first-child? [ag]
  (= 1 (count (zip/lefts ag))))

(def parent zip/up)
(def $ z-nth)
(def lexme (comp zip/node $))

(defn with-memoized-attributes [attr-vars f]
  (let [attrs (zipmap attr-vars (map (comp memoize deref) attr-vars))]
    (with-redefs-fn attrs f)))

(defn ->attributed-tree [tree attr-vars]
  (with-memoized-attributes attr-vars
    #(loop [loc (zip/vector-zip tree)]
       (if (zip/end? loc)
         (zip/node loc)
         (recur (zip/next (if (zip/branch? loc)
                            (if-let [acc (some->> (for [k attr-vars
                                                        :let [v (k loc)]
                                                        :when (some? v)]
                                                    [(:name (meta k)) v])
                                                  (not-empty)
                                                  (into {}))]
                              (zip/edit loc vary-meta merge acc)
                              loc)
                            loc)))))))

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
    (if-let [p (zip/up loc)]
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
   (zip/vector-zip
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
   (zip/vector-zip
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
        (recur (zip/left n) name))))

  (declare l-decl)

  (defn g-decl [n name]
    (case (ctor (parent n))
      :Block (or (find-l-decl n name)
                 (some-> n
                         #_(parent)
                         (parent)
                         (g-decl name)))
      :Prog (or (find-l-decl n name)
                (zip/vector-zip [:DErr]))))

  (defn l-decl [n name]
    (case (ctor n)
      :Decl (when (= (lexme n 1) name)
              n)))

  (defn type' [n]
    (case (ctor n)
      :Use (type' (g-decl n (lexme n 1)))
      :Decl (lexme n 1)
      :DErr "ErrorType"
      (:Prog :Block) (type' (zip/rightmost (zip/down n)))))

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
   (zip/vector-zip
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
      :num (if (= 2 (count (zip/children n)))
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
          (zip/vector-zip
           [:based-num
            [:num
             [:num
              [:num
               [:num
                [:digit "3"]]
               [:digit "4"]]
              [:digit "5"]]]
            [:basechar "o"]]))))))

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
    (if-some [r (zip/right z)]
      (zip/left (f r))
      z)))

(defn- all-tp-down [f]
  (fn [z]
    (if-some [d (zip/down z)]
      (zip/up (f d))
      z)))

(defn all-tp [f]
  (seq-tp (all-tp-down f) (all-tp-right f)))

(defn one-tp-right [f]
  (fn [z]
    (some->> (zip/right z)
             (f)
             (zip/left))))

(defn one-tp-down [f]
  (fn [z]
    (some->> (zip/down z)
             (f)
             (zip/up))))

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
  (let [pattern-loc (zip/vector-zip pattern)]
    (fn [z]
      (when-let [env (zip-match pattern-loc z (::env (meta z)))]
        (vary-meta z assoc ::env env)))))

(defn build [pattern]
  (fn [z]
    (when-let [env (::env (meta z))]
      (zip/replace z (w/postwalk-replace env pattern)))))

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
  ([p1 p2 {:keys [where]}]
   ;; NOTE: this should also capture vars inside where, but this would
   ;; require more plumbing as this will be a function, can manually
   ;; wrap the rule in a scope.
   (let [free-vars (filterv symbol? (flatten (concat p1 p2)))]
     (scope free-vars (seq-tp (match p1) (match where) (build p2))))))

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
    (when-some [d (zip/down z)]
      (f d))))

(defn- all-tu-right [f]
  (fn [z]
    (when-some [r (zip/right z)]
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
    (some-> (zip/node z) (f z))))

(defn z-try-reduce-m [f]
  (fn [z]
    (some-> (zip/node z) (f))))

(defn z-try-apply-mz [f]
  (fn [z]
    (some->> (f (zip/node z) z)
             (zip/replace z))))

(defn z-try-apply-m [f]
  (fn [z]
    (some->> (f (zip/node z))
             (zip/replace z))))

(defn with-tu-monoid [z mempty mappend]
  (vary-meta z assoc :zip/mempty mempty :zip/mappend mappend))

;; Alternative Zipper implementation:

(defrecord Node [left up right])

(defrecord Loc [tree path]
  IDeref
  (deref [_] tree)

  Indexed
  (nth [this n]
    (.nth this n nil))

  (nth [this n not-found]
    (let [t (.tree this)]
      (if (and (vector? t)
               (< n (count t)))
        (with-meta
          (->Loc (nth t n) (->Node (with-meta (vec (take n t)) (meta t))
                                   (.path this)
                                   (drop (inc n) t)))
          (meta this))
        not-found))))

(prefer-method print-method IRecord IDeref)

(defn ->zipper ^core2.rewrite.Loc [x]
  (->Loc x nil))

(defn left ^core2.rewrite.Loc [^Loc loc]
  (when-let [^Node p (.path loc)]
    (let [left (.left p)]
      (when-let [l (last left)]
        (with-meta
          (->Loc l (->Node (subvec left 0 (dec (count left)))
                           (.up p)
                           (cons (.tree loc) (.right p))))
          (meta loc))))))

(defn right ^core2.rewrite.Loc [^Loc loc]
  (when-let [^Node p (.path loc)]
    (let [right (.right p)]
      (when-let [r (first right)]
        (with-meta
          (->Loc r (->Node (conj (.left p) (.tree loc))
                           (.up p)
                           (rest right)))
          (meta loc))))))

(defn up ^core2.rewrite.Loc [^Loc loc]
  (when-let [^Node p (.path loc)]
    (with-meta
      (->Loc (into (.left p) (cons (.tree loc) (.right p))) (.up p))
      (meta loc))))

(defn down ^core2.rewrite.Loc [^Loc loc]
  (nth loc 0))

(defn root ^core2.rewrite.Loc [^Loc loc]
  (loop [loc loc]
    (if (.path loc)
      (recur (up loc))
      loc)))

(defn edit ^core2.rewrite.Loc [^Loc loc f & args]
  (when loc
    (with-meta
      (->Loc (apply f (.tree loc) args) (.path loc))
      (meta loc))))

(defn- skip-subtree ^core2.rewrite.Loc [^Loc loc]
  (or (right loc)
      (loop [p loc]
        (when-let [p (up p)]
          (or (right p)
              (recur p))))))

(defn zip2-next ^core2.rewrite.Loc [^Loc loc]
  (or (and (vector? @loc) (down loc))
      (skip-subtree loc)))

(defn- zip2-match
  ([pattern-loc loc]
   (zip2-match pattern-loc loc {}))
  ([pattern-loc loc acc]
   (loop [pattern-loc pattern-loc
          loc loc
          acc acc]
     (cond
       (nil? pattern-loc)
       acc

       (nil? loc)
       nil

       (and (vector? @pattern-loc)
            (vector? @loc))
       (when (= (count @pattern-loc)
                (count @loc))
         (recur (down pattern-loc) (down loc) acc))

       :else
       (let [pattern-node @pattern-loc
             node @loc]
         (cond
           (= pattern-node node)
           (recur (skip-subtree pattern-loc) (skip-subtree loc) acc)

           (and (symbol? pattern-node)
                (= loc (get acc pattern-node loc)))
           (recur (zip2-next pattern-loc)
                  (skip-subtree loc)
                  (cond-> acc
                    (not= '_ pattern-node) (assoc pattern-node loc)))

           :else
           nil))))))

(defmacro zcase {:style/indent 1} [loc & [pattern expr & clauses]]
  (when pattern
    (if expr
      (let [vars (->> (flatten pattern)
                      (filter symbol?)
                      (remove '#{_}))]
        `(let [loc# ~loc
               loc# (if (instance? Loc loc#)
                      loc#
                      (->zipper loc#))]
           (if-let [{:syms [~@vars] :as acc#} (zip2-match (->zipper '~pattern) loc#)]
             ~expr
             (zcase loc# ~@clauses))))
      pattern)))

(comment
  ;; Based number example, alternative zippers.

  (defn base [n]
    (zcase n
      [:basechar x] (case @x
                      "o" 8
                      "d" 10)
      [:based-num _ basechar] (base basechar)
      _ (base (up n))))

  (defn value [n]
    (zcase n
      [:digit x] (let [v (Double/parseDouble @x)]
                   (if (> v (base n))
                     Double/NaN
                     v))
      [:num x] (value x)
      [:num x y] (+ (* (base n)
                       (value x))
                    (value y))
      [:based-num x _] (value x)))

  (time
   (= 229.0
      (with-memoized-attributes
        [#'base #'value]
        #(value
          (->zipper
           [:based-num
            [:num
             [:num
              [:num
               [:num
                [:digit "3"]]
               [:digit "4"]]
              [:digit "5"]]]
            [:basechar "o"]])))))

  ;; ZipperAG example:

  (defn- use-attributes
    ([attr-fn loc]
     (use-attributes attr-fn conj loc))
    ([attr-fn f loc]
     (loop [loc (right (down loc))
            acc (f)]
       (if loc
         (recur (right loc)
                (if (vector? @loc)
                  (if-some [v (attr-fn loc)]
                    (f acc v)
                    acc)
                  acc))
         (f acc)))))

  (declare env)

  (defn dcli [ag]
    (zcase ag
      [:root _] {}
      [:let _ _] (zcase (up ag)
                   [:root _] (dcli (up ag))
                   [:cons-let _ _ _] (env (up ag)))
      (zcase (up ag)
        [:cons x _ _] (assoc (dcli (up ag))
                             @x
                             (up ag))
        [:cons-let x _ _] (assoc (dcli (up ag))
                                 @x
                                 (up ag))
        _ (dcli (up ag)))))

  (defn dclo [ag]
    (zcase ag
      [:root x] (dclo x)
      [:let x _] (dclo x)
      [:cons _ _ x] (dclo x)
      [:cons-let _ _ x] (dclo x)
      [:empty] (dcli ag)))

  (defn env [ag]
    (zcase ag
      [:root _] (dclo ag)
      [:let _ _] (zcase (up ag)
                   [:cons-let _ _ _] (dclo ag)
                   _ (env (up ag)))
      _ (env (up ag))))

  (defn lev [ag]
    (zcase ag
      [:root _] 0
      [:let _ _] (zcase (up ag)
                   [:cons-let _ _ _] (inc (lev (up ag)))
                   _ (lev (up ag)))
      _ (lev (up ag))))

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
    (zcase ag
      [:cons x y z] (->> [(must-not-be-in (dcli ag) @x ag)
                          (errs y)
                          (errs z)]
                         (reduce into))
      [:cons-let x y z] (->> [(must-not-be-in (dcli ag) @x ag)
                              (errs y)
                              (errs z)]
                             (reduce into))
      [:variable x] (must-be-in (env ag) @x)
      _ (use-attributes errs into ag)))

  (errs
   (->zipper
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
   (->zipper
    [:root
     [:let
      [:cons "a" [:plus [:variable "z"] [:constant 3]]
       [:cons "c" [:constant 8]
        [:cons "a" [:minus [:times [:variable "c"] [:constant 3]] [:variable "c"]]
         [:empty]]]]
      [:times [:plus [:variable "a"] [:constant 7]] [:variable "c"]]]])))

(comment
  (let [tree [:fork [:fork [:leaf 1] [:leaf 2]] [:fork [:leaf 3] [:leaf 4]]]
        tree (->attributed-tree tree [#'repmin #'locmin #'globmin])]
    (keep meta (tree-seq vector? seq tree)))

  (zip-match (zip/vector-zip '[:leaf n])
             (zip/vector-zip [:leaf 2]))

  ((full-td-tp (adhoc-tp id-tp (fn [x] (prn x) (when (number? x) (str x)))))
   (zip/vector-zip [:fork [:fork [:leaf 1] [:leaf 2]] [:fork [:leaf 3] [:leaf 4]]]))

  ((full-bu-tu (mono-tu (fn [x] (prn x) (when (number? x) [x]))))
   (zip/vector-zip [:fork [:fork [:leaf 1] [:leaf 2]] [:fork [:leaf 3] [:leaf 4]]]))

  (= ["a" "c" "b" "c"]
     ((full-td-tu (mono-tu
                   #(zmatch %
                      [:assign s _ _] [s]
                      [:nested-let s _ _] [s]
                      _ [])))
      (zip/vector-zip
       [:let
        [:assign "a"
         [:add [:var "b"] [:const 0]]
         [:assign "c" [:const 2]
          [:nested-let "b" [:let [:assign "c" [:const 3] [:empty-list]]
                            [:add [:var "c"] [:var "c"]]]
           [:empty-list]]]]
        [:sub [:add [:var "a"] [:const 7]] [:var "c"]]]))))
