(ns core2.rewrite
  (:require [clojure.string :as str]
            [clojure.zip :as zip]))

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

(defn- ->ctx [loc]
  (first (::ctx (meta loc))))

(defn- init-ctx [loc ctx]
  (vary-meta loc assoc ::ctx [ctx]))

(defn vary-ctx [loc f & args]
  (vary-meta loc update-in [::ctx 0] (fn [ctx]
                                       (apply f ctx args))))

(defn conj-ctx [loc k v]
  (vary-ctx loc update k conj v))

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

(defn ->text [kw]
  (->before
   (fn [loc _]
     (vary-ctx loc assoc kw (str/join (text-nodes loc))))))

(defn ->scoped
  ([rule-overrides]
   (->scoped rule-overrides nil (fn [loc _]
                                  loc)))
  ([rule-overrides after-fn]
   (->scoped rule-overrides nil after-fn))
  ([rule-overrides ->ctx-fn after-fn]
   (let [->ctx-fn (or ->ctx-fn ->ctx)]
     (reify Rule
       (--> [_ loc]
         (push-ctx loc (update (->ctx-fn loc) :rules merge rule-overrides)))

       (<-- [_ loc]
         (-> (pop-ctx loc)
             (after-fn (->ctx loc))))))))

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

;; Attribute Grammar spike.

;; See:
;; https://inkytonik.github.io/kiama/Attribution
;; https://arxiv.org/pdf/2110.07902.pdf
;; https://haslab.uminho.pt/prmartins/files/phd.pdf
;; https://github.com/christoff-buerger/racr

;; Ideas:

;; Strategies?
;; Replace rest of this rewrite ns entirely?

(defn- attr-children [attr-fn loc]
  (loop [loc (zip/right (zip/down loc))
         acc []]
    (if loc
      (recur (zip/right loc)
             (if (zip/branch? loc)
               (if-some [v (attr-fn loc)]
                 (conj acc v)
                 acc)
               acc))
      acc)))

(defn- zip-next-skip-subtree [loc]
  (or (zip/right loc)
      (loop [p loc]
        (when-let [p (zip/up p)]
          (or (zip/right p)
              (recur p))))))

(defn- zip-match [pattern-loc loc]
  (loop [pattern-loc pattern-loc
         loc loc
         acc {}]
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
          nil)))))

(defmacro zmatch {:style/indent 1} [loc & [pattern expr & clauses]]
  (when pattern
    (if expr
      (let [vars (->> (flatten pattern)
                      (filter symbol?)
                      (remove '#{_}))]
        `(if-let [{:syms [~@vars] :as acc#} (zip-match (zip/vector-zip '~pattern) ~loc)]
           ~expr
           (zmatch ~loc ~@clauses)))
      pattern)))

(declare repmin globmin locmin)

(defn repmin [loc]
  (zmatch loc
    [:fork _ _] (->> (attr-children repmin loc)
                     (into [:fork]))
    [:leaf _] [:leaf (globmin loc)]))

(defn locmin [loc]
  (zmatch loc
    [:fork _ _] (->> (attr-children locmin loc)
                     (reduce min))
    [:leaf n] n))

(defn globmin [loc]
  (if-let [p (zip/up loc)]
    (globmin p)
    (locmin loc)))

(defn annotate-tree [tree attr-vars]
  (let [attrs (zipmap attr-vars (map (comp memoize deref) attr-vars))]
    (with-redefs-fn attrs
      #(loop [loc (zip/vector-zip tree)]
         (if (zip/end? loc)
           (zip/node loc)
           (recur (zip/next (if (zip/branch? loc)
                              (if-let [acc (some->> (for [[k attr-fn] attrs
                                                          :let [v (attr-fn loc)]
                                                          :when (some? v)]
                                                      [(:name (meta k)) v])
                                                    (not-empty)
                                                    (into {}))]
                                (zip/edit loc vary-meta merge acc)
                                loc)
                              loc))))))))

;; Strategic Zippers based on Ztrategic
;; https://arxiv.org/pdf/2110.07902.pdf

;; Type Preserving

(declare all-tp-down all-tp-right maybe-keep)

(defmacro seq-tp
  ([x y] `(fn [z#]
            (seq-tp ~x ~y z#)))
  ([x y z]
   `(maybe-keep ~x ~y ~z)))

(defmacro choice-tp
  ([x y] `(fn [z#]
            (choice-tp ~x ~y z#)))
  ([x y z]
   `(let [z# ~z]
      (if-some [z# (~y z#)]
        z#
        (~x z#)))))

(defn full-td-tp [f]
  (->> f
       (seq-tp (all-tp-right (full-td-tp f)))
       (seq-tp (all-tp-down (full-td-tp f)))))

(defn full-bu-tp [f]
  (->> (all-tp-down (full-bu-tp f))
       (seq-tp (all-tp-right (full-bu-tp f)))
       (seq-tp f)))

(declare one-tp-down one-tp-right)

(defn once-td-tp [f]
  (->> f
       (choice-tp (one-tp-right (once-td-tp f)))
       (choice-tp (one-tp-down (once-td-tp f)))))

(defn once-bu-tp [f]
  (->> (one-tp-down (once-bu-tp f))
       (choice-tp (one-tp-right (once-bu-tp f)))
       (choice-tp f)))

(defn stop-td-tp [f]
  (->> (one-tp-right (stop-td-tp f))
       (seq-tp f)
       (choice-tp (one-tp-down (stop-td-tp f)))))

(defn stop-bu-tp [f]
  (->> (one-tp-down (stop-bu-tp f))
       (choice-tp f)
       (seq-tp (one-tp-right (stop-bu-tp f)))))

(declare z-try-apply-m z-try-apply-mz)

(defn adhoc-tp [f g]
  (maybe-keep f (z-try-apply-m g)))

(defn adhoc-tpz [f g]
  (maybe-keep f (z-try-apply-mz g)))

(defn id-tp [x] x)

(defn fail-tp [_])

(defn maybe-keep
  ([x y]
   (partial maybe-keep x y))
  ([x y z]
   (if-some [r (y z)]
     (if-some [k (x r)]
       k
       r)
     (x z))))

(defn all-tp-right
  ([f] (partial all-tp-right f))
  ([f z]
   (if-some [r (zip/right z)]
     (zip/left (f r))
     z)))

(defn all-tp-down
  ([f] (partial all-tp-down f))
  ([f z]
   (if-some [d (zip/down z)]
     (zip/up (f d))
     z)))

(defn one-tp-right
  ([f] (partial one-tp-right f))
  ([f z]
   (when-some [r (zip/right z)]
     (zip/left (f r)))))

(defn one-tp-down
  ([f] (partial one-tp-down f))
  ([f z]
   (when-some [d (zip/down z)]
     (zip/up (f d)))))

(def mono-tp (partial adhoc-tp fail-tp))

(def mono-tpz (partial adhoc-tpz fail-tp))

(defn try-tp [s]
  (choice-tp id-tp s))

(defn repeat-tp [s]
  (try-tp (fn [z]
            (when-some [z (s z)]
              ((repeat-tp s) z)))))

(defn innermost [s]
  (repeat-tp (once-bu-tp s)))

(defn outermost [s]
  (repeat-tp (once-td-tp s)))

;; Type Unifying

(defmacro seq-tu
  ([x y] `(fn [z#]
            (seq-tu ~x ~y z#)))
  ([x y z]
   `(let [xs# (~x ~z)
          ys# (~y ~z)]
      (into (or (empty ys#) (empty xs#))
            (concat xs# ys#)))))

(defmacro choice-tu
  ([x y] `(fn [z#]
            (choice-tu ~x ~y z#)))
  ([x y z]
   `(let [z# ~z]
      (if-some [z# (~y z#)]
        z#
        (~x z#)))))

(declare all-tu-down all-tu-right)

(defn full-td-tu [f]
  (->> (all-tu-right (full-td-tu f))
       (seq-tu (all-tu-down (full-td-tu f)))
       (seq-tu f)))

(defn full-bu-tu [f]
  (->> f
       (seq-tu (all-tu-down (full-bu-tu f)))
       (seq-tu (all-tu-right (full-bu-tu f)))))

(declare one-tu-down one-tu-right)

(defn once-td-tu [f]
  (->> (all-tu-right (once-td-tu f))
       (choice-tu (all-tu-down (once-td-tu f)))
       (choice-tu f)))

(defn once-bu-tu [f]
  (->> f
       (choice-tu (all-tu-down (once-bu-tu f)))
       (choice-tu (all-tu-right (once-bu-tu f)))))

(defn stop-td-tu [f]
  (->> (all-tu-right (stop-td-tu f))
       (seq-tu (choice-tu f (all-tu-down (stop-td-tu f))))))

(defn stop-bu-tu [f]
  (->> (choice-tu (all-tu-down (stop-bu-tu f)) f)
       (seq-tu (all-tu-right (stop-bu-tu f)))))

(defn all-tu-down
  ([f] (partial all-tu-down f))
  ([f z]
   (when-some [d (zip/down z)]
     (f d))))

(defn all-tu-right
  ([f] (partial all-tu-right f))
  ([f z]
   (when-some [r (zip/right z)]
     (f r))))

(declare z-try-reduce-m z-try-reduce-mz)

(defn adhoc-tu
  ([f g] (partial adhoc-tu f g))
  ([f g z]
   (if-some [id (z-try-reduce-m g z)]
     id
     (f z))))

(defn adhoc-tuz
  ([f g] (partial adhoc-tuz f g))
  ([f g z]
   (if-some [id (z-try-reduce-mz g z)]
     id
     (f z))))

(defn fail-tu [_])

(defn const-tu [x]
  (constantly x))

(def mono-tu (partial adhoc-tu fail-tu))

(def mono-tuz (partial adhoc-tuz fail-tu))

(defn z-try-reduce-mz
  ([f] (partial z-try-reduce-mz f))
  ([f z]
   (some-> (zip/node z) (f z))))

(defn z-try-reduce-m
  ([f] (partial z-try-reduce-m f))
  ([f z]
   (some-> (zip/node z) (f))))

(defn z-try-apply-mz
  ([f] (partial z-try-apply-mz f))
  ([f z]
   (some->> (f (zip/node z) z)
            (zip/replace z))))

(defn z-try-apply-m
  ([f] (partial z-try-apply-m f))
  ([f z]
   (some->> (f (zip/node z))
            (zip/replace z))))

(comment
  (let [tree [:fork [:fork [:leaf 1] [:leaf 2]] [:fork [:leaf 3] [:leaf 4]]]
        tree (annotate-tree tree [#'repmin #'locmin #'globmin])]
    (keep meta (tree-seq vector? seq tree)))

  (zip-match (zip/vector-zip '[:leaf n])
             (zip/vector-zip [:leaf 2]))

  ((full-td-tp (mono-tp (fn [x] (prn x) (when (number? x) (str x)))))
   (zip/vector-zip [:fork [:fork [:leaf 1] [:leaf 2]] [:fork [:leaf 3] [:leaf 4]]]))

  ((full-bu-tu (mono-tu (fn [x] (prn x) (when (number? x) [x]))))
   (zip/vector-zip [:fork [:fork [:leaf 1] [:leaf 2]] [:fork [:leaf 3] [:leaf 4]]])))
