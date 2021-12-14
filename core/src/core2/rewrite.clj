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

;; Note:

;; Currently a bit mixed, tries to maintain zipper and memoize on the
;; nodes, but won't do this properly as the active zipper may
;; change. Needs to stay same for this to work properly.

;; Ideas:

;; Strategies?
;; Replace rest of this rewrite ns entirely?
;
(comment
  (do
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

    (let [tree [:fork [:fork [:leaf 1] [:leaf 2]] [:fork [:leaf 3] [:leaf 4]]]
          tree (annotate-tree tree [#'repmin #'locmin #'globmin])]
      (keep meta (tree-seq vector? seq tree)))

    (zip-match (zip/vector-zip '[:leaf n])
               (zip/vector-zip [:leaf 2]))))
