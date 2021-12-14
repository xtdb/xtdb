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
;; Proper unification match on node instead of keyword - optionally bind zippers with ^:z?

(comment
  (do
    (defn- attr-children [attr-fn loc]
      (loop [loc (zip/right (zip/down loc))
             acc []]
        (if loc
          (recur (zip/right loc)
                 (if-some [v (and (zip/branch? loc)
                                  (attr-fn loc))]
                   (conj acc v)
                   acc))
          acc)))

    (defmacro defattr [name [loc :as attrs] & body]
      `(defn ~name ~attrs
         (when (zip/branch? ~loc)
           (let [node# (zip/node ~loc)]
             (case (first node#)
               ~@body)))))

    (declare repmin globmin locmin)

    (defattr repmin [loc]
      :fork (->> (attr-children repmin loc)
                 (into [:fork]))
      :leaf [:leaf (globmin loc)])

    (defattr locmin [loc]
      :fork (->> (attr-children locmin loc)
                 (reduce min))
      :leaf (second (zip/node loc)))

    (defattr globmin [loc]
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
                                  (->> (for [[k attr-fn] attrs
                                             :let [v (attr-fn loc)]
                                             :when (some? v)]
                                         [(:name (meta k)) v])
                                       (into {})
                                       (zip/edit loc vary-meta merge))
                                  loc))))))))

    (let [tree [:fork [:fork [:leaf 1] [:leaf 2]] [:fork [:leaf 3] [:leaf 4]]]
          tree (annotate-tree tree [#'repmin #'locmin #'globmin])]
      (keep meta (tree-seq vector? seq tree)))))
