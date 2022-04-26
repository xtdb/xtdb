(ns core2.rewrite
  (:require [clojure.string :as str]
            [clojure.walk :as w]
            [clojure.zip :as z])
  (:import java.util.regex.Pattern))

(set! *unchecked-math* :warn-on-boxed)

;; Zipper pattern matching

(defn ->zipper [x]
  (cond
    (:zip/make-node (meta x))
    x

    (or (vector? x)
        (symbol? x)
        (instance? Pattern x))
    (z/vector-zip x)

    (sequential? x)
    (z/seq-zip x)

    :else
    (throw (IllegalArgumentException. (str "No zipper constructor for: " (type x))))))

(defn- build-zmatcher [pattern]
  (cond
    (= '_ pattern)
    (fn [acc _]
      acc)

    (and (symbol? pattern)
         (:z (meta pattern)))
    (fn [acc z]
      (when-not (contains? acc pattern)
        (assoc acc pattern z)))

    (symbol? pattern)
    (fn [acc z]
      (let [node (z/node z)]
        (when (= node (get acc pattern node))
          (assoc acc pattern node))))

    (keyword? pattern)
    (fn [acc z]
      (let [node (z/node z)]
        (when (and (ident? node)
                   (= (name pattern) (name node)))
          acc)))

    (instance? Pattern pattern)
    (fn [acc z]
      (let [node (z/node z)]
        (when (and (string? node)
                   (re-find pattern node))
          acc)))

    (sequential? pattern)
    (let [matchers (object-array (map build-zmatcher pattern))
          len (count matchers)]
      (fn [acc z]
        (when-let [z (z/down z)]
          (when (= len (inc (count (z/rights z))))
            (loop [acc acc
                   z z
                   n 0]
              (if (= n len)
                acc
                (when z
                  (when-let [acc ((aget matchers n) acc z)]
                    (recur acc (z/right z) (inc n))))))))))

    :else
    (fn [acc z]
      (when (= pattern (z/node z))
        acc))))

(def zmatchers (atom {}))

(defmacro zmatch {:style/indent 1} [loc & [pattern expr & clauses :as all-clauses]]
  (when pattern
    (if (> (count all-clauses) 1)
      (let [vars (if (symbol? pattern)
                   #{pattern}
                   (->> (flatten pattern)
                        (filter symbol?)
                        (remove '#{_})))
            matcher-key (binding  [*print-meta* true]
                          (pr-str pattern))]
        (swap! zmatchers update matcher-key (fn [matcher]
                                              (or matcher (build-zmatcher pattern))))
        `(let [loc# (->zipper ~loc)]
           (if-let [{:syms [~@vars] :as acc#} ((get @zmatchers '~matcher-key) {} loc#)]
             ~expr
             (zmatch loc# ~@clauses))))
      pattern)))

;; Attribute Grammar spike.

;; See related literature:
;; https://inkytonik.github.io/kiama/Attribution (no code is borrowed)
;; https://arxiv.org/pdf/2110.07902.pdf
;; https://haslab.uminho.pt/prmartins/files/phd.pdf
;; https://github.com/christoff-buerger/racr

(defn ctor [ag]
  (when ag
    (let [node (z/node ag)]
      (when (sequential? node)
        (first node)))))

(defn ctor? [kw ag]
  (= kw (ctor ag)))

(defn- z-nth [ag ^long n]
  (reduce
   (fn [ag f]
     (f ag))
   (z/down ag)
   (repeat (if (neg? n)
             (+ (count (z/children ag)) n)
             n)
           z/right)))

(def vector-zip z/vector-zip)
(def node z/node)
(def root z/root)
(def left z/left)
(def right z/right)
(def prev z/prev)

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

(defmacro with-memoized-attributes [attrs & body]
  `(binding [~@(interleave attrs (map (partial list 'memoize) attrs))]
     ~@body))

;; Strategic Zippers based on Ztrategic

;; https://arxiv.org/pdf/2110.07902.pdf
;; https://www.di.uminho.pt/~joost/publications/SBLP2004LectureNotes.pdf

;; Strafunski:
;; https://www.di.uminho.pt/~joost/publications/AStrafunskiApplicationLetter.pdf
;; https://arxiv.org/pdf/cs/0212048.pdf
;; https://arxiv.org/pdf/cs/0204015.pdf
;; https://arxiv.org/pdf/cs/0205018.pdf

;; Type Preserving

(defn seq-tp [x y]
  (fn [z]
    (when-some [z (x z)]
      (y z))))

(defn choice-tp [x y]
  (fn [z]
    (if-some [z (x z)]
      z
      (y z))))

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
   ((once-td-tp f) z)))

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

(def bottomup full-bu-tp)

;; Type Unifying

(defn- monoid [z]
  (get (meta z) :zip/monoid into))

;; TODO: should this short-circuit properly? Ztrategic doesn't seem
;; to.
(defn seq-tu [x y]
  (fn [z]
    (let [m (monoid z)]
      (-> (m)
          (m (x z))
          (m (y z))))))

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
    (when-some [d (z/down z)]
      (loop [z d]
        (if-some [x (f z)]
          x
          (when-some [r (z/right z)]
            (recur r)))))))

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

(defn with-tu-monoid [z m]
  (vary-meta z assoc :zip/monoid m))

(defn collect
  ([f]
   (full-td-tu f))
  ([f z]
   (full-td-tu f z))
  ([f m z]
   (full-td-tu f (with-tu-monoid z m))))

(defn collect-stop
  ([f]
   (stop-td-tu f))
  ([f z]
   (stop-td-tu f z))
  ([f m z]
   (stop-td-tu f (with-tu-monoid z m))))

(def select once-td-tu)
