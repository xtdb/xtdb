(ns core2.rewrite
  (:require [clojure.walk :as w]
            [clojure.core.match :as m])
  (:import java.util.regex.Pattern
           [java.util ArrayList List]
           clojure.lang.IPersistentVector))

(set! *unchecked-math* :warn-on-boxed)

(defrecord Zip [node ^int idx parent])

(defn ->zipper [x]
  (if (instance? Zip x)
    x
    (Zip. x -1 nil)))

(defn- zupdate-parent [^Zip z]
  (when-let [^Zip parent (.parent z)]
    (let [node (.node z)
          ^List level (.node parent)
          idx (.idx z)]
      (if (identical? node (.get level idx))
        parent
        (Zip. (.assocN ^IPersistentVector level idx node) (.idx parent) (.parent parent))))))

(defn znode [^Zip z]
  (when z
    (.node z)))

(defn zbranch? [^Zip z]
  (vector? (.node z)))

(defn zleft [^Zip z]
  (when z
    (when-let [^Zip parent (zupdate-parent z)]
      (let [idx (dec (.idx z))]
        (when-not (neg? idx)
          (let [^List level (.node parent)]
            (Zip. (.get level idx) idx parent)))))))

(defn zright [^Zip z]
  (when z
    (when-let [^Zip parent (zupdate-parent z)]
      (let [idx (inc (.idx z))
            ^List level (.node parent)]
        (when (< idx (.size level))
          (Zip. (.get level idx) idx parent))))))

(defn znth [^Zip z ^long idx]
  (when z
    (when (zbranch? z)
      (let [^List node (.node z)
            idx (if (neg? idx)
                  (+ (.size node) idx)
                  idx)]
        (when (and (< idx (.size node))
                   (not (neg? idx)))
          (Zip. (.get node idx) idx z))))))

(defn zdown [^Zip z]
  (when z
    (let [node (.node z)]
      (when (and (zbranch? z)
                 (not (.isEmpty ^List node)))
        (Zip. (.get ^List node 0) 0 z)))))

(defn zup [^Zip z]
  (when z
    (let [idx (.idx z)]
      (when-not (neg? idx)
        (zupdate-parent z)))))

(defn zroot [^Zip z]
  (if-let [z (zup z)]
    (recur z)
    (.node z)))

(defn zprev [z]
  (if-let [z (zleft z)]
    (loop [z z]
      (if-let [z (znth z -1)]
        (recur z)
        z))
    (zup z)))

(defn zchild-idx ^long [^Zip z]
  (when z
    (.idx z)))

(defn zchildren [^Zip z]
  (vec (.node ^Zip (.parent z))))

(defn zrights [^Zip z]
  (let [idx (inc (.idx z))
        ^List children (zchildren z)]
    (when (< idx (.size children))
      (seq (subvec children idx)))))

(defn zlefts [^Zip z]
  (seq (subvec (zchildren z) 0 (.idx z))))

(defn zreplace [^Zip z x]
  (when z
    (if (identical? (.node z) x)
      z
      (Zip. x (.idx z) (.parent z)))))

;; Zipper pattern matching

(derive ::m/zip ::m/vector)

(defmethod m/nth-inline ::m/zip
  [t ocr i]
  `(let [^Zip z# ~ocr]
     (Zip. (.get ^List (.node z#) ~i) ~i z#)))

(defmethod m/count-inline ::m/zip
  [t ocr]
  `(let [^Zip z# ~ocr]
     (if (zbranch? z#)
       (.size ^List (znode z#))
       0)))

(defmethod m/subvec-inline ::m/zip
  ([_ ocr start] (throw (UnsupportedOperationException.)))
  ([_ ocr start end] (throw (UnsupportedOperationException.))))

(defmethod m/tag ::m/zip
  [_] "core2.rewrite.Zip")

(defmacro zmatch {:style/indent 1} [z & clauses]
  (let [pattern+exprs (partition 2 clauses)
        else-clause (when (odd? (count clauses))
                      (last clauses))
        variables (->> pattern+exprs
                       (map first)
                       (flatten)
                       (filter symbol?))
        zip-matches? (some (comp :z meta) variables)
        shadowed-locals (filter (set variables) (keys &env))]
    (when-not (empty? shadowed-locals)
      (throw (IllegalArgumentException. (str "Match variables shadow locals: " (set shadowed-locals)))))
    (if-not zip-matches?
      `(let [z# ~z
             node# (if (instance? Zip z#)
                     (znode z#)
                     z#)]
         (m/match node#
                  ~@(->> (for [[pattern expr] pattern+exprs]
                           [pattern expr])
                         (reduce into []))
                  :else ~else-clause))
      `(m/matchv ::m/zip [(->zipper ~z)]
                 ~@(->> (for [[pattern expr] pattern+exprs]
                          [[(w/postwalk
                             (fn [x]
                               (cond
                                 (symbol? x)
                                 (if (or (= '_ x)
                                         (:z (meta x)))
                                   x
                                   {:node x})

                                 (vector? x)
                                 x

                                 :else
                                 {:node x}))
                             pattern)]
                           expr])
                        (reduce into []))
                 :else ~else-clause))))

;; Attribute Grammar spike.

;; See related literature:
;; https://inkytonik.github.io/kiama/Attribution (no code is borrowed)
;; https://arxiv.org/pdf/2110.07902.pdf
;; https://haslab.uminho.pt/prmartins/files/phd.pdf
;; https://github.com/christoff-buerger/racr

(defn ctor [ag]
  (when ag
    (let [node (znode ag)]
      (when (vector? node)
        (.nth ^IPersistentVector node 0 nil)))))

(defn ctor? [kw ag]
  (= kw (ctor ag)))

(def vector-zip ->zipper)
(def node znode)
(def root zroot)
(def left zleft)
(def right zright)
(def prev zprev)

(def parent zup)
(def $ znth)
(def child-idx zchild-idx)

(defn lexeme [ag ^long n]
  (some-> ($ ag n) (znode)))

(defn first-child? [ag]
  (= 1 (count (zlefts ag))))

(defn left-or-parent [ag]
  (if (first-child? ag)
    (parent ag)
    (zleft ag)))

(defmacro inherit [ag]
  `(some-> ~ag (parent) (recur)))

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
    (if-some [d (zdown z)]
      (loop [z d]
        (when-some [z (f z)]
          (if-some [r (zright z)]
            (recur r)
            (zup z))))
      z)))

(defn one-tp [f]
  (fn [z]
    (when-some [d (zdown z)]
      (loop [z d]
        (if-some [z (f z)]
          (zup z)
          (when-some [r (zright z)]
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
             (zreplace z))))

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

(def ^:private ^:dynamic *monoid*
  (fn
    ([] (ArrayList.))
    ([x] (vec x))
    ([^List x ^List y]
     (if y
       (doto x
         (.addAll y))
       x))))

;; TODO: should this short-circuit properly? Ztrategic doesn't seem
;; to.
(defn- seq-tu [x y]
  (fn [z]
    (let [m *monoid*]
      (-> (m)
          (m (x z))
          (m (y z))))))

(def choice-tu choice-tp)

(defn- all-tu [f]
  (fn [z]
    (let [m *monoid*
          acc (m)]
      (if-some [d (zdown z)]
        (loop [z d
               acc acc]
          (when-some [x (f z)]
            (let [acc (m acc x)]
              (if-some [r (zright z)]
                (recur r acc)
                acc))))
        acc))))

(defn- one-tu [f]
  (fn [z]
    (when-some [d (zdown z)]
      (loop [z d]
        (if-some [x (f z)]
          x
          (when-some [r (zright z)]
            (recur r)))))))

(defn- full-td-tu
  ([f]
   (fn self [z]
     ((seq-tu f (all-tu self)) z)))
  ([f z]
   ((full-td-tu f) z)))

(defn- full-bu-tu
  ([f]
   (fn self [z]
     ((seq-tu (all-tu self) f) z)))
  ([f z]
   ((full-bu-tu f) z)))

(defn- once-td-tu
  ([f]
   (fn self [z]
     ((choice-tu f (one-tu self)) z)))
  ([f z]
   ((once-td-tu f) z)))

(defn- once-bu-tu
  ([f]
   (fn self [z]
     ((choice-tu (one-tu self) f) z)))
  ([f z]
   ((once-bu-tu f) z)))

(defn- stop-td-tu
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

(defn collect
  ([f]
   (*monoid* (full-td-tu f)))
  ([f z]
   (*monoid* (full-td-tu f z)))
  ([f m z]
   (binding [*monoid* m]
     (m (full-td-tu f z)))))

(defn collect-stop
  ([f]
   (*monoid* (stop-td-tu f)))
  ([f z]
   (*monoid* (stop-td-tu f z)))
  ([f m z]
   (binding [*monoid* m]
     (m (stop-td-tu f z)))))

(def select once-td-tu)
