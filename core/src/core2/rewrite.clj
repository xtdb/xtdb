(ns core2.rewrite
  (:require [clojure.walk :as w]
            [clojure.core.match :as m])
  (:import java.util.regex.Pattern
           [java.util ArrayList HashMap List Map]
           [clojure.lang Box IPersistentVector ILookup MapEntry]
           core2.BitUtil))

(set! *unchecked-math* :warn-on-boxed)

(deftype Zip [node ^int idx parent ^:unsynchronized-mutable ^int hash_]
  ILookup
  (valAt [_ k]
    (when (= :node k)
      node))

  (valAt [_ k not-found]
    (if (= :node k)
      node
      not-found))

  Object
  (equals [this other]
    (if (identical? this other)
      true
      (let [^Zip other other]
        (and (instance? Zip other)
             (= (.node this) (.node other))
             (= (.idx this) (.idx other))
             (= (.parent this) (.parent other))))))

  (hashCode [this]
    (if (zero? hash_)
      (let [result 1
            result (+ (* 31 result) (if node
                                      (long (.hashCode node))
                                      0))
            result (+ (* 31 result) (long (Integer/hashCode idx)))
            result (+ (* 31 result) (if parent
                                      (long (.hashCode parent))
                                      0))
            result (unchecked-int result)]
        (set! (.hash_ this) result)
        result)
      hash_)))

(defn ->zipper [x]
  (if (instance? Zip x)
    x
    (Zip. x -1 nil 0)))

(defn- zupdate-parent [^Zip z]
  (when-let [^Zip parent (.parent z)]
    (let [node (.node z)
          ^List level (.node parent)
          idx (.idx z)]
      (if (identical? node (.get level idx))
        parent
        (Zip. (.assocN ^IPersistentVector level idx node) (.idx parent) (.parent parent) 0)))))

(defn znode [^Zip z]
  (.node z))

(defn zbranch? [^Zip z]
  (vector? (.node z)))

(defn zleft [^Zip z]
  (when-let [^Zip parent (zupdate-parent z)]
    (let [idx (dec (.idx z))]
      (when (BitUtil/bitNot (neg? idx))
        (let [^List level (.node parent)]
          (Zip. (.get level idx) idx parent 0))))))

(defn zright [^Zip z]
  (when-let [^Zip parent (zupdate-parent z)]
    (let [idx (inc (.idx z))
          ^List level (.node parent)]
      (when (< idx (.size level))
        (Zip. (.get level idx) idx parent 0)))))

(defn znth [^Zip z ^long idx]
  (when (zbranch? z)
    (let [^List node (.node z)
          idx (if (neg? idx)
                (+ (.size node) idx)
                idx)]
      (when (and (< idx (.size node))
                 (BitUtil/bitNot (neg? idx)))
        (Zip. (.get node idx) idx z 0)))))

(defn zdown [^Zip z]
  (let [node (.node z)]
    (when (and (zbranch? z)
               (BitUtil/bitNot (.isEmpty ^List node)))
      (Zip. (.get ^List node 0) 0 z 0))))

(defn zup [^Zip z]
  (let [idx (.idx z)]
    (when (BitUtil/bitNot (neg? idx))
      (zupdate-parent z))))

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
      (Zip. x (.idx z) (.parent z) 0))))

;; Zipper pattern matching

(derive ::m/zip ::m/vector)

(defmethod m/nth-inline ::m/zip
  [t ocr i]
  `(let [^Zip z# ~ocr]
     (Zip. (.get ^List (.node z#) ~i) ~i z# 0)))

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

(defmacro vector-zip [x]
  `(->zipper ~x))
(defmacro node [x]
  `(znode ~x))
(defmacro root [x]
  `(zroot ~x))
(defmacro left [x]
  `(zleft ~x))
(defmacro right [x]
  `(zright ~x))
(defmacro prev [x]
  `(zprev ~x))
(defmacro parent [x]
  `(zup ~x))
(defmacro $ [x n]
  `(znth ~x ~n))
(defmacro child-idx [x]
  `(zchild-idx ~x))

(defn lexeme [ag ^long n]
  (some-> ($ ag n) (znode)))

(defn first-child? [ag]
  (= 1 (count (zlefts ag))))

(defn left-or-parent [ag]
  (if (first-child? ag)
    (parent ag)
    (zleft ag)))

(defmacro zcase {:style/indent 1} [ag & body]
  `(case (ctor ~ag) ~@body))

(def ^:dynamic *memo*)

(defn zmemoize-with-inherited [f]
  (fn [x]
    (let [^Map memo *memo*
          memo-box (Box. nil)]
      (loop [x x
             inherited? false]
        (let [k (MapEntry/create f x)
              ^Box stored-memo-box (.getOrDefault memo k memo-box)]
          (if (identical? memo-box stored-memo-box)
            (let [v (f x)]
              (.put memo k memo-box)
              (if (= ::inherit v)
                (some-> x (parent) (recur true))
                (do (set! (.val memo-box) v)
                    v)))
            (let [v (.val stored-memo-box)]
              (when inherited?
                (set! (.val memo-box) v))
              v)))))))

(defn zmemoize [f]
  (fn [x]
    (let [^Map memo *memo*
          k (MapEntry/create f x)
          v (.getOrDefault memo k ::not-found)]
      (if (= ::not-found v)
        (let [v (f x)]
          (if (= ::inherit v)
            (some-> x (parent) (recur))
            (doto v
              (->> (.put memo k)))))
        v))))

;; Strategic Zippers based on Ztrategic

;; https://arxiv.org/pdf/2110.07902.pdf
;; https://www.di.uminho.pt/~joost/publications/SBLP2004LectureNotes.pdf

;; Strafunski:
;; https://www.di.uminho.pt/~joost/publications/AStrafunskiApplicationLetter.pdf
;; https://arxiv.org/pdf/cs/0212048.pdf
;; https://arxiv.org/pdf/cs/0204015.pdf
;; https://arxiv.org/pdf/cs/0205018.pdf

;; Type Preserving

(defn seq-tp
  ([x y]
   (fn [z]
     (seq-tp x y z)))
  ([x y z]
   (when-some [z (x z)]
     (y z))))

(defn choice-tp
  ([x y]
   (fn [z]
     (choice-tp x y z)))
  ([x y z]
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
     (seq-tp f (all-tp self) z)))
  ([f z]
   ((full-td-tp f) z)))

(defn full-bu-tp
  ([f]
   (fn self [z]
     (seq-tp (all-tp self) f z)))
  ([f z]
   ((full-bu-tp f) z)))

(defn once-td-tp
  ([f]
   (fn self [z]
     (choice-tp f (one-tp self) z)))
  ([f z]
   ((once-td-tp f) z)))

(defn once-bu-tp
  ([f]
   (fn self [z]
     (choice-tp (one-tp self) f z)))
  ([f z]
   ((once-bu-tp f) z)))

(defn stop-td-tp
  ([f]
   (fn self [z]
     (choice-tp f (all-tp self) z)))
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
     (seq-tp (all-tp self) (try-tp (seq-tp f self)) z)))
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
(defn- seq-tu
  ([x y]
   (fn [z]
     (seq-tu x y z)))
  ([x y z]
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
     (seq-tu f (all-tu self) z)))
  ([f z]
   ((full-td-tu f) z)))

(defn- full-bu-tu
  ([f]
   (fn self [z]
     (seq-tu (all-tu self) f z)))
  ([f z]
   ((full-bu-tu f) z)))

(defn- once-td-tu
  ([f]
   (fn self [z]
     (choice-tu f (one-tu self) z)))
  ([f z]
   ((once-td-tu f) z)))

(defn- once-bu-tu
  ([f]
   (fn self [z]
     (choice-tu (one-tu self) f z)))
  ([f z]
   ((once-bu-tu f) z)))

(defn- stop-td-tu
  ([f]
   (fn self [z]
     (choice-tu f (all-tu self) z)))
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
