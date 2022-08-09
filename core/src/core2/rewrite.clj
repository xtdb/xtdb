(ns core2.rewrite
  (:require [clojure.walk :as w])
  (:import java.util.regex.Pattern
           [java.util ArrayList HashMap List Map Objects]
           [clojure.lang Box ILookup Indexed IObj IPersistentVector MapEntry]))

(set! *unchecked-math* :warn-on-boxed)

(deftype Zip [node ^int idx parent ^:unsynchronized-mutable ^int hash_ ^int depth]
  ILookup
  (valAt [_ k]
    (when (= :node k)
      node))

  (valAt [_ k not-found]
    (if (= :node k)
      node
      not-found))

  Indexed
  (count [_]
    (if (instance? IPersistentVector node)
      (.count ^IPersistentVector node)
      0))

  (nth [this idx]
    (Zip. (.nth ^IPersistentVector node idx) idx this 0 (unchecked-inc-int (.depth this))))

  (nth [this idx not-found]
    (Zip. (.nth ^IPersistentVector node idx not-found) idx this 0 (unchecked-inc-int (.depth this))))

  Object
  (equals [this other]
    (if (identical? this other)
      true
      (let [^Zip other other]
        (and (identical? Zip (.getClass other))
             (= (.idx this) (.idx other))
             (= (.depth this) (.depth other))
             (= (.node this) (.node other))
             (= (.parent this) (.parent other))))))

  (hashCode [this]
    (if (zero? hash_)
      (let [result (unchecked-int 1)
            prime (unchecked-int 31)
            result (unchecked-add-int (unchecked-multiply-int prime result)
                                      (Objects/hashCode node))
            result (unchecked-add-int (unchecked-multiply-int prime result)
                                      (Integer/hashCode idx))
            result (unchecked-add-int (unchecked-multiply-int prime result)
                                      (Objects/hashCode node))
            result (unchecked-add-int (unchecked-multiply-int prime result)
                                      (Integer/hashCode depth))]
        (set! (.hash_ this) result)
        result)
      hash_)))

(deftype StrategyDone [^Zip z])
(deftype StrategyRepeat [^Zip z])

(defmacro zstate-done? [r]
  `(let [^Object r# ~r]
     (identical? StrategyDone (.getClass r#))))

(defmacro zstate-repeat? [r]
  `(let [^Object r# ~r]
     (identical? StrategyRepeat (.getClass r#))))

(defmacro zip? [z]
  `(let [^Object z# ~z]
     (identical? Zip (.getClass z#))))

(defn ->zipper [x]
  (if (zip? x)
    x
    (Zip. x -1 nil 0 0)))

(defmacro zup [z]
  `(let [^Zip z# ~z]
     (when-let [^Zip parent# (.parent z#)]
       (let [node# (.node z#)
             ^IPersistentVector level# (.node parent#)
             idx# (.idx z#)]
         (if (identical? node# (.nth level# idx#))
           parent#
           (Zip. (.assocN level# idx# node#) (.idx parent#) (.parent parent#) 0 (.depth parent#)))))))

(defmacro znode [z]
  `(let [^Zip z# ~z]
     (.node z#)))

(defmacro zbranch? [z]
  `(instance? IPersistentVector (znode ~z)))

(defmacro zleft [z]
  `(let [^Zip z# ~z]
     (when-let [^Zip parent# (zup z#)]
       (let [idx# (unchecked-dec-int (.idx z#))
             ^IPersistentVector level# (.node parent#)]
         (when-some [child-node# (.nth level# idx# nil)]
           (Zip. child-node# idx# parent# 0 (.depth z#)))))))

(defmacro zleft-no-edit [z]
  `(let [^Zip z# ~z]
     (when-let [^Zip parent# (.parent z#)]
       (let [idx# (unchecked-dec-int (.idx z#))
             ^IPersistentVector level# (.node parent#)]
         (when-some [child-node# (.nth level# idx# nil)]
           (Zip. child-node# idx# parent# 0 (.depth z#)))))))

(defmacro zright [z]
  `(let [^Zip z# ~z]
     (when-let [^Zip parent# (zup z#)]
       (let [idx# (unchecked-inc-int (.idx z#))
             ^IPersistentVector level# (.node parent#)]
         (when-some [child-node# (.nth level# idx# nil)]
           (Zip. child-node# idx# parent# 0 (.depth z#)))))))

(defmacro zright-no-edit [z]
  `(let [^Zip z# ~z]
     (when-let [^Zip parent# (.parent z#)]
       (let [idx# (unchecked-inc-int (.idx z#))
             ^IPersistentVector level# (.node parent#)]
         (when-some [child-node# (.nth level# idx# nil)]
           (Zip. child-node# idx# parent# 0 (.depth z#)))))))

(defmacro znth [z idx]
  `(let [^Zip z# ~z
         ^IPersistentVector node# (.node z#)]
     (when (instance? IPersistentVector node#)
       (let [idx# (if (neg? ~idx)
                    (unchecked-add-int (.count node#) ~idx)
                    ~idx)]
         (when-some [child-node# (.nth node# idx# nil)]
           (Zip. child-node# idx# z# 0 (unchecked-inc-int (.depth z#))))))))

(defmacro zdown [z]
  `(let [^Zip z# ~z
         ^IPersistentVector node# (.node z#)]
     (when (instance? IPersistentVector node#)
       (when-some [child-node# (.nth node# 0 nil)]
         (Zip. child-node# 0 z# 0 (unchecked-inc-int (.depth z#)))))))

(defmacro zups [z depth]
  `(loop [^Zip z# ~z]
     (if (= ~depth (.depth z#))
       z#
       (recur (zup z#)))))

(defmacro zroot [z]
  `(loop [^Zip z# ~z]
     (if-let [z# (zup z#)]
       (recur z#)
       (.node z#))))

(defmacro zright-or-up [z depth]
  `(loop [^Zip z# ~z]
     (if (= ~depth (.depth z#))
       (StrategyDone. z#)
       (or (zright z#)
           (recur (zup z#))))))

(defmacro znext
  ([z]
   `(let [^Zip z# ~z]
      (znext z# (.depth z#))))
  ([z depth]
   `(let [^Zip z# ~z]
      (or (zdown z#)
          (zright-or-up z# ~depth)))))

(defmacro zright-or-up-bu [z depth out-fn]
  `(loop [^Zip z# ~z]
     (if (= ~depth (.depth z#))
       (StrategyDone. z#)
       (when-let [^Zip z# (~out-fn z#)]
         (if (zstate-repeat? z#)
           (.z ^StrategyRepeat z#)
           (or (zright z#)
               (recur (zup z#))))))))

(defmacro znext-bu [z depth out-fn]
  `(let [^Zip z# ~z]
     (or (zdown z#)
         (zright-or-up-bu z# ~depth ~out-fn))))

(defmacro zright-or-up-no-edit [z depth]
  `(loop [z# ~z]
     (when-not (= ~depth (.depth z#))
       (or (zright-no-edit z#)
           (recur (.parent z#))))))

(defmacro znext-no-edit [z depth]
  `(let [^Zip z# ~z]
     (or (zdown z#)
         (zright-or-up-no-edit z# ~depth))))

(defmacro zprev [z]
  `(let [^Zip z# ~z]
     (if-let [z# (zleft z#)]
       (loop [z# z#]
         (if-let [z# (znth z# -1)]
           (recur z#)
           z#))
       (zup z#))))

(defmacro zprev-no-edit [z]
  `(let [^Zip z# ~z]
     (if-let [^Zip z# (zleft-no-edit z#)]
       (loop [z# z#]
         (if-let [z# (znth z# -1)]
           (recur z#)
           z#))
       (.parent z#))))

(defmacro zchild-idx [z]
  `(let [^Zip z# ~z]
     (.idx z#)))

(defmacro zchildren [z]
  `(vec (.node ^Zip (.parent ~z))))

(defmacro zrights [z]
  `(let [^Zip z# ~z]
     (seq (subvec (zchildren z#) (unchecked-inc-int (.idx z#))))))

(defmacro zlefts [z]
  `(let [^Zip z# ~z]
     (seq (subvec (zchildren z#) 0 (.idx z#)))))

(defmacro zreplace [z x]
  `(let [^Zip z# ~z
         x# ~x]
     (if (identical? (.node z#) x#)
       z#
       (Zip. x# (.idx z#) (.parent z#) 0 (.depth z#)))))

;; Zipper pattern matching

;; https://julesjacobs.com/notes/patternmatching/patternmatching.pdf

(defn- init-clauses [var clauses]
  (-> (vec (for [[pattern body] (partition 2 clauses)]
             [{var pattern} body]))
      (conj [{var '_} (when (odd? (count clauses))
                        (last clauses))])))

(defn- add-bindings [clause zip?]
  (let [[patterns body] clause
        smap (->> (for [[k v] patterns
                        :when (symbol? v)]
                    [v (if (or (not zip?)
                               (:z (meta v)))
                         k
                         `(.node ~(with-meta k {:tag 'core2.rewrite.Zip})))])
                  (into {}))]
    [(into {} (remove (comp (partial contains? smap) val) patterns))
     (if (instance? IObj body)
       (vary-meta body update :bindings (fnil into []) (reduce into [] (dissoc smap '_)))
       body)]))

(defn- find-branch-var [patterns clauses]
  (let [freq (frequencies (for [[ps _] clauses
                                var (keys ps)]
                            var))]
    (apply max-key freq (keys patterns))))

(defmacro count-indexed [x]
  `(if (instance? Indexed ~x)
     (.count ~(with-meta x {:tag `Indexed}))
     (unchecked-int -1)))

(defn- if-expr? [x]
  (and (sequential? x)
       (= 'if (first x))
       (= 4 (count x))))

(defn- case-expr? [x]
  (and (sequential? x)
       (= `case (first x))))

(defn- equals-expr? [x]
  (and (sequential? x)
       (= `= (first x))
       (= 3 (count x))))

(defn- flatten-ifs-to-case [x]
  (w/postwalk
   (fn [x]
     (or (when (if-expr? x)
           (let [[_ cond-1 then-1 else-1] x]
             (when (equals-expr? cond-1)
               (cond
                 (if-expr? else-1)
                 (let [[_ cond-2 then-2 else-2] else-1]
                   (when (and (equals-expr? cond-2)
                              (= (last cond-1)
                                 (last cond-2)))
                     `(case ~(last cond-1)
                        ~(second cond-1)
                        ~then-1
                        ~(second cond-2)
                        ~then-2
                        ~else-2)))

                 (case-expr? else-1)
                 (let [[_ case-test & clauses] else-1]
                   (when (= (last cond-1) case-test)
                     `(case ~case-test
                        ~(second cond-1)
                        ~then-1
                        ~@clauses)))))))
         x))
   x))

(defn- generate-match-tree
  ([clauses]
   (generate-match-tree clauses false))
  ([clauses zip?]
   (if (empty? clauses)
     (throw (IllegalArgumentException. "Non-exhaustive pattern"))
     (let [[[patterns body] :as clauses] (for [clause clauses]
                                           (add-bindings clause zip?))]
       (if (empty? patterns)
         (if-let [bindings (not-empty (:bindings (meta body)))]
           `(let [~@bindings]
              ~body)
           body)
         (let [branch-var (find-branch-var patterns clauses)
               pattern-1 (get patterns branch-var)]
           (if (vector? pattern-1)
             (let [vars (vec (repeatedly (count pattern-1) #(gensym '__x_)))
                   yes-no (for [[patterns body :as clause] clauses]
                            (if (contains? patterns branch-var)
                              (let [pattern-2 (get patterns branch-var)]
                                (if (and (vector? pattern-2)
                                         (= (count pattern-1) (count pattern-2)))
                                  {:yes [(merge (dissoc patterns branch-var)
                                                (->> (for [[^long idx var] (map-indexed vector vars)
                                                           :when (not= '_ (nth pattern-2 idx))]
                                                       [var (nth pattern-2 idx)])
                                                     (into {})))
                                         body]}
                                  {:no clause}))
                              {:yes clause :no clause}))
                   yes-tree (generate-match-tree (keep :yes yes-no) zip?)
                   yes-vars (set (filter symbol? (flatten yes-tree)))]
               `(if (= ~(count pattern-1) (count-indexed ~branch-var))
                  (let [~@(->> (for [[^long idx var] (map-indexed vector vars)
                                     :when (contains? yes-vars var)]
                                 `[~var (.nth ~(with-meta branch-var {:tag `Indexed}) ~idx)])
                               (reduce into []))]
                    ~yes-tree)
                  ~(generate-match-tree (keep :no yes-no) zip?)))
             (let [yes-no (for [[patterns body :as clause] clauses]
                            (if (contains? patterns branch-var)
                              (let [pattern-2 (get patterns branch-var)]
                                (if (= pattern-1 pattern-2)
                                  {:yes [(dissoc patterns branch-var) body]}
                                  {:no clause}))
                              {:yes clause :no clause}))]
               `(if (~(cond
                        (keyword? pattern-1)
                        `identical?
                        (instance? Pattern pattern-1)
                        `re-find
                        :else
                        `=) ~pattern-1 ~(if zip?
                                          `(.node ~(with-meta branch-var {:tag 'core2.rewrite.Zip}))
                                          branch-var))
                  ~(generate-match-tree (keep :yes yes-no) zip?)
                  ~(generate-match-tree (keep :no yes-no) zip?))))))))))

(defmacro match-tree {:style/indent 1} [z zip? & clauses]
  (flatten-ifs-to-case (generate-match-tree (init-clauses z clauses) zip?)))

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
             node# (if (zip? z#)
                     (znode z#)
                     z#)]
         (match-tree
             node#
             false
             ~@(->> (for [[pattern expr] pattern+exprs]
                      [pattern expr])
                    (reduce into []))
             ~else-clause))
      `(let [z# (->zipper ~z)]
         (match-tree
             z#
             true
             ~@(->> (for [[pattern expr] pattern+exprs]
                      [pattern expr])
                    (reduce into []))
             ~else-clause)))))

;; Attribute Grammar spike.

;; See related literature:
;; https://inkytonik.github.io/kiama/Attribution (no code is borrowed)
;; https://arxiv.org/pdf/2110.07902.pdf
;; https://haslab.uminho.pt/prmartins/files/phd.pdf
;; https://github.com/christoff-buerger/racr

(defn ctor [^Zip ag]
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

(defmacro lexeme [ag n]
  `(some-> ($ ~ag ~n) (znode)))

(defmacro first-child? [ag]
  `(= 1 (count (zlefts ~ag))))

(defmacro left-or-parent [ag]
  `(let [^Zip ag# ~ag]
     (if (first-child? ag#)
       (parent ag#)
       (zleft ag#))))

(defmacro zcase {:style/indent 1} [ag & body]
  `(case (ctor ~ag) ~@body))

(def ^:dynamic *memo*)

(defn zmemoize-with-inherited [f]
  (fn [x]
    (let [^Map memo *memo*
          memo-box (Box. nil)]
      (loop [^Zip x x
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
  (fn [^Zip x]
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

(defn find-first [f z]
  (loop [z (zdown z)]
    (when z
      (if (f z)
        z
        (recur (zright z))))))

;; Strategic Zippers based on Ztrategic

;; https://arxiv.org/pdf/2110.07902.pdf
;; https://www.di.uminho.pt/~joost/publications/SBLP2004LectureNotes.pdf

;; Strafunski:
;; https://www.di.uminho.pt/~joost/publications/AStrafunskiApplicationLetter.pdf
;; https://arxiv.org/pdf/cs/0212048.pdf
;; https://arxiv.org/pdf/cs/0204015.pdf
;; https://arxiv.org/pdf/cs/0205018.pdf

;; Type Preserving

(defn choice-tp [x y]
  (fn [z]
    (if-some [z (x z)]
      z
      (y z))))

(defn full-td-tp [f ^Zip z]
  (let [depth (.depth z)]
    (loop [z z]
      (when-let [^Zip z (f z)]
        (let [z (znext z depth)]
          (if (zstate-done? z)
            (.z ^StrategyDone z)
            (recur z)))))))

(defn full-bu-tp [f ^Zip z]
  (let [depth (.depth z)]
    (loop [^Zip z z]
      (when-let [z (znext-bu z depth f)]
        (if (zstate-done? z)
          (f (.z ^StrategyDone z))
          (recur z))))))

(defn once-td-tp [f ^Zip z]
  (let [depth (.depth z)]
    (loop [z z]
      (if-let [^Zip z (f z)]
        (zups z depth)
        (when-let [z (znext z depth)]
          (when-not (zstate-done? z)
            (recur z)))))))

(defn z-try-apply-m [f]
  (fn [^Zip z]
    (some->> (f z)
             (zreplace z))))

(defn adhoc-tp [f g]
  (choice-tp (z-try-apply-m g) f))

(defn id-tp [x] x)

(defn fail-tp [_])

(def mono-tp (partial adhoc-tp fail-tp))

(defn innermost [f ^Zip z]
  (let [depth (.depth z)
        inner-f (fn [z]
                  (if-let [z (f z)]
                    (StrategyRepeat. z)
                    z))]
    (loop [^Zip z z]
      (if-let [z (znext-bu z depth inner-f)]
        (if (zstate-done? z)
          (if-let [z (f (.z ^StrategyDone z))]
            (recur z)
            (.z ^StrategyDone z))
          (recur z))))))

(def topdown full-td-tp)

(def bottomup full-bu-tp)

;; Type Unifying

(defn- into-array-list
  ([] (ArrayList. 0))
  ([x] (vec x))
  ([^List x ^List y]
   (if y
     (doto x
       (.addAll y))
     x)))

(defn collect
  ([f z]
   (collect f into-array-list z))
  ([f m ^Zip z]
   (let [depth (.depth z)]
     (loop [z z
            acc (m)]
       (let [acc (if-some [x (f z)]
                   (m acc x)
                   acc)]
         (if-let [z (znext-no-edit z depth)]
           (recur z acc)
           (m acc)))))))

(defn collect-stop
  ([f z]
   (collect-stop f into-array-list z))
  ([f m ^Zip z]
   (let [depth (.depth z)]
     (loop [z z
            acc (m)]
       (let [x (f z)
             stop? (some? x)
             acc (if stop?
                   (m acc x)
                   acc)]
         (if-let [z (if stop?
                      (zright-or-up-no-edit z depth)
                      (znext-no-edit z depth))]
           (recur z acc)
           (m acc)))))))
