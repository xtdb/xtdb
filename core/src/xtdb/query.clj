(ns ^:no-doc xtdb.query
  (:require [clojure.java.io :as io]
            [clojure.pprint :as pp]
            [clojure.set :as set]
            [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            [clojure.walk :as w]
            [juxt.clojars-mirrors.dependency.v1v0v0.com.stuartsierra.dependency :as dep]
            [juxt.clojars-mirrors.eql.v2021v02v28.edn-query-language.core :as eql]
            [juxt.clojars-mirrors.nippy.v3v1v1.taoensso.nippy :as nippy]
            [xtdb.api :as xt]
            [xtdb.bus :as bus]
            [xtdb.cache :as cache]
            [xtdb.codec :as c]
            [xtdb.db :as db]
            [xtdb.error :as err]
            [xtdb.index :as idx]
            [xtdb.io :as xio]
            [xtdb.memory :as mem]
            [xtdb.pull :as pull]
            [xtdb.system :as sys]
            [xtdb.tx :as tx]
            [xtdb.with-tx :as with-tx])
  (:import (clojure.lang Box ExceptionInfo MapEntry)
           (java.io Closeable Writer)
           (java.util Collection Comparator Date HashMap List Map UUID)
           (java.util.concurrent Executors Future ScheduledExecutorService TimeoutException TimeUnit)
           (java.util.function Function)
           (org.agrona DirectBuffer)
           (xtdb.codec EntityTx)))

(defrecord VarBinding [result-index])

(defn logic-var? [x]
  (and (symbol? x)
       (not (contains? '#{... . $ %} x))))

(def literal? (complement (some-fn vector? logic-var?)))

(declare pred-constraint aggregate)

(defn- expression-spec [sym spec]
  (s/and seq?
         #(= sym (first %))
         (s/conformer next (fn [v] [sym v]))
         spec))

(def ^:private built-ins '#{and})

(defn- pred-constraint? [x]
  (contains? (methods pred-constraint) x))

(defn- aggregate? [x]
  (contains? (methods aggregate) x))

(s/def ::triple
  (s/and vector? (s/cat :e (some-fn logic-var? c/valid-id? set?)
                        :a (s/and c/valid-id? some?)
                        :v (s/? (some-fn logic-var? literal?)))))

(s/def ::args-list (s/coll-of logic-var? :kind vector? :min-count 1))

(s/def ::pred-fn
  (s/and symbol?
         (complement built-ins)
         (s/conformer #(or (if (pred-constraint? %)
                             %
                             (if (qualified-symbol? %)
                               (requiring-resolve %)
                               (ns-resolve 'clojure.core %)))
                           %))
         (some-fn var? logic-var?)))

(s/def ::binding
  (s/or :scalar logic-var?
        :tuple ::args-list
        :collection (s/tuple logic-var? '#{...})
        :relation (s/tuple ::args-list)))

(s/def ::pred
  (s/and vector? (s/cat :pred (s/and seq?
                                     (s/cat :pred-fn ::pred-fn
                                            :args (s/* any?)))
                        :return (s/? ::binding))))

(s/def ::rule (s/and list? (s/cat :name (s/and symbol? (complement built-ins))
                                  :args (s/+ any?))))

(s/def ::range-op '#{< <= >= > =})
(s/def ::range
  (s/tuple (s/and list?
                  (s/or :sym-val (s/cat :op ::range-op
                                        :sym logic-var?
                                        :val literal?)
                        :val-sym (s/cat :op ::range-op
                                        :val literal?
                                        :sym logic-var?)
                        :sym-sym (s/cat :op ::range-op
                                        :sym-a logic-var?
                                        :sym-b logic-var?)))))

(s/def ::rule-args
  (s/alt :explicit-bound-args (s/cat :bound-args ::args-list
                                     :free-args (s/* logic-var?))
         :just-args (s/* logic-var?)))

(s/def ::not
  (expression-spec 'not (s/+ ::term)))

(s/def ::not-join
  (expression-spec 'not-join (s/cat :args ::args-list
                                    :terms (s/+ ::term))))

(s/def ::and (expression-spec 'and (s/+ ::term)))

(s/def ::or-branches
  (s/+ (s/and (s/or :term ::term
                    :and ::and)
              (s/conformer (fn [[type arg]]
                             (case type
                               :term [arg]
                               :and arg))))))

(s/def ::or
  (s/and (expression-spec 'or ::or-branches)
         (s/conformer (fn [branches]
                        {:branches branches}))))

(s/def ::or-join
  (expression-spec 'or-join (s/cat :args (s/and vector? ::rule-args)
                                   :branches ::or-branches)))

(s/def ::term
  (s/or :triple ::triple
        :not ::not
        :not-join ::not-join
        :or ::or
        :or-join ::or-join
        :range ::range
        :rule ::rule
        :pred ::pred))

(s/def ::aggregate
  (s/cat :aggregate-fn aggregate?
         :args (s/* literal?)
         :form ::form))

(s/def ::form
  (s/or :nil nil?
        :boolean boolean?
        :number number?
        :string string?
        :symbol symbol?
        :keyword keyword?
        :aggregate ::aggregate
        :set (s/coll-of ::form, :kind set?)
        :vector (s/coll-of ::form, :kind vector?)
        :list (s/coll-of ::form, :kind list?)
        :map (s/map-of ::form ::form, :conform-keys true)))

(s/def ::pull-arg
  (s/cat :pull #{'pull}
         :logic-var logic-var?
         :pull-spec ::eql/query))

(s/def ::find-arg
  (s/or :pull ::pull-arg
        :form ::form))

(s/def ::find (s/coll-of ::find-arg :kind vector? :min-count 1))

(s/def ::keys (s/coll-of symbol? :kind vector?))
(s/def ::syms (s/coll-of symbol? :kind vector?))
(s/def ::strs (s/coll-of symbol? :kind vector?))

(s/def ::where (s/coll-of ::term :kind vector? :min-count 1))

(s/def ::arg-tuple (s/map-of (some-fn logic-var? keyword?) any?))
(s/def ::args (s/coll-of ::arg-tuple :kind vector?))

(s/def ::rule-head
  (s/and list?
         (s/cat :name (s/and symbol? (complement built-ins))
                :args ::rule-args)))

(s/def ::rule-definition
  (s/and vector?
         (s/cat :head ::rule-head
                :body (s/+ ::term))))

(s/def ::rules (s/coll-of ::rule-definition :kind vector? :min-count 1))

(s/def ::offset nat-int?)
(s/def ::limit nat-int?)

(s/def ::order-element
  (s/and vector?
         (s/cat :find-arg (s/or :form ::form)
                :direction (s/? #{:asc :desc}))))

(s/def ::order-by (s/coll-of ::order-element :kind vector?))

(s/def ::timeout nat-int?)
(s/def ::batch-size pos-int?)

(s/def ::in
  (s/and vector? (s/cat :source-var (s/? '#{$})
                        :bindings (s/* ::binding))))

(defmulti pred-args-spec first)

(defmethod pred-args-spec 'q [_]
  (s/cat :pred-fn #{'q}
         :args (s/spec (s/cat :query (s/or :quoted-query (s/cat :quote #{'quote} :query ::query)
                                           :query ::query)
                              :args (s/* any?)))
         :return (s/? ::binding)))

(defmethod pred-args-spec 'get-attr [_]
  (s/cat :pred-fn #{'get-attr}
         :args (s/spec (s/cat :e-var logic-var?, :attr literal?, :not-found (s/? any?)))
         :return (s/? ::binding)))

(defmethod pred-args-spec 'get-start-valid-time [_]
  (s/cat :pred-fn #{'get-start-valid-time}
         :args (s/spec (s/cat :e-var logic-var?))
         :return logic-var?))

(defmethod pred-args-spec 'get-end-valid-time [_]
  (s/cat :pred-fn #{'get-end-valid-time}
         :args (s/spec (s/cat :e-var logic-var?, :not-found (s/? any?)))
         :return logic-var?))

(defmethod pred-args-spec '== [_]
  (s/cat :pred-fn #{'==} :args (s/tuple some? some?)))

(defmethod pred-args-spec '!= [_]
  (s/cat :pred-fn #{'!=} :args (s/tuple some? some?)))

(defmethod pred-args-spec :default [_]
  (s/cat :pred-fn (s/or :var logic-var? :fn fn?)
         :args (s/coll-of any?)
         :return (s/? ::binding)))

(s/def ::pred-args (s/multi-spec pred-args-spec first))

(defmulti aggregate-args-spec first)

(defmethod aggregate-args-spec 'max [_]
  (s/cat :aggregate-fn '#{max}
         :args (s/or :zero empty?, :one (s/tuple pos-int?))))

(defmethod aggregate-args-spec 'min [_]
  (s/cat :aggregate-fn '#{min}
         :args (s/or :zero empty?, :one (s/tuple pos-int?))))

(defmethod aggregate-args-spec 'rand [_]
  (s/cat :aggregate-fn '#{rand}
         :args (s/tuple pos-int?)))

(defmethod aggregate-args-spec 'sample [_]
  (s/cat :aggregate-fn '#{sample}
         :args (s/tuple pos-int?)))

(defmethod aggregate-args-spec :default [_]
  (s/cat :aggregate-fn symbol?
         :args empty?))

(s/def ::aggregate-args (s/multi-spec aggregate-args-spec first))

(defn normalize-query [q]
  (cond
    (vector? q) (into {} (for [[[k] v] (->> (partition-by keyword? q)
                                            (partition-all 2))]
                           [k (if (and (or (nat-int? (first v))
                                           (boolean? (first v)))
                                       (= 1 (count v)))
                                (first v)
                                (vec v))]))
    (string? q) (if-let [q (try
                             (c/read-edn-string-with-readers q)
                             (catch Exception _))]
                  (normalize-query q)
                  q)
    :else q))

(s/def ::query
  (s/and (s/conformer #'normalize-query)

         (s/keys :req-un [::find]
                 :opt-un [::keys ::syms ::strs
                          ::in ::where ::args ::rules
                          ::offset ::limit ::order-by
                          ::timeout ::batch-size])

         (fn [{:keys [find] :as q}]
           (->> (keep q [:keys :syms :strs])
                (every? (fn [ks]
                          (= (count ks) (count find))))))))

(defrecord ConformedQuery [q-normalized q-conformed])

(defn- normalize-and-conform-query ^ConformedQuery [conform-cache q]
  (let [{:keys [args] :as q} (try
                               (normalize-query q)
                               (catch Exception _
                                 q))
        conformed-query (cache/compute-if-absent
                         conform-cache
                         (if (map? q)
                           (dissoc q :args)
                           q)
                         identity
                         (fn [q]
                           (when-not (s/valid? ::query q)
                             (throw (err/illegal-arg :query-spec-failed
                                                     {::err/message "Query didn't match expected structure"
                                                      :explain (s/explain-data ::query q)})))
                           (let [q (normalize-query q)]
                             (->ConformedQuery q (s/conform ::query q)))))]
    (if args
      (do
        (when-not (s/valid? ::args args)
          (throw (err/illegal-arg :args-spec-failed
                                  {::err/message "Query args didn't match expected structure"
                                   :explain (s/explain-data ::args args)})))
        (-> conformed-query
            (assoc-in [:q-normalized :args] args)
            (assoc-in [:q-conformed :args] args)))
      conformed-query)))

(declare open-index-snapshot build-sub-query)

;; NOTE: :min-count generates boxed math warnings, so this goes below
;; the spec.
(set! *unchecked-math* :warn-on-boxed)

#_{:clj-kondo/ignore [:unused-binding]}
(defmulti pred-constraint
  (fn [{:keys [pred return] {:keys [pred-fn]} :pred :as clause}
       {:keys [value-serde idx-id arg-bindings return-type
               return-vars-tuple-idxs-in-join-order rule-name->rules]}]
    pred-fn))

#_{:clj-kondo/ignore [:unused-binding]}
(defmulti aggregate (fn [name & args] name))

(set! *unchecked-math* true)

(defn- maybe-ratio [n]
  (if (ratio? n)
    (double n)
    n))

(defmethod aggregate 'count [_]
  (fn aggregate-count
    (^long [] 0)
    (^long [^long acc] acc)
    (^long [^long acc _] (inc acc))))

(defmethod aggregate 'count-distinct [_]
  (fn aggregate-count-distinct
    ([] (transient #{}))
    ([acc] (count (persistent! acc)))
    ([acc x] (conj! acc x))))

(defmethod aggregate 'sum [_]
  (fn aggregate-sum
    ([] 0)
    ([acc] acc)
    ([acc x] (+ acc x))))

(defmethod aggregate 'avg [_]
  (let [count (aggregate 'count)
        sum (aggregate 'sum)]
    (fn aggregate-average
      ([] [(count) (sum)])
      ([[c s]] (maybe-ratio (/ s c)))
      ([[c s] x]
       [(count c x) (sum s x)]))))

(defmethod aggregate 'median [_]
  (fn aggregate-median
    ([] (transient []))
    ([acc] (let [acc (persistent! acc)
                 acc (sort acc)
                 n (count acc)
                 half-n (quot n 2)]
             (if (odd? n)
               (nth acc half-n)
               (maybe-ratio (/ (+ (nth acc half-n)
                                  (nth acc (dec half-n))) 2)))))
    ([acc x] (conj! acc x))))

(defmethod aggregate 'variance [_]
  (let [mean (aggregate 'avg)]
    (fn aggregate-variance
      ([] [0 (mean)])
      ([[m2 [c _]]] (maybe-ratio (/ m2 c)))
      ([[m2 [c _ :as m]] x]
       (let [delta (if (zero? c)
                     x
                     (- x (mean m)))
             m (mean m x)
             delta2 (- x (mean m))]
         [(+ m2 (* delta delta2)) m])))))

(defmethod aggregate 'stddev [_]
  (let [variance (aggregate 'variance)]
    (fn aggregate-stddev
      ([] (variance))
      ([v] (Math/sqrt (variance v)))
      ([v x]
       (variance v x)))))

(defmethod aggregate 'distinct [_]
  (fn aggregate-distinct
    ([] (transient #{}))
    ([acc] (persistent! acc))
    ([acc x] (conj! acc x))))

(defmethod aggregate 'rand [_ n]
  (fn aggregate-rand
    ([] (transient []))
    ([acc] (->> (persistent! acc)
                (partial shuffle)
                repeatedly
                (apply concat)
                (take n)
                vec))
    ([acc x] (conj! acc x))))

(defmethod aggregate 'sample [_ n]
  (fn aggregate-sample
    ([] (transient #{}))
    ([acc] (vec (take n (shuffle (persistent! acc)))))
    ([acc x] (conj! acc x))))

(defmethod aggregate 'min
  ([_]
   (fn aggregate-min
     ([])
     ([acc] acc)
     ([acc x]
      (if acc
        (if (pos? (compare acc x))
          x
          acc)
        x))))
  ([_ n]
   (fn aggregate-min-n
     ([] (sorted-set))
     ([acc] (vec acc))
     ([acc x]
      (let [acc (conj acc x)]
        (if (> (count acc) n)
          (disj acc (last acc))
          acc))))))

(defmethod aggregate 'max
  ([_]
   (fn aggregate-max
     ([])
     ([acc] acc)
     ([acc x]
      (if acc
        (if (neg? (compare acc x))
          x
          acc)
        x))))
  ([_ n]
   (fn aggregate-max-n
     ([] (sorted-set))
     ([acc] (vec acc))
     ([acc x]
      (let [acc (conj acc x)]
        (if (> (count acc) n)
          (disj acc (first acc))
          acc))))))

(set! *unchecked-math* :warn-on-boxed)

;; NOTE: important that `normalize-clause` is idempotent - in sub queries,
;; we pass the sub-query back to `normalize-clauses` again

#_{:clj-kondo/ignore #{:unused-binding}}
(defmulti normalize-clause (fn [type clause] type), :default ::default)
(alter-meta! #'normalize-clause assoc :private true)

(defmethod normalize-clause ::default [type clause] [[type clause]])

(defn- normalize-clauses [clauses]
  (->> clauses
       (mapcat #(apply normalize-clause %))))

(defn- group-clauses-by-type [clauses]
  (->> clauses
       (reduce (fn group-by-type [acc [type clause]]
                 (-> acc (update type (fnil conj []) clause)))
               {})))

(defn- find-binding-vars [binding]
  (some->> binding (vector) (flatten) (filter logic-var?)))

(defn- collect-vars [{triple-clauses :triple
                      not-join-clauses :not-join
                      or-join-clauses :or-join
                      pred-clauses :pred
                      range-clauses :range
                      rule-clauses :rule}]
  {:e-vars (set (for [{:keys [e]} triple-clauses
                      :when (logic-var? e)]
                  e))
   :v-vars (set (for [{:keys [v]} triple-clauses
                      :when (logic-var? v)]
                  v))
   :not-vars (set (for [not-join-clause not-join-clauses
                        arg (:args not-join-clause)]
                    arg))
   :pred-arg-vars (set (for [{:keys [pred]} pred-clauses
                             var (cond->> (:args pred)
                                   (not (pred-constraint? (:pred-fn pred))) (cons (:pred-fn pred)))
                             :when (logic-var? var)]
                         var))
   :pred-return-vars (set (for [{:keys [return]} pred-clauses
                                return-var (find-binding-vars return)]
                            return-var))
   :range-vars (set (for [{:keys [sym sym-a sym-b]} range-clauses
                          sym [sym sym-a sym-b]
                          :when (logic-var? sym)]
                      sym))
   :or-vars (set (for [or-join-clause or-join-clauses
                       :let [[args-type args-val] (:args or-join-clause)]
                       arg (case args-type
                             :explicit-bound-args (mapcat args-val [:bound-args :free-args])
                             :just-args args-val)]
                   arg))
   :rule-vars (set (for [{:keys [args]} rule-clauses
                         arg args
                         :when (logic-var? arg)]
                     arg))})

(defn- blank-var? [v]
  (and (logic-var? v)
       (re-find #"^_\d*$" (name v))))

(defmethod normalize-clause :triple [_ clause]
  (as-> clause {:keys [e a v] :as clause}
    (cond-> clause
      (or (blank-var? v)
          (not (contains? clause :v)))
      (-> (assoc :v (gensym (or v "_")))
          (with-meta {:ignore-v? true}))

      (blank-var? e)
      (assoc :e (gensym e))

      (or (nil? a) (= a :xt/id))
      (assoc :a :crux.db/id))

    ;; self join
    (if (and (logic-var? e) (= e v))
      (let [v-var (gensym (str "self-join_" v "_"))]
        [[:triple (-> (assoc clause :v v-var)
                      (with-meta {:self-join? true}))]
         [:pred {:pred {:pred-fn '==, :args [v-var e]}}]])
      [[:triple clause]])))

(def ^:private pred->built-in-range-pred
  {< (comp neg? compare)
   <= (comp not pos? compare)
   > (comp pos? compare)
   >= (comp not neg? compare)
   = =})

(defmethod normalize-clause :pred [_ {:keys [pred return] :as clause}]
  [[:pred (let [{:keys [pred-fn args]} pred
                range-pred (and (= 2 (count args))
                                (every? logic-var? args)
                                (get pred->built-in-range-pred pred-fn))]
            (cond-> clause
              range-pred (assoc-in [:pred :pred-fn] range-pred)
              return (assoc :return (w/postwalk #(cond-> % (blank-var? %) gensym)
                                                return))))]])

(def ^:private range->inverse-range
  '{< >
    <= >=
    > <
    >= <=
    = =})

(defmethod normalize-clause :range [_ [[order clause]]]
  (let [[order clause] (if (= :sym-sym order) ;; NOTE: to deal with rule expansion
                         (let [{:keys [op sym-a sym-b]} clause]
                           (cond
                             (literal? sym-a)
                             [:val-sym {:op op :val sym-a :sym sym-b}]
                             (literal? sym-b)
                             [:sym-val {:op op :val sym-b :sym sym-a}]
                             :else
                             [order clause]))
                         [order clause])
        {:keys [op sym val] :as clause} (cond-> clause
                                          (= :val-sym order) (update :op range->inverse-range))]
    (if (and (not= :sym-sym order)
             (not (logic-var? sym)))
      [[:pred {:pred {:pred-fn (get pred->built-in-range-pred (var-get (resolve op)))
                      :args [sym val]}}]]

      [[:range clause]])))

(defmethod normalize-clause :or [_ {:keys [branches]}]
  [[:or-join {:args [:just-args (->> (mapcat normalize-clauses branches)
                                     (group-clauses-by-type)
                                     (collect-vars)
                                     (into #{} (comp (mapcat val) (remove blank-var?))))]
              :branches branches}]])

(defmethod normalize-clause :not [_ terms]
  [[:not-join {:args (->> (normalize-clauses terms)
                          (group-clauses-by-type)
                          (collect-vars)
                          (into #{} (mapcat val)))
               :terms terms}]])

(defn- ->approx-in-var-cardinalities [{in-bindings :bindings} in-args]
  (assert (= (count in-bindings) (count in-args))
          (pr-str [(count in-bindings) (count in-args)]))

  (->> (mapcat (fn [[b-type :as in-binding] in-arg]
                 (let [b-vars (find-binding-vars in-binding)
                       cardinality (case b-type
                                     (:scalar :tuple) 1
                                     (:collection :relation) (Long/highestOneBit (count in-arg)))]
                   (case b-type
                     (:scalar :collection)
                     [(MapEntry/create (first b-vars) cardinality)]

                     (:tuple :relation)
                     (for [b-var b-vars]
                       (MapEntry/create b-var cardinality)))))
               in-bindings in-args)
       (into {})))

(defn- ->binary-index-fn [{:keys [e a v] :as clause}]
  (let [attr-buffer (mem/copy-to-unpooled-buffer (c/->id-buffer a))]
    (fn [{:keys [entity-resolver-fn index-snapshot]} {:keys [vars-in-join-order]}]
      (let [order (filter #(or (= e %) (= v %)) vars-in-join-order)
            nested-index-snapshot (db/open-nested-index-snapshot index-snapshot)]
        (if (= v (first order))
          (let [v-idx (idx/new-deref-index
                       (idx/new-seek-fn-index
                        (fn [k]
                          (db/av nested-index-snapshot attr-buffer k))))
                e-idx (idx/new-seek-fn-index
                       (fn [k]
                         (db/ave nested-index-snapshot attr-buffer (.deref v-idx) k entity-resolver-fn)))]
            (log/debug :join-order :ave (xio/pr-edn-str v) e (xio/pr-edn-str clause))
            (idx/new-n-ary-join-layered-virtual-index [v-idx e-idx]))
          (let [e-idx (idx/new-deref-index
                       (idx/new-seek-fn-index
                        (fn [k]
                          (db/ae nested-index-snapshot attr-buffer k))))
                v-idx (idx/new-seek-fn-index
                       (fn [k]
                         (db/aev nested-index-snapshot attr-buffer (.deref e-idx) k entity-resolver-fn)))]
            (log/debug :join-order :aev e (xio/pr-edn-str v) (xio/pr-edn-str clause))
            (idx/new-n-ary-join-layered-virtual-index [e-idx v-idx])))))))

(defn- ->literal-index-fn [v value-serde]
  (if (c/multiple-values? v)
    (let [v-bufs (mapv (partial db/encode-value value-serde) v)]
      (fn [_ _]
        (idx/new-collection-virtual-index v-bufs)))
    (let [v-buf (db/encode-value value-serde v)]
      (fn [_ _]
        (idx/new-scalar-virtual-index v-buf)))))

(defn- triple-joins [triple-clauses value-serde var->joins]
  (reduce (fn [var->joins {:keys [e v] :as clause}]
            (let [join {:id (gensym "triple")
                        :idx-fn (->binary-index-fn clause)}]
              (-> var->joins
                  (update v (fnil conj []) join)
                  (update e (fnil conj []) join)

                  (cond-> (literal? e) (update e conj {:idx-fn (->literal-index-fn e value-serde)})
                          (literal? v) (update v conj {:idx-fn (->literal-index-fn v value-serde)})))))
          var->joins
          triple-clauses))

(defn- in-joins [in var->joins]
  (reduce
   (fn [[acc var->joins] in]
     (let [bind-vars (find-binding-vars in)
           idx-id (gensym "in")
           join {:id idx-id
                 :idx-fn (fn [_ _]
                           (idx/new-relation-virtual-index [] (count bind-vars)))}]
       [(conj acc idx-id)
        (->> bind-vars
             (reduce
              (fn [var->joins bind-var]
                (-> var->joins
                    (update bind-var (fnil conj []) join)))
              var->joins))]))
   [[] var->joins]
   in))

(defn- pred-joins [pred-clauses var->joins]
  (->> pred-clauses
       (reduce
        (fn [[pred-clause+idx-ids var->joins] {:keys [return] :as pred-clause}]
          (if return
            (let [idx-id (gensym "pred-return")
                  return-vars (find-binding-vars return)
                  join {:id idx-id
                        :idx-fn (fn [_ _]
                                  (idx/new-relation-virtual-index [] (count return-vars)))}]
              [(conj pred-clause+idx-ids [pred-clause idx-id])
               (->> return-vars
                    (reduce
                     (fn [var->joins return-var]
                       (-> var->joins
                           (update return-var (fnil conj []) join)))
                     var->joins))])
            [(conj pred-clause+idx-ids [pred-clause])
             var->joins]))
        [[] var->joins])))

;; TODO: This is a naive, but not totally irrelevant measure. Aims to
;; bind variables as early and cheaply as possible.
(defn- clause-complexity [clause]
  (count (xio/pr-edn-str clause)))

(defn- single-e-var-triple? [vars where]
  (and (= 1 (count where))
       (let [[[type {:keys [e v]}]] where]
         (and (= :triple type)
              (contains? vars e)
              (logic-var? e)
              (literal? v)
              (not (c/multiple-values? v))))))

;; NOTE: this isn't exact, used to detect vars that can be bound
;; before an or sub query. Is there a better way to incrementally
;; build up the join order? Done until no new vars are found to catch
;; all vars, doesn't care about cyclic dependencies, these will be
;; caught by the real dependency check later.
(defn- add-pred-returns-bound-at-top-level [known-vars pred-clauses]
  (loop [known-vars known-vars]
    (let [new-known-vars (->> pred-clauses
                              (reduce
                               (fn [acc {:keys [pred return]}]
                                 (if (->> (cond->> (:args pred)
                                            (not (pred-constraint? (:pred-fn pred))) (cons (:pred-fn pred)))
                                          (filter logic-var?)
                                          (set)
                                          (set/superset? acc))
                                   (apply conj acc (find-binding-vars return))
                                   acc))
                               known-vars))]
      (if (= new-known-vars known-vars)
        new-known-vars
        (recur new-known-vars)))))

(defn- analyze-or-vars [{or-join-clauses :or-join, :as type->clauses} known-vars]
  (let [[or-join-clauses known-vars]
        (->> (sort-by clause-complexity or-join-clauses)
             (reduce (fn [[or-clauses known-vars] {:keys [args branches] :as clause}]
                       (letfn [(assert-distinct-or-join-vars [{:keys [free-args]}]
                                 (when-not (= (count free-args)
                                              (count (set free-args)))
                                   (throw (err/illegal-arg :indistinct-or-join-vars
                                                           {::err/message "Or join free variables not distinct"
                                                            :clause clause
                                                            :free-args free-args}))))

                               (assert-branches-have-same-free-vars [branch-vars free-args]
                                 (when-let [missing-free-vars (not-empty (set/difference (set free-args) branch-vars))]
                                   (throw (err/illegal-arg :missing-vars-in-or-branch
                                                           {::err/message "`or` branches must have the same free variables"
                                                            :free-vars free-args
                                                            :branch-vars branch-vars
                                                            :missing missing-free-vars
                                                            :clause clause}))))]

                         (let [[args-type args-val] args
                               {:keys [bound-args free-args]} (case args-type
                                                                :explicit-bound-args (doto args-val (assert-distinct-or-join-vars))
                                                                :just-args (let [args (set args-val)]
                                                                             {:bound-args (set/intersection args known-vars)
                                                                              :free-args (set/difference args known-vars)}))

                               branches (for [branch-clauses branches]
                                          (let [branch-vars (->> (normalize-clauses branch-clauses)
                                                                 (group-clauses-by-type)
                                                                 (collect-vars)
                                                                 (vals)
                                                                 (into #{} (comp cat (remove blank-var?))))
                                                bound-vars (set/intersection branch-vars bound-args)]
                                            (assert-branches-have-same-free-vars branch-vars free-args)

                                            (-> branch-clauses
                                                (vary-meta (fnil into {})
                                                           {:single-e-var-triple? (single-e-var-triple? bound-vars branch-clauses)
                                                            :bound-vars bound-vars
                                                            :free-vars free-args}))))]

                           [(conj or-clauses (-> {:args [:explicit-bound-args {:bound-args bound-args, :free-args free-args}]
                                                  :branches branches}
                                                 (with-meta (meta clause))))
                            (into known-vars free-args)])))

                     [[] known-vars]))]
    [(-> type->clauses
         (assoc :or-join or-join-clauses))
     known-vars]))

(defn- or-joins [or-clauses var->joins]
  (->> or-clauses
       (reduce (fn [[acc var->joins] {:keys [args], :as or-clause}]
                 (let [[args-type {:keys [free-args]}] args
                       _ (assert (= args-type :explicit-bound-args))
                       idx-id (gensym "or-free-vars")
                       join (when (seq free-args)
                              {:id idx-id
                               :idx-fn (fn [_ _]
                                         (idx/new-relation-virtual-index [] (count free-args)))})]
                   [(conj acc
                          (-> or-clause (vary-meta assoc :idx-id idx-id)))
                    (reduce (fn [var->joins free-var]
                              (-> var->joins (update free-var (fnil conj []) join)))
                            var->joins
                            free-args)]))
               [[] var->joins])))

(defn- calculate-constraint-join-depth [var->bindings vars]
  (->> (for [var vars]
         (get-in var->bindings [var :result-index] -1))
       (apply max -1)
       (long)
       (inc)))

;; TODO: Get rid of assumption that value-buffer-type-id is always one
;; byte. Or better, move construction or handling of ranges to the
;; IndexSnapshot and remove the need for the type-prefix completely.
(defn- new-range-constraint-wrapper-fn [op ^Box val]
  (case op
    = #(idx/new-equals-virtual-index % val)
    < #(-> (idx/new-less-than-virtual-index % val)
           (idx/new-prefix-equal-virtual-index val c/value-type-id-size))
    <= #(-> (idx/new-less-than-equal-virtual-index % val)
            (idx/new-prefix-equal-virtual-index val c/value-type-id-size))
    > #(-> (idx/new-greater-than-virtual-index % val)
           (idx/new-prefix-equal-virtual-index val c/value-type-id-size))
    >= #(-> (idx/new-greater-than-equal-virtual-index % val)
            (idx/new-prefix-equal-virtual-index val c/value-type-id-size))))

(defn- build-var-range-constraints [value-serde range-clauses]
  (->> (for [[var clauses] (group-by :sym range-clauses)
             :when (logic-var? var)]
         [var (->> (for [{:keys [op val]} clauses
                         :let [val (db/encode-value value-serde val)]]
                     (new-range-constraint-wrapper-fn op (Box. val)))
                   (apply comp))])
       (into {})))

(defn- build-logic-var-range-constraint-fns [range-clauses var->bindings]
  (->> (for [{:keys [op sym-a sym-b]} range-clauses
             :when (and (logic-var? sym-a)
                        (logic-var? sym-b))
             :let [sym+index [[sym-a (.result-index ^VarBinding (get var->bindings sym-a))]
                              [sym-b (.result-index ^VarBinding (get var->bindings sym-b))]]
                   [[first-sym first-index]
                    [second-sym _second-index]] (sort-by second sym+index)
                   op (if (= sym-a first-sym)
                        (get range->inverse-range op)
                        op)]]
         {second-sym
          [(fn []
             (let [range-join-depth (calculate-constraint-join-depth var->bindings [first-sym])
                   val (Box. mem/empty-buffer)]
               {:join-depth range-join-depth
                :range-constraint-wrapper-fn (new-range-constraint-wrapper-fn op val)
                :constraint-fn (fn range-constraint [_db _idx-id->idx ^List join-keys]
                                 (set! (.-val val) (.get join-keys first-index))
                                 true)}))]})
       (apply merge-with into {})))

(defn bound-result-for-var [value-serde ^VarBinding var-binding ^List join-keys]
  (->> (.get join-keys (.result-index var-binding))
       (db/decode-value value-serde)))

(defn- validate-existing-vars [type->clauses known-vars]
  (doseq [[clause-type clauses] type->clauses
          clause clauses
          var (case clause-type
                :pred (let [{:keys [pred]} clause
                            {:keys [pred-fn args]} pred]
                        (cond-> args
                          (not (pred-constraint? pred-fn)) (conj pred-fn)))

                :range (map clause [:sym :sym-a :sym-b])

                :or (let [[_args-type {:keys [bound-args]}] (:args clause)]
                      bound-args)

                :not (:args clause)

                nil)
          :when (logic-var? var)
          :when (not (contains? known-vars var))]

    (throw (err/illegal-arg :clause-unknown-var
                            {::err/message (str "Clause refers to unknown variable: " var " " (xio/pr-edn-str clause))}))))

(defn bind-binding [bind-type value-serde tuple-idxs-in-join-order idx result]
  (let [encode-value (partial db/encode-value value-serde)]
    (case bind-type
      :scalar
      (do (idx/update-relation-virtual-index! idx [(encode-value result)] true)
          true)

      :collection
      (do (idx/update-relation-virtual-index! idx (mapv encode-value result) true)
          (not-empty result))

      (:tuple :relation)
      (let [result (if (= :relation bind-type)
                     result
                     [result])]
        (->> (for [tuple result]
               (mapv #(encode-value (nth tuple % nil)) tuple-idxs-in-join-order))
             (idx/update-relation-virtual-index! idx))
        (not-empty result))

      result)))

(defmethod pred-constraint 'get-attr [_ {:keys [idx-id arg-bindings return-type tuple-idxs-in-join-order]}]
  (let [arg-bindings (rest arg-bindings)
        [e-var attr not-found] arg-bindings
        attr (mem/copy-to-unpooled-buffer (c/->id-buffer attr))
        not-found? (= 3 (count arg-bindings))
        e-result-index (.result-index ^VarBinding e-var)]
    (fn pred-get-attr-constraint [{:keys [entity-resolver-fn value-serde index-snapshot]} idx-id->idx ^List join-keys]
      (let [e (.get join-keys e-result-index)
            vs (db/aev index-snapshot attr e nil entity-resolver-fn)
            is-empty? (or (nil? vs) (.isEmpty ^Collection vs))]
        (if (and (= :collection return-type)
                 (not is-empty?))
          (do (idx/update-relation-virtual-index! (get idx-id->idx idx-id) vs true)
              true)
          (let [values (if (and is-empty? not-found?)
                         [not-found]
                         (mapv #(db/decode-value value-serde %) vs))]
            (bind-binding return-type value-serde tuple-idxs-in-join-order (get idx-id->idx idx-id) (not-empty values))))))))

(defmethod pred-constraint 'get-start-valid-time [_ {:keys [idx-id arg-bindings tuple-idxs-in-join-order]}]
  (let [e-var (second arg-bindings)
        e-result-index (.result-index ^VarBinding e-var)]
    (fn pred-get-valid-start-time [{:keys [value-serde index-snapshot valid-time tx-id]} idx-id->idx ^List join-keys]
      (let [e (->> (.get join-keys e-result-index)
                   (db/decode-value value-serde))
            history (db/entity-history index-snapshot e :desc
                                       {:start-valid-time valid-time, :start-tx {::xt/tx-id tx-id}})
            ch (some-> ^EntityTx (first history) (.content-hash))
            value (some-> ^EntityTx (last (take-while #(= ch (.content-hash ^EntityTx %)) history))
                          (.vt))]
        (bind-binding :scalar value-serde tuple-idxs-in-join-order (get idx-id->idx idx-id) value)))))

(defmethod pred-constraint 'get-end-valid-time [_ {:keys [idx-id arg-bindings tuple-idxs-in-join-order]}]
  (let [arg-bindings (rest arg-bindings)
        [e-var not-found] arg-bindings
        not-found? (= 2 (count arg-bindings))
        e-result-index (.result-index ^VarBinding e-var)]
    (fn pred-get-end-valid-time [{:keys [value-serde index-snapshot valid-time ^long tx-id]} idx-id->idx ^List join-keys]
      (let [e (->> (.get join-keys e-result-index)
                   (db/decode-value value-serde))
            etx-before (first (db/entity-history index-snapshot e :desc
                                                 {:start-valid-time valid-time
                                                  :start-tx {::xt/tx-id tx-id}}))
            vs (cond->> (db/entity-history index-snapshot e :asc
                                           {:start-valid-time (Date. (inc (.getTime ^Date valid-time)))
                                            :end-tx {::xt/tx-id (inc tx-id)}})
                 etx-before (remove #(= (.content-hash ^EntityTx etx-before) (.content-hash ^EntityTx %)))
                 true (map #(.vt ^EntityTx %)))
            is-empty? (or (nil? vs) (.isEmpty ^Collection vs))
            value (if (and is-empty? not-found?)
                    not-found
                    (first vs))]
        (bind-binding :scalar value-serde tuple-idxs-in-join-order (get idx-id->idx idx-id) value)))))

(defmethod pred-constraint 'q [_ {:keys [idx-id arg-bindings rule-name->rules return-type tuple-idxs-in-join-order]}]
  (let [parent-rules (:rules (meta rule-name->rules))
        query (cond-> (normalize-query (second arg-bindings))
                (nil? return-type) (assoc :limit 1)
                (seq parent-rules) (update :rules (comp vec concat) parent-rules))]
    (fn pred-constraint [{:keys [value-serde] :as db} idx-id->idx join-keys]
      (let [[_ _ & args] (for [arg-binding arg-bindings]
                           (if (instance? VarBinding arg-binding)
                             (bound-result-for-var value-serde arg-binding join-keys)
                             arg-binding))]
        (with-open [pred-result (xt/open-q* db query (object-array args))]
          (bind-binding return-type value-serde tuple-idxs-in-join-order (get idx-id->idx idx-id) (iterator-seq pred-result)))))))

(defn- built-in-unification-pred [unifier-fn {:keys [value-serde arg-bindings]}]
  (let [arg-bindings (vec (for [arg-binding (rest arg-bindings)]
                            (if (instance? VarBinding arg-binding)
                              arg-binding
                              (->> (map (partial db/encode-value value-serde) (c/vectorize-value arg-binding))
                                   (into (sorted-set-by mem/buffer-comparator))))))]
    (fn unification-constraint [_db _idx-id->idx ^List join-keys]
      (let [values (for [arg-binding arg-bindings]
                     (if (instance? VarBinding arg-binding)
                       (sorted-set-by mem/buffer-comparator (.get join-keys (.result-index ^VarBinding arg-binding)))
                       arg-binding))]
        (unifier-fn values)))))

(defmethod pred-constraint '== [_ pred-ctx]
  (built-in-unification-pred #(boolean (not-empty (apply set/intersection %))) pred-ctx))

(defmethod pred-constraint '!= [_ pred-ctx]
  (built-in-unification-pred #(empty? (apply set/intersection %)) pred-ctx))

(defmethod pred-constraint :default [_clause {:keys [idx-id arg-bindings return-type tuple-idxs-in-join-order]}]
  (fn pred-constraint [{:keys [value-serde] :as db} idx-id->idx join-keys]
    (let [[pred-fn & args] (for [arg-binding arg-bindings]
                             (cond
                               (instance? VarBinding arg-binding)
                               (bound-result-for-var value-serde arg-binding join-keys)

                               (= '$ arg-binding)
                               db

                               :else
                               arg-binding))
          pred-result (apply pred-fn args)]
      (bind-binding return-type value-serde tuple-idxs-in-join-order (get idx-id->idx idx-id) pred-result))))

(defn- build-tuple-idxs-in-join-order [bind-vars vars-in-join-order]
  (let [bind-vars->tuple-idx (zipmap bind-vars (range))]
    (vec (for [var vars-in-join-order
               :let [idx (get bind-vars->tuple-idx var)]
               :when idx]
           idx))))

(defn- maybe-unquote [x]
  (if (and (seq? x) (= 'quote (first x)) (= 2 (count x)))
    (recur (second x))
    x))

(defn- build-pred-constraints [{:keys [pred-clause+idx-ids var->bindings vars-in-join-order] :as pred-ctx}]
  (for [[{:keys [pred return] :as clause} idx-id] pred-clause+idx-ids
        :let [{:keys [pred-fn args]} pred
              pred-vars (filter logic-var? (cons pred-fn args))
              pred-join-depth (calculate-constraint-join-depth var->bindings pred-vars)
              arg-bindings (for [arg (cons pred-fn args)]
                             (if (and (logic-var? arg)
                                      (not (pred-constraint? arg)))
                               (get var->bindings arg)
                               (maybe-unquote arg)))
              return-vars (find-binding-vars return)
              return-vars-tuple-idxs-in-join-order (build-tuple-idxs-in-join-order return-vars vars-in-join-order)
              return-type (first return)
              pred-ctx (assoc pred-ctx
                              :idx-id idx-id
                              :arg-bindings arg-bindings
                              :return-type return-type
                              :tuple-idxs-in-join-order return-vars-tuple-idxs-in-join-order)]]
    (do (when-not (= (count return-vars)
                     (count (set return-vars)))
          (throw (err/illegal-arg :return-vars-not-distinct
                                  {::err/message (str "Return variables not distinct: " (xio/pr-edn-str clause))})))
        (s/assert ::pred-args (cond-> [pred-fn (vec args)]
                                return (conj (second return))))
        {:join-depth pred-join-depth
         :constraint-fn (pred-constraint clause pred-ctx)})))

;; TODO: For or (but not or-join) it might be possible to embed the
;; entire or expression into the parent join via either OrVirtualIndex
;; (though as all joins now are binary they have variable order
;; dependency so this might work easily) or NAryOrVirtualIndex for the
;; generic case. As constants are represented by relations, which
;; introduce new vars which would have to be lifted up to the parent
;; join as all or branches need to have the same variables. Another
;; problem when embedding joins are the sub joins constraints which
;; need to fire at the right level, but they won't currently know how
;; to translate their local join depth to the join depth in the
;; parent, which is what will be used when walking the tree. Due to
;; the way or-join (and rules) work, they likely have to stay as sub
;; queries. Recursive rules always have to be sub queries.

(defn- or-single-e-var-triple-fast-path [{:keys [entity-resolver-fn index-snapshot] :as _db} {:keys [a v]} eid]
  (let [v (db/encode-value index-snapshot v)
        found-v (first (db/aev index-snapshot a eid v entity-resolver-fn))]
    (when (and found-v (mem/buffers=? v found-v))
      [])))

;; TODO: This tabling mechanism attempts at avoiding infinite
;; recursion, but does not actually cache anything. Short-circuits
;; identical sub trees. Passes tests, unsure if this really works in
;; the general case. Depends on the eager expansion of rules for some
;; cases to pass. One alternative is maybe to try to cache the
;; sequence and reuse it, somehow detecting if it loops.
(def ^:private ^:dynamic *recursion-table* {})

(defn- build-or-constraints
  [or-clauses rule-name->rules var->bindings vars-in-join-order]
  (for [{:keys [args branches] :as or-clause} or-clauses]
    (let [{:keys [idx-id rule-name]} (meta or-clause)
          [arg-type {:keys [bound-args free-args]}] args
          _ (assert (= arg-type :explicit-bound-args))
          or-join-depth (calculate-constraint-join-depth var->bindings bound-args)
          free-args-in-join-order (filter (set free-args) vars-in-join-order)
          has-free-args? (boolean (seq free-args))
          bound-args (vec bound-args)
          bound-var-bindings (mapv var->bindings bound-args)
          or-in-bindings {:bindings (if (seq bound-args)
                                      [[:tuple bound-args]]
                                      [])}]
      {:join-depth or-join-depth
       :constraint-fn
       (fn or-constraint [db idx-id->idx ^List join-keys]
         (let [in-args (when (seq bound-args)
                         [(vec (for [^VarBinding var-binding bound-var-bindings]
                                 (.get join-keys (.result-index var-binding))))])
               branch-results (for [[branch-index branch-clauses] (map-indexed vector branches)
                                    :let [{:keys [single-e-var-triple?]} (meta branch-clauses)
                                          cache-key (when rule-name
                                                      [rule-name branch-index (count free-args) in-args])
                                          cached-result (when cache-key
                                                          (get *recursion-table* cache-key))]]
                                (with-open [index-snapshot ^Closeable (open-index-snapshot db)]
                                  (let [db (assoc db :index-snapshot index-snapshot)]
                                    (cond
                                      cached-result
                                      cached-result

                                      single-e-var-triple?
                                      (let [[[_ clause]] branch-clauses]
                                        (or-single-e-var-triple-fast-path
                                         db
                                         clause
                                         (ffirst in-args)))

                                      :else
                                      (binding [*recursion-table* (if cache-key
                                                                    (assoc *recursion-table* cache-key [])
                                                                    *recursion-table*)]
                                        (let [{:keys [var->bindings results]} (build-sub-query db branch-clauses or-in-bindings in-args rule-name->rules)
                                              free-args-in-join-order-bindings (map var->bindings free-args-in-join-order)]
                                          (when-let [idx-seq (seq results)]
                                            (if has-free-args?
                                              (vec (for [^List join-keys idx-seq]
                                                     (vec (for [^VarBinding var-binding free-args-in-join-order-bindings]
                                                            (mem/copy-buffer (.get join-keys (.result-index var-binding)))))))
                                              []))))))))]
           (when (seq (remove nil? branch-results))
             (when has-free-args?
               (let [free-results (->> branch-results
                                       (apply concat)
                                       (distinct)
                                       (vec))]
                 (idx/update-relation-virtual-index! (get idx-id->idx idx-id) free-results)))
             true)))})))

(defn- build-not-constraints [not-clauses rule-name->rules var->bindings]
  (for [{:keys [args terms]} not-clauses
        :let [not-vars (vec (remove blank-var? args))
              not-in-bindings {:bindings [[:tuple not-vars]]}
              not-var-bindings (mapv var->bindings not-vars)
              not-join-depth (calculate-constraint-join-depth var->bindings not-vars)]]
    {:join-depth not-join-depth
     :constraint-fn
     (fn not-constraint [db _idx-id->idx ^List join-keys]
       (with-open [index-snapshot ^Closeable (open-index-snapshot db)]
         (let [db (assoc db :index-snapshot index-snapshot)
               in-args (when (seq not-vars)
                         [(vec (for [^VarBinding var-binding not-var-bindings]
                                 (.get join-keys (.result-index var-binding))))])
               {:keys [results]} (build-sub-query db terms not-in-bindings in-args rule-name->rules)]
           (empty? results))))}))

(defn- sort-triple-clauses [triple-clauses stats]
  (->> triple-clauses
       (sort-by (fn [{:keys [a]}]
                  (db/doc-count stats a)))))

(defn- expand-leaf-preds [{triple-clauses :triple
                           pred-clauses :pred
                           :as type->clauses}
                          in-vars
                          stats]
  (let [collected-vars (collect-vars type->clauses)
        invalid-leaf-vars (set/union in-vars
                                     (into #{} (mapcat collected-vars)
                                           [:e-vars :range-vars :not-vars :or-vars :pred-arg-vars]))
        non-leaf-v-vars (set (for [[v-var non-leaf-group] (group-by :v triple-clauses)
                                   :when (> (count non-leaf-group) 1)]
                               v-var))
        potential-leaf-v-vars (set/difference (:v-vars collected-vars) invalid-leaf-vars non-leaf-v-vars)
        leaf-groups (->> (for [[e-var leaf-group] (group-by :e (filter (comp potential-leaf-v-vars :v) triple-clauses))
                               :when (logic-var? e-var)]
                           [e-var (sort-triple-clauses leaf-group stats)])
                         (into {}))
        leaf-triple-clauses (->> (for [[_e-var leaf-group] leaf-groups]
                                   leaf-group)
                                 (into #{} cat))
        triple-clauses (remove leaf-triple-clauses triple-clauses)
        new-triple-clauses (for [[_e-var leaf-group] leaf-groups]
                             ;; we put these last s.t. if the rest of the query doesn't yield any tuples,
                             ;; we don't need to project out the leaf vars
                             (with-meta (first leaf-group) {:ignore-v? true}))
        leaf-preds (for [[_e-var leaf-group] leaf-groups
                         {:keys [e a v]} (next leaf-group)]
                     {:pred {:pred-fn 'get-attr :args [e a]}
                      :return [:collection v]})]
    [(assoc type->clauses
            :triple (vec (concat triple-clauses new-triple-clauses))
            :pred (vec (concat pred-clauses leaf-preds)))
     (set/difference (set (map :v leaf-triple-clauses))
                     (reduce set/union (vals (dissoc collected-vars :v-vars))))]))

(defn- triple-join-order [type->clauses in-var-cardinalities stats]
  ;; TODO make more use of in-var-cardinalities
  (let [[type->clauses project-only-leaf-vars] (expand-leaf-preds type->clauses (set (keys in-var-cardinalities)) stats)
        {triple-clauses :triple, range-clauses :range, pred-clauses :pred} type->clauses

        collected-vars (collect-vars type->clauses)

        pred-var-frequencies (frequencies
                              (for [{:keys [pred return]} pred-clauses
                                    :when (not return)
                                    arg (:args pred)
                                    :when (logic-var? arg)]
                                arg))

        range-var-frequencies (frequencies
                               (for [{:keys [sym sym-a sym-b]} range-clauses
                                     sym [sym sym-a sym-b]
                                     :when (logic-var? sym)]
                                 sym))

        var->clauses (merge-with into
                                 (group-by :v triple-clauses)
                                 (group-by :e triple-clauses))

        literal-vars (->> (keys var->clauses)
                          (into #{} (filter literal?)))

        single-value-in-vars (->> in-var-cardinalities
                                  (into #{} (comp (filter #(= (double (val %)) 1.0))
                                                  (map key))))

        cardinality-for-var (fn [var cardinality]
                              (cond-> (double (cond
                                                (literal? var) 0.0

                                                (contains? in-var-cardinalities var)
                                                (/ 0.5 (double cardinality))

                                                :else cardinality))

                                (or (contains? (:not-vars collected-vars) var)
                                    (contains? (:pred-return-vars collected-vars) var))
                                (Math/pow 0.25)

                                (contains? pred-var-frequencies var)
                                (Math/pow (/ 0.25 (double (get pred-var-frequencies var))))

                                (contains? range-var-frequencies var)
                                (Math/pow (/ 0.5 (double (get range-var-frequencies var))))))

        update-cardinality (fn [acc {:keys [e a v] :as clause}]
                             (let [{:keys [self-join? ignore-v?]} (meta clause)
                                   es (double (cardinality-for-var e (cond->> (double (db/eid-cardinality stats a))
                                                                       (or (literal? v) (single-value-in-vars v)) (/ 1.0))))
                                   vs (cond
                                        ignore-v? Double/MAX_VALUE
                                        self-join? (Math/nextUp es)
                                        :else
                                        (cardinality-for-var v (cond->> (double (db/value-cardinality stats a))
                                                                 (or (literal? e) (single-value-in-vars e)) (/ 1.0))))]
                               (-> acc
                                   (update v (fnil min Double/MAX_VALUE) vs)
                                   (update e (fnil min Double/MAX_VALUE) es))))

        var->cardinality (doto (reduce update-cardinality {} triple-clauses)
                           (->> (log/debug :triple-joins-var->cardinality)))

        start-vars (set/union literal-vars single-value-in-vars)

        triple-clause-var-order (loop [vars (->> (sort-by val var->cardinality) (map key) (filter logic-var?) (remove #(contains? start-vars %)))
                                       join-order (vec start-vars)
                                       reachable-var-groups (list)]
                                  (if-not (seq vars)
                                    (vec (distinct join-order))

                                    (let [var (first (or (not-empty (for [reachable-var-group reachable-var-groups
                                                                          var (->> (filter reachable-var-group vars)
                                                                                   (partition-by var->cardinality)
                                                                                   (first)
                                                                                   (sort-by (comp count var->clauses))
                                                                                   (reverse))]
                                                                      var))
                                                         vars))
                                          new-reachable-vars (set (for [{:keys [e v]} (get var->clauses var)
                                                                        var [e v]
                                                                        :when (logic-var? var)]
                                                                    var))
                                          new-vars-to-add (->> (for [{:keys [v]} (get var->clauses var)
                                                                     :when (and (logic-var? v)
                                                                                (= 1 (count (get var->clauses v))))]
                                                                 var)
                                                               (sort-by var->cardinality)
                                                               (cons var))]
                                      (recur (remove (set new-vars-to-add) vars)
                                             (vec (concat join-order new-vars-to-add))
                                             (cons (set/difference new-reachable-vars (set new-vars-to-add)) reachable-var-groups)))))]

    [type->clauses
     (doto triple-clause-var-order
       (->> (log/debug :triple-clause-var-order)))
     project-only-leaf-vars]))

(defn- calculate-join-order [type->clauses stats in-var-cardinalities]
  (let [in-vars (set (keys in-var-cardinalities))
        [type->clauses triple-clause-var-order project-only-leaf-vars] (triple-join-order type->clauses in-var-cardinalities stats)

        g (as-> (dep/graph) g
            (reduce (fn [g [a b]]
                      (dep/depend g b a))
                    g
                    (->> triple-clause-var-order (partition 2 1)))

            (reduce (fn [g {:keys [pred return]}]
                      (let [pred-vars (cond->> (:args pred)
                                        (not (pred-constraint? (:pred-fn pred))) (cons (:pred-fn pred)))]
                        (->> (for [pred-var (filter logic-var? pred-vars)
                                   :when return
                                   return-var (find-binding-vars return)]
                               [return-var pred-var])
                             (reduce
                              (fn [g [r a]]
                                (dep/depend g r a))
                              g))))
                    g
                    (:pred type->clauses))

            (reduce (fn [g {:keys [args] :as _or-clause}]
                      (let [{:keys [bound-args free-args]} (second args)]
                        (->> (for [bound-arg bound-args
                                   free-arg free-args]
                               [free-arg bound-arg])
                             (reduce
                              (fn [g [f b]]
                                (dep/depend g f b))
                              g))))
                    g
                    (:or-join type->clauses))

            (reduce (fn [g v]
                      (dep/depend g v ::root))
                    g
                    (set/union (dep/nodes g)

                               ;; other vars that might not be bound anywhere else
                               in-vars
                               (set triple-clause-var-order)
                               (->> (map (collect-vars type->clauses) [:pred-return-vars :or-vars])
                                    (into #{} (mapcat seq))))))]

    [type->clauses
     (vec (concat (->> (dep/topo-sort g)
                       (remove (conj project-only-leaf-vars ::root)))
                  project-only-leaf-vars))]))

(defn- rule-name->rules [rules]
  (group-by (comp :name :head) rules))

(defn- expand-rules [where rule-name->rules seen-rules]
  (->> (for [[type clause :as sub-clause] where]
         (cond
           (not= :rule type) [sub-clause]
           (contains? seen-rules (:name clause)) [sub-clause]

           :else
           (let [rule-name (:name clause)
                 rules (get rule-name->rules rule-name)]
             (when-not rules
               (throw (err/illegal-arg :unknown-rule
                                       {::err/message (str "Unknown rule: " (xio/pr-edn-str sub-clause))})))
             (let [rule-args+num-bound-args+body
                   (for [{:keys [head body]} rules
                         :let [[args-type args-val] (:args head)]]
                     (case args-type
                       :explicit-bound-args (let [{:keys [bound-args free-args]} args-val]
                                              [(vec (concat bound-args free-args))
                                               (count bound-args)
                                               body])
                       :just-args [(vec args-val) nil body]))

                   [arity :as arities] (->> rule-args+num-bound-args+body
                                            (map (comp count first))
                                            (distinct))

                   [num-bound-args :as num-bound-args-groups] (->> rule-args+num-bound-args+body
                                                                   (map second)
                                                                   (distinct))]

               (when-not (= 1 (count arities))
                 (throw (err/illegal-arg :rule-definition-require-same-arity
                                         {::err/message (str "Rule definitions require same arity: " (xio/pr-edn-str rules))})))

               (when-not (= 1 (count num-bound-args-groups))
                 (throw (err/illegal-arg :rule-definition-require-same-num-bound-args
                                         {::err/message (str "Rule definitions require same number of bound args: " (xio/pr-edn-str rules))})))

               (when-not (= arity (count (:args clause)))
                 (throw (err/illegal-arg :rule-invocation-wrong-arity
                                         {::err/message (str "Rule invocation has wrong arity, expected: " arity " " (xio/pr-edn-str sub-clause))})))

               (let [expanded-rules (for [[args _ body] rule-args+num-bound-args+body
                                          :let [rule-arg->query-arg (zipmap args (:args clause))
                                                body-vars (->> (normalize-clauses body)
                                                               (group-clauses-by-type)
                                                               (collect-vars)
                                                               (vals)
                                                               (into #{} cat))
                                                body-var->hidden-var (zipmap body-vars
                                                                             (map gensym body-vars))]]
                                      (-> (w/postwalk-replace (merge body-var->hidden-var rule-arg->query-arg) body)
                                          (expand-rules rule-name->rules (conj seen-rules rule-name))))]

                 (if (= 1 (count expanded-rules))
                   (first expanded-rules)
                   (when (seq expanded-rules)
                     [[:or-join
                       (-> {:args (if num-bound-args
                                    (let [[bound-args free-args] (split-at (or num-bound-args 0) (:args clause))]
                                      [:explicit-bound-args {:bound-args (filterv logic-var? bound-args)
                                                             :free-args (filterv logic-var? free-args)}])
                                    [:just-args (filter logic-var? (:args clause))])
                            :branches (vec expanded-rules)}
                           (vary-meta assoc :rule-name rule-name))]])))))))
       (into [] cat)))

(def default-allow-list
  (->> (slurp (io/resource "query-allowlist.edn"))
       (read-string)))

(s/def ::fn-allow-list
  (s/and
   (s/coll-of (s/or :sym symbol? :str string?))
   (s/conformer (fn [fal]
                  (reduce
                   (fn [allow-map [type pred]]
                     (let [pred-symbol (cond-> pred
                                         (= :str type) symbol)]
                       (if (qualified-symbol? pred-symbol)
                         (update allow-map :allowed-fns conj pred-symbol)
                         (update allow-map :allowed-ns conj pred-symbol))))
                   {:allowed-ns #{} :allowed-fns default-allow-list}
                   fal)))))

(defn- assert-allowed-fn [f-var fn-allow-list]
  (when-let [{:keys [allowed-ns allowed-fns]} fn-allow-list]
    (let [sym (symbol f-var)]
      (when-not (or (contains? allowed-fns sym)
                    (contains? allowed-ns (-> f-var (meta) ^clojure.lang.Namespace (:ns) (.getName))))
        (throw (err/illegal-arg :fn-not-allowed {::err/message (str "Query used a function that was not in the allowlist: " sym)}))))))

(defn- build-pred-fns [clauses fn-allow-list]
  (->> (for [[type clause :as sub-clause] clauses]
         (let [pred-var (get-in clause [:pred :pred-fn])]
           (if (and (= :pred type) (var? pred-var))
             (do
               (assert-allowed-fn pred-var fn-allow-list)
               (update-in sub-clause [1 :pred :pred-fn] var-get))
             sub-clause)))
       (into [])))

(defn- update-depth->constraints [depth->join-depth constraints]
  (reduce
   (fn [acc {:keys [join-depth constraint-fn]}]
     (update acc join-depth (fnil conj []) constraint-fn))
   depth->join-depth
   constraints))

(defn- break-cycle [where {:keys [node dependency]}]
  (->> (for [[type clause] where]
         (if (and (= :pred type)
                  (contains? (set (get-in clause [:pred :args])) dependency)
                  (contains? (set (find-binding-vars (:return clause))) node))
           (let [cycle-var (gensym (str "cycle_" node "_"))]
             [[:pred (update clause :return (partial w/postwalk-replace {node cycle-var}))]
              [:pred {:pred {:pred-fn '== :args [node cycle-var]}}]])
           [[type clause]]))
       (into [] cat)))

(def ^:private ^:dynamic *broken-cycles* #{})

(defn ->stats [index-snapshot]
  (reify db/AttributeStats
    (all-attrs [_] (db/all-attrs index-snapshot))
    (doc-count [_ a] (db/doc-count index-snapshot a))
    (doc-value-count [_ a] (db/doc-value-count index-snapshot a))
    (eid-cardinality [_ a] (db/eid-cardinality index-snapshot a))
    (value-cardinality [_ a] (db/value-cardinality index-snapshot a))))

(defn- compile-sub-query [{:keys [fn-allow-list pred-ctx value-serde] :as db}
                          stats where in in-var-cardinalities
                          rule-name->rules]
  (try
    (let [in-vars (set (keys in-var-cardinalities))

          type->clauses (-> (expand-rules where rule-name->rules #{})
                            (build-pred-fns fn-allow-list)
                            (normalize-clauses)
                            (group-clauses-by-type))

          collected-vars (collect-vars type->clauses)

          known-vars (-> (set/union in-vars
                                    (into #{} (mapcat collected-vars) [:e-vars :v-vars]))
                         (add-pred-returns-bound-at-top-level (:pred type->clauses)))

          [type->clauses known-vars] (analyze-or-vars type->clauses known-vars)

          ;; we add pred returns now, assume they're all available,
          ;; and deal with potential cycles later
          known-vars (into known-vars (:pred-return-vars collected-vars))

          _ (validate-existing-vars type->clauses known-vars)

          [type->clauses vars-in-join-order] (calculate-join-order type->clauses stats in-var-cardinalities)

          var->bindings (->> vars-in-join-order
                             (into {} (map-indexed (fn [idx var]
                                                     [var (->VarBinding idx)]))))

          {triple-clauses :triple
           range-clauses :range
           pred-clauses :pred
           not-join-clauses :not-join
           or-join-clauses :or-join} type->clauses

          var->joins (triple-joins triple-clauses value-serde {})
          [in-idx-ids var->joins] (in-joins (:bindings in) var->joins)
          [pred-clause+idx-ids var->joins] (pred-joins pred-clauses var->joins)
          [or-join-clauses var->joins] (or-joins or-join-clauses var->joins)]

      {:depth->constraints (->> (concat (build-pred-constraints (assoc pred-ctx
                                                                       :rule-name->rules rule-name->rules
                                                                       :value-serde value-serde
                                                                       :pred-clause+idx-ids pred-clause+idx-ids
                                                                       :var->bindings var->bindings
                                                                       :vars-in-join-order vars-in-join-order))
                                        (build-not-constraints not-join-clauses rule-name->rules var->bindings)
                                        (build-or-constraints or-join-clauses rule-name->rules var->bindings vars-in-join-order))
                                (update-depth->constraints (vec (repeat (inc (count vars-in-join-order)) nil))))

       :var->range-constraints (build-var-range-constraints value-serde range-clauses)
       :var->logic-var-range-constraint-fns (build-logic-var-range-constraint-fns range-clauses var->bindings)
       :vars-in-join-order vars-in-join-order
       :var->joins var->joins
       :var->bindings var->bindings

       :in-bindings (vec (for [[idx-id [bind-type binding]] (map vector in-idx-ids (:bindings in))
                               :let [bind-vars (find-binding-vars binding)]]
                           {:idx-id idx-id
                            :bind-type bind-type
                            :tuple-idxs-in-join-order (build-tuple-idxs-in-join-order bind-vars vars-in-join-order)}))})

    (catch ExceptionInfo e
      (let [{:keys [reason] :as cycle} (ex-data e)]
        (if (and (= ::dep/circular-dependency reason)
                 (not (contains? *broken-cycles* cycle)))
          (binding [*broken-cycles* (conj *broken-cycles* cycle)]
            (compile-sub-query db stats (break-cycle where cycle) in in-var-cardinalities rule-name->rules))
          (throw e))))))

(defn- build-idx-id->idx [db {:keys [var->joins] :as compiled-query}]
  (->> (for [[_ joins] var->joins
             {:keys [id idx-fn]} joins
             :when id]
         [id idx-fn])
       (reduce
        (fn [acc [id idx-fn]]
          (if (contains? acc id)
            acc
            (assoc acc id (idx-fn db compiled-query))))
        {})))

(defn- add-logic-var-constraints [{:keys [var->logic-var-range-constraint-fns]
                                   :as compiled-query}]
  (if (seq var->logic-var-range-constraint-fns)
    (let [logic-var+range-constraint (for [[v fs] var->logic-var-range-constraint-fns
                                           f fs]
                                       [v (f)])]
      (-> compiled-query
          (update :depth->constraints update-depth->constraints (map second logic-var+range-constraint))
          (update :var->range-constraints (fn [var->range-constraints]
                                            (reduce (fn [acc [v {:keys [range-constraint-wrapper-fn]}]]
                                                      (update acc v (fn [x]
                                                                      (cond-> range-constraint-wrapper-fn
                                                                        x (comp x)))))
                                                    var->range-constraints
                                                    logic-var+range-constraint)))))
    compiled-query))

(defrecord CachedSerde [inner-serde, unpooled?, ^Map hash-cache]
  db/ValueSerde
  (encode-value [_ v]
    (if (instance? DirectBuffer v)
      v
      (let [v-buf (cond-> (db/encode-value inner-serde v)
                    unpooled? mem/copy-to-unpooled-buffer)]
        (when-not (c/can-decode-value-buffer? v-buf)
          (.put hash-cache (cond-> v-buf (not unpooled?) mem/copy-buffer) v))
        v-buf)))

  (decode-value [_ v-buf]
    (or (.get hash-cache v-buf)
        (db/decode-value inner-serde v-buf))))

(defn- ^xtdb.query.CachedSerde ->cached-serde
  ([inner-serde] (->cached-serde inner-serde {}))

  ([inner-serde {:keys [unpooled?]}]
   (->CachedSerde inner-serde unpooled? (HashMap.))))

(defn- merge-hash-cache! [^CachedSerde cached-serde, ^Map hash-cache]
  (.putAll ^Map (.hash-cache cached-serde) hash-cache))

(defn- build-sub-query [{:keys [query-cache value-serde index-snapshot] :as db} where in in-args rule-name->rules]
  ;; NOTE: this implies argument sets with different vars get compiled differently.
  (let [in-var-cardinalities (->approx-in-var-cardinalities in in-args)
        {:keys [depth->constraints
                vars-in-join-order
                var->range-constraints
                var->joins
                var->bindings
                in-bindings
                static-hash-cache]
         :as compiled-query} (-> (cache/compute-if-absent
                                  query-cache
                                  [where in in-var-cardinalities rule-name->rules]
                                  identity
                                  (fn [_]
                                    (let [static-serde (->cached-serde value-serde {:unpooled? true})]
                                      (-> (compile-sub-query (assoc db :value-serde static-serde)
                                                             (->stats index-snapshot)
                                                             where in in-var-cardinalities
                                                             rule-name->rules)
                                          (assoc :static-hash-cache (.hash-cache static-serde))))))
                                 (add-logic-var-constraints))
        _ (merge-hash-cache! value-serde static-hash-cache)
        idx-id->idx (build-idx-id->idx db compiled-query)
        unary-join-indexes (for [v vars-in-join-order]
                             (-> (idx/new-unary-join-virtual-index
                                  (vec (for [{:keys [id idx-fn]} (get var->joins v)]
                                         (or (get idx-id->idx id)
                                             (idx-fn db compiled-query)))))
                                 (idx/wrap-with-range-constraints (get var->range-constraints v))))
        constrain-result-fn (fn [join-keys ^long depth]
                              (every? (fn [f]
                                        (f db idx-id->idx join-keys))
                                      (.get ^List depth->constraints depth)))]

    {:var->bindings var->bindings
     :results (lazy-seq
               (binding [nippy/*freeze-fallback* :write-unfreezable]
                 (doseq [[{:keys [idx-id bind-type tuple-idxs-in-join-order]} in-arg] (map vector in-bindings in-args)]
                   (bind-binding bind-type value-serde tuple-idxs-in-join-order (get idx-id->idx idx-id) in-arg)))

               (when (constrain-result-fn [] 0)
                 (idx/layered-idx->seq
                  (idx/new-n-ary-join-layered-virtual-index unary-join-indexes constrain-result-fn))))}))

(defn- open-index-snapshot ^java.io.Closeable [{:keys [index-store index-snapshot] :as _db}]
  (if index-snapshot
    (db/open-nested-index-snapshot index-snapshot)
    (db/open-index-snapshot index-store)))

(defn- with-entity-resolver-cache [entity-resolver-fn {:keys [entity-cache-size]}]
  (let [entity-cache (cache/->cache {:cache-size entity-cache-size})]
    (fn [k]
      (cache/compute-if-absent entity-cache k mem/copy-to-unpooled-buffer entity-resolver-fn))))

(defn- new-entity-resolver-fn [{:keys [valid-time tx-id index-snapshot] :as db}]
  (with-entity-resolver-cache #(when tx-id (db/entity-as-of-resolver index-snapshot % valid-time tx-id)) db))

(defn- validate-in [in]
  (doseq [binding (:bindings in)
          :let [binding-vars (find-binding-vars binding)]]
    (when-not (= (count binding-vars)
                 (count (set binding-vars)))
      (throw (err/illegal-arg :indistinct-in-binding-variables
                              {::err/message "In binding variables not distinct"
                               :variables binding})))))

;; NOTE: For ascending sort, it might be possible to pick the right
;; join order so the resulting seq is already sorted, by ensuring the
;; first vars of the join order overlap with the ones in order
;; by. Depending on the query this might not be possible. For example,
;; when using or-join/rules the order from the sub queries cannot be
;; guaranteed. The order by vars must be in the set of bound vars for
;; all or statements in the query for this to work. This is somewhat
;; related to embedding or in the main query. Also, this sort is based
;; on the actual values, and not the byte arrays, which would give
;; different sort order for example for ids, where the hash used in
;; the indexes won't sort the same as the actual value. For this to
;; work well this would need to be revisited.
(defn- order-by-comparator [find order-by]
  (let [find-arg->index (zipmap find (range))]
    (reify Comparator
      (compare [_ a b]
        (loop [diff 0
               [{:keys [find-arg direction]} & order-by] order-by]
          (if (or (not (zero? diff))
                  (nil? find-arg))
            diff
            (let [index (get find-arg->index find-arg)]
              (recur (long (cond-> (compare (get a index)
                                            (get b index))
                             (= :desc direction) -))
                     order-by))))))))

(defn- emit-projection [form {:keys [agg-allowed? fn-allow-list]
                              :or {agg-allowed? true}
                              :as opts}]
   (let [[form-type form-arg] form]
     (case form-type
       (:nil :number :string :boolean :keyword) {:logic-vars #{}, :code form-arg}
       :symbol {:logic-vars #{form-arg}, :code form-arg}

       :aggregate (if-not agg-allowed?
                    (throw (err/illegal-arg :nested-agg-in-projection
                                            {::err/message "nested aggregates are disallowed"
                                             :form form}))

                    (let [{:keys [aggregate-fn args form]} form-arg
                          agg-sym (gensym aggregate-fn)
                          {:keys [logic-vars code]} (emit-projection form (-> opts (assoc :agg-allowed? false)))]
                      {:aggregates {agg-sym {:logic-vars logic-vars
                                             :code code
                                             :aggregate-fn (apply aggregate aggregate-fn args)}}

                       :code agg-sym}))

       (:set :vector) (let [emitted-els (mapv #(emit-projection % opts) form-arg)]
                        {:logic-vars (into #{} (mapcat :logic-vars) emitted-els)
                         :aggregates (into {} (mapcat :aggregates) emitted-els)
                         :code (case form-type
                                 :set (into #{} (map :code) emitted-els)
                                 :vector (mapv :code emitted-els))})

       :map (let [emitted-els (mapcat (fn [[k v]]
                                        [(emit-projection k opts)
                                         (emit-projection v opts)])
                                      form-arg)]
              {:logic-vars (into #{} (mapcat :logic-vars) emitted-els)
               :aggregates (into {} (mapcat :aggregates) emitted-els)
               :code (->> emitted-els (map :code) (partition-all 2) (map vec) (into {}))})

       :list (let [[f-form & arg-forms] form-arg
                   [f-type f-arg] f-form

                   f-sym (or (when (= f-type :symbol)
                               (or (#{'if} f-arg)
                                   (some-> (if (qualified-symbol? f-arg)
                                             (requiring-resolve f-arg)
                                             (ns-resolve 'user f-arg))
                                           (doto (assert-allowed-fn fn-allow-list))
                                           symbol)))
                             (throw (err/illegal-arg :invalid-f-in-agg-expr
                                                     {::err/message "Invalid function in aggregate expr"
                                                      :f f-arg})))

                   emitted-args (mapv #(emit-projection % opts) arg-forms)
                   arg-code (mapv :code emitted-args)]

               {:logic-vars (into #{} (mapcat :logic-vars) emitted-args)
                :aggregates (into {} (mapcat :aggregates) emitted-args)
                :code (case f-arg
                        if (do
                             (when-not (<= 2 (count arg-forms) 3)
                               (throw (err/illegal-arg :invalid-if-arity
                                                       {::err/message "Arity error to `if`"
                                                        :form form-arg})))

                             `(if ~@arg-code))

                        `(~f-sym ~@arg-code))}))))

(defn- compile-projection [form opts]
  (let [{:keys [logic-vars aggregates code]} (emit-projection form opts)
        ordered-inputs (vec (concat logic-vars (keys aggregates)))]
    {:logic-vars logic-vars
     :inputs ordered-inputs
     :aggregates (->> (for [[agg-sym {:keys [logic-vars code aggregate-fn]}] aggregates]
                        (let [ordered-inputs (vec logic-vars)]
                          (MapEntry/create agg-sym
                                           {:inputs ordered-inputs
                                            :logic-vars logic-vars
                                            :->result (eval `(fn [_db# ~@ordered-inputs]
                                                               ~code))
                                            :aggregate-fn aggregate-fn})))
                      (into {}))
     :->result (eval `(fn [_db# ~@ordered-inputs]
                        ~code))}))

(defn- compile-find-args [conformed-find {:keys [fn-allow-list]}]
  (for [[var-type arg] conformed-find]
    (case var-type
      :form (let [{:keys [logic-vars inputs aggregates ->result]} (compile-projection arg {:fn-allow-list fn-allow-list})]
              {:find-arg-type :form
               :aggregates aggregates
               :logic-vars logic-vars
               :inputs inputs
               :->result ->result})

      :pull (let [{:keys [logic-var pull-spec]} arg]
              {:logic-vars #{logic-var}
               :inputs [logic-var]
               :find-arg-type :pull
               :->result (let [pull-fn (pull/compile-pull-spec (s/unform ::eql/query pull-spec))]
                           (fn [db logic-var]
                             (pull-fn logic-var db)))}))))

(defn- ->value-fn [{:keys [inputs ->result]} {:keys [var->bindings]} {:keys [value-serde] :as db}]
  (let [var-bindings (mapv var->bindings inputs)]
    (fn [row]
      (apply ->result db (for [var-binding var-bindings]
                           (bound-result-for-var value-serde var-binding row))))))

(defn- agg-find-fn [find-args built-query]
  (fn [rows db]
    (let [->group (let [ordered-inputs (into [] (comp (mapcat :logic-vars) (distinct)) find-args)]
                    (->value-fn {:inputs ordered-inputs
                                 :->result (fn [_db & values]
                                             (zipmap ordered-inputs values))}
                                built-query
                                db))
          agg-fns (->> find-args
                       (mapcat :aggregates)
                       (mapv (fn [[agg-k {:keys [aggregate-fn] :as agg}]]
                               (let [->value (->value-fn agg built-query db)]
                                 [agg-k (fn
                                          ([] (aggregate-fn))
                                          ([acc row] (aggregate-fn acc (->value row)))
                                          ([acc] (aggregate-fn acc)))]))))]
      (letfn [(init-aggs []
                (mapv (fn [[agg-k aggregate-fn]]
                        [agg-k aggregate-fn (volatile! (aggregate-fn))])
                      agg-fns))

              (step-aggs [^Map acc row]
                (let [group-accs (.computeIfAbsent acc (->group row)
                                                   (reify Function
                                                     (apply [_ _group]
                                                       (init-aggs))))]
                  (doseq [[_agg-k agg-fn !agg-acc] group-accs]
                    (vswap! !agg-acc agg-fn row))))]

        (let [acc (HashMap.)]
          (doseq [row rows]
            (step-aggs acc row))

          (for [[group-k group-accs] acc]
            (let [env (into group-k
                            (for [[agg-k agg-fn !agg-acc] group-accs]
                              (MapEntry/create agg-k (agg-fn @!agg-acc))))]
              (mapv (fn [{:keys [->result inputs]}]
                      (apply ->result db (map env inputs)))
                    find-args))))))))

(defn- compile-find [conformed-find {:keys [var->bindings] :as built-query} {:keys [find-cache fn-allow-list]}]
  (let [find-args (cache/compute-if-absent find-cache conformed-find identity #(compile-find-args % {:fn-allow-list fn-allow-list}))
        find-arg-types (into #{} (map :find-arg-type) find-args)]

    (when-let [unknown-vars (not-empty (->> find-args
                                            (into #{} (comp (mapcat (fn [{:keys [logic-vars aggregates]}]
                                                                      (into logic-vars (mapcat :logic-vars) (vals aggregates))))
                                                            (remove var->bindings)))))]
      (throw (err/illegal-arg :find-unknown-vars
                              {::err/message (str "Find refers to unknown variables: " (pr-str unknown-vars))
                               :unknown-vars unknown-vars})))

    {:find-arg-types find-arg-types
     :find-fn (if-not (every? (comp empty? :aggregates) find-args)
                (agg-find-fn find-args built-query)

                (fn [rows db]
                  (let [value-fns (mapv #(->value-fn % built-query db) find-args)]
                    (for [row rows]
                      (mapv #(% row) value-fns)))))}))

(defn- arg-for-var [arg var]
  (second
   (or (find arg (symbol (name var)))
       (find arg (keyword (name var))))))

(defn- find-arg-vars [args]
  (let [ks (keys (first args))]
    (set (for [k ks]
           (symbol (name k))))))

(defn- add-legacy-args [{:keys [args in] :as _query} in-args]
  (if-let [arg-vars (not-empty (find-arg-vars args))]
    (let [arg-vars (vec arg-vars)]
      [(update in :bindings #(vec (cons [:relation [arg-vars]] %)))
       (vec (cons (vec (for [arg-tuple args]
                         (mapv #(arg-for-var arg-tuple %) arg-vars)))
                  in-args))])
    [in in-args]))

(defn query-plan-for
  ([db q] (query-plan-for db q []))
  ([db q in-args]
   (s/assert ::query q)
   (with-open [index-snapshot (open-index-snapshot db)]
     (let [value-serde (->cached-serde index-snapshot)
           db (assoc db
                     :index-snapshot index-snapshot
                     :value-serde value-serde)
           {:keys [where rules] :as conformed-q} (s/conform ::query q)
           [in in-args] (add-legacy-args conformed-q in-args)]
       (compile-sub-query db (->stats index-snapshot) where
                          in (->approx-in-var-cardinalities in in-args)
                          (rule-name->rules rules))))))

(defn- ->return-maps [{:keys [keys syms strs]}]
  (let [ks (or (some->> keys (mapv keyword))
               (some->> syms (mapv symbol))
               (some->> strs (mapv str)))]
    (fn [row]
      (zipmap ks row))))

(defn query [{:keys [index-snapshot] :as db} ^ConformedQuery conformed-q in-args]
  (let [q (.q-normalized conformed-q)
        q-conformed (.q-conformed conformed-q)
        {:keys [find where rules offset limit order-by]} q-conformed
        [in in-args] (add-legacy-args q-conformed in-args)]

    (when (:full-results? q-conformed)
      (throw (err/illegal-arg :full-results-removed
                              {::err/message (str "`full-results?` was removed - use 'pull' instead: "
                                                  "https://xtdb.com/reference/queries.html#pull")})))

    (log/debug :query (xio/pr-edn-str (-> q
                                          (assoc :in (or in []))
                                          (dissoc :args))))
    (validate-in in)
    (let [rule-name->rules (with-meta (rule-name->rules rules) {:rules (:rules q)})
          value-serde (->cached-serde index-snapshot)
          db (assoc db
                    :index-snapshot index-snapshot
                    :value-serde value-serde
                    :entity-resolver-fn (or (:entity-resolver-fn db)
                                            (new-entity-resolver-fn db)))
          {:keys [results] :as built-query} (build-sub-query db where in in-args rule-name->rules)
          {:keys [find-arg-types find-fn]} (compile-find find built-query db)
          return-maps? (some q [:keys :syms :strs])]

      (doseq [{:keys [find-arg]} order-by
              :when (not (some #{find-arg} find))]
        (throw (err/illegal-arg :order-by-requires-find-element
                                {::err/message (str "Order by requires an element from :find. unreturned element: " find-arg)})))

      (lazy-seq
       (cond->> (find-fn results db)
         order-by (xio/external-sort (order-by-comparator find order-by))
         offset (drop offset)
         limit (take limit)
         (contains? find-arg-types :pull) (pull/->pull-result db q-conformed)
         return-maps? (map (->return-maps q)))))))

(defn entity-tx [{:keys [valid-time tx-id] :as _db} index-snapshot eid]
  (when tx-id
    (some-> (db/entity-as-of index-snapshot eid valid-time tx-id)
            (c/entity-tx->edn))))

(defn- entity [{:keys [document-store] :as db} index-snapshot eid]
  (when-let [content-hash (some-> (entity-tx db index-snapshot eid)
                                  ::xt/content-hash)]
    (-> (db/fetch-docs document-store #{content-hash})
        (get content-hash)
        (c/keep-non-evicted-doc)
        (c/crux->xt))))

(defn- with-history-bounds [{:keys [sort-order start-tx end-tx] :as opts}
                            {:keys [^long tx-id ^Date valid-time]}]
  (letfn [(with-upper-bound [v upper-bound]
            (if (or (nil? v) (pos? (compare v upper-bound)))
              upper-bound
              v))]
    (-> opts
        (cond-> (= sort-order :desc) (update :start-valid-time with-upper-bound valid-time))
        (assoc :start-tx {::xt/tx-id (cond-> (::xt/tx-id start-tx)
                                       (= sort-order :desc) (with-upper-bound tx-id))
                          ::xt/tx-time (::xt/tx-time start-tx)})

        (cond-> (= sort-order :asc) (update :end-valid-time with-upper-bound (Date. (inc (.getTime valid-time)))))

        (assoc :end-tx {::xt/tx-id (cond-> (::xt/tx-id end-tx)
                                     (= sort-order :asc) (with-upper-bound (inc tx-id)))
                        ::xt/tx-time (::xt/tx-time end-tx)}))))

(defrecord QueryDatasource [document-store index-store bus tx-indexer
                            ^Date valid-time ^Date tx-time ^Long tx-id
                            ^ScheduledExecutorService interrupt-executor
                            conform-cache query-cache find-cache
                            index-snapshot
                            entity-resolver-fn
                            fn-allow-list]
  Closeable
  (close [_]
    (when index-snapshot
      (.close ^Closeable index-snapshot)))

  xt/PXtdbDatasource
  (entity [this eid]
    (with-open [index-snapshot (open-index-snapshot this)]
      (entity this index-snapshot eid)))

  (entity-tx [this eid]
    (with-open [index-snapshot (open-index-snapshot this)]
      (entity-tx this index-snapshot eid)))

  (q* [this query args]
    (let [result-coll-fn (if (some (normalize-query query) [:order-by :limit :offset]) vec set)
          !timed-out? (atom false)
          ^Future
          interrupt-job (when-let [timeout-ms (get query :timeout (:query-timeout this))]
                          (let [caller-thread (Thread/currentThread)]
                            (.schedule interrupt-executor
                                       ^Runnable
                                       (fn []
                                         (reset! !timed-out? true)
                                         (.interrupt caller-thread))
                                       ^long timeout-ms
                                       TimeUnit/MILLISECONDS)))]
      (try
        (with-open [res (xt/open-q* this query args)]
          (result-coll-fn (iterator-seq res)))
        (catch InterruptedException e
          (throw (if @!timed-out?
                   (TimeoutException. "Query timed out.")
                   e)))
        (finally
          (when interrupt-job
            (.cancel interrupt-job false))))))


  (open-q* [this query args]
    (let [conformed-query (normalize-and-conform-query conform-cache query)
          query-id (str (UUID/randomUUID))
          safe-query (-> conformed-query .q-normalized (dissoc :args))
          index-snapshot (open-index-snapshot this)]
      (when bus
        (bus/send bus {::xt/event-type ::submitted-query
                       ::query safe-query
                       ::query-id query-id}))
      (try
        (let [db (as-> this db
                   (assoc db :index-snapshot index-snapshot)
                   (assoc db :entity-resolver-fn (or entity-resolver-fn (new-entity-resolver-fn db))))]

          (->> (xtdb.query/query db conformed-query args)
               (xio/->cursor (fn []
                               (xio/try-close index-snapshot)
                               (when bus
                                 (bus/send bus {::xt/event-type ::completed-query
                                                ::query safe-query
                                                ::query-id query-id}))))))
        (catch Throwable e
          (xio/try-close index-snapshot)
          (when bus
            (bus/send bus {::xt/event-type ::failed-query
                           ::query safe-query
                           ::query-id query-id
                           ::error {:type (xio/pr-edn-str (type e))
                                    :message (.getMessage e)}}))
          (throw e)))))

  (pull [db projection eid]
    (let [?eid (gensym '?eid)
          projection (cond-> projection (string? projection) c/read-edn-string-with-readers)]
      (->> (xt/q db
                 {:find [(list 'pull ?eid projection)]
                  :in [?eid]}
                 eid)
           ffirst)))

  (pull-many [db projection eids]
    (let [?eid (gensym '?eid)
          projection (cond-> projection (string? projection) c/read-edn-string-with-readers)]
      (mapv (->> (xt/q db
                       {:find [?eid (list 'pull ?eid projection)]
                        :in [[?eid '...]]}
                       eids)
                 (into {}))
            eids)))

  (entity-history [this eid sort-order] (xt/entity-history this eid sort-order {}))

  (entity-history [this eid sort-order opts]
    (with-open [history (xt/open-entity-history this eid sort-order opts)]
      (into [] (iterator-seq history))))

  (open-entity-history [this eid sort-order] (xt/open-entity-history this eid sort-order {}))

  (open-entity-history [this eid sort-order opts]
    (if-not tx-id
      xio/empty-cursor
      (let [opts (assoc opts :sort-order sort-order)
            index-snapshot (open-index-snapshot this)
            {:keys [with-docs?] :as opts} (with-history-bounds opts this)]
        (xio/->cursor #(.close index-snapshot)
                      (->> (for [history-batch (->> (db/entity-history index-snapshot eid sort-order opts)
                                                    (partition-all 100))
                                 :let [docs (when with-docs?
                                              (->> (db/fetch-docs document-store
                                                                  (->> history-batch
                                                                       (into #{}
                                                                             (comp (keep #(.content-hash ^EntityTx %))
                                                                                   (remove #{(c/new-id c/nil-id-buffer)})))))
                                                   (xio/map-vals c/crux->xt)))]]
                             (->> history-batch
                                  (map (fn [^EntityTx etx]
                                         (cond-> {::xt/tx-time (.tt etx)
                                                  ::xt/tx-id (.tx-id etx)
                                                  ::xt/valid-time (.vt etx)
                                                  ::xt/content-hash (.content-hash etx)}
                                           with-docs? (assoc ::xt/doc (get docs (.content-hash etx))))))))
                           (mapcat seq))))))

  (valid-time [_] valid-time)
  (transaction-time [_] tx-time)
  (db-basis [_]
    {::xt/valid-time valid-time
     ::xt/tx {::xt/tx-time tx-time,
              ::xt/tx-id tx-id}})

  (with-tx [this tx-ops]
    (with-tx/->db this tx-ops)))

(defmethod print-method QueryDatasource [{:keys [valid-time tx-id]} ^Writer w]
  (.write w (format "#<XtdbDB %s>" (xio/pr-edn-str {::xt/valid-time valid-time, ::xt/tx-id tx-id}))))

(defmethod pp/simple-dispatch QueryDatasource [it]
  (print-method it *out*))

(defn- ->basis [valid-time-or-basis]
  (if (instance? Date valid-time-or-basis)
    {::xt/valid-time valid-time-or-basis}
    {::xt/valid-time (::xt/valid-time valid-time-or-basis)
     ::xt/tx (or (::xt/tx valid-time-or-basis)
                (select-keys valid-time-or-basis [::xt/tx-time ::xt/tx-id]))}))

(defprotocol PredContext
  (assoc-pred-ctx! [_ k v]))

(defrecord QueryEngine [^ScheduledExecutorService interrupt-executor document-store
                        index-store bus !pred-ctx
                        query-cache conform-cache find-cache]
  xt/DBProvider
  (db [this] (xt/db this nil))
  (db [this valid-time tx-time] (xt/db this {::xt/valid-time valid-time, ::xt/tx-time tx-time}))
  (db [this valid-time-or-basis]
    (let [{::xt/keys [valid-time] :as basis} (->basis valid-time-or-basis)
          valid-time (or valid-time (Date.))
          resolved-tx (with-open [index-snapshot (db/open-index-snapshot index-store)]
                        (db/resolve-tx index-snapshot (::xt/tx basis)))]

      ;; we can't have QueryEngine depend on the main tx-indexer, because of a cyclic dependency
      (map->QueryDatasource (assoc this
                                   :tx-indexer (tx/->tx-indexer {:index-store index-store
                                                                 :document-store document-store
                                                                 :bus bus
                                                                 :query-engine this})
                                   :pred-ctx @!pred-ctx
                                   :valid-time valid-time
                                   :tx-time (::xt/tx-time resolved-tx)
                                   :tx-id (::xt/tx-id resolved-tx)))))

  (open-db [this] (xt/open-db this nil nil))
  (open-db [this valid-time tx-time] (xt/open-db this {::xt/valid-time valid-time, ::xt/tx-time tx-time}))

  (open-db [this valid-time-or-basis]
    (let [db (xt/db this valid-time-or-basis)
          index-snapshot (open-index-snapshot db)
          db (assoc db :index-snapshot index-snapshot)
          entity-resolver-fn (new-entity-resolver-fn db)]
      (assoc db :entity-resolver-fn entity-resolver-fn)))

  PredContext
  (assoc-pred-ctx! [_ k v]
    (swap! !pred-ctx assoc k v))

  Closeable
  (close [_]
    (when interrupt-executor
      (doto interrupt-executor
        (.shutdownNow)
        (.awaitTermination 5000 TimeUnit/MILLISECONDS)))))

(defn ->query-engine {::sys/deps {:index-store :xtdb/index-store
                                  :bus :xtdb/bus
                                  :document-store :xtdb/document-store
                                  :query-cache {:xtdb/module 'xtdb.cache/->cache
                                                :cache-size 10240}
                                  :conform-cache {:xtdb/module 'xtdb.cache/->cache
                                                  :cache-size 10240}
                                  :find-cache {:xtdb/module 'xtdb.cache/->cache
                                               :cache-size 10240}}
                      ::sys/args {:entity-cache-size {:doc "Query Entity Cache Size"
                                                      :default (* 32 1024)
                                                      :spec ::sys/nat-int}
                                  :query-timeout {:doc "Query Timeout ms"
                                                  :default 30000
                                                  :spec ::sys/nat-int}
                                  :batch-size {:doc "Batch size of results"
                                               :default 100
                                               :required? true
                                               :spec ::sys/pos-int}
                                  :fn-allow-list {:doc "Predicate Allowlist"
                                                  :default nil
                                                  :spec ::fn-allow-list}}}
  [opts]
  (map->QueryEngine (assoc opts
                           :interrupt-executor (Executors/newSingleThreadScheduledExecutor (xio/thread-factory "xtdb-query-interrupter"))
                           :!pred-ctx (atom {}))))
