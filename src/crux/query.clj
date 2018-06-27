(ns crux.query
  (:require [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            [crux.db :as db]
            [crux.index :as idx])
  (:import [java.util ArrayList]))

(defn- expression-spec [sym spec]
  (s/and seq?
         #(= sym (first %))
         (s/conformer second)
         spec))

(defn- logic-var? [x]
  (symbol? x))

(s/def ::pred-fn (s/and symbol?
                        (s/conformer #(some-> % resolve var-get))
                        fn?))
(s/def ::find (s/coll-of logic-var? :kind vector?))
(s/def ::bgp (s/and vector?
                    (s/cat :e (some-fn logic-var? keyword?)
                           :a keyword?
                           :v (s/? any?))))
(s/def ::term (s/or :bgp ::bgp
                    :not (expression-spec 'not ::term)
                    :or (expression-spec 'or ::where)
                    :and (expression-spec 'and ::where)
                    :not-join (s/cat :pred #{'not-join}
                                     :bindings (s/coll-of logic-var? :kind vector?)
                                     :terms ::where)
                    :range (s/cat :op '#{< <= >= >}
                                  :sym logic-var?
                                  :val (complement logic-var?))
                    :unify (s/cat :op '#{== !=}
                                  :x any?
                                  :y any?)
                    :pred (s/and list?
                                 (s/cat :pred-fn ::pred-fn
                                        :args (s/* any?)))))
(s/def ::where (s/coll-of ::term :kind vector?))
(s/def ::query (s/keys :req-un [::find ::where]))

(defn- v-for-comparison [v]
  (if (satisfies? db/Entity v)
    (db/->id v)
    v))

;; TODO: this needs to deal with comparsions of ids against their
;; source, like a keyword. Needs work, but don't want to expose
;; id->bytes here just yet.
(defn- compare-vals? [v1 v2]
  (if (coll? v1)
    (contains? v1 v2)
    (= v1 v2)))

(defn- entity-literal? [e]
  (and (satisfies? idx/IdToBytes e)
       (not (string? e))))

(defn- value-matches? [qc db {:keys [e a v]} result]
  (let [entity (get result e)]
    (or (not v)
        (when-let [entity-v (db/attr-val entity a)]
          (or (and (logic-var? v)
                   (compare-vals? entity-v (some-> (result v) v-for-comparison)))
              (compare-vals? entity-v v))))))

(defn- range-op [x {:keys [sym val op]}]
  (let [diff (compare x val)]
    (case op
      < (neg? diff)
      <= (not (pos? diff))
      > (pos? diff)
      >= (not (neg? diff)))))

(defn binding-xform [k f]
  (fn [rf]
    (fn
      ([]
       (rf))
      ([result]
       (rf result))
      ([result input]
       (if (get input k)
         (rf result input)
         (reduce rf result (map (partial assoc input k) (f input))))))))

(defn binding-agg-xform [k aggregate-fn]
  (fn [rf]
    (let [buf (ArrayList.)]
      (fn
        ([]
         (rf))
        ([result]
         (rf (reduce rf result (aggregate-fn buf))))
        ([result input]
         (if (get input k)
           (rf result input)
           (do
             (.add buf input)
             result)))))))

(defprotocol Binding
  (bind-key [this])
  (bind [this query-context]))

(defrecord EntityBinding [e a v range-vals join-attributes]
  Binding
  (bind-key [this] e)
  (bind [this query-context]
    (binding-agg-xform e (fn [results]
                           (when (seq results)
                             (let [db (get (first results) '$)]
                               (cond (and (logic-var? v) (some #(get % v) results))
                                     (do (log/debug :secondary-index-result-join e a v join-attributes)
                                         (for [[distinct-v results] (group-by (comp v-for-comparison #(get % v)) results)
                                               je (db/entities-for-attribute-value db query-context a {:min-v distinct-v
                                                                                                       :inclusive-min-v? true
                                                                                                       :max-v distinct-v
                                                                                                       :inclusive-max-v? true})
                                               r results]
                                           (assoc r e je)))

                                     (and (logic-var? v) join-attributes)
                                     (do (log/debug :secondary-index-leapfrog-join e a v join-attributes range-vals)
                                         (let [[min-v max-v] range-vals
                                               range-constraints {:min-v (:val min-v)
                                                                  :inclusive-min-v? (= '>= (:op min-v))
                                                                  :max-v (:val max-v)
                                                                  :inclusive-max-v? (= '<= (:op max-v))}]
                                           (for [r results je (db/entity-join db query-context join-attributes range-constraints)]
                                             (assoc r e je))))

                                     (and (logic-var? v) range-vals)
                                     (do (log/debug :secondary-index-range e a v range-vals)
                                         (let [[min-v max-v] range-vals
                                               range-constraints {:min-v (:val min-v)
                                                                  :inclusive-min-v? (= '>= (:op min-v))
                                                                  :max-v (:val max-v)
                                                                  :inclusive-max-v? (= '<= (:op max-v))}]
                                           (for [r results je (db/entities-for-attribute-value db query-context a range-constraints)]
                                             (assoc r e je))))

                                     (and v (not (logic-var? v)))
                                     (do (log/debug :secondary-index-lookup e a v)
                                         (for [r results je (db/entities-for-attribute-value db query-context a {:min-v v
                                                                                                                 :inclusive-min-v? true
                                                                                                                 :max-v v
                                                                                                                 :inclusive-max-v? true})]
                                           (assoc r e je)))

                                     :else
                                     (do (log/debug :secondary-index-scan e a v)
                                         (for [r results je (db/entity-join db query-context [a] {:min-v nil
                                                                                                  :inclusive-min-v? true
                                                                                                  :max-v nil
                                                                                                  :inclusive-max-v? true})]
                                           (assoc r e je))))))))))

(defrecord LiteralEntityBinding [e a v range-vals]
  Binding
  (bind-key [this] e)
  (bind [this query-context]
    (binding-agg-xform e (fn [results]
                           (when (seq results)
                             (let [db (get (first results) '$)]
                               (when-let [entity (db/entity db query-context e)]
                                 (let [[min-v max-v] range-vals
                                       actual-v (db/attr-val entity a)]
                                   (when (or (and (logic-var? v)
                                                  (if min-v
                                                    (range-op actual-v min-v)
                                                    true)
                                                  (if max-v
                                                    (range-op actual-v max-v)
                                                    true))
                                             (compare-vals? actual-v v))
                                     (for [r results]
                                       (assoc r e entity)))))))))))

(defn- find-subsequent-range-terms [v terms]
  (when (logic-var? v)
    (let [range-terms (->> terms
                           (filter (fn [[op]] (= :range op)))
                           (map second)
                           (filter #(= v (:sym %))))
          min-value (last (sort-by :val (filter #(contains? '#{> >=} (:op %)) range-terms)))
          max-value (first (sort-by :val (filter #(contains? '#{< <=} (:op %)) range-terms)))]
      (when (or min-value max-value)
        [min-value max-value]))))

(defn- find-subsequent-join-terms [a first-v terms]
  (when (logic-var? first-v)
    (->> (for [[op term] terms
               :when (= :bgp op)
               :let [{:keys [a v]} term]
               :when (= first-v v)]
           a)
         (not-empty)
         (into [a])
         (distinct))))

(defn- find-subsequent-join-literals [as terms]
  (let [as (set as)]
    (->> (for [[op term] terms
               :when (= :bgp op)
               :let [{:keys [_ a v]} term]
               :when (and (contains? as a)
                          (not (nil? v))
                          (not (logic-var? v)))]
           v)
         (not-empty)
         (into (sorted-set)))))

(defn- bgp->entity-binding [{:keys [e a v]} terms]
  (let [join-attributes (find-subsequent-join-terms a v terms)
        join-literals (find-subsequent-join-literals join-attributes terms)
        range-vals (if (seq join-literals)
                     [(first join-literals) (last join-literals)]
                     (find-subsequent-range-terms v terms))]
    (if (entity-literal? e)
      (do (log/debug :literal-entity-binding e a v range-vals)
          (->LiteralEntityBinding e a v range-vals))
      (do (log/debug :entity-binding e a v range-vals join-attributes)
          (->EntityBinding e a v range-vals join-attributes)))))

(defrecord VarBinding [e a s]
  Binding
  (bind-key [this] s)
  (bind [this qc]
    (binding-xform s (fn [input]
                       (let [db (get input '$)
                             v (db/attr-val (get input e) a)
                             v (or (some-> (when (entity-literal? v)
                                             (get input v (db/entity db qc v)))
                                           (vector))
                                   v)]
                         (log/debug :var-bind this e a s v)
                         (if (coll? v) v [v]))))))

(defn- bgp->var-binding [{:keys [e a v]}]
  (when (and v (logic-var? v))
    (log/debug :var-binding e a v)
    (->VarBinding e a v)))

(defn- query-plan->xform
  "Create a tranduce from the query-plan."
  [db query-context plan]
  (apply comp (for [[term-bindings pred-f] plan
                    :let [binding-transducers (map #(bind % query-context) term-bindings)]]
                (comp (apply comp binding-transducers)
                      (filter (partial pred-f db))))))

(defn- query-terms->plan
  "Converts a sequence of query terms into a sequence of executable
  query stages."
  [query-context [[op t :as term] & terms]]
  (when term
    (let [stage (condp = op
                  :bgp
                  [(remove nil? [(bgp->entity-binding t terms)
                                 (bgp->var-binding t)])
                   (fn [db result] (value-matches? query-context db t result))]

                  :and
                  (let [sub-plan (query-terms->plan query-context t)]
                    [(mapcat first sub-plan)
                     (fn [db result]
                       (every? (fn [[_ pred-fn]]
                                 (pred-fn db result))
                               sub-plan))])

                  :not
                  (let [query-plan (query-terms->plan query-context [t])
                        [bindings pred-fn?] (first query-plan)]
                    [bindings
                     (fn [db result] (not (pred-fn? db result)))])

                  :or
                  (let [sub-plan (query-terms->plan query-context t)]
                    (assert (->> sub-plan
                                 (map #(into #{} (map bind-key (first %))))
                                 (apply =)))
                    [(mapcat first sub-plan)
                     (fn [db result]
                       (some (fn [[_ pred-fn :as s]]
                               (pred-fn db result))
                             sub-plan))])

                  :not-join
                  (let [e (-> t :bindings first)]
                    [nil
                     (let [or-results (atom nil)]
                       (fn [db result]
                         (let [or-results (or @or-results
                                              (let [query-xform (query-plan->xform db query-context (query-terms->plan query-context (:terms t)))]
                                                (reset! or-results (into #{} query-xform [{'$ db}]))))]
                           (when-not (some #(db/eq? (get result e) (get % e)) or-results)
                             result))))])

                  :pred
                  (let [{:keys [args pred-fn]} t]
                    [nil (fn [_ result]
                           (let [args (map #(or (and (logic-var? %) (result %)) %) args)]
                             (apply pred-fn args)))])

                  :range
                  [nil (fn [_ result]
                         (range-op (result (:sym t)) t))])]
      (cons stage (query-terms->plan query-context terms)))))

(defn- term-symbols [terms]
  (->> terms
       (mapcat first)
       (map bind-key)
       (into #{})))

(defn- validate-query [find plan]
  (let [variables (term-symbols plan)]
    (doseq [binding find]
      (when-not (variables binding)
        (throw (IllegalArgumentException. (str "Find clause references unbound variable: " binding))))))
  plan)

(defn- find-projection [find result]
  (map (fn [find-clause]
         (let [v (get result find-clause)]
           (if (satisfies? db/Entity v) (db/->id v) v)))
       find))

(defn q
  [db q]
  (let [{:keys [find where] :as q} (s/conform ::query q)]
    (when (= :clojure.spec.alpha/invalid q)
      (throw (ex-info "Invalid input" (s/explain-data ::query q))))
    (with-open [query-context (db/new-query-context db)]
      (let [xform (->> where
                       (query-terms->plan query-context)
                       (validate-query find)
                       (query-plan->xform db query-context))]
        (into #{} (comp xform (map (partial find-projection find))) [{'$ db}])))))
