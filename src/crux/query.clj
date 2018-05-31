(ns crux.query
  (:require [clojure.spec.alpha :as s]
            [crux.db :as db]))

(defn- expression-spec [sym spec]
  (s/and seq?
         #(= sym (first %))
         (s/conformer second)
         spec))

(s/def ::pred-fn (s/and symbol?
                        (s/conformer #(some-> % resolve var-get))
                        fn?))
(s/def ::find (s/coll-of symbol? :kind vector?))
(s/def ::fact (s/coll-of #(or (keyword? %)
                              (symbol? %)
                              (string? %))
                         :kind vector?))
(s/def ::term (s/or :fact ::fact
                    :not (expression-spec 'not ::term)
                    :or (expression-spec 'or ::where)
                    :and (expression-spec 'and ::where)
                    :not-join (s/cat :pred #{'not-join}
                                     :bindings (s/coll-of symbol? :kind vector?)
                                     :terms ::where)
                    :range (s/cat ::fn (s/and ::pred-fn #{< >})
                                  ::sym symbol?
                                  ::val (complement symbol?))
                    :pred (s/cat ::pred-fn ::pred-fn
                                 ::args (s/* any?))))
(s/def ::where (s/coll-of ::term :kind vector?))
(s/def ::query (s/keys :req-un [::find ::where]))

(defn- v-for-comparison [v]
  (if (satisfies? db/Entity v)
    (db/->id v)
    v))

(defn- compare-vals? [v1 v2]
  (if (coll? v1)
    (contains? v1 v2)
    (= v1 v2)))

(defn- value-matches? [db [term-e term-a term-v] result]
  (when-let [v (db/attr-val (get result term-e) term-a)]
    (or (not term-v)
        (and (symbol? term-v)
             (compare-vals? v (some-> (result term-v) v-for-comparison)))
        (compare-vals? v term-v))))

(defprotocol Binding
  (bind-key [this])
  (bind [this input]))

(defrecord EntityBinding [e fetch-entities-fn]
  Binding
  (bind-key [this] e)
  (bind [this input] (fetch-entities-fn input)))

(defn- fetch-entities-within-range [a min-v max-v db]
  (db/entities-for-attribute-value db a min-v max-v))

(defn- find-subsequent-range-terms [v terms]
  (when (symbol? v)
    (let [range-terms (->> terms
                           (filter (fn [[op]] (= :range op)))
                           (map second)
                           (filter #(= v (::sym %))))
          min-value (::val (first (filter #(= > (::fn %)) range-terms)))
          max-value (::val (first (filter #(= < (::fn %)) range-terms)))]
      (when (or min-value max-value)
        [min-value max-value]))))

(defn- fact->entity-binding [[e a v] terms]
  (let [subsequent-range (find-subsequent-range-terms v terms)
        fetch-entities-fn (fn [e]
                            (let [db (get e '$)]
                              (cond (and (symbol? v) (get e v))
                                    (let [v (v-for-comparison (get e v))]
                                      (db/entities-for-attribute-value db a v v))

                                    (and (symbol? v) subsequent-range)
                                    (apply db/entities-for-attribute-value db a subsequent-range)

                                    (and v (not (symbol? v)))
                                    (db/entities-for-attribute-value db a v v)

                                    :else
                                    (db/entities db))))]
    (EntityBinding. e fetch-entities-fn)))

(defrecord VarBinding [e a s]
  Binding
  (bind-key [this] s)
  (bind [this input]
    (let [v (db/attr-val (get input e) a)]
      (if (coll? v) v [v]))))

(defn- fact->var-binding [[e a v]]
  (when (and v (symbol? v))
    (VarBinding. e a v)))

(defn binding-xform [b]
  (fn [rf]
    (fn
      ([]
       (rf))
      ([result]
       (rf result))
      ([result input]
       (if (get input (bind-key b))
         (rf result input)
         (transduce (map (partial assoc input (bind-key b))) rf result (bind b input)))))))

(defn- query-plan->xform
  "Create a tranduce from the query-plan."
  [db plan]
  (apply comp (for [[term-bindings pred-f] plan
                    :let [binding-transducers (map binding-xform term-bindings)]]
                (comp (apply comp binding-transducers)
                      (filter (partial pred-f db))))))

(defn- query-terms->plan
  "Converts a sequence of query terms into a sequence of executable
  query stages."
  [[[op t :as term] & terms]]
  (when term
    (let [stage (condp = op
                  :fact
                  [(remove nil? [(fact->entity-binding t terms)
                                 (fact->var-binding t)])
                   (fn [db result] (value-matches? db t result))]

                  :and
                  (let [sub-plan (query-terms->plan t)]
                    [(mapcat first sub-plan)
                     (fn [db result]
                       (every? (fn [[_ pred-fn]]
                                 (pred-fn db result))
                               sub-plan))])

                  :not
                  (let [query-plan (query-terms->plan [t])
                        [bindings pred-fn?] (first query-plan)]
                    [bindings
                     (fn [db result] (not (pred-fn? db result)))])

                  :or
                  (let [sub-plan (query-terms->plan t)]
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
                                              (let [query-xform (query-plan->xform db (query-terms->plan (:terms t)))]
                                                (reset! or-results (into #{} query-xform [{'$ db}]))))]
                           (when-not (some #(= (get result e) (get % e)) or-results)
                             result))))])

                  :pred
                  (let [{:keys [::args ::pred-fn]} t]
                    [nil (fn [_ result]
                           (let [args (map #(or (and (symbol? %) (result %)) %) args)]
                             (apply pred-fn args)))])

                  :range
                  [nil (fn [_ result]
                         ((::fn t) (result (::sym t)) (::val t)))])]
      (cons stage (query-terms->plan terms)))))

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
    (let [xform (->> where
                     (query-terms->plan)
                     (validate-query find)
                     (query-plan->xform db))]
      (into #{} (comp xform (map (partial find-projection find))) [{'$ db}]))))
