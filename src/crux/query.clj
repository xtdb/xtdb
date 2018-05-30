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

(defn- compare-vals? [v1 v2]
  (if (coll? v1)
    (contains? v1 v2)
    (= v1 v2)))

(defn- value-matches? [db [term-e term-a term-v] result]
  (when-let [v (db/attr-val (get result term-e) term-a)]
    (or (not term-v)
        (and (symbol? term-v)
             (compare-vals? v (if-let [result-v (result term-v)]
                                (if (satisfies? db/Entity result-v)
                                  (db/->id result-v)
                                  result-v))))
        (compare-vals? v term-v))))

(defprotocol Binding
  (bind-key [this])
  (bind [this db]))

(defrecord EntityBinding [e fetch-entities-fn]
  Binding
  (bind-key [this] e)
  (bind [this db]
    (fn [rf]
      (fn
        ([]
         (rf))
        ([result]
         (rf result))
        ([result input]
         (if (satisfies? db/Datasource input)
           (transduce (map (partial hash-map e)) rf result (fetch-entities-fn db))
           (if (get input e)
             (rf result input)
             (transduce (map #(assoc input e %)) rf result (fetch-entities-fn db)))))))))

(defrecord VarBinding [e a s]
  Binding
  (bind-key [this] s)
  (bind [this db]
    (fn [rf]
      (fn
        ([]
         (rf))
        ([result]
         (rf result))
        ([result input]
         (if (get input s)
           (rf result input)
           (let [v (db/attr-val (get input e) a)]
             (if (coll? v)
               (transduce (map #(assoc input s %)) rf result v)
               (rf result (assoc input s v))))))))))

(defn- fetch-entities-within-range [a min-v max-v db]
  (db/entities-for-attribute-value db a min-v max-v))

(defn- fact->entity-binding [[e a v] terms]
  (let [fetch-entities-fn (cond (symbol? v)
                                (let [range-terms (->> terms
                                                       (filter (fn [[op]] (= :range op)))
                                                       (map second)
                                                       (filter #(= v (::sym %))))
                                      min-value (::val (first (filter #(= > (::fn %)) range-terms)))
                                      max-value (::val (first (filter #(= < (::fn %)) range-terms)))]
                                  (if (or min-value max-value)
                                    (partial fetch-entities-within-range a min-value max-value)
                                    db/entities))

                                v
                                (partial fetch-entities-within-range a v v)

                                :else
                                db/entities)]
    (EntityBinding. e fetch-entities-fn)))

(defn- fact->var-binding [[e a v]]
  (when (and v (symbol? v))
    (VarBinding. e a v)))

(defn- query-plan->xform
  "Create a tranduce from the query-plan."
  [db plan]
  (apply comp (for [[term-bindings pred-f] plan
                    :let [binding-transducers (map (fn [b] (bind b db)) term-bindings)]]
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
                    [(map #(EntityBinding. % db/entities) (:bindings t))
                     (let [
                           or-results (atom nil)]
                       (fn [db result]
                         (let [or-results (or @or-results
                                              (let [query-xform (query-plan->xform db (query-terms->plan (:terms t)))]
                                                (reset! or-results (into #{} query-xform [db]))))]
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
  [db {:keys [find where] :as q}]
  (let [{:keys [find where] :as q} (s/conform ::query q)]
    (when (= :clojure.spec.alpha/invalid q)
      (throw (ex-info "Invalid input" (s/explain-data ::query q))))
    (let [xform (->> where
                     (query-terms->plan)
                     (validate-query find)
                     (query-plan->xform db))]
      (into #{} (comp xform (map (partial find-projection find))) [db]))))
