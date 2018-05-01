(ns crux.query
  (:require [clojure.spec.alpha :as s]
            [crux.db]
            [crux.kv]))

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
                    :pred (s/cat ::pred-fn ::pred-fn
                                 ::args (s/* any?))))
(s/def ::where (s/coll-of ::term :kind vector?))
(s/def ::query (s/keys :req-un [::find ::where]))

(defn- value-matches? [[term-e term-a term-v] result]
  (when-let [v (-> result (get term-e) (crux.db/attr-val term-a))]
    (or (not term-v)
        (and (symbol? term-v) (= (result term-v) v))
        (= term-v v))))

(defprotocol Binding
  (bind-key [this])
  (bind [this db results]))

(defrecord EntityBinding [e a v]
  Binding
  (bind-key [this] e)
  (bind [this db results]
    ;; Could be an issue hanging on to the head:
    (let [entities (if (and v (not (symbol? v)))
                     (crux.db/entities-for-attribute-value db a v)
                     (crux.db/entities db))]
      (if (empty? results)
        ;; First entity, assign this to every potential entity in the DB
        (map #(hash-map e %) entities)
        ;; New entity, join the results (todo, look at hash-join algos)
        (for [result results eid entities]
          (assoc result e eid))))))

(defrecord VarBinding [e a s]
  Binding
  (bind-key [this] s)
  (bind [this db results]
    (->> results
         (map #(assoc % s (crux.db/attr-val (get % e) a))))))

(defn- fact->entity-binding [[e a v]]
  (EntityBinding. e a v))

(defn- fact->var-binding [[e a v]]
  (when (and v (symbol? v))
    (VarBinding. e a v)))

(defn- do-bindings [db bindings results]
  (reduce (fn [results binding]
            (bind binding db results))
          results bindings))

(defn- query-plan->results
  "Reduce over the query plan, unifying the results across each
  stage."
  [db plan]
  (second (reduce (fn [[bindings results] [term-bindings pred-f]]
                    [(into bindings (map bind-key term-bindings))
                     (->> results
                          (do-bindings db (remove (comp bindings bind-key) term-bindings))
                          (filter (partial pred-f db))
                          (into #{}))])
                  [#{} nil] plan)))

(defn- query-terms->plan
  "Converts a sequence of query terms into a sequence of executable
  query stages."
  [terms]
  (for [[op t] terms]
    (condp = op
      :fact
      [(remove nil? [(fact->entity-binding t)
                     (fact->var-binding t)])
       (fn [_ result] (value-matches? t result))]

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
        [(map #(EntityBinding. % nil nil) (:bindings t))
         (let [query-plan (query-terms->plan (:terms t))
               or-results (atom nil)]
           (fn [db result]
             (let [or-results (or @or-results (reset! or-results (query-plan->results db query-plan)))]
               (when-not (some #(= (get result e) (get % e)) or-results)
                 result))))])

      :pred
      (let [{:keys [::args ::pred-fn]} t]
        [nil (fn [_ result]
               (let [args (map #(or (and (symbol? %) (result %)) %) args)]
                 (apply pred-fn args)))]))))

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
  (map (fn [k] (when-let [r (get result k)]
                 (if (satisfies? crux.db/Entity r) (crux.db/raw-val r) r))) find))

(defn q
  [db {:keys [find where] :as q}]
  (let [{:keys [find where] :as q} (s/conform ::query q)]
    (when (= :clojure.spec.alpha/invalid q)
      (throw (ex-info "Invalid input" (s/explain-data ::query q))))
    (->> where
         (query-terms->plan)
         (validate-query find)
         (query-plan->results db)
         (map (partial find-projection find))
         (into #{}))))
