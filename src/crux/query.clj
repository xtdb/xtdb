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
                    :not (expression-spec 'not ::fact)
                    :or (expression-spec 'or ::where)
                    :not-join (s/cat :pred #{'not-join}
                                     :bindings (s/coll-of symbol? :kind vector?)
                                     :terms ::where)
                    :pred (s/cat ::pred-fn ::pred-fn
                                 ::args (s/* any?))))
(s/def ::where (s/coll-of ::term :kind vector?))
(s/def ::query (s/keys :req-un [::find ::where]))

(defn- value-matches? [[term-e term-a term-v] result]
  (when-let [v (result term-a)]
    (and v (or (not term-v)
               (and (symbol? term-v) (= (result term-v) v))
               (= term-v v)))))

(defn- apply-bindings [[term-e term-a term-v] result]
  (if term-a
    (let [e (result term-e)
          v (crux.db/attr-val e term-a)
          result (assoc result term-a v)]
      (if (and (symbol? term-v) (not (result term-v)))
        (assoc result term-v v)
        result))
    result))

(defn- query-plan->results
  "Reduce over the query plan, unifying the results across each
  stage."
  [db plan]
  (reduce (fn [results [e bindings pred-f]]
            (let [results
                  (cond (not results)
                        (map #(hash-map e %) (crux.db/entities db))

                        (get (first results) e)
                        results

                        :else
                        (for [result results eid (crux.db/entities db)]
                          (assoc result e eid)))]
              (->> results
                   (map (partial apply-bindings bindings))
                   (filter (partial pred-f db))
                   (into #{}))))
          nil plan))

#_(s/conform :crux.query/where [['e :name 'name]
                              '(not (or [[e :last-name "Ivanov"]
                                         [e :name "Bob"]]))])

(defn- query-terms->plan
  "Converts a sequence of query terms into a sequence of executable
  query stages."
  [terms]
  (for [[op t] terms]
    (condp = op
      :fact
      [(first t)
       t
       (fn [_ result] (value-matches? t result))]

      :not
      [(first t)
       t
       (fn [_ result] (not (value-matches? t result)))]

      :or
      (let [sub-plan (query-terms->plan t)
            e (ffirst sub-plan)
            facts (filter #(= :fact (first %)) t)]
        (assert (->> facts
                     (map #(into #{} (filter symbol? (second %))))
                     (apply =)))
        [(-> facts first second first)
         (-> facts first second)
         (fn [db result]
           (when (some (fn [[_ _ pred-fn]]
                         (pred-fn db result))
                       sub-plan)
             result))])

      :not-join
      (let [e (-> t :bindings first)]
        [e
         nil
         (let [query-plan (query-terms->plan (:terms t))
               or-results (atom nil)]
           (fn [db result]
             (let [or-results (or @or-results (reset! or-results (query-plan->results db query-plan)))]
               (when-not (some #(= (get result e) (get % e)) or-results)
                 result))))])

      :pred
      (let [{:keys [::args ::pred-fn]} t]
        ['e nil (fn [_ result]
                  (let [args (map #(or (and (symbol? %) (result %)) %) args)]
                    (apply pred-fn args)))]))))

(defn term-symbols [terms]
  (reduce into #{}
          (for [[op t] terms]
            (condp = op
              :fact (filter symbol? t)
              :not (filter symbol? t)
              :or (term-symbols (:terms t))
              :not-join (term-symbols (:terms t))
              :pred []))))

(defn- validate-query [{:keys [find where]}]
  (let [variables (term-symbols where)]
    (doseq [binding find]
      (when-not (variables binding)
        (throw (IllegalArgumentException. (str "Find clause references unbound variable: " binding)))))))

(defn- find-projection [find result]
  (map (fn [k] (when-let [r (get result k)]
                 (if (satisfies? crux.db/Entity r) (crux.db/raw-val r) r))) find))

(defn q
  [db {:keys [find where] :as q}]
  (let [{:keys [find where] :as q} (s/conform ::query q)]
    (validate-query q)
    (when (= :clojure.spec.alpha/invalid q)
      (throw (ex-info "Invalid input" (s/explain-data ::query q))))
    (->> where
         (query-terms->plan)
         (query-plan->results db)
         (map (partial find-projection find))
         (into #{}))))
