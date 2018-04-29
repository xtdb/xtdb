(ns crux.query
  (:require [clojure.spec.alpha :as s]
            [crux.datasource]
            [crux.core]))

(defn- expression-spec [sym spec]
  (s/and seq?
         #(= sym (first %))
         (s/conformer second)
         spec))

(s/def ::pred-fn (s/and symbol?
                        (s/conformer #(some-> % resolve var-get))
                        fn?))
(s/def ::find (s/coll-of symbol? :kind vector?))
(s/def ::rule (s/coll-of #(or (keyword? %)
                              (symbol? %)
                              (string? %))
                         :kind vector?))
(s/def ::term (s/or :term ::rule
                    :not (expression-spec 'not ::rule)
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
          v (crux.datasource/attr-val e term-a)
          result (assoc result term-a v)]
      (if (and (symbol? term-v) (not (result term-v)))
        (assoc result term-v v)
        result))
    result))

(defn- query-plan->results
  "Reduce over the query plan, unifying the results across each
  stage."
  [datasource plan]
  (reduce (fn [results [e bindings pred-f]]
            (let [results
                  (cond (not results)
                        (map #(hash-map e %) (crux.datasource/entities datasource))

                        (get (first results) e)
                        results

                        :else
                        (for [result results eid (crux.datasource/entities datasource)]
                          (assoc result e eid)))]
              (->> results
                   (map (partial apply-bindings bindings))
                   (filter (partial pred-f datasource))
                   (into #{}))))
          nil plan))

(defn- query-terms->plan
  "Converts a sequence of query terms into a sequence of executable
  query stages."
  [terms]
  (for [[op t] terms]
    (condp = op
      :term
      [(first t)
       t
       (fn [_ result] (value-matches? t result))]

      :not
      [(first t)
       t
       (fn [_ result] (not (value-matches? t result)))]

      :or
      (let [sub-plan (query-terms->plan t)
            e (ffirst sub-plan)]
        [e
         (-> t first second)
         (fn [datasource result]
           (when (some (fn [[_ _ pred-fn]]
                         (pred-fn datasource result))
                       sub-plan)
             result))])

      :not-join
      (let [e (-> t :bindings first)]
        [e
         nil
         (let [query-plan (query-terms->plan (:terms t))
               or-results (atom nil)]
           (fn [datasource result]
             (let [or-results (or @or-results (reset! or-results (query-plan->results datasource query-plan)))]
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
              :term (filter symbol? t)
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
                 (if (satisfies? crux.datasource/Entity r) (crux.datasource/raw-val r) r))) find))

(defn q
  ([db terms]
   (q db terms (java.util.Date.)))
  ([db {:keys [find where] :as q} ts]
   (let [{:keys [find where] :as q} (s/conform ::query q)
         datasource (crux.core/map->KvDatasource {:db db :at-ts ts})]
     (validate-query q)
     (when (= :clojure.spec.alpha/invalid q)
       (throw (ex-info "Invalid input" (s/explain-data ::query q))))
     (->> where
          (query-terms->plan)
          (query-plan->results datasource)
          (map (partial find-projection find))
          (into #{})))))
