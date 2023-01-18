(ns core2.datalog
  (:require [clojure.set :as set]
            [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [core2.error :as err]
            [core2.operator :as op]
            [core2.vector.writer :as vw])
  (:import clojure.lang.MapEntry
           java.lang.AutoCloseable
           org.apache.arrow.memory.BufferAllocator))

(s/def ::logic-var
  (s/and simple-symbol?
         (comp #(str/starts-with? % "?") name)))

;; TODO flesh out
(def ^:private eid? (some-fn string? number? inst? keyword?))

(s/def ::eid eid?)
(s/def ::value (some-fn eid?))

(s/def ::aggregate
  (s/cat :agg-fn simple-symbol?
         :param ::logic-var))

(s/def ::find-arg
  (s/or :logic-var ::logic-var
        :aggregate ::aggregate))

(s/def ::not-join
  (s/cat :not-join '#{not-join}
         :args ::args-list
         :terms (s/+ ::term)))

(s/def ::term
  (s/or :triple ::triple
        :not-join ::not-join
        :predicate ::predicate))

(s/def ::find (s/coll-of ::find-arg :kind vector? :min-count 1))
(s/def ::keys (s/coll-of symbol? :kind vector?))

(s/def ::args-list (s/coll-of ::logic-var, :kind vector?, :min-count 1))

(s/def ::source
  (s/and simple-symbol?
         (comp #(str/starts-with? % "$") name)))

(s/def ::in-binding
  (s/or :source ::source
        :scalar ::logic-var
        :tuple ::args-list
        :collection (s/tuple ::logic-var '#{...})
        :relation (s/tuple ::args-list)))

(s/def ::in (s/* ::in-binding))

;; 'triple' ;)
(s/def ::triple
  (s/and vector?
         (s/conformer identity vec)
         (s/cat :src (s/? ::source)
                :e (s/or :literal ::eid, :logic-var ::logic-var)
                :a keyword?
                :v (s/? (s/or :literal ::value, :logic-var ::logic-var)))
         (s/conformer (fn [triple]
                        (-> triple (update :src (some-fn identity (constantly '$)))))
                      identity)))

(s/def ::call
  (s/and list?
         (s/cat :f simple-symbol?
                :args (s/* (s/or :logic-var ::logic-var
                                 :value ::value)))))

(s/def ::predicate
  (s/and vector?
         (s/cat :call (s/spec ::call))))

(s/def ::where
  (s/coll-of ::term :kind vector? :min-count 1))

(s/def ::offset nat-int?)
(s/def ::limit nat-int?)

(s/def ::order-element
  (s/and vector?
         (s/cat :find-arg (s/or :logic-var ::logic-var
                                :aggregate ::aggregate)
                :direction (s/? #{:asc :desc}))))

(s/def ::order-by (s/coll-of ::order-element :kind vector?))

(s/def ::query
  (s/keys :req-un [::find]
          :opt-un [::keys ::in ::where ::order-by ::offset ::limit]))

(s/def ::relation-arg
  (s/or :maps (s/coll-of (s/map-of simple-keyword? any?))
        :vecs (s/coll-of vector?)))

(defn- conform-query [query]
  (let [conformed-query (s/conform ::query query)]
    (when (s/invalid? conformed-query)
      (throw (err/illegal-arg :malformed-query
                              {::err/message "Malformed query"
                               :query query
                               :explain (s/explain-data ::query query)})))
    conformed-query))

(declare plan-query)

(defn- wrap-select [plan predicates]
  (case (count predicates)
    0 plan
    1 [:select (first predicates) plan]
    [:select (list* 'and predicates) plan]))

(defn- wrap-scan-col-preds [scan-col col-preds]
  (case (count col-preds)
    0 scan-col
    1 {scan-col (first col-preds)}
    {scan-col (list* 'and col-preds)}))

(defn- analyse-in [{in-bindings :in}]
  (let [in-bindings (->> in-bindings
                         (into [] (map-indexed
                                   (fn [idx [binding-type binding-arg]]
                                     (let [prefix (str "in" idx)
                                           table-key (symbol (str "?" prefix))]
                                       (letfn [(with-param-prefix [lv]
                                                 (symbol (str "?" prefix "_" (subs (str lv) 1))))
                                               (with-table-col-prefix [lv]
                                                 (symbol (str prefix "_" (subs (str lv) 1))))]
                                         (-> (case binding-type
                                               :source {:in-col binding-arg}
                                               :scalar {:var->col {binding-arg (with-param-prefix binding-arg)}, :in-col binding-arg}
                                               :tuple {:var->col (->> binding-arg (into {} (map (juxt identity with-param-prefix))))
                                                       :in-cols binding-arg}
                                               :relation (let [cols (first binding-arg)]
                                                           {:table-key table-key
                                                            :in-cols cols
                                                            :var->col (->> cols (into {} (map (juxt identity with-table-col-prefix))))})
                                               :collection (let [col (first binding-arg)]
                                                             {:table-key table-key
                                                              :var->col {col (with-table-col-prefix col)}
                                                              :in-col col}))
                                             (assoc :binding-type binding-type))))))))]
    {:in-bindings in-bindings
     :var->cols (-> in-bindings
                    (->> (mapcat :var->col)
                         (group-by key))
                    (update-vals #(into #{} (map val) %)))}))

(defn- plan-in-tables [{:keys [in-bindings]}]
  (->> in-bindings
       (into [] (keep (fn [{:keys [table-key var->col]}]
                        (when table-key
                          [:rename var->col
                           [:table table-key]]))))))

(defn- analyse-triples [triples]
  (letfn [(->triple-rel [^long idx, [[src e] triples]]
            (let [triples (->> (conj triples {:e e, :a :id, :v e})
                               (map #(update % :a symbol)))
                  prefix (symbol (str "t" idx))
                  var->attrs (-> triples
                                 (->> (keep (fn [{:keys [a], [v-type v-arg] :v}]
                                              (when (= :logic-var v-type)
                                                {:a a, :lv v-arg})))
                                      (group-by :lv))
                                 (update-vals #(into #{} (map :a) %)))]

              {:src src, :e e, :prefix prefix,
               :attrs (into #{} (map :a) triples)
               :attr->lits (-> triples
                               (->> (keep (fn [{:keys [a], [v-type v-arg] :v}]
                                            (when (= :literal v-type)
                                              {:a a, :lit v-arg})))
                                    (group-by :a))
                               (update-vals #(into #{} (map :lit) %)))
               :var->attrs var->attrs
               :var->col (->> (keys var->attrs)
                              (into {} (map (juxt identity
                                                  #(symbol (str prefix "_" (subs (str %) 1)))))))}))]

    (->> (group-by (juxt :src :e) triples)
         (into [] (map-indexed ->triple-rel)))))

(defn- analyse-pred [{{:keys [args] :as call} :call}]
  {:call call
   :vars (->> args
              (into #{} (keep (fn [[var-type var-arg]]
                                (when (= :logic-var var-type)
                                  var-arg)))))})

(defn- analyse-not-joins [not-join-clauses]
  ;; TODO check args all bind
  (->> not-join-clauses
       (into [] (map-indexed (fn [idx {:keys [args terms]}]
                               (let [var->col (->> args
                                                   (into {}
                                                         (map (juxt identity #(symbol (str "nj" idx "_" (subs (str %) 1)))))))]
                                 {:inner-q {:find (vec (for [arg args]
                                                         [:logic-var arg]))
                                            :keys (vec (for [arg args]
                                                         (keyword (var->col arg))))
                                            :where terms}
                                  :var->col var->col}))))
       (not-empty)))

(defn- wrap-not-joins [plan {{:keys [not-join-clauses]} :where-attrs
                             {body-var->col :var->col} :body-attrs}]
  (reduce (fn [plan {nj-var->col :var->col, :keys [inner-q]}]
            [:anti-join (->> nj-var->col
                             (mapv (fn [[lv nv-col]]
                                     {(get body-var->col lv) nv-col})))
             plan

             (:plan (plan-query inner-q))])
          plan
          not-join-clauses))

(defn- analyse-where [{where-clauses :where}]
  (let [{triple-clauses :triple
         pred-clauses :predicate
         not-join-clauses :not-join} (-> where-clauses
                                         (->> (group-by first))
                                         (update-vals #(mapv second %)))

        triple-rels (analyse-triples triple-clauses)]

    {:triple-rels triple-rels
     :preds (mapv analyse-pred pred-clauses)
     :not-join-clauses (analyse-not-joins not-join-clauses)
     :var->cols (-> triple-rels
                    (->> (mapcat :var->col)
                         (group-by key))
                    (update-vals #(into #{} (map val) %)))}))

(defn- plan-where-rels [{:keys [triple-rels]}]
  (for [{:keys [src attrs attr->lits var->col var->attrs]} triple-rels]
    [:project (vec (for [[lv col] var->col]
                     {col (first (get var->attrs lv))}))
     (-> [:scan src 'xt_docs
          (->> (into attrs '#{application_time_start application_time_end})
               (into [] (map (fn [attr]
                               (-> attr
                                   (wrap-scan-col-preds
                                    (concat (for [lit (get attr->lits attr)]
                                              (list '= attr lit))
                                            (case attr
                                              application_time_start ['(<= application_time_start (current-timestamp))]
                                              application_time_end ['(> application_time_end (current-timestamp))]
                                              nil))))))))]

         (wrap-select (for [attrs (->> (vals var->attrs)
                                       (filter #(> (count %) 1)))
                            [a1 a2] (partition 2 1 attrs)]
                        (list '= a1 a2))))]))

(defn- wrap-predicates [plan {{:keys [preds]} :where-attrs
                              {:keys [var->col]} :body-attrs}]
  (-> plan
      (wrap-select
       (for [{:keys [call]} preds]
         (s/unform ::call
                   (-> call
                       (update :args (fn [args]
                                       (->> args
                                            (mapv (fn [[arg-type arg]]
                                                    (if (= arg-type :logic-var)
                                                      [:logic-var (get var->col arg)]
                                                      [arg-type arg]))))))))))))

(defn- ->body-attrs [{:keys [where-attrs in-attrs]}]
  (let [var->cols (reduce (fn [acc [lv col]]
                            (-> acc
                                (update lv (fnil into #{}) col)))
                          (:var->cols where-attrs)
                          (:var->cols in-attrs))]
    {:var->cols var->cols
     :var->col (->> (keys var->cols)
                    (into {} (map (juxt identity #(symbol (subs (str %) 1))))))}))

(defn- plan-relations [{:keys [where-attrs in-attrs],
                        {:keys [var->cols var->col]} :body-attrs
                        :as attrs}]
  (let [in-rels (plan-in-tables in-attrs)
        where-rels (plan-where-rels where-attrs)

        unify-preds (vec
                     (for [cols (vals var->cols)
                           :when (> (count cols) 1)
                           ;; this picks an arbitrary binary order if there are >2
                           ;; once mega-join has multi-way joins we could throw the multi-way `=` over the fence
                           [c1 c2] (partition 2 1 cols)]
                       (list '= c1 c2)))]

    (-> [:project (vec (for [[lv cols] var->cols]
                         {(get var->col lv) (first cols)}))
         (-> [:mega-join []
              (vec (concat in-rels where-rels))]
             (wrap-select unify-preds))]
        (wrap-predicates attrs)
        (wrap-not-joins attrs))))

(defn- analyse-find-clauses [{find-clauses :find, rename-keys :keys}]
  (when-let [clauses (mapv (fn [[clause-type clause] rename-key]
                             (let [rename-sym (some-> rename-key symbol)]
                               (-> (case clause-type
                                     :logic-var {:lv clause
                                                 :col rename-sym
                                                 :vars #{clause}}
                                     :aggregate (let [{:keys [agg-fn param]} clause]
                                                  {:aggregate clause
                                                   :col (or rename-sym (symbol (str agg-fn "-" param)))
                                                   :vars #{param}}))
                                   (assoc :clause-type clause-type))))
                           find-clauses
                           (or rename-keys (repeat nil)))]

    (let [{agg-clauses :aggregate, lv-clauses :logic-var} (->> clauses (group-by :clause-type))]
      (into {:clauses (mapv #(select-keys % [:lv :clause-type :col]) clauses)}

            (when-let [aggs (->> agg-clauses
                                 (into {} (comp (filter #(= :aggregate (:clause-type %)))
                                                (map (juxt :col :aggregate)))))]
              {:aggs aggs
               :grouping-vars (->> lv-clauses
                                   (into #{} (comp (remove :aggregate) (mapcat :vars))))})))))

(defn- analyse-order-by [{:keys [order-by]}]
  (when order-by
    (let [clause-attrs (analyse-find-clauses {:find (map :find-arg order-by)})]
      (-> clause-attrs
          (update :clauses (fn [clauses]
                             (mapv (fn [clause {:keys [direction]}]
                                     (-> clause (assoc :direction (or direction :asc))))
                                   clauses order-by)))))))

(defn- analyse-head [query]
  (let [find-attrs (analyse-find-clauses query)
        order-by-attrs (analyse-order-by query)]
    {:find-clauses (:clauses find-attrs)
     :order-by-clauses (not-empty (:clauses order-by-attrs))
     :aggs (->> (merge (:aggs find-attrs)
                       (:aggs order-by-attrs))
                (not-empty))

     ;; HACK: need to error-check this
     :grouping-vars (set/union (set (:grouping-vars find-attrs))
                               (set (:grouping-vars order-by-attrs)))}))

(defn- wrap-group-by [plan {{:keys [aggs grouping-vars]} :head-attrs
                            :keys [body-var->col]}]
  (if aggs
    [:group-by (into (mapv body-var->col grouping-vars)
                     (map (fn [[col agg]]
                            {col (s/unform ::aggregate
                                           (-> agg
                                               (update :param body-var->col)))}))
                     aggs)
     plan]
    plan))

(defn- wrap-order-by [plan order-by-clauses {:keys [body-var->col]}]
  (if order-by-clauses
    [:order-by (mapv (fn [{:keys [clause-type direction] :as clause}]
                       [(case clause-type
                          :logic-var (body-var->col (:lv clause))
                          :aggregate (:col clause))
                        {:direction direction}])
                     order-by-clauses)
     plan]
    plan))

(defn- wrap-head [plan {{:keys [find-clauses order-by-clauses] :as head-attrs} :head-attrs
                        {body-var->col :var->col} :body-attrs}]
  [:project (->> find-clauses
                 (mapv (fn [{:keys [clause-type] :as clause}]
                         (case clause-type
                           :logic-var (let [in-col (body-var->col (:lv clause))]
                                        (if-let [out-col (:col clause)]
                                          {out-col in-col}
                                          in-col))
                           :aggregate (:col clause)))))
   (-> plan
       (wrap-group-by {:head-attrs head-attrs
                       :body-var->col body-var->col})
       (wrap-order-by order-by-clauses {:body-var->col body-var->col}))])

(defn- with-top [plan {:keys [limit offset]}]
  (if (or limit offset)
    [:top (cond-> {}
            offset (assoc :skip offset)
            limit (assoc :limit limit))
     plan]

    plan))

(defn- plan-query [conformed-query]
  (let [head-attrs (analyse-head conformed-query)
        {:keys [in-bindings] :as in-attrs} (analyse-in conformed-query)
        where-attrs (analyse-where conformed-query)
        body-attrs (->body-attrs {:where-attrs where-attrs, :in-attrs in-attrs})]

    {:plan (-> (plan-relations {:where-attrs where-attrs, :in-attrs in-attrs, :body-attrs body-attrs})
               (wrap-head {:head-attrs head-attrs, :body-attrs body-attrs})
               (with-top conformed-query))

     :in-bindings in-bindings}))

(defn compile-query [query]
  (plan-query (conform-query query)))

(comment
  (compile-query '{:find [?parent ?child ?a1 ?a2]
                   :in [[[?a1 ?a2]]]
                   :where [[?parent :name "Ivan"]
                           [?child :parent ?parent]
                           [?parent :id ?id]
                           [?parent :age ?a1]
                           [?child :age ?a2]]})

  (compile-query '{:find [?e ?name]
                   :where [[?e :first-name ?name]
                           [?e :last-name ?name]]}))

(defn- args->params [args in-bindings]
  (->> (mapcat (fn [{:keys [binding-type in-col in-cols var->col]} arg]
                 (case binding-type
                   (:source :scalar) [(MapEntry/create (var->col in-col) arg)]
                   :tuple (zipmap (map var->col in-cols) arg)
                   (:collection :relation) nil))
               in-bindings
               args)
       (into {})))

(defn- args->tables [args in-bindings]
  (->> (mapcat (fn [{:keys [binding-type in-col in-cols table-key var->col]} arg]
                 (letfn [(col->kw [col] (-> col var->col keyword))]
                   (case binding-type
                     (:source :scalar :tuple) nil

                     :collection (let [binding-k (col->kw in-col)]
                                   (if-not (coll? arg)
                                     (throw (err/illegal-arg :bad-collection
                                                             {:binding in-col
                                                              :coll arg}))
                                     [(MapEntry/create table-key
                                                       (vec (for [v arg]
                                                              {binding-k v})))]))

                     :relation (let [conformed-arg (s/conform ::relation-arg arg)]
                                 (if (s/invalid? conformed-arg)
                                   (throw (err/illegal-arg :bad-relation
                                                           {:binding in-cols
                                                            :relation arg
                                                            :explain-data (s/explain-data ::relation-arg arg)}))
                                   (let [[rel-type rel] conformed-arg
                                         ks (mapv col->kw in-cols)]
                                     [(MapEntry/create table-key
                                                       (case rel-type
                                                         :maps (mapv #(update-keys % (fn [k]
                                                                                       (col->kw (symbol (str "?" (symbol k))))))
                                                                     rel)
                                                         :vecs (mapv #(zipmap ks %) rel)))]))))))
               in-bindings
               args)
       (into {})))

(defn open-datalog-query ^core2.IResultSet [^BufferAllocator allocator query db args]
  (let [{:keys [plan in-bindings]} (compile-query (dissoc query :basis :basis-timeout :default-tz))

        pq (op/prepare-ra plan)]

    (when (not= (count in-bindings) (count args))
      (throw (err/illegal-arg :in-arity-exception {::err/message ":in arity mismatch"
                                                   :expected (count in-bindings)
                                                   :actual args})))

    (let [^AutoCloseable
          params (vw/open-params allocator (args->params args in-bindings))]
      (try
        (-> (.bind pq {:srcs {'$ db}, :params params, :table-args (args->tables args in-bindings)
                       :current-time (get-in query [:basis :current-time])
                       :default-tz (:default-tz query)})
            (.openCursor)
            (op/cursor->result-set params))
        (catch Throwable t
          (.close params)
          (throw t))))))
