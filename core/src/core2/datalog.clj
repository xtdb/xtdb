(ns core2.datalog
  (:require [clojure.set :as set]
            [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [core2.error :as err]
            [core2.logical-plan :as lp]
            [core2.operator :as op]
            [core2.vector.writer :as vw])
  (:import clojure.lang.MapEntry
           java.lang.AutoCloseable
           org.apache.arrow.memory.BufferAllocator))

(s/def ::logic-var
  (s/and simple-symbol?
         (comp #(not (str/starts-with? % "$")) name)))

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

(s/def ::semi-join
  (s/cat :exists '#{exists?}
         :args ::args-list
         :terms (s/+ ::term)))

(s/def ::anti-join
  (s/cat :not-exists '#{not-exists?}
         :args ::args-list
         :terms (s/+ ::term)))

(s/def ::find (s/coll-of ::find-arg :kind vector? :min-count 1))
(s/def ::keys (s/coll-of symbol? :kind vector?))

(s/def ::args-list (s/coll-of ::logic-var, :kind vector?, :min-count 1))

(s/def ::binding
  (s/or :collection (s/tuple ::logic-var '#{...})
        :relation (s/tuple ::args-list)
        :scalar ::logic-var
        :tuple ::args-list))

(s/def ::source
  (s/and simple-symbol?
         (comp #(str/starts-with? % "$") name)))

(s/def ::in-binding
  (s/or :source ::source
        :collection (s/tuple ::logic-var '#{...})
        :relation (s/tuple ::args-list)
        :scalar ::logic-var
        :tuple ::args-list))

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

(s/def ::fn-call
  (s/and list?
         (s/cat :f simple-symbol?
                :args (s/* ::form))))

(s/def ::form
  (s/or :logic-var ::logic-var
        :fn-call ::fn-call
        :value ::value))

(s/def ::call-clause
  (s/and vector?
         ;; top-level can only be a fn-call because otherwise the EDN Datalog syntax is ambiguous
         ;; (it wasn't ever meant to handle this...)
         (s/cat :form (s/spec (s/or :fn-call ::fn-call))
                :return (s/? ::binding))))

(s/def ::and (s/cat :and #{'and}
                    :terms (s/+ ::term)))

(s/def ::or-branches
  (s/+ (s/and (s/or :term ::term, :and ::and)
              (s/conformer (fn [[type arg]]
                             (case type
                               :term [arg]
                               :and (:terms arg)))
                           (fn [terms]
                             (if (= (count terms) 1)
                               [:term (first terms)]
                               [:and {:and 'and, :terms terms}]))))))

(s/def ::union-join
  (s/cat :union-join #{'union-join}
         :args ::args-list
         :branches ::or-branches))

(s/def ::term
  (s/or :triple ::triple
        :semi-join ::semi-join
        :anti-join ::anti-join
        :union-join ::union-join
        :call ::call-clause))

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

(defn- col-sym [col]
  (vary-meta (symbol col) assoc :column? true))

(defn- form-vars [form]
  (letfn [(form-vars* [[form-type form-arg]]
            (case form-type
              :fn-call (into #{} (mapcat form-vars*) (:args form-arg))
              :logic-var #{form-arg}
              :value #{}))]
    (form-vars* form)))

(defn- combine-term-vars [term-varses]
  (let [{:keys [provided-vars] :as vars} (->> term-varses
                                              (apply merge-with set/union))]
    (-> vars
        (update :required-vars set/difference provided-vars))))

(defn- term-vars [[term-type term-arg]]
  (letfn [(sj-term-vars [spec]
            (let [{:keys [args terms]} term-arg
                  arg-vars (set args)
                  {:keys [required-vars]} (combine-term-vars (map term-vars terms))]

              (when-let [unsatisfied-vars (not-empty (set/difference required-vars arg-vars))]
                (throw (err/illegal-arg :unsatisfied-vars
                                        {:vars unsatisfied-vars
                                         :term (s/unform spec term-arg)})))

              ;; semi-joins do not provide vars
              {:required-vars required-vars}))]

    (case term-type
      :call (let [{:keys [form return]} term-arg]
              {:required-vars (form-vars form)
               :provided-vars (when-let [[return-type return-arg] return]
                                (case return-type
                                  :scalar return-arg))})

      :triple {:provided-vars (into #{}
                                    (comp (map term-arg)
                                          (keep (fn [[val-type val-arg]]
                                                  (when (= :logic-var val-type)
                                                    val-arg))))
                                    [:e :v])}

      :union-join (let [{:keys [args branches]} term-arg
                        arg-vars (set args)
                        branches-vars (for [branch branches]
                                        (into {:branch branch}
                                              (combine-term-vars (map term-vars branch))))
                        provided-vars (->> branches-vars
                                           (map (comp set :provided-vars))
                                           (apply set/intersection))
                        required-vars (->> branches-vars
                                           (map (comp set :required-vars))
                                           (apply set/union))]

                    (when-let [unsatisfied-vars (not-empty (set/difference required-vars arg-vars))]
                      (throw (err/illegal-arg :unsatisfied-vars
                                              {:vars unsatisfied-vars
                                               :term (s/unform ::union-join term-arg)})))

                    {:provided-vars (set/intersection arg-vars provided-vars)
                     :required-vars (set/difference arg-vars provided-vars)})

      :semi-join (sj-term-vars ::semi-join)
      :anti-join (sj-term-vars ::anti-join))))

(defn- analyse-in [{in-bindings :in}]
  (let [in-bindings (->> in-bindings
                         (into [] (map-indexed
                                   (fn [idx [binding-type binding-arg]]
                                     (let [prefix (str "in" idx)
                                           table-key (symbol (str "?" prefix))]
                                       (letfn [(with-param-prefix [lv]
                                                 (symbol (str "?" prefix "_" (str lv))))
                                               (with-table-col-prefix [lv]
                                                 (col-sym (str prefix "_" (str lv))))]
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

(defn- plan-in-tables [in-bindings]
  (->> in-bindings
       (into [] (keep (fn [{:keys [table-key var->col]}]
                        (when table-key
                          [:rename var->col
                           [:table table-key]]))))))

(defn- analyse-triples [triples]
  (letfn [(->triple-rel [^long idx, [[src e] triples]]
            (let [triples (->> (conj triples {:e e, :a :id, :v e})
                               (map #(update % :a col-sym)))
                  prefix (str "t" idx)
                  var->attrs (-> triples
                                 (->> (keep (fn [{:keys [a], [v-type v-arg] :v}]
                                              (when (= :logic-var v-type)
                                                {:a a, :lv v-arg})))
                                      (group-by :lv))
                                 (update-vals #(into #{} (map :a) %)))
                  attr->lits (-> triples
                                 (->> (keep (fn [{:keys [a], [v-type v-arg] :v}]
                                              (when (= :literal v-type)
                                                {:a a, :lit v-arg})))
                                      (group-by :a))
                                 (update-vals #(into #{} (map :lit) %)))]

              {:src src, :e e,
               :attrs (-> (into #{} (map :a) triples)
                          (disj '_table))
               :table (or (some-> (first (attr->lits '_table))
                                  symbol)
                          'xt_docs)
               :attr->lits (dissoc attr->lits '_table)
               :var->attrs var->attrs

               :var->col (->> (keys var->attrs)
                              (into {} (map (juxt identity
                                                  #(col-sym (str prefix "_" (str %)))))))}))]

    (let [triple-rels (->> (group-by (juxt :src :e) triples)
                           (into [] (map-indexed ->triple-rel)))]
      {:triple-rels triple-rels
       :var->cols (-> triple-rels
                      (->> (mapcat :var->col)
                           (group-by key))
                      (update-vals #(into #{} (map val) %)))})))

(defn- analyse-calls [calls]
  (->> calls
       (into [] (map-indexed (fn [idx {:keys [form return]}]
                               (into {:form form, :required-vars (form-vars form)}

                                     (when-let [[return-type return-arg] return]
                                       (let [prefix (str "p" idx "_")]
                                         (-> (case return-type
                                               :scalar (let [return-col (col-sym (str prefix (str return-arg)))]
                                                         {:return-col return-col
                                                          :var->col {return-arg return-col}}))
                                             (assoc :return-type return-type))))))))))

(defn- wrap-calls [plan {:keys [calls var->col]}]
  (letfn [(map-lvs [[form-type form-arg]]
            [form-type
             (case form-type
               :logic-var (get var->col form-arg)
               :fn-call (-> form-arg (update :args #(mapv map-lvs %)))
               :value form-arg)])

          (form->sexp [{:keys [form]}]
            (s/unform ::form (map-lvs form)))

          (wrap-scalars [plan scalars]
            (if scalars
              [:map (vec
                     (for [{:keys [return-col] :as call} scalars]
                       {return-col (form->sexp call)}))
               plan]
              plan))]

    (let [{selects nil, scalars :scalar} (group-by :return-type calls)]
      (-> plan
          (wrap-scalars scalars)
          (wrap-select (map form->sexp selects))))))

(defn- analyse-semi-joins [sj-type sj-clauses]
  (->> sj-clauses
       (into [] (map-indexed (fn [sj-idx {:keys [args terms] :as sj}]
                               (let [prefix (format "%s%d_"
                                                    (case sj-type :semi-join "sj", :anti-join "aj")
                                                    sj-idx)
                                     {sj-required-vars :required-vars} (term-vars [sj-type sj])

                                     var->col (->> args
                                                   (into {}
                                                         (map (juxt identity #(col-sym (str prefix %))))))

                                     required-vars (if (seq sj-required-vars) (set args) #{})]

                                 {:var->col var->col
                                  :required-vars required-vars
                                  :query (cond-> {:find (vec (for [arg args]
                                                               [:logic-var arg]))
                                                  :keys (vec (for [arg args]
                                                               (keyword (var->col arg))))
                                                  :where terms}
                                           (seq required-vars) (assoc :in [[:tuple args]]))}))))
       (not-empty)))

(defn- wrap-semi-joins [plan sj-type {outer-var->col :var->col, :as attrs}]
  (->> (get attrs (case sj-type :semi-join :semi-joins, :anti-join :anti-joins))
       (mapv (fn [{:keys [required-vars query var->col]}]
               (let [plan (plan-query query)
                     apply-mapping (when required-vars
                                     (let [{:keys [in-bindings]} plan
                                           {:keys [var->col]} (first in-bindings)]
                                       (->> var->col
                                            (into {} (map (fn [[lv in-var]]
                                                            (MapEntry/create (get outer-var->col lv) in-var)))))))]
                 (-> plan
                     (assoc :join-condition (->> var->col
                                                 (mapv (fn [[lv col]]
                                                         {(get outer-var->col lv) col})))

                            :apply-mapping apply-mapping)))))


       (reduce (fn [outer-plan {:keys [plan apply-mapping join-condition]}]
                 (if (seq apply-mapping)
                   [:apply sj-type apply-mapping
                    outer-plan
                    plan]

                   [sj-type join-condition
                    outer-plan
                    plan]))
               plan)))

(defn- analyse-union-joins [union-join-clauses]
  (->> union-join-clauses
       (into [] (map-indexed
                 (fn [uj-idx {:keys [args branches] :as uj}]
                   (let [{uj-required-vars :required-vars} (term-vars [:union-join uj])

                         in-vars (vec uj-required-vars)

                         var->col (->> args
                                       (into {}
                                             (map (juxt identity #(col-sym (str "uj" uj-idx "_" (str %)))))))]

                     {:var->col var->col
                      :required-vars uj-required-vars
                      :branch-queries (->> branches
                                           (mapv (fn [branch]
                                                   {:find (vec (for [arg args]
                                                                 [:logic-var arg]))
                                                    :in [[:tuple in-vars]]
                                                    :keys (vec (for [arg args]
                                                                 (keyword (var->col arg))))
                                                    :where branch})))}))))
       (not-empty)))

(defn- wrap-union-joins [plan {outer-var->col :var->col, :keys [union-joins]}]
  (if union-joins
    (let [union-joins (->> union-joins
                           (mapv
                            (fn [{:keys [required-vars branch-queries]}]
                              (let [branch-plans (->> branch-queries
                                                      (mapv (fn [query]
                                                              (plan-query query))))]

                                {:plan (->> branch-plans
                                            (map :plan)
                                            (reduce (fn [acc plan]
                                                      [:union-all acc plan])))

                                 :apply-mapping (when required-vars
                                                  (let [{:keys [in-bindings]} (first branch-plans)
                                                        {:keys [var->col]} (first in-bindings)]
                                                    (->> var->col
                                                         (into {} (map (fn [[lv in-var]]
                                                                         (MapEntry/create
                                                                          (get outer-var->col lv)
                                                                          in-var)))))))}))))]

      (if-let [apply-mapping (not-empty (into {} (mapcat :apply-mapping) union-joins))]
        [:apply :cross-join apply-mapping
         plan
         [:mega-join []
          (mapv :plan union-joins)]]

        [:mega-join []
         (into [plan]
               (map :plan)
               union-joins)]))

    plan))

(defn- analyse-body [{where-clauses :where, :as query}]
  (let [{:keys [in-bindings] :as in-attrs} (analyse-in query)
        {triple-clauses :triple
         call-clauses :call
         semi-join-clauses :semi-join
         anti-join-clauses :anti-join
         union-join-clauses :union-join} (-> where-clauses
                                             (->> (group-by first))
                                             (update-vals #(mapv second %)))

        {:keys [triple-rels] :as triples} (analyse-triples triple-clauses)
        calls (analyse-calls call-clauses)

        l0-var->cols (reduce (fn [acc [lv col]]
                               (-> acc
                                   (update lv (fnil into #{}) col)))
                             (:var->cols triples)
                             (:var->cols in-attrs))

        l0-var->col (->> (keys l0-var->cols)
                         (into {} (map (juxt identity #(col-sym (str %))))))]


    (loop [calls calls
           union-joins (analyse-union-joins union-join-clauses)
           semi-joins (analyse-semi-joins :semi-join semi-join-clauses)
           anti-joins (analyse-semi-joins :anti-join anti-join-clauses)
           var->col l0-var->col
           levels [{:triple-rels triple-rels
                    :var->cols l0-var->cols
                    :var->col l0-var->col}]]

      (if (and (empty? calls) (empty? semi-joins) (empty? anti-joins) (empty? union-joins))
        {:in-bindings in-bindings
         :levels levels
         :var->col var->col}

        (let [{available-calls true
               unavailable-calls false} (->> calls
                                             (group-by (fn [{:keys [required-vars]}]
                                                         (set/superset? (set (keys var->col)) required-vars))))

              {available-ujs true
               unavailable-ujs false} (->> union-joins
                                           (group-by (fn [{:keys [required-vars]}]
                                                       (set/superset? (set (keys var->col)) required-vars))))

              {available-sjs true
               unavailable-sjs false} (->> semi-joins
                                           (group-by (fn [{:keys [required-vars]}]
                                                       (set/superset? (set (keys var->col)) required-vars))))
              {available-ajs true
               unavailable-ajs false} (->> anti-joins
                                           (group-by (fn [{:keys [required-vars]}]
                                                       (set/superset? (set (keys var->col)) required-vars))))]

          (if (and (empty? available-calls) (empty? available-ujs) (empty? available-sjs) (empty? available-ajs))
            (throw (err/illegal-arg :no-available-clauses
                                    {:known-vars (set (keys var->col))
                                     :unavailable-calls unavailable-calls
                                     :unavailable-union-joins unavailable-ujs
                                     :unavailable-semi-joins unavailable-sjs
                                     :unavailable-anti-joins unavailable-ajs}))

            (let [new-vars (into #{} (mapcat (comp keys :var->col)) (concat available-calls available-ujs))

                  new-var->col (into var->col
                                     (map (juxt identity #(col-sym (str %))))
                                     new-vars)

                  new-var->cols (-> (concat var->col (mapcat :var->col (concat available-calls available-ujs)))
                                    (->> (group-by key))
                                    (update-vals #(into #{} (map val) %)))]

              (recur unavailable-calls unavailable-ujs unavailable-sjs unavailable-ajs
                     new-var->col
                     (conj levels {:calls available-calls
                                   :semi-joins available-sjs
                                   :anti-joins available-ajs
                                   :union-joins available-ujs
                                   :var->col new-var->col
                                   :var->cols new-var->cols})))))))))

(defn- plan-triples [triple-rels]
  (for [{:keys [src table attrs attr->lits var->col var->attrs]} triple-rels]
    [:project (vec (for [[lv col] var->col]
                     {col (first (get var->attrs lv))}))
     (-> [:scan src table
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

(defn- wrap-unify-vars [plan {:keys [var->cols var->col]}]
  [:project (vec (for [[lv cols] var->cols
                       :let [out-col (get var->col lv)
                             in-col (first cols)]]
                   (if (= out-col in-col)
                     out-col
                     {out-col in-col})))
   (-> plan
       (wrap-select (vec
                     (for [cols (vals var->cols)
                           :when (> (count cols) 1)
                           ;; this picks an arbitrary binary order if there are >2
                           ;; once mega-join has multi-way joins we could throw the multi-way `=` over the fence
                           [c1 c2] (partition 2 1 cols)]
                       (list '= c1 c2)))))])

(defn- plan-body [{:keys [in-bindings levels]}]
  (reduce (fn [plan {:keys [triple-rels], :as level}]
            (-> (if plan
                  (-> plan
                      (wrap-calls level)
                      (wrap-semi-joins :semi-join level)
                      (wrap-semi-joins :anti-join level)
                      (wrap-union-joins level))

                  (if-let [rels (not-empty (vec (concat (plan-in-tables in-bindings)
                                                        (plan-triples triple-rels))))]
                    [:mega-join [] rels]
                    [:table [{}]]))

                (wrap-unify-vars level)))
          nil

          levels))

(defn- analyse-find-clauses [{find-clauses :find, rename-keys :keys}]
  (when-let [clauses (mapv (fn [[clause-type clause] rename-key]
                             (let [rename-sym (some-> rename-key col-sym)]
                               (-> (case clause-type
                                     :logic-var {:lv clause
                                                 :col rename-sym
                                                 :vars #{clause}}
                                     :aggregate (let [{:keys [agg-fn param]} clause]
                                                  {:aggregate clause
                                                   :col (or rename-sym (col-sym (str agg-fn "-" param)))
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
        {:keys [in-bindings] :as body-attrs} (analyse-body conformed-query)]

    {:plan (-> (plan-body body-attrs)
               (wrap-head {:head-attrs head-attrs, :body-attrs body-attrs})
               (with-top conformed-query))

     :in-bindings in-bindings}))

(defn compile-query [query]
  (plan-query (conform-query query)))

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
                                                                                       (col->kw (symbol k))))
                                                                     rel)
                                                         :vecs (mapv #(zipmap ks %) rel)))]))))))
               in-bindings
               args)
       (into {})))

(defn open-datalog-query ^core2.IResultSet [^BufferAllocator allocator query db args]
  (let [{:keys [plan in-bindings]} (compile-query (dissoc query :basis :basis-timeout :default-tz))

        plan (-> plan
                 #_(doto clojure.pprint/pprint)
                 (lp/rewrite-plan {})
                 #_(doto clojure.pprint/pprint)
                 #_(doto (lp/validate-plan)))

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
          (throw t))))) )
