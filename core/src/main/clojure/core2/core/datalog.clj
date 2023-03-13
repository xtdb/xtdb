(ns core2.core.datalog
  (:require [clojure.set :as set]
            [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [core2.error :as err]
            [core2.logical-plan :as lp]
            [core2.operator :as op]
            [core2.util :as util]
            [core2.vector.writer :as vw])
  (:import clojure.lang.MapEntry
           core2.operator.IRaQuerySource
           (java.time LocalDate Instant ZonedDateTime)
           java.lang.AutoCloseable
           java.util.Date
           org.apache.arrow.memory.BufferAllocator))

(s/def ::logic-var simple-symbol?)

;; TODO flesh out
(def ^:private eid? (some-fn string? number? inst? keyword? (partial instance? LocalDate)))

(s/def ::eid eid?)
(s/def ::value (some-fn eid?))

(s/def ::fn-call
  (s/and list?
         (s/cat :f simple-symbol?
                :args (s/* ::form))))

(s/def ::form
  (s/or :logic-var ::logic-var
        :fn-call ::fn-call
        :value ::value))

(s/def ::find-arg
  (s/or :logic-var ::logic-var
        :form ::form))

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

(s/def ::e (s/or :literal ::eid, :logic-var ::logic-var))

(s/def ::triple
  (s/and vector?
         (s/conformer identity vec)
         (s/cat :e ::e
                :a keyword?
                :v (s/? (s/or :literal ::value,
                              :logic-var ::logic-var
                              :unwind (s/tuple ::logic-var #{'...}))))))

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

(s/def ::sub-query
  (s/cat :q #{'q}, :query ::query))

(s/def ::temporal-literal ::util/datetime-value)

(s/def ::for-all-app-time (s/cat :k #{'for-all-app-time}, :e ::e))

(s/def ::for-app-time-at
  (s/cat :k #{'for-app-time-at}, :e ::e,
         :time-at ::temporal-literal))

(s/def ::for-app-time-in
  (s/cat :k #{'for-app-time-in}, :e ::e
         :time-from (s/nilable ::temporal-literal)
         :time-to (s/nilable ::temporal-literal)))

(s/def ::for-all-sys-time (s/cat :k #{'for-all-sys-time}, :e ::e))

(s/def ::for-sys-time-at
  (s/cat :k #{'for-sys-time-at}, :e ::e,
         :time-at ::temporal-literal))

(s/def ::for-sys-time-in
  (s/cat :k #{'for-sys-time-in}, :e ::e
         :time-from (s/nilable ::temporal-literal)
         :time-to (s/nilable ::temporal-literal)))

(s/def ::term
  (s/or :triple ::triple
        :semi-join ::semi-join
        :anti-join ::anti-join
        :union-join ::union-join

        :for-all-app-time ::for-all-app-time
        :for-app-time-in ::for-app-time-in
        :for-app-time-at ::for-app-time-at

        :for-all-sys-time ::for-all-sys-time
        :for-sys-time-in ::for-sys-time-in
        :for-sys-time-at ::for-sys-time-at

        :call ::call-clause
        :sub-query ::sub-query))

(s/def ::where
  (s/coll-of ::term :kind vector? :min-count 1))

(s/def ::offset nat-int?)
(s/def ::limit nat-int?)

(s/def ::order-element
  (s/and vector?
         (s/cat :find-arg ::find-arg
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

(defn- col-sym
  ([col]
   (-> (symbol col) (vary-meta assoc :column? true)))
  ([prefix col]
   (col-sym (str (format "%s_%s" prefix col)))))

(defn- wrap-select [plan predicates]
  (-> (case (count predicates)
        0 plan
        1 [:select (first predicates) plan]
        [:select (list* 'and predicates) plan])
      (with-meta (meta plan))))

(defn- unify-preds [var->cols]
  ;; this enumerates all the binary join conditions
  ;; once mega-join has multi-way joins we could throw the multi-way `=` over the fence
  (->> (vals var->cols)
       (filter #(> (count %) 1))
       (mapcat
         (fn [cols]
           (->> (set (for [col cols
                           col2 cols
                           :when (not= col col2)]
                       (set [col col2])))
                (map #(list* '= %)))))
       (vec)))

(defn- wrap-unify [plan var->cols]
  (-> [:project (vec (for [[lv cols] var->cols]
                       (or (cols lv)
                           {(col-sym lv) (first cols)})))
       (-> plan
           (wrap-select (unify-preds var->cols)))]
      (with-meta (-> (meta plan) (assoc ::vars (set (keys var->cols)))))))

(defn- with-unique-cols [plans param-vars]
  (as-> plans plans
    (->> plans
         (into [] (map-indexed
                   (fn [idx plan]
                     (let [{::keys [vars]} (meta plan)
                           var->col (->> vars
                                         (into {} (map (juxt col-sym (partial col-sym (str "_r" idx))))))]
                       (-> [:rename var->col
                            plan]
                           (with-meta (into (meta plan)
                                            {::vars (set (vals var->col))
                                             ::var->col var->col}))))))))
    (-> plans
        (with-meta {::var->cols (-> (concat (->> plans (mapcat (comp ::var->col meta)))
                                            param-vars)
                                    (->> (group-by key))
                                    (update-vals #(into #{} (map val) %)))}))))

(defn- mega-join [rels param-vars]
  (let [rels (with-unique-cols rels param-vars)]
    (-> (case (count rels)
          0 (-> [:table [{}]] (with-meta {::vars #{}}))
          1 (first rels)
          [:mega-join [] rels])
        (wrap-unify (::var->cols (meta rels))))))

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
                                  :scalar #{return-arg}))})

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
      :anti-join (sj-term-vars ::anti-join)
      :sub-query (let [{:keys [query]} term-arg]
                   {:provided-vars (->> (map form-vars (:find query))
                                        (apply set/union))}))))

(defn- ->param-sym [lv]
  (-> (symbol (str "?" (name lv)))
      (with-meta {::param? true})))

(defn- plan-in-tables [{in-bindings :in}]
  (let [in-bindings (->> in-bindings
                         (into [] (map-indexed
                                   (fn [idx [binding-type binding-arg]]
                                     (let [table-key (symbol (str "?in" idx))]
                                       (-> (case binding-type
                                             :source {::in-cols [binding-arg]}
                                             :scalar {::vars #{binding-arg}, ::in-cols [(->param-sym binding-arg)]}
                                             :tuple {::vars (set binding-arg), ::in-cols (mapv ->param-sym binding-arg)}
                                             :relation (let [cols (first binding-arg)]
                                                         {::table-key table-key, ::in-cols cols, ::vars (set cols)})
                                             :collection (let [col (first binding-arg)]
                                                           {::table-key table-key, ::vars #{col}, ::in-cols [col]}))
                                           (assoc ::binding-type binding-type)))))))]
    (-> in-bindings
        (->> (into [] (keep (fn [{::keys [table-key in-cols vars]}]
                              (when table-key
                                (-> [:table in-cols table-key]
                                    (with-meta {::vars vars})))))))
        (with-meta {::in-bindings in-bindings
                    ::param-vars (into {}
                                       (comp (remove ::table-key)
                                             (mapcat ::vars)
                                             (map (juxt identity ->param-sym)))
                                       in-bindings)}))))

(defn- ->temporal-preds [temporal-clauses]
  (letfn [(->start-k [time-k]
            (case time-k :app-time 'application_time_start, :sys-time 'system_time_start))

          (->end-k [time-k]
            (case time-k :app-time 'application_time_end, :sys-time 'system_time_end))

          (with-time-in-preds [preds time-k {:keys [time-from time-to]}]
            (let [start-k (->start-k time-k), end-k (->end-k time-k)]
              (-> preds
                  (update time-k
                          (fn [acc]
                            (-> acc
                                ;; app-time overlaps [time-from time-to]
                                (cond-> time-from (update start-k (fnil conj []) (list '< start-k time-to)))
                                (cond-> time-to (update end-k (fnil conj []) (list '> end-k time-from)))))))))

          (with-time-at-preds [preds time-k {:keys [time-at]}]
            (let [start-k (->start-k time-k), end-k (->end-k time-k)]
              (-> preds
                  (update-in [time-k start-k] (fnil conj []) (list '<= start-k time-at))
                  (update-in [time-k end-k] (fnil conj []) (list '> end-k time-at)))))]

    (->> temporal-clauses
         (reduce (fn [acc {:keys [e] :as clause}]
                   (-> acc
                       (update e
                               (fn [e-preds {:keys [k] :as clause}]
                                 (case k
                                   for-all-app-time (-> e-preds (update :app-time identity))
                                   for-app-time-in (-> e-preds (with-time-in-preds :app-time clause))
                                   for-app-time-at (-> e-preds (with-time-at-preds :app-time clause))

                                   for-all-sys-time (-> e-preds
                                                        (update-in [:sys-time 'system_time_end] (fnil conj [])
                                                                   '(<= system_time_end core2/end-of-time)))
                                   for-sys-time-in (-> e-preds (with-time-in-preds :sys-time clause))
                                   for-sys-time-at (-> e-preds (with-time-at-preds :sys-time clause))))
                               clause)))
                 {}))))

(defn- wrap-scan-col-preds [scan-col col-preds]
  (case (count col-preds)
    0 scan-col
    1 {scan-col (first col-preds)}
    {scan-col (list* 'and col-preds)}))

(defn- plan-scan [triples temporal-preds]
  (let [attrs (into #{} (map :a) triples)

        attr->lits (-> triples
                       (->> (keep (fn [{:keys [a], [v-type v-arg] :v}]
                                    (when (= :literal v-type)
                                      {:a a, :lit v-arg})))
                            (group-by :a))
                       (update-vals #(into #{} (map :lit) %)))

        app-time-preds (get temporal-preds :app-time
                            '{application_time_start [(<= application_time_start (current-timestamp))]
                              application_time_end [(> application_time_end (current-timestamp))]})

        ;; defaults handled by apply-src-tx
        sys-time-preds (get temporal-preds :sys-time)]

    (-> [:scan (or (some-> (first (attr->lits '_table)) symbol)
                   'xt_docs)
         (-> attrs
             (cond-> app-time-preds (conj 'application_time_start 'application_time_end)
                     sys-time-preds (conj 'system_time_start 'system_time_end))
             (disj '_table)
             (->> (mapv (fn [attr]
                          (-> attr
                              (wrap-scan-col-preds
                               (concat (for [lit (get attr->lits attr)]
                                         (list '= attr lit))
                                       (case attr
                                         application_time_start (get app-time-preds 'application_time_start)
                                         application_time_end (get app-time-preds 'application_time_end)
                                         system_time_start (get sys-time-preds 'system_time_start)
                                         system_time_end (get sys-time-preds 'system_time_end)
                                         nil))))))))]
        (with-meta {::vars attrs}))))

(defn- attr->unwind-col [a]
  (col-sym "__uw" a))

(defn- wrap-unwind [plan triples]
  (->> triples
       (transduce
        (comp (keep (fn [{:keys [a], [v-type _v-arg] :v}]
                      (when (= v-type :unwind)
                        a)))
              (distinct))

        (completing (fn [plan a]
                      (let [uw-col (attr->unwind-col a)]
                        (-> [:unwind {uw-col a}
                             plan]
                            (vary-meta update ::vars conj uw-col)))))
        plan)))

(defn- plan-triples [triples temporal-preds]
  (let [triples (group-by :e triples)]
    (vec
     (for [e (set/union (set (keys triples))
                        (set (keys temporal-preds)))
           :let [triples (->> (conj (get triples e) {:e e, :a :id, :v e})
                              (map #(update % :a col-sym)))
                 temporal-preds (get temporal-preds e)]]
       (let [var->cols (-> triples
                           (->> (keep (fn [{:keys [a], [v-type v-arg] :v}]
                                        (case v-type
                                          :logic-var {:lv v-arg, :col a}
                                          :unwind {:lv (first v-arg), :col (attr->unwind-col a)}
                                          nil)))
                                (group-by :lv))
                           (update-vals #(into #{} (map :col) %)))]

         (-> (plan-scan triples temporal-preds)
             (wrap-unwind triples)
             (wrap-unify var->cols)))))))

(defn- plan-call [{:keys [form return]}]
  (letfn [(with-col-metadata [[form-type form-arg]]
            [form-type
             (case form-type
               :logic-var (if (str/starts-with? (name form-arg) "?")
                            form-arg
                            (col-sym form-arg))
               :fn-call (-> form-arg (update :args #(mapv with-col-metadata %)))
               :value form-arg)])]
    (-> (s/unform ::form (with-col-metadata form))
        (with-meta (into {::required-vars (form-vars form)}

                         (when-let [[return-type return-arg] return]
                           (-> (case return-type
                                 :scalar {::return-var return-arg
                                          ::vars #{return-arg}})
                               (assoc ::return-type return-type))))))))

(defn- wrap-calls [plan calls]
  (letfn [(wrap-scalars [plan scalars]
            (let [scalars (->> scalars
                               (into [] (map-indexed
                                         (fn [idx form]
                                           (let [{::keys [return-var]} (meta form)]
                                             (-> form
                                                 (vary-meta assoc ::return-col (col-sym (str "_c" idx) return-var))))))))
                  var->cols (-> (concat (->> (::vars (meta plan)) (map (juxt identity identity)))
                                        (->> scalars (map (comp (juxt ::return-var ::return-col) meta))))
                                (->> (group-by first))
                                (update-vals #(into #{} (map second) %)))]
              (-> [:map (vec (for [form scalars]
                               {(::return-col (meta form)) form}))
                   plan]
                  (with-meta (-> (meta plan) (update ::vars into (map ::return-col scalars))))
                  (wrap-unify var->cols))))]

    (let [{selects nil, scalars :scalar} (group-by (comp ::return-type meta) calls)]
      (-> plan
          (cond-> scalars (wrap-scalars scalars))
          (wrap-select selects)))))

(defn- ->apply-mapping [apply-params]
  ;;TODO symbol names will clash with nested applies
  ;; (where an apply is nested inside the dep side of another apply)
  (when (seq apply-params)
    (->> (for [param apply-params]
           (let [param-symbol (-> (symbol (str "?ap_" param))
                                  (with-meta {:correlated-column? true}))]
             (MapEntry/create param param-symbol)))
         (into {}))))

(defn- plan-semi-join [sj-type {:keys [args terms] :as sj}]
  (let [{sj-required-vars :required-vars} (term-vars [sj-type sj])
        required-vars (if (seq sj-required-vars) (set args) #{})
        apply-mapping (->apply-mapping required-vars)]

    (-> (plan-query
         (cond-> {:find (vec (for [arg args]
                               [:logic-var arg]))
                  :where terms}
           (seq required-vars) (assoc ::apply-mapping apply-mapping)))
        (vary-meta into {::required-vars required-vars
                         ::apply-mapping apply-mapping}))))

(defn- wrap-semi-joins [plan sj-type semi-joins]
  (->> semi-joins
       (reduce (fn [acc sq-plan]
                 (let [{::keys [apply-mapping]} (meta sq-plan)]
                   (-> (if apply-mapping
                         [:apply sj-type apply-mapping
                          acc sq-plan]

                         [sj-type (->> (::vars (meta sq-plan))
                                       (mapv (fn [v] {v v})))
                          acc sq-plan])
                       (with-meta (meta acc)))))
               plan)))

(defn- plan-union-join [{:keys [args branches] :as uj}]
  (let [{:keys [required-vars]} (term-vars [:union-join uj])
        apply-mapping (->apply-mapping required-vars)]
    (-> branches
        (->> (mapv (fn [branch]
                     (plan-query
                      {:find (vec (for [arg args]
                                    [:logic-var arg]))
                       ::apply-mapping apply-mapping
                       :where branch})))
             (reduce (fn [acc plan]
                       (-> [:union-all acc plan]
                           (with-meta (meta acc))))))
        (vary-meta into {::required-vars required-vars, ::apply-mapping apply-mapping}))))

(defn- wrap-union-joins [plan union-joins param-vars]
  (if-let [apply-mapping (->> union-joins
                              (into {} (mapcat (comp ::apply-mapping meta)))
                              not-empty)]
    (let [sq-plan (mega-join union-joins param-vars)
          [plan-u sq-plan-u :as rels] (with-unique-cols [plan sq-plan] param-vars)
          apply-mapping-u (update-keys apply-mapping (::var->col (meta plan-u)))]
      (-> [:apply :cross-join apply-mapping-u
           plan-u sq-plan-u]
          (wrap-unify (::var->cols (meta rels)))))


    (mega-join (into [plan] union-joins) param-vars)))

(defn- plan-sub-query [{:keys [query]}]
  (let [required-vars (->> (:in query)
                           (into #{} (map
                                      (fn [[in-type in-arg :as in]]
                                        (when-not (= in-type :scalar)
                                          (throw (err/illegal-arg :non-scalar-subquery-param
                                                                  (s/unform ::in-binding in))))
                                        in-arg))))
        apply-mapping (->apply-mapping required-vars)]
    (-> (plan-query (-> query
                        (dissoc :in)
                        (assoc ::apply-mapping apply-mapping)))
        (vary-meta into {::required-vars required-vars, ::apply-mapping apply-mapping}))))

(defn- wrap-sub-queries [plan sub-queries param-vars]
  (if-let [apply-mapping (->> sub-queries
                              (into {} (mapcat (comp ::apply-mapping meta)))
                              (not-empty))]
    (let [sq-plan (mega-join sub-queries param-vars)
          [plan-u sq-plan-u :as rels] (with-unique-cols [plan sq-plan] param-vars)
          apply-mapping-u (update-keys apply-mapping (::var->col (meta plan-u)))]
      (-> [:apply :cross-join apply-mapping-u
           plan-u sq-plan-u]
          (wrap-unify (::var->cols (meta rels)))))

    (mega-join (into [plan] sub-queries) param-vars)))

(defn- plan-body [{where-clauses :where, apply-mapping ::apply-mapping, :as query}]
  (let [in-rels (plan-in-tables query)
        {::keys [param-vars]} (meta in-rels)

        {triple-clauses :triple, call-clauses :call, sub-query-clauses :sub-query
         semi-join-clauses :semi-join, anti-join-clauses :anti-join, union-join-clauses :union-join
         :as grouped-clauses}
        (-> where-clauses
            (->> (group-by first))
            (update-vals #(mapv second %)))

        temporal-clauses (mapcat grouped-clauses [:for-all-app-time :for-app-time-at :for-app-time-in
                                                  :for-all-sys-time :for-sys-time-at :for-sys-time-in])
        temporal-preds (->temporal-preds temporal-clauses)]

    (loop [plan (mega-join (vec (concat in-rels (plan-triples triple-clauses temporal-preds)))
                           (concat param-vars apply-mapping))

           calls (some->> call-clauses (mapv plan-call))
           union-joins (some->> union-join-clauses (mapv plan-union-join))
           semi-joins (some->> semi-join-clauses (mapv (partial plan-semi-join :semi-join)))
           anti-joins (some->> anti-join-clauses (mapv (partial plan-semi-join :anti-join)))
           sub-queries (some->> sub-query-clauses (mapv plan-sub-query))]

      (if (and (empty? calls) (empty? sub-queries)
               (empty? semi-joins) (empty? anti-joins) (empty? union-joins))
        (-> plan
            (vary-meta assoc ::in-bindings (::in-bindings (meta in-rels))))

        (let [{available-vars ::vars} (meta plan)]
          (letfn [(available? [clause]
                    (set/superset? available-vars (::required-vars (meta clause))))]

            (let [{available-calls true, unavailable-calls false} (->> calls (group-by available?))
                  {available-sqs true, unavailable-sqs false} (->> sub-queries (group-by available?))
                  {available-ujs true, unavailable-ujs false} (->> union-joins (group-by available?))
                  {available-sjs true, unavailable-sjs false} (->> semi-joins (group-by available?))
                  {available-ajs true, unavailable-ajs false} (->> anti-joins (group-by available?))]

              (if (and (empty? available-calls) (empty? available-sqs)
                       (empty? available-ujs) (empty? available-sjs) (empty? available-ajs))
                (throw (err/illegal-arg :no-available-clauses
                                        {:available-vars available-vars
                                         :unavailable-subqs unavailable-sqs
                                         :unavailable-calls unavailable-calls
                                         :unavailable-union-joins unavailable-ujs
                                         :unavailable-semi-joins unavailable-sjs
                                         :unavailable-anti-joins unavailable-ajs}))

                (recur (cond-> plan
                         union-joins (wrap-union-joins union-joins param-vars)
                         available-calls (wrap-calls available-calls)
                         available-sjs (wrap-semi-joins :semi-join available-sjs)
                         available-ajs (wrap-semi-joins :anti-join available-ajs)
                         available-sqs (wrap-sub-queries available-sqs param-vars))

                       unavailable-calls unavailable-sqs
                       unavailable-ujs unavailable-sjs unavailable-ajs)))))))))

;; HACK these are just the grouping-fns used in TPC-H
;; - we're going to want a better way to recognise them
(def ^:private grouping-fn? '#{count count-distinct sum avg min max})

(defn- plan-head-exprs [{find-clause :find, :keys [order-by]}]
  (letfn [(with-col-name [prefix idx fc]
            (-> (vec fc) (with-meta {::col (col-sym prefix (str idx))})))

          (plan-head-form [col [form-type form-arg :as form]]
            (if (and (= form-type :fn-call)
                     (grouping-fn? (:f form-arg)))
              (let [{:keys [f], [[agg-arg-type :as agg-arg]] :args} form-arg]
                (if (= agg-arg-type :logic-var)
                  (-> col
                      (with-meta {::agg {col (s/unform ::form form)}, ::col col}))

                  (let [projection-sym (col-sym col "_agg")]
                    (-> col
                        (with-meta {::col col
                                    ::agg {col (list f projection-sym)}
                                    ::agg-projection {projection-sym (s/unform ::form agg-arg)}})))))

              (-> (s/unform ::form form)
                  (with-meta {::grouping-vars (form-vars form), ::col col}))))]

    (->> (concat (->> find-clause
                      (into [] (map-indexed (partial with-col-name "_column"))))
                 (->> order-by
                      (into [] (comp (map :find-arg)
                                     (map-indexed (partial with-col-name "_ob"))))))
         (into {} (comp (distinct)
                        (map
                         (fn [[arg-type arg :as find-arg]]
                           [find-arg
                            (case arg-type
                              :logic-var (-> arg (with-meta {::grouping-vars #{arg}, ::col arg}))
                              :form (let [{::keys [col]} (meta find-arg)]
                                      (plan-head-form col arg)))])))))))

(defn- plan-find [{find-args :find, rename-keys :keys} head-exprs]
  (let [clauses (->> find-args
                     (mapv (fn [rename-key clause]
                             (let [expr (get head-exprs clause)
                                   {::keys [col]} (meta expr)
                                   col (or (some-> rename-key col-sym) col)]
                               (-> (if (= col expr) col {col expr})
                                   (with-meta {::var col}))))
                           (or rename-keys (repeat nil))))]
    (-> clauses
        (with-meta {::vars (->> clauses (into #{} (map (comp ::var meta))))}))))

(defn- wrap-find [plan find-clauses]
  (-> [:project find-clauses plan]
      (with-meta (-> (meta plan) (assoc ::vars (::vars (meta find-clauses)))))))

(defn- plan-order-by [{:keys [order-by]} head-exprs]
  (some->> order-by
           (mapv (fn [{:keys [find-arg direction] :or {direction :asc}}]
                   [(::col (meta (get head-exprs find-arg)))
                    {:direction direction}]))))

(defn- wrap-order-by [plan order-by-clauses]
  (-> [:order-by order-by-clauses plan]
      (with-meta (meta plan))))

(defn- plan-group-by [head-exprs]
  (let [head-exprs (vals head-exprs)]
    (when-let [aggs (->> head-exprs (into {} (keep (comp ::agg meta))) (not-empty))]
      (let [grouping-vars (->> head-exprs (into [] (mapcat (comp ::grouping-vars meta))))]
        (-> (into grouping-vars
                  (map (fn [[col agg]]
                         {col agg}))
                  aggs)
            (with-meta {::vars (into (set grouping-vars) (map key) aggs)
                        ::agg-projections (->> head-exprs
                                               (into [] (keep (comp ::agg-projection meta)))
                                               (not-empty))}))))))

(defn- wrap-group-by [plan group-by-clauses]
  (let [{::keys [agg-projections]} (meta group-by-clauses)]
    (-> [:group-by group-by-clauses
         (if agg-projections
           [:map agg-projections plan]
           plan)]
        (with-meta (-> (meta plan) (assoc ::vars (::vars (meta group-by-clauses))))))))

(defn- wrap-head [plan query]
  (let [head-exprs (plan-head-exprs query)
        find-clauses (plan-find query head-exprs)
        order-by-clauses (plan-order-by query head-exprs)
        group-by-clauses (plan-group-by head-exprs)]

    (-> plan
        (cond-> group-by-clauses (wrap-group-by group-by-clauses)
                order-by-clauses (wrap-order-by order-by-clauses))
        (wrap-find find-clauses))))

(defn- wrap-top [plan {:keys [limit offset]}]
  (if (or limit offset)
    (-> [:top (cond-> {}
                offset (assoc :skip offset)
                limit (assoc :limit limit))
         plan]
        (with-meta (meta plan)))

    plan))

(defn- plan-query [conformed-query]
  (-> (plan-body conformed-query)
      (wrap-head conformed-query)
      (wrap-top conformed-query)))

(defn compile-query [query]
  (-> (conform-query query)
      (plan-query)))

(defn- args->params [args in-bindings]
  (->> (mapcat (fn [{::keys [binding-type in-cols]} arg]
                 (case binding-type
                   (:source :scalar) [(MapEntry/create (first in-cols) arg)]
                   :tuple (zipmap in-cols arg)
                   (:collection :relation) nil))
               in-bindings
               args)
       (into {})))

(defn- args->tables [args in-bindings]
  (->> (mapcat (fn [{::keys [binding-type in-cols table-key]} arg]
                 (case binding-type
                   (:source :scalar :tuple) nil

                   :collection (let [in-col (first in-cols)
                                     binding-k (keyword in-col)]
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
                                       ks (mapv keyword in-cols)]
                                   [(MapEntry/create table-key
                                                     (case rel-type
                                                       :maps rel
                                                       :vecs (mapv #(zipmap ks %) rel)))])))))
               in-bindings
               args)
       (into {})))

(defn open-datalog-query ^core2.IResultSet [^BufferAllocator allocator, ^IRaQuerySource ra-src, wm-src
                                            {:keys [basis] :as query} args]
  (let [plan (compile-query (dissoc query :basis :basis-timeout))
        {::keys [in-bindings]} (meta plan)

        plan (-> plan
                 #_(doto clojure.pprint/pprint)
                 #_(->> (binding [*print-meta* true]))
                 (lp/rewrite-plan {})
                 #_(doto clojure.pprint/pprint)
                 (doto (lp/validate-plan)))

        ^core2.operator.PreparedQuery pq (.prepareRaQuery ra-src plan)]

    (when (not= (count in-bindings) (count args))
      (throw (err/illegal-arg :in-arity-exception {::err/message ":in arity mismatch"
                                                   :expected (count in-bindings)
                                                   :actual args})))

    (let [^AutoCloseable
          params (vw/open-params allocator (args->params args in-bindings))]
      (try
        (-> (.bind pq wm-src
                   {:params params, :table-args (args->tables args in-bindings), :basis basis})
            (.openCursor)
            (op/cursor->result-set params))
        (catch Throwable t
          (.close params)
          (throw t))))))
