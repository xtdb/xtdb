(ns xtdb.datalog
  (:require [clojure.set :as set]
            [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [xtdb.error :as err]
            [xtdb.logical-plan :as lp]
            [xtdb.operator :as op]
            [xtdb.operator.group-by :as group-by]
            [xtdb.rewrite :as r]
            [xtdb.util :as util]
            [xtdb.vector.writer :as vw])
  (:import (clojure.lang MapEntry)
           (java.lang AutoCloseable)
           (org.apache.arrow.memory BufferAllocator)
           xtdb.IResultSet
           (xtdb.operator IRaQuerySource)
           (xtdb.operator.scan IScanEmitter)
           (xtdb.watermark IWatermarkSource Watermark)))

(s/def ::logic-var symbol?)

(s/def ::eid ::lp/value)
(s/def ::attr keyword?)
(s/def ::value ::lp/value)
(s/def ::table (s/and keyword? (s/conformer symbol keyword)))
(s/def ::column symbol?)

(s/def ::fn-call
  (s/and list?
         (s/cat :f simple-symbol?
                :args (s/* ::form))))

(s/def ::form
  (s/or :logic-var ::logic-var
        :sub-query ::sub-query
        :fn-call ::fn-call
        :literal ::value))

(s/def ::find-arg
  (s/or :logic-var ::logic-var
        :form ::form))

(s/def ::sub-query (s/cat :sq-type #{'q}, :sub-query ::query))
(s/def ::left-join (s/cat :sq-type #{'left-join}, :sub-query ::query))
(s/def ::semi-join (s/cat :sq-type #{'exists?}, :sub-query ::query))
(s/def ::anti-join (s/cat :sq-type #{'not-exists?}, :sub-query ::query))

(s/def ::find (s/coll-of ::find-arg :kind vector?))
(s/def ::keys (s/coll-of symbol? :kind vector?))

(s/def ::args-list (s/coll-of ::logic-var, :kind vector?, :min-count 0))

(s/def ::binding
  (s/or :collection (s/tuple ::logic-var '#{...})
        :relation (s/tuple ::args-list)
        :scalar ::logic-var
        :tuple ::args-list))

(s/def ::in-binding
  (s/or :collection (s/tuple ::logic-var '#{...})
        :relation (s/tuple ::args-list)
        :scalar ::logic-var
        :tuple ::args-list))

(s/def ::in (s/* ::in-binding))

(s/def ::triple-value
  (s/or :literal ::value,
        :logic-var ::logic-var
        :unwind (s/tuple ::logic-var #{'...})))

(s/def ::match-map-spec
  (-> (s/map-of ::attr ::triple-value)
      (s/and (s/conformer #(vec (map (fn [[attr val]] [attr val]) %))
                          #(->> %
                                (map (fn [[attr val]] [attr val]))
                                (into {} ))))))

(s/def ::match-spec
  (-> (s/or :map ::match-map-spec
            :vector (-> (s/or :column ::column
                              :map ::match-map-spec)
                        (s/and (s/conformer (fn [[tag arg]]
                                              (case tag :map arg, :column {(keyword arg) [:logic-var arg]}))
                                            (fn [arg]
                                              [:map arg])))
                        (s/coll-of :kind vector?)))

      (s/and (s/conformer (fn [[tag arg]]
                            (case tag
                              :map (vec arg)
                              :vector (into [] cat arg)))
                          (fn [v]
                            [:vector (mapv #(conj {} %) v)])))))

(s/def ::temporal-opts
  (-> (s/keys :opt-un [::lp/for-valid-time ::lp/for-system-time])
      (s/nonconforming)))

(s/def ::match
  (s/and list?
         (s/cat :tag #{'$ 'match}
                :table ::table,
                :match ::match-spec,
                :temporal-opts (s/? ::temporal-opts))))

(s/def ::triple
  (s/and vector?
         (s/conformer identity vec)
         (s/cat :e (s/or :literal ::eid, :logic-var ::logic-var)
                :a ::attr
                :v (s/? ::triple-value))))

(s/def ::call-clause
  (s/and vector?
         ;; top-level can only be a fn-call because otherwise the EDN Datalog syntax is ambiguous
         ;; (it wasn't ever meant to handle this...)
         (s/cat :form (s/spec (s/or :fn-call ::fn-call :sub-query ::sub-query))
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

(s/def ::rule
  (s/and list?
         (s/cat :name simple-symbol?
                :args (s/+ (s/or :literal ::value,
                                 :logic-var ::logic-var)))))

(s/def ::rule-head
  (s/and list?
         (s/cat :name simple-symbol?
                :args (s/+ ::logic-var))))

(s/def ::rule-definition
  (s/and vector?
         (s/cat :head ::rule-head
                :body (s/+ ::term))))

(s/def ::rules (s/coll-of ::rule-definition :kind vector?))

(s/def ::term
  (s/or :inner-join ::sub-query
        :left-join ::left-join
        :semi-join ::semi-join
        :anti-join ::anti-join

        :union-join ::union-join

        :triple ::triple
        :call ::call-clause
        :match ::match
        :rule ::rule))

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
          :opt-un [::keys ::in ::where ::order-by ::offset ::limit ::rules]))

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

(defn- unify-preds
  [var->cols]
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

(defn- binding-vars [binding]
  (letfn [(binding-vars* [[binding-type binding-arg]]
            (case binding-type
              :collection #{(first binding-arg)}
              :relation (set (first binding-arg))
              :scalar #{binding-arg}
              :tuple (set binding-arg)))]
    (binding-vars* binding)))

(defn- form-vars [form]
  (letfn [(form-vars* [[form-type form-arg]]
            (case form-type
              :fn-call (into #{} (mapcat form-vars*) (:args form-arg))
              :logic-var #{form-arg}
              :literal #{}
              :form (form-vars form-arg)
              :sub-query (into #{} (mapcat binding-vars) (-> form-arg :sub-query :in))))]
    (form-vars* form)))

(defn- combine-term-vars [term-varses]
  (let [{:keys [provided-vars] :as vars} (->> term-varses
                                              (apply merge-with set/union))]
    (-> vars
        (update :required-vars set/difference provided-vars))))

(defn- term-vars [[term-type term-arg]]
  (case (keyword term-type)
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

    (:inner-join :semi-join :anti-join :left-join :single-join)
    (let [{:keys [sub-query]} term-arg]
      {:provided-vars (set (or (:keys sub-query)
                               (->> (:find sub-query)
                                    (filter (comp #{:logic-var} first))
                                    (map form-vars)
                                    (apply set/union))))
       :required-vars (->> (:in sub-query)
                           (into #{} (map (fn [[in-type in-arg :as in]]
                                            (when-not (= in-type :scalar)
                                              (throw (err/illegal-arg :non-scalar-subquery-param
                                                                      (s/unform ::in-binding in))))
                                            in-arg))))})

    :match {:provided-vars (->> term-arg
                                :match
                                (map second)
                                (filter (comp #{:logic-var} first))
                                (map second)
                                set)}))

(defn- ->param-sym [lv]
  (-> (symbol (str "?" (name lv)))
      (with-meta {::param? true})))

(defn- plan-in-tables [{in-bindings :in}]
  (let [in-bindings (->> in-bindings
                         (into [] (map-indexed
                                   (fn [idx [binding-type binding-arg]]
                                     (let [table-key (symbol (str "?in" idx))]
                                       (-> (case binding-type
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

(defn- wrap-scan-col-preds [scan-col col-preds]
  (case (count col-preds)
    0 scan-col
    1 {scan-col (first col-preds)}
    {scan-col (list* 'and col-preds)}))

(def app-time-period-sym 'xt/valid-time)
(def app-time-from-sym 'xt/valid-from)
(def app-time-to-sym 'xt/valid-to)
(def app-temporal-cols {:period app-time-period-sym
                        :from app-time-from-sym
                        :to app-time-to-sym})


(def system-time-period-sym 'xt/system-time)
(def system-time-from-sym 'xt/system-from)
(def system-time-to-sym 'xt/system-to)
(def sys-temporal-cols {:period system-time-period-sym
                        :from system-time-from-sym
                        :to system-time-to-sym})

(defn replace-period-cols-with-temporal-attrs
  [original-attrs]
  (cond-> original-attrs
    (contains? original-attrs app-time-period-sym)
    (-> (disj app-time-period-sym)
        (conj app-time-from-sym app-time-to-sym))

    (contains? original-attrs system-time-period-sym)
    (-> (disj system-time-period-sym)
        (conj system-time-from-sym system-time-to-sym))))

(defn create-period-constructor [match {:keys [period from to]}]
  (when-let [[_ [lv-type lv]] (first (filter #(= period (first %)) match))]
    (if (= :logic-var lv-type)
      {(col-sym lv) (list 'period (col-sym from) (col-sym to))}

      (throw (err/illegal-arg :temporal-period-requires-logic-var
                              {::err/message "Temporal period must be bound to logic var"
                               :period period
                               :value lv})))))

(defn wrap-with-period-constructor [plan match]
  (if-let [period-constructors (not-empty
                                 (keep
                                   #(create-period-constructor match %)
                                   [app-temporal-cols sys-temporal-cols]))]
    [:map (vec period-constructors)
     plan]
    plan))

(defn- plan-scan [table match temporal-opts]
  (let [original-attrs (set (keys match))

        attrs (replace-period-cols-with-temporal-attrs original-attrs)

        attr->lits (-> match
                       (->> (keep (fn [[a [v-type v-arg]]]
                                    (when (= :literal v-type)
                                      {:a a, :lit v-arg})))
                            (group-by :a))
                       (update-vals #(into #{} (map :lit) %)))
        plan (-> [:scan {:table table
                         :for-valid-time (:for-valid-time temporal-opts)
                         :for-system-time (:for-system-time temporal-opts)}
                  (-> attrs
                      (->> (mapv (fn [attr]
                                   (-> attr
                                       (wrap-scan-col-preds (for [lit (get attr->lits attr)]
                                                              (list '= attr lit))))))))])]

    (with-meta
      (wrap-with-period-constructor plan match)
      {::vars attrs})))

(defn- attr->unwind-col [a]
  (col-sym "__uw" a))

(defn- wrap-unwind [plan triples]
  (->> triples
       (transduce
        (comp (keep (fn [[a [v-type _v-arg]]]
                      (when (= v-type :unwind)
                        a)))
              (distinct))

        (completing (fn [plan a]
                      (let [uw-col (attr->unwind-col a)]
                        (-> [:unwind {uw-col a}
                             plan]
                            (vary-meta update ::vars conj uw-col)))))
        plan)))

(defn- match->eids [{:keys [match]}]
  (->> match
       (filter (comp #{:xt/id} first))
       (map second)
       set))

(defn add-triples-to-matches [matches->eids [e triples]]
  (let [avs (for [{:keys [a v]} triples]
              (MapEntry/create a v))
        [matches->eids matched?]
        (reduce-kv (fn [[m matched?] match eids]
                     (if (contains? eids e)
                       [(assoc m (update match :match into avs) eids) true]
                       [(assoc m match eids) matched?]))
                   [{} false] matches->eids)]
    (when-not matched?
      (throw (err/illegal-arg :unspecified-table
                              {:triples (map #(s/unform ::triple %) triples)})))
    matches->eids))

(defn- plan-from [{:keys [triples matches]}]
  (let [matches-eids (zipmap matches (map match->eids matches))
        tables (->> (group-by :e triples)
                    (reduce add-triples-to-matches matches-eids)
                    keys)]
    (vec
     (for [{:keys [table match temporal-opts]} tables]
       (let [match (->> match (mapv (fn [[a v]] (MapEntry/create (col-sym a) v))))
             var->cols (-> match
                           (->> (keep (fn [[a [v-type v-arg]]]
                                        (case v-type
                                          :logic-var {:lv v-arg
                                                      :col (if (contains?
                                                                 #{app-time-period-sym
                                                                   system-time-period-sym}
                                                                 a)
                                                             (col-sym v-arg)
                                                             a)}
                                          :unwind {:lv (first v-arg), :col (attr->unwind-col a)}
                                          nil)))
                                (group-by :lv))
                           (update-vals #(into #{} (map :col) %)))]
         (-> (plan-scan table match temporal-opts)
             (wrap-unwind match)
             (wrap-unify var->cols)))))))

(def ^:dynamic *gensym* gensym)

(defn seeded-gensym
  ([] (seeded-gensym "" 0))
  ([count-start] (seeded-gensym "" count-start))
  ([suffix count-start]
   (let [ctr (atom (dec count-start))]
     (fn gensym-seed
       ([] (symbol (str "gensym" suffix (swap! ctr inc))))
       ([prefix] (symbol (str prefix suffix (swap! ctr inc))))))))

(defn scalar-sub-query-placeholder [sub-query]
  (with-meta (*gensym* "_sq") {::sub-query sub-query, :column? true}))

(defn scalar-sub-query-referent [sub-query-placeholder]
  (::sub-query (meta sub-query-placeholder)))

(defn scalar-sub-query-placeholder? [ra-expr]
  (and (symbol? ra-expr) (some? (scalar-sub-query-referent ra-expr))))

(defn find-scalar-sub-query-placeholders [ra-expr]
  (cond
    (scalar-sub-query-placeholder? ra-expr) {ra-expr (scalar-sub-query-referent ra-expr)}
    (seq? ra-expr) (reduce conj {} (map find-scalar-sub-query-placeholders (rest ra-expr)))
    :else {}))

(defn- plan-call [{:keys [form return]}]
  (letfn [(with-col-metadata [[form-type form-arg]]
            (case form-type
              :logic-var [:logic-var (if (str/starts-with? (name form-arg) "?")
                                       form-arg
                                       (col-sym form-arg))]
              :fn-call [:fn-call (-> form-arg (update :args #(mapv with-col-metadata %)))]
              :literal [:literal form-arg]
              :sub-query [:logic-var (scalar-sub-query-placeholder form-arg)]))]
    (-> (s/unform ::form (with-col-metadata form))
        (vary-meta merge
                   {::required-vars (form-vars form)}
                   (when-let [[return-type return-arg] return]
                     (-> (case return-type
                           :scalar {::return-var return-arg
                                    ::vars #{return-arg}})
                         (assoc ::return-type return-type)))))))

(defn- ->apply-mapping [apply-params]
  ;;TODO symbol names will clash with nested applies
  ;; (where an apply is nested inside the dep side of another apply)
  (when (seq apply-params)
    (->> (for [param apply-params]
           (let [param-symbol (-> (symbol (str "?ap_" param))
                                  (with-meta {:correlated-column? true}))]
             (MapEntry/create param param-symbol)))
         (into {}))))

(defn- plan-sub-query [sq-type {:keys [sub-query] :as term-arg}]
  (let [{:keys [required-vars provided-vars]} (term-vars [sq-type term-arg])
        apply-mapping (->apply-mapping required-vars)]
    (-> (plan-query (-> sub-query
                        (dissoc :in)
                        (assoc ::apply-mapping apply-mapping)))
        (vary-meta into {::provided-vars provided-vars
                         ::required-vars required-vars
                         ::apply-mapping apply-mapping}))))

(defn- wrap-scalar-sub-query [plan binding-sym scalar-sub-query param-vars]
  (let [col-count (count (:find scalar-sub-query))
        _ (when (not= 1 col-count) (throw (err/illegal-arg :scalar-sub-query-requires-one-column {::err/message "scalar sub query requires exactly one column"})))
        sq-plan (plan-sub-query :single-join {:sub-query (assoc scalar-sub-query :keys [binding-sym])})
        {::keys [apply-mapping]} (meta sq-plan)
        sq-plan (vary-meta sq-plan assoc ::vars #{binding-sym})
        [plan-u sq-plan-u :as rels] (with-unique-cols [plan sq-plan] param-vars)
        apply-mapping-u (update-keys apply-mapping (::var->col (meta plan-u)))]
    (-> [:apply :single-join apply-mapping-u plan-u sq-plan-u]
        (wrap-unify (::var->cols (meta rels))))))

(defn- wrap-dependent-sub-queries [plan conformed-sub-queries param-vars]
  (reduce-kv
    (fn [plan sym sq]
      (wrap-scalar-sub-query plan sym (:sub-query sq) param-vars))
    plan
    conformed-sub-queries))

(defn- wrap-calls [plan calls param-vars]
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
    (let [sub-queries (reduce conj {} (map find-scalar-sub-query-placeholders calls))
          {selects nil, scalars :scalar} (group-by (comp ::return-type meta) calls)]
      (-> plan
          (wrap-dependent-sub-queries sub-queries param-vars)
          (cond-> scalars (wrap-scalars scalars))
          (wrap-select selects)))))

(defn- wrap-inner-joins [plan sub-queries param-vars]
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

(defn- wrap-joins [plan join-type join-plans]
  (->> join-plans
       (reduce (fn [acc sq-plan]
                 (let [{::keys [apply-mapping provided-vars]} (meta sq-plan)
                       {::keys [vars]} (meta acc)
                       provided-vars-apply-mapping (-> (->apply-mapping provided-vars)
                                                       (select-keys vars))]
                   (-> (if apply-mapping
                         [:apply join-type (merge apply-mapping provided-vars-apply-mapping)
                          acc (->> provided-vars-apply-mapping
                                   (map (fn [[v1 v2]]
                                          (list '= (with-meta v1 {:column? true}) v2)))
                                   (wrap-select sq-plan))]
                         [join-type (->> (::vars (meta sq-plan))
                                         (filter vars)
                                         (mapv (fn [v] {v v})))
                          acc sq-plan])
                       (with-meta (cond-> (meta acc)
                                    (= :left-outer-join join-type) (update ::vars into provided-vars))))))
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

;;;;;;;;;;;;;;;;;;;;;;;;
;; rule substitution
;;;;;;;;;;;;;;;;;;;;;;;;

(defrecord ReplacementCtx [var->replacement replacement-fn])

(defn ->replacement-ctx [var->replacement replacement-fn]
  (->ReplacementCtx var->replacement replacement-fn))

(defn get-replacement [{:keys [var->replacement replacement-fn] :as ctx} var]
  (if-let [replacement (var->replacement var)]
    [replacement ctx]
    (let [replacement (replacement-fn var)]
      [replacement (update ctx :var->replacement assoc var replacement)])))

(defn new-replacement-ctx [vars-to-keep {:keys [var->replacement] :as ctx}]
  (assoc ctx :var->replacement (select-keys var->replacement vars-to-keep)))

(defn wrap-replacement [replacement]
  (if (simple-symbol? replacement) [:logic-var replacement] [:literal replacement]))

(defn replace-arg-list [arg-list ctx]
  (reduce (fn [[arg-list ctx] var]
            (let [[replacement ctx] (get-replacement ctx var)]
              [(if (simple-symbol? replacement)
                 (conj arg-list replacement)
                 arg-list) ctx]))
          [[] ctx]
          arg-list))

(declare replace-vars*)

(defmulti replace-vars (fn [clause _replace-ctx] (first clause))
  :default ::default)

(defmethod replace-vars ::default [[type _] _]
  (throw (ex-info "No such clause known!" {:type type})))

(defmethod replace-vars :logic-var [[_ var] replace-ctx]
  (let [[replacement new-ctx] (get-replacement replace-ctx var)]
    [(wrap-replacement replacement) new-ctx]))

(defmethod replace-vars :literal [literal replace-ctx]
  [literal replace-ctx])

(defmethod replace-vars :scalar [[_ var]replace-ctx]
  (let [[replacement new-ctx] (get-replacement replace-ctx var)]
    [[:scalar replacement] new-ctx]))

(defmethod replace-vars :form [[_ form] replace-ctx]
  (let [[replacement new-ctx] (replace-vars form replace-ctx)]
    [[:form replacement] new-ctx]))

(defmethod replace-vars :triple [[_ {:keys [e v]} :as triple] replace-cxt]
  (let [[new-e replace-ctx] (replace-vars e replace-cxt)
        [new-v replace-ctx] (replace-vars v replace-ctx)]
    [(-> triple (update 1 assoc :e new-e) (update 1 assoc :v new-v))
     replace-ctx]))

(defmethod replace-vars :match [[_ {match-specs :match :as match}] replace-cxt]
  (let [[new-values new-ctx] (-> (map second match-specs)
                                 (replace-vars* replace-cxt))]
    [[:match (assoc match :match (mapv vector (map first match-specs) new-values))]
     new-ctx]))

(defn- replace-sq-vars [{:keys [find in keys where order-by rules]} replace-ctx]
  (if-not rules
    (let [[new-find replace-ctx] (replace-vars* find replace-ctx)
          [new-in replace-ctx] (replace-vars* in replace-ctx)
          [new-keys replace-ctx] (reduce (fn [[res replace-ctx] key-symbol]
                                           (let [[replacement new-replace-ctx] (get-replacement replace-ctx key-symbol)]
                                             [(conj res replacement) new-replace-ctx]))
                                         [[] replace-ctx]
                                         keys)
          [new-order-by replace-ctx] (let [[new-order-by-elements replace-ctx]
                                           (-> (map :find-arg order-by)
                                               (replace-vars* replace-ctx))]
                                       [(map #(assoc %1 :find-arg %2) order-by new-order-by-elements) replace-ctx])
          new-ctx (new-replacement-ctx (->> (concat (map form-vars find)
                                                    (map binding-vars in))
                                            (apply set/union))
                                       replace-ctx)
          [new-where _] (replace-vars* where new-ctx)]
      [(cond-> {:find new-find
                :in new-in
                :where new-where}
         keys (assoc :keys new-keys)
         order-by (assoc :order-by new-order-by))
       replace-ctx])

    (throw (err/illegal-arg :rules-not-supported-in-subquery
                            (s/unform ::rules rules)))))

(defmethod replace-vars :semi-join [[_ {:keys [sub-query]}] replace-ctx]
  (let [[new-sq replace-ctx] (replace-sq-vars sub-query replace-ctx)]
    [[:semi-join
      {:sq-type 'exists?
       :sub-query new-sq}]
     replace-ctx]))

(defmethod replace-vars :anti-join [[_ {:keys [sub-query]}] replace-ctx]
  (let [[new-sq replace-ctx] (replace-sq-vars sub-query replace-ctx)]
    [[:anti-join
      {:sq-type 'not-exists?
       :sub-query new-sq}]
     replace-ctx]))

(defmethod replace-vars :union-join [[_ {:keys [args branches]}] replace-ctx]
  (let [[new-args replace-ctx] (replace-arg-list args replace-ctx)
        new-ctx (new-replacement-ctx args replace-ctx)
        [new-branches _] (reduce (fn [[brs ctx] branch]
                                   (let [[new-br ctx] (replace-vars* branch ctx)]
                                     [(conj brs new-br) ctx]))
                                 [[] new-ctx]
                                 branches)]
    [[:union-join
      {:union-join 'union-join
       :args new-args
       :branches new-branches}]
     replace-ctx]))

(defmethod replace-vars :call [[_ {:keys [form]} :as _call] replace-ctx]
  (let [[fn-call new-ctx] (replace-vars form replace-ctx)]
    [[:call {:form fn-call}] new-ctx]))

(defmethod replace-vars :fn-call [[_ {:keys [args]} :as fn-call] replace-ctx]
  (let [[new-args new-ctx]
        (reduce (fn [[new-args replace-ctx] arg]
                  (let [[new-arg replace-ctx] (replace-vars arg replace-ctx)]
                    [(conj new-args new-arg) replace-ctx]))
                [[] replace-ctx]
                args)]
    [(update fn-call 1 assoc :args new-args) new-ctx]))

(defmethod replace-vars :inner-join [[_ {:keys [sub-query]}] replace-ctx]
  (let [[new-sq replace-ctx] (replace-sq-vars sub-query replace-ctx)]
    [[:inner-join
      {:sq-type 'q
       :sub-query new-sq}]
     replace-ctx]))

(defmethod replace-vars :rule [rule replace-ctx]
  (let [[new-args new-replace-ctx]
        (->> rule second :args
             (reduce (fn [[new-args replace-ctx] [arg-type value :as arg]]
                       (case arg-type
                         :logic-var (let [[replacement replace-ctx] (get-replacement replace-ctx value)]
                                      [(conj new-args (wrap-replacement replacement)) replace-ctx])
                         :literal [(conj new-args arg) replace-ctx]))
                     [[] replace-ctx]))]
    [(update rule 1 assoc :args new-args) new-replace-ctx]))

(defn replace-vars* [exprs replace-ctx]
  (reduce (fn [[res replace-ctx] clause]
            (let [[updated-clause new-replace-ctx] (replace-vars clause replace-ctx)]
              [(conj res updated-clause) new-replace-ctx]))
          [[] replace-ctx]
          exprs))

(defn unique-symbol-fn [prefix]
  (let [cnt (atom -1)]
    (fn [suffix]
      (symbol (str "_" prefix "_" (swap! cnt inc) "_" suffix)))))

(defn gensym-rule [{:keys [head body] :as rule}]
  (let [unique-rule-arg-symbol (unique-symbol-fn "rule_arg")
        args (:args head)
        new-args (mapv unique-rule-arg-symbol args)
        old->new (zipmap args new-args)
        replace-ctx (->replacement-ctx old->new unique-rule-arg-symbol)]
    (-> rule
        (assoc :head (assoc head :args new-args))
        (assoc :body (first (replace-vars* body replace-ctx))))))

(defn gensym-rules [rules]
  (map gensym-rule rules))

(declare expand-rules)

(defn rewrite-rule [rule-name->rules {:keys [name args] :as rule-invocation}]
  (if-let [rules (get rule-name->rules name)]
    (if (= (count args) (-> rules first :head :args count))
      (let [branches (->> rules
                          (mapv (fn [{:keys [head body] :as _rule}]
                                  (let [var-inner->var-outer (zipmap (:args head) (map second args))
                                        replace-ctx (->replacement-ctx var-inner->var-outer identity)]
                                    (-> (expand-rules rule-name->rules body)
                                        (replace-vars* replace-ctx)
                                        first)))))]
        [:union-join
         {:union-join 'union-join
          :args (->> (filter (comp #{:logic-var} first) args)
                     (mapv second))
          :branches branches}])
      (throw (err/illegal-arg :rule-wrong-arity
                              {:rule-invocation (s/unform ::rule rule-invocation)})))
    (throw (err/illegal-arg :unknown-rule
                            {:rule-invocation (s/unform ::rule rule-invocation)}))))

(defn- expand-rules [rule-name->rules where-clauses]
  (mapv (fn [[type arg :as clause]]
          (case type
            (:triple :call) clause

            :union-join
            (->> clause second :branches
                 (mapv (partial expand-rules rule-name->rules))
                 (update clause 1 assoc :branches))

            (:semi-join :anti-join :inner-join :left-join)
            (->> clause second :sub-query :where
                 (expand-rules rule-name->rules)
                 (update-in clause [1 :sub-query] assoc :where))

            :rule (rewrite-rule rule-name->rules arg)

            clause))
        where-clauses))

(defn check-rule-arity [rule-name->rules]
  (run! (fn [[name rules]]
          (let [arities (map #(-> % :head :args count) rules)]
            (when-not (apply = arities)
              (throw (err/illegal-arg :rule-definitions-require-unique-arity
                                      {:rule-name name})))))
        rule-name->rules))

(defn- plan-body [{where-clauses :where, apply-mapping ::apply-mapping, rules :rules} in-rels]
  (let [{::keys [param-vars]} (meta in-rels)
        rule-name->rules (->> rules
                              gensym-rules
                              (group-by (comp :name :head)))
        _ (check-rule-arity rule-name->rules)
        where-clauses (expand-rules rule-name->rules where-clauses)

        {match-clauses :match, triple-clauses :triple, call-clauses :call
         inner-join-clauses :inner-join, left-join-clauses :left-join
         semi-join-clauses :semi-join, anti-join-clauses :anti-join, union-join-clauses :union-join}
        (-> where-clauses
            (->> (group-by first))
            (update-vals #(mapv second %)))]

    (loop [plan (mega-join (vec (concat in-rels (plan-from {:matches match-clauses
                                                            :triples triple-clauses})))
                           (concat param-vars apply-mapping))

           calls (some->> call-clauses (mapv plan-call))
           union-joins (some->> union-join-clauses (mapv plan-union-join))
           inner-joins (some->> inner-join-clauses (mapv (partial plan-sub-query :inner-join)))
           left-joins (some->> left-join-clauses (mapv (partial plan-sub-query :left-join)))
           semi-joins (some->> semi-join-clauses (mapv (partial plan-sub-query :semi-join)))
           anti-joins (some->> anti-join-clauses (mapv (partial plan-sub-query :anti-join)))]

      (if (and (empty? calls) (empty? inner-joins) (empty? left-joins)
               (empty? semi-joins) (empty? anti-joins) (empty? union-joins))
        (-> plan
            (vary-meta assoc ::in-bindings (::in-bindings (meta in-rels))))

        (let [{available-vars ::vars} (meta plan)]
          (letfn [(available? [clause]
                    (set/superset? available-vars (::required-vars (meta clause))))]

            (let [{available-calls true, unavailable-calls false} (->> calls (group-by available?))
                  {available-ijs true, unavailable-ijs false} (->> inner-joins (group-by available?))
                  {available-ljs true, unavailable-ljs false} (->> left-joins (group-by available?))
                  {available-ujs true, unavailable-ujs false} (->> union-joins (group-by available?))
                  {available-sjs true, unavailable-sjs false} (->> semi-joins (group-by available?))
                  {available-ajs true, unavailable-ajs false} (->> anti-joins (group-by available?))]

              (if (and (empty? available-calls) (empty? available-ijs) (empty? available-ljs)
                       (empty? available-ujs) (empty? available-sjs) (empty? available-ajs))
                (throw (err/illegal-arg :no-available-clauses
                                        {:available-vars available-vars
                                         :unavailable-inner-joins unavailable-ijs
                                         :unavailable-left-joins unavailable-ljs
                                         :unavailable-calls unavailable-calls
                                         :unavailable-union-joins unavailable-ujs
                                         :unavailable-semi-joins unavailable-sjs
                                         :unavailable-anti-joins unavailable-ajs}))

                (recur (cond-> plan
                         union-joins (wrap-union-joins union-joins param-vars)
                         available-calls (wrap-calls available-calls param-vars)
                         available-ijs (wrap-inner-joins available-ijs param-vars)
                         available-sjs (wrap-joins :semi-join available-sjs)
                         available-ajs (wrap-joins :anti-join available-ajs)
                         available-ljs (wrap-joins :left-outer-join available-ljs))

                       unavailable-calls unavailable-ijs unavailable-ljs
                       unavailable-ujs unavailable-sjs unavailable-ajs)))))))))

(def ^:private aggregate-fn? (->> group-by/->aggregate-factory
                                  (methods)
                                  (keys)
                                  (map symbol)
                                  (set)))

(defn- wrap-with-meta [x m]
  (with-meta {:obj x} (assoc m ::wrapped true)))

(defn- unwrap-with-meta [{:keys [obj]}] obj)

(defn- plan-head-call [[form-type form :as conformed-form]]
  (case form-type
    :literal form
    (plan-call {:form conformed-form})))

(defn- plan-head-exprs [{find-clause :find, :keys [order-by]}]
  (letfn [(with-col-name [prefix idx fc]
            (-> (vec fc) (with-meta {::col (col-sym prefix (str idx))})))

          (plan-head-form [col [form-type form-arg :as form]]
            (if (and (= form-type :fn-call)
                     (aggregate-fn? (:f form-arg)))
              (let [{:keys [f], [[agg-arg-type :as agg-arg]] :args} form-arg]
                (if (= agg-arg-type :logic-var)
                  (-> col
                      (with-meta {::agg {col (s/unform ::form form)}, ::col col}))

                  (let [projection-sym (col-sym col "_agg")]
                    (-> col
                        (with-meta {::col col
                                    ::agg {col (list f projection-sym)}
                                    ::agg-projection {projection-sym (s/unform ::form agg-arg)}})))))
              (let [planned-form (plan-head-call form)
                    m {::grouping-vars (form-vars form), ::col col}]
                (if (instance? clojure.lang.IMeta planned-form)
                  (-> planned-form (vary-meta merge m))
                  (wrap-with-meta planned-form m)))))]

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
                                   {::keys [col wrapped]} (meta expr)
                                   col (or (some-> rename-key col-sym) col)
                                   expr (cond->> expr
                                          wrapped unwrap-with-meta)]
                               (-> (if (= col expr) col {col expr})
                                   (with-meta {::var col}))))
                           (or rename-keys (repeat nil))))]
    (-> clauses
        (with-meta {::vars (->> clauses (into #{} (map (comp ::var meta))))}))))

(defn- wrap-find [plan find-clauses param-vars]
  (let [cols (map (fn [projection] (if (map? projection) (first (vals projection)) projection)) find-clauses)
        sub-queries (reduce conj {} (map find-scalar-sub-query-placeholders cols))
        plan' (wrap-dependent-sub-queries plan sub-queries param-vars)]
    (-> [:project find-clauses plan']
        (with-meta (-> (meta plan') (assoc ::vars (::vars (meta find-clauses))))))))

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

(defn unbound-var-check [required-vars provided-vars location]
  (when-let [missing-vars (not-empty (set/difference required-vars provided-vars))]
    (throw (err/illegal-arg :xtdb.datalog/unbound-logic-var
                            {::err/message (str "Logic variables in " location " clause must be bound in where: " (str/join ", " missing-vars))
                             :vars missing-vars}))))

(defn- check-head-vars [{:keys [find order-by]} {body-provided-vars ::vars}]
  (let [find-vars (set (mapcat form-vars find))
        order-by-vars (set (mapcat (comp form-vars :find-arg) order-by))]
    (unbound-var-check find-vars body-provided-vars "find")
    (unbound-var-check order-by-vars body-provided-vars "order-by")))

(defn- wrap-head [plan query param-vars]

  (check-head-vars query (meta plan))

  (let [head-exprs (plan-head-exprs query)
        find-clauses (plan-find query head-exprs)
        order-by-clauses (plan-order-by query head-exprs)
        group-by-clauses (plan-group-by head-exprs)]

    (-> plan
        (cond-> group-by-clauses (wrap-group-by group-by-clauses)
                order-by-clauses (wrap-order-by order-by-clauses))
        (wrap-find find-clauses param-vars))))

(defn- wrap-top [plan {:keys [limit offset]}]
  (if (or limit offset)
    (-> [:top (cond-> {}
                offset (assoc :skip offset)
                limit (assoc :limit limit))
         plan]
        (with-meta (meta plan)))

    plan))

(defn- plan-query [conformed-query]
  (let [in-rels (plan-in-tables conformed-query)]
    (-> (plan-body conformed-query in-rels)
        (wrap-head conformed-query (::param-vars (meta in-rels)))
        (wrap-top conformed-query))))

(defn compile-query [query]
  (binding [*gensym* (seeded-gensym "_" 0)]
    (-> (conform-query query)
        (plan-query))))

(defn- args->params [args in-bindings]
  (->> (mapcat (fn [{::keys [binding-type in-cols]} arg]
                 (case binding-type
                   :scalar [(MapEntry/create (first in-cols) arg)]
                   :tuple (zipmap in-cols arg)
                   (:collection :relation) nil))
               in-bindings
               args)
       (into {})))

(defn- args->tables [args in-bindings]
  (->> (mapcat (fn [{::keys [binding-type in-cols table-key]} arg]
                 (case binding-type
                   (:scalar :tuple) nil

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

(def row-alias-sym 'xt/*)

(defn- str->datalog-form-kw [s]
  (if-let [i (str/index-of s "/")]
    (keyword (subs s 0 i) (subs s (inc i)))
    (keyword s)))

(defn- str->datalog-form-symbol [s]
  (if-let [i (str/index-of s "/")]
    (symbol (subs s 0 i) (subs s (inc i)))
    (symbol s)))

(defn apply-datalog-specific-rewrites [plan basis
                                       ^IWatermarkSource wm-src,
                                       ^IScanEmitter scan-emitter]
  (let [{:keys [tx, after-tx]} basis
        wm-tx (or tx after-tx)
        wm-delay (delay (.openWatermark wm-src wm-tx))
        table-col-names
        (memoize
         (fn [table]
           (let [^Watermark wm @wm-delay]
             (into #{} (map util/normal-form-str->datalog-form-str) (.tableColNames scan-emitter wm (str table))))))]
    (try
      (letfn [(rewrite-row-alias [z]
                (r/zmatch
                 z
                 [:scan scan-opts scan-cols]
                 (when (some #(= row-alias-sym %) scan-cols)
                   (let [table (:table scan-opts)
                         table-cols (table-col-names table)
                         struct-keys (map str->datalog-form-kw table-cols)
                         scan-row-cols (map str->datalog-form-symbol table-cols)]
                     [:map [{row-alias-sym (into {} (zipmap struct-keys scan-row-cols))}]
                      [:scan scan-opts (into [] (comp cat (filter #(not= row-alias-sym %))) [scan-cols, scan-row-cols])]]))))]
        (->> plan
             (r/vector-zip)
             (r/innermost (r/mono-tp rewrite-row-alias))
             (r/node)))
      (finally
        (when (realized? wm-delay) (util/try-close @wm-delay))))))

(defn open-datalog-query ^xtdb.IResultSet [^BufferAllocator allocator, ^IRaQuerySource ra-src, wm-src, ^IScanEmitter scan-emitter
                                           query {:keys [args default-all-valid-time? basis default-tz explain?]}]
  (let [plan (compile-query query)
        {::keys [in-bindings]} (meta plan)

        plan (-> plan
                 (apply-datalog-specific-rewrites basis wm-src scan-emitter)

                 #_(doto clojure.pprint/pprint)
                 #_(->> (binding [*print-meta* true]))
                 (lp/rewrite-plan {})
                 #_(doto clojure.pprint/pprint)
                 (doto (lp/validate-plan)))]

    (if explain?
      (let [col-types '{plan :clj-form}
            ^Iterable res [{:plan plan}]
            it (.iterator res)]
        (reify IResultSet
          (columnTypes [_] col-types)
          (hasNext [_] (.hasNext it))
          (next [_] (.next it))
          (close [_])))

      (let [^xtdb.operator.PreparedQuery pq (.prepareRaQuery ra-src plan)]
        (when (not= (count in-bindings) (count args))
          (throw (err/illegal-arg :in-arity-exception {::err/message ":in arity mismatch"
                                                       :expected (count in-bindings)
                                                       :actual args})))

        (util/with-close-on-catch [^AutoCloseable params (vw/open-params allocator (args->params args in-bindings))]
          (-> (.bind pq wm-src {:params params, :table-args (args->tables args in-bindings),
                                :basis basis, :default-tz default-tz :default-all-valid-time? default-all-valid-time?})
              (.openCursor)
              (op/cursor->result-set params)))))))
