(ns xtdb.xtql.plan
  (:require [clojure.set :as set]
            [xtdb.error :as err]
            [xtdb.expression :as expr]
            [xtdb.operator.group-by :as group-by]
            xtdb.tx-ops
            [xtdb.util :as util]
            [xtdb.xtql :as xtql])
  (:import (clojure.lang MapEntry)
           (xtdb.api.query Binding Expr$Bool Expr$Call Expr$Double Expr$Exists Expr$Get Expr$ListExpr Expr$LogicVar Expr$Long Expr$MapExpr Expr$Null Expr$Obj Expr$Param Expr$Pull Expr$PullMany Expr$SetExpr Expr$Subquery TemporalFilter$AllTime TemporalFilter$At TemporalFilter$In)
           (xtdb.xtql Aggregate DocsRelation From Join LeftJoin Limit Offset OrderBy ParamRelation Pipeline QueryWithParams Return Unify Unnest Where With Without)))

;;TODO consider helper for [{sym expr} sym] -> provided vars set
;;TODO Should all user supplied lv be planned via plan-expr, rather than explicit calls to col-sym.
;;keeps the conversion to java AST to -> clojure sym in one place.
;;TODO Document var->cols purpose, and/or give it a more descriptive name

(def ^:dynamic *table-info* nil)

(defprotocol PlanQuery
  (plan-query [query]))

(defprotocol PlanQueryTail
  (plan-query-tail [query-tail plan]))

(defprotocol PlanUnifyClause
  (plan-unify-clause [clause]))

(defprotocol PlanTemporalFilter
  (plan-temporal-filter [temporal-filter]))

(def ^:dynamic *gensym* gensym)

(defprotocol ExprPlan
  (plan-expr [expr])
  (required-vars [expr]))

(defn- col-sym
  ([col]
   (-> (symbol col)
       util/symbol->normal-form-symbol
       (vary-meta assoc :column? true)))

  ([prefix col] (col-sym (format "%s_%s" prefix col))))

(defn- param-sym [v]
  (-> (symbol v)
      util/symbol->normal-form-symbol
      (with-meta {:param? true})))

(defn- apply-param-sym
  ([v]
   (apply-param-sym v nil))
  ([v prefix]
   (-> (symbol (str "?_sq_" prefix v))
       util/symbol->normal-form-symbol
       (with-meta {:correlated-column? true}))))

(defn- unifying-vars->apply-param-mapping [unifying-vars]
  ;; creates a param for each var that needs to unify, so that we can place the
  ;; equality predicate on the dependant side, within the left join.
  ;;TODO symbol names will clash with nested applies (is this still true?)
  ;; (where an apply is nested inside the dep side of another apply)
  (when (seq unifying-vars)
    (->> (for [var unifying-vars]
           (MapEntry/create var (apply-param-sym var "ap")))
         ;;TODO this symbol can clash with user space params, need to do something better here.
         ;; classic problem rearing its head again.
         (into {}))))

(defn expr-subquery-placeholder []
  (col-sym
   (str "xt$_" (*gensym* "sqp"))))

(defn gen-col
  ([] (col-sym (str "xt$_" (*gensym* "gc"))))
  ([prefix] (col-sym prefix (str "xt$_" (*gensym* "gc")))))

(declare plan-arg-spec)

(def ^:dynamic *subqueries* nil)
(def ^:dynamic *agg-fns* nil)

(defn expr-subquery-required-vars [subquery-args]
  ;; planning arg-specs here to get required vars, could call required-vars on the exprs
  ;; directly, but wanted to avoid duplicating that code.
  (apply set/union (map (comp :required-vars plan-arg-spec) subquery-args)))

(def ^:private aggregate-fn?
  (comp (->> group-by/->aggregate-factory
             (methods)
             (keys)
             (set))
        expr/normalise-fn-name))

(defn- plan-arg-bindings [args]
  (let [arg-bindings (mapv plan-arg-spec args)
        temporary-expr-symbols (map (comp #(gen-col %) :l) arg-bindings)]
    {:arg-subqueries (mapcat (comp :subqueries :r) arg-bindings)
     :tmp-expr-sym->apply-param-sym (zipmap temporary-expr-symbols (map (comp apply-param-sym :l) arg-bindings))
     :tmp-expr-sym->expr-vec (mapv hash-map temporary-expr-symbols (map (comp :expr :r) arg-bindings))}))

(extend-protocol ExprPlan
  Expr$Null
  (plan-expr [_this] nil)
  (required-vars [_this] #{})

  Expr$LogicVar
  (plan-expr [this] (col-sym (.lv this)))
  (required-vars [this] #{(col-sym (.lv this))})

  Expr$Param
  (plan-expr [this] (param-sym (.v this)))
  (required-vars [_this] #{})

  Expr$ListExpr
  (plan-expr [this] (into [] (map plan-expr) (.elements this)))
  (required-vars [this] (into #{} (mapcat required-vars (.elements this))))

  Expr$SetExpr
  (plan-expr [this] (into #{} (map plan-expr (.elements this))))
  (required-vars [this] (into #{} (mapcat required-vars (.elements this))))

  Expr$MapExpr
  (plan-expr [this] (into {} (map (juxt (comp keyword util/str->normal-form-str key) (comp plan-expr val)) (.elements this))))
  (required-vars [this] (into #{} (mapcat required-vars (vals (.elements this)))))

  Expr$Obj
  (plan-expr [o] (.obj o))
  (required-vars [_] #{})

  Expr$Bool
  (plan-expr [this] (.bool this))
  (required-vars [_] #{})

  Expr$Long
  (plan-expr [this] (.lng this))
  (required-vars [_] #{})

  Expr$Double
  (plan-expr [this] (.dbl this))
  (required-vars [_] #{})

  Expr$Call
  (plan-expr [call]
    (let [fn (symbol (.f call))
          placeholder (expr-subquery-placeholder)]
      (if (aggregate-fn? fn)
        (do (swap! *agg-fns* conj {:agg-fn fn
                                   :placeholder placeholder
                                   :sub-expr (.args call)})
            placeholder)

        ;; Map = to == for SQL-like numeric coercion semantics
        (list* (case fn
                 = '==
                 (symbol (.f call)))
               (mapv plan-expr (.args call))))))
  (required-vars [call]
    (if (aggregate-fn? (symbol (.f call)))
      #{} ;; required vars should not traverse into aggregate fns, agg-fn sub-exprs are planned independently
      (into #{} (mapcat required-vars) (.args call))))

  Expr$Get
  (plan-expr [this] (list '. (plan-expr (.expr this)) (keyword (.field this)))) ;;keywords are safer than symbols in the RA plan
  (required-vars [this] (required-vars (.expr this)))

  Expr$Exists
  (plan-expr [this]
    (when-not *subqueries*
      (throw (UnsupportedOperationException. "TODO subqueries not bound, subquery not allowed in expr here")))
    (let [placeholder (expr-subquery-placeholder)
          {:keys [ra-plan provided-vars]} (plan-query (.query this))]
      (swap! *subqueries* conj
             (merge {:type :exists
                     :placeholder placeholder
                     :provided-vars provided-vars
                     :subquery ra-plan}
                    (plan-arg-bindings (.args this))))
      placeholder))
  (required-vars [this] (expr-subquery-required-vars (.args this)))

  Expr$Subquery
  (plan-expr [this]
    (when-not *subqueries*
      (throw (UnsupportedOperationException. "TODO subqueries not bound, subquery not allowed in expr here")))
    (let [placeholder (expr-subquery-placeholder)
          {:keys [ra-plan provided-vars]} (plan-query (.query this))]
      (when-not (= (count provided-vars) 1)
        (throw (err/incorrect :xtql/invalid-scalar-subquery
                              "Scalar subquery must only return a single column"
                              {:subquery (str (.query this)), :provided-vars provided-vars})))
      (swap! *subqueries* conj
             (merge {:type :scalar
                     :placeholder placeholder
                     :provided-vars provided-vars
                     :subquery [:project {:projections [{placeholder (first provided-vars)}]}
                                ra-plan]}
                    (plan-arg-bindings (.args this))))
      placeholder))
  (required-vars [this] (expr-subquery-required-vars (.args this)))

  Expr$Pull
  (plan-expr [this]
    (when-not *subqueries*
      (throw (UnsupportedOperationException. "TODO subqueries not bound, subquery not allowed in expr here")))
    (let [placeholder (expr-subquery-placeholder)
          {:keys [ra-plan provided-vars]} (plan-query (.query this))]
      (swap! *subqueries* conj
             (merge {:type :scalar
                     :placeholder placeholder
                     :provided-vars provided-vars
                     :subquery [:project {:projections [{placeholder '*}]}
                                ra-plan]}
                    (plan-arg-bindings (.args this))))
      placeholder))
  (required-vars [this] (expr-subquery-required-vars (.args this)))

  Expr$PullMany
  (plan-expr [this]
    (when-not *subqueries*
      (throw (UnsupportedOperationException. "TODO subqueries not bound, subquery not allowed in expr here")))
    (let [placeholder (expr-subquery-placeholder)
          struct-col (expr-subquery-placeholder)
          {:keys [ra-plan provided-vars]} (plan-query (.query this))]
      (swap! *subqueries* conj
             (merge {:type :scalar
                     :placeholder placeholder
                     :provided-vars provided-vars
                     :subquery [:group-by {:columns [{placeholder (list 'array-agg struct-col)}]}
                                [:project {:projections [{struct-col '*}]}
                                 ra-plan]]}
                    (plan-arg-bindings (.args this))))
      placeholder))
  (required-vars [this] (expr-subquery-required-vars (.args this)))

  nil
  (plan-expr [_] nil)
  (required-vars [_] nil))

(defn plan-expr-with-subqueries [expr]
  (binding [*subqueries* (atom [])
            *agg-fns* (atom [])]
    (let [planned-expr (plan-expr expr)]
      {:expr planned-expr
       :subqueries @*subqueries*
       :agg-fns @*agg-fns*})))

(defn- required-vars-available? [expr provided-vars]
  (let [required-vars (required-vars expr)]
    (when (not (set/subset? required-vars provided-vars))
      (throw (err/incorrect :xtql/invalid-expression
                            "Not all variables in expression are in scope"
                            {:expr (str expr), :required-vars required-vars, :available-vars provided-vars})))))

(defn- wrap-select [ra-plan predicates]
  (case (count predicates)
    0 ra-plan
    1 [:select {:predicate (first predicates)} ra-plan]
    [:select {:predicate (list* 'and predicates)} ra-plan]))

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
               (map #(list* '== %)))))
       (vec)))

(defn- wrap-unify [{:keys [ra-plan]} var->cols]
  ;; wrap-unify doesn't depend on provided/required vars it will
  ;; return provided-vars based on var->cols
  {:ra-plan [:project {:projections (vec (for [[lv cols] var->cols]
                                           (or (cols lv)
                                               {(col-sym lv) (first cols)})))}
             (-> ra-plan
                 (wrap-select (unify-preds var->cols)))]

   :provided-vars (set (keys var->cols))})

(defn- with-unique-cols [plans]
  (as-> plans plans
    (->> plans
         (into [] (map-indexed
                   (fn [idx {:keys [provided-vars ra-plan]}]
                     (let [var->col (->> provided-vars
                                         (into {} (map (juxt col-sym (if (= idx 0) col-sym (partial col-sym (str "_r" idx)))))))]
                       ;;by not prefixing the leftmost rels columns, apply params need not be rewritten in the case its an apply binary join.
                       {:ra-plan [:rename {:columns var->col}
                                  ra-plan]
                        :provided-vars (set (vals var->col))
                        :var->col var->col})))))
    {:rels plans
     :var->cols (-> plans
                    (->> (mapcat :var->col)
                         (group-by key))
                    (update-vals #(into #{} (map val) %)))}))

(defn- mega-join [plans]
  (let [{:keys [rels var->cols]} (with-unique-cols plans)]
    (-> (case (count rels)
          0 {:ra-plan [:table {:rows [{}]}]}
          1 (first rels)
          {:ra-plan [:mega-join {:conditions []} (mapv :ra-plan rels)]})
        (wrap-unify var->cols))))

(defn- plan-out-spec [^Binding bind-spec]
  (let [col (col-sym (.getBinding bind-spec))
        expr (.getExpr bind-spec)]
    {:l col :r (plan-expr expr) :literal? (not (instance? Expr$LogicVar expr))}))
;;TODO defining literal as not an LV seems flakey, but might be okay?
;;this seems like the kind of thing the AST should encode as a type/interface?

(defn- plan-arg-spec [^Binding bind-spec]
  ;;TODO expr here is far to permissive.
  ;;In the outer query this has to be a literal
  ;;in the subquery case it must be a col, as thats all apply supports
  ;;In reality we could support a full expr here, additionally top level query args perhaps should
  ;;use a different spec. Delaying decision here for now.
  (let [var (col-sym (.getBinding bind-spec))
        expr (.getExpr bind-spec)]
    {:l var :r (plan-expr-with-subqueries expr) :required-vars (required-vars expr)}))

(def app-time-period-sym '_valid_time)
(def app-time-from-sym '_valid_from)
(def app-time-to-sym '_valid_to)
(def app-temporal-cols {:period app-time-period-sym
                        :from app-time-from-sym
                        :to app-time-to-sym})


(def system-time-period-sym '_system_time)
(def system-time-from-sym '_system_from)
(def system-time-to-sym '_system_to)
(def sys-temporal-cols {:period system-time-period-sym
                        :from system-time-from-sym
                        :to system-time-to-sym})

(defn replace-temporal-period-with-cols
  [cols]
  (mapcat
   #(cond
      (= app-time-period-sym %)
      [app-time-from-sym app-time-to-sym]
      (= system-time-period-sym %)
      [system-time-from-sym system-time-to-sym]
      :else
      [%])
   cols))

(extend-protocol PlanTemporalFilter
  TemporalFilter$AllTime
  (plan-temporal-filter [_this]
    :all-time)

  TemporalFilter$At
  (plan-temporal-filter [this]
    ;;TODO could be better to have its own error, to make it clear you can't
    ;;ref logic vars in temporal opts
    (required-vars-available? (.getAt this) #{})
    [:at (or (plan-expr (.getAt this)) :now)])

  TemporalFilter$In
  (plan-temporal-filter [this]
    (required-vars-available? (.getFrom this) #{})
    (required-vars-available? (.getTo this) #{})
    [:in (or (plan-expr (.getFrom this)) 'xtdb/start-of-time) (plan-expr (.getTo this))])

  nil
  (plan-temporal-filter [_this]
    nil))

(defn create-period-constructor [bindings {:keys [period from to]}]
  (when-let [{:keys [r literal?]} (first (filter #(= period (:l %)) bindings))]
    (if literal?
      {:type :selection
       :expr (list '== (list 'period (col-sym from) (col-sym to)) r)}
      {:type :projection
       :expr {(col-sym period) (list 'period (col-sym from) (col-sym to))}})))

(defn- wrap-map [plan projections]
  ;;currently callers job to handle updating :provided-vars etc. as this returns unested plan
  ;;decicsion based off wrap-select and current caller
  (if (seq projections)
    [:map {:projections (vec projections)}
     plan]
    plan))

(defn wrap-with-period-projection-or-selection [plan bindings]
  (let [{:keys [selection projection]}
        (group-by
         :type
         (keep
          #(create-period-constructor bindings %)
          [app-temporal-cols sys-temporal-cols]))]
    (-> plan
        (wrap-select (map :expr selection))
        (wrap-map (map :expr projection)))))

(defn wrap-with-ra-plan [unnested-plan]
  {:ra-plan unnested-plan})

(defn- wrap-scan-col-preds [scan-col col-preds]
  (case (count col-preds)
    0 scan-col
    1 {scan-col (first col-preds)}
    {scan-col (list* 'and col-preds)}))

(defn- plan-from [{:keys [table for-valid-time for-system-time bindings project-all-cols?]}]
  (let [planned-bind-specs (concat (cond-> (mapv plan-out-spec bindings)
                                     project-all-cols?
                                     (concat (->> (get *table-info* table)
                                                  (mapv symbol)
                                                  (mapv #(hash-map :l % :r %))))))
        distinct-scan-cols (distinct (replace-temporal-period-with-cols (mapv :l planned-bind-specs)))
        literal-preds-by-col (-> (->> planned-bind-specs
                                      (filter :literal?)
                                      (map #(assoc % :pred (list '== (:l %) (:r %))))
                                      (group-by :l))
                                 (update-vals #(map :pred %)))]
    (-> [:scan {:table table
                :columns (mapv #(wrap-scan-col-preds % (get literal-preds-by-col %)) distinct-scan-cols)
                :for-valid-time (plan-temporal-filter for-valid-time)
                :for-system-time (plan-temporal-filter for-system-time)}]
        (wrap-with-period-projection-or-selection planned-bind-specs)
        (wrap-with-ra-plan)
        (wrap-unify (-> planned-bind-specs
                        (->> (remove :literal?)
                             (group-by :r))
                        (update-vals (comp set #(mapv :l %))))))))

(defn- plan-where [{:keys [preds]}]
  (for [pred preds
        :let [{:keys [expr subqueries]} (plan-expr-with-subqueries pred)]]
    {:expr expr
     :subqueries subqueries
     :required-vars (required-vars pred)}))

(defn wrap-out-binding-projection [{:keys [ra-plan _provided-vars]} out-bindings]
  ;;TODO check subquery provided vars line up with bindings (although maybe this isn't an error?)
  [:project {:projections (mapv (fn [{:keys [l r]}] {r l}) out-bindings)}
   ra-plan])

(defn- plan-rel [table-opts out-bindings]
  (when (some :literal? out-bindings)
    (throw (UnsupportedOperationException. "TODO what should literals in out specs do outside of scan")))
  {:ra-plan (wrap-out-binding-projection {:ra-plan [:table table-opts]} out-bindings)
   :provided-vars (set (map :r out-bindings))})

(extend-protocol PlanQuery
  QueryWithParams (plan-query [{:keys [query]}] (plan-query query))

  From (plan-query [from] (plan-from from))

  Pipeline
  (plan-query [{:keys [head tails]}]
    (reduce (fn [plan query-tail]
              (plan-query-tail query-tail plan))
            (plan-query head)
            tails))

  DocsRelation
  (plan-query [{:keys [documents bindings]}]
    (plan-rel {:rows (mapv #(into {} (map (fn [[k v]] (MapEntry/create (util/kw->normal-form-kw (keyword k)) (plan-expr v)))) %)
                           documents)}
              (mapv plan-out-spec bindings)))

  ParamRelation
  (plan-query [{:keys [param bindings]}]
    (plan-rel {:param (plan-expr param)} (mapv plan-out-spec bindings))))

(declare wrap-expr-subqueries*)

(defn- wrap-expr-subquery [plan {:keys [type placeholder subquery arg-subqueries
                                        tmp-expr-sym->expr-vec tmp-expr-sym->apply-param-sym]}]
  (case type
    :scalar
    (if (seq tmp-expr-sym->expr-vec)
      [:apply {:mode :single-join, :columns tmp-expr-sym->apply-param-sym}
       [:map {:projections tmp-expr-sym->expr-vec}
        (wrap-expr-subqueries* plan arg-subqueries)]
       subquery]
      [:single-join {:conditions [true]}
       plan
       subquery])
    :exists
    (if (seq tmp-expr-sym->expr-vec)
      [:apply {:mode :mark-join, :columns tmp-expr-sym->apply-param-sym, :mark-join-projection {placeholder true}}
       [:map {:projections tmp-expr-sym->expr-vec}
        (wrap-expr-subqueries* plan arg-subqueries)]
       subquery]
      [:mark-join {:mark-spec {placeholder [true]}}
       plan
       subquery])))

(defn- wrap-expr-subqueries* [plan subqueries]
  (->> subqueries
       (reduce (fn [plan sq]
                 (wrap-expr-subquery plan sq))
               plan)))

(defn- wrap-project [plan projection]
  [:project {:projections projection} plan])

(defn- wrap-expr-subqueries [plan provided-vars subqueries]
  (-> (wrap-expr-subqueries* plan subqueries)
      ;; here the provided vars are the ones from the plan + the placeholder from the subquery
      (wrap-project (into provided-vars (map :placeholder subqueries)))))

(defn- plan-col-spec [^Binding col-spec provided-vars]
  (let [col (col-sym (.getBinding col-spec))
        spec-expr (.getExpr col-spec)
        _ (required-vars-available? spec-expr provided-vars)
        required-vars (required-vars spec-expr)
        {:keys [subqueries expr agg-fns]} (plan-expr-with-subqueries spec-expr)]
    {:l col, :r expr
     :subqueries subqueries
     :agg-fns agg-fns
     :required-vars required-vars
     :logic-var? (instance? Expr$LogicVar spec-expr)}))

(defn- plan-var-spec [^Binding spec]
  (let [var (col-sym (.getBinding spec))
        spec-expr (.getExpr spec)
        {:keys [subqueries expr]} (plan-expr-with-subqueries spec-expr)]
    {:l var, :r expr, :required-vars (required-vars spec-expr)
     :subqueries subqueries :logic-var? (instance? Expr$LogicVar spec-expr)}))

(extend-protocol PlanQueryTail
  Where
  (plan-query-tail [{:keys [preds] :as this} {:keys [ra-plan provided-vars] :as _acc-plan}]
    (doseq [pred preds]
      (required-vars-available? pred provided-vars))

    (let [planned-where-exprs (plan-where this)]
      {:ra-plan (-> ra-plan
                    (wrap-expr-subqueries provided-vars (mapcat :subqueries planned-where-exprs))
                    (wrap-select (map :expr planned-where-exprs))
                    (wrap-project (vec provided-vars)))
       :provided-vars provided-vars}))

  With
  (plan-query-tail [{:keys [with-bindings]} {:keys [ra-plan provided-vars]}]
    (let [projections (map #(plan-col-spec % provided-vars) with-bindings)
          return-vars (set/union
                       provided-vars
                       (set (map :l projections)))]
      {:ra-plan [:project {:projections (vec return-vars)}
                 [:map {:projections (mapv (fn [{:keys [l r]}] {l r}) projections)}
                  (-> ra-plan
                      (wrap-expr-subqueries provided-vars (mapcat :subqueries projections)))]]
       :provided-vars return-vars}))

  Without
  (plan-query-tail [{:keys [without-cols]} {:keys [ra-plan provided-vars]}]
    (let [cols-to-be-removed (into #{} (map col-sym) without-cols)
          output-projection (set/difference provided-vars cols-to-be-removed)]
      {:ra-plan [:project {:projections (vec output-projection)} ra-plan]
       :provided-vars output-projection}))

  Return
  (plan-query-tail [{:keys [return-bindings]} {:keys [ra-plan provided-vars]}]
    (let [projections (->> return-bindings
                           (mapv (fn [col]
                                   (let [expr (.getExpr ^Binding col)]
                                     (required-vars-available? expr provided-vars)
                                     {(col-sym (.getBinding ^Binding col)) (plan-expr expr)}))))]
      {:ra-plan [:project {:projections projections} ra-plan]
       :provided-vars (set (map #(first (keys %)) projections))}))

  Unnest
  (plan-query-tail [{:keys [unnest-binding]} {:keys [ra-plan provided-vars]}]
    (let [{:keys [l r subqueries]} (plan-col-spec unnest-binding provided-vars)
          pre-col (gen-col)
          return-vars (conj provided-vars l)]
      {:ra-plan [:project {:projections (vec return-vars)}
                 [:unnest {:columns {l pre-col}}
                  [:map {:projections [{pre-col r}]}
                   (wrap-expr-subqueries ra-plan provided-vars subqueries)]]]
       :provided-vars return-vars})))

(defn plan-join [join-type query args binding]
  (let [out-bindings (mapv plan-out-spec binding) ;;TODO refelection (interface here?)
        arg-bindings (mapv plan-arg-spec args)
        subquery (plan-query query)
        provided-vars (set (map :r out-bindings))]
    (when (some :literal? out-bindings)
      (throw (UnsupportedOperationException. "TODO what should literals in out specs do outside of scan")))
    [[:join (merge {:ra-plan (wrap-out-binding-projection subquery out-bindings)
                    :join-type join-type
                    :provided-vars provided-vars
                    :required-vars (apply set/union (map :required-vars arg-bindings))}
                   (plan-arg-bindings args))]]))

(extend-protocol PlanUnifyClause
  From
  (plan-unify-clause [{:keys [project-all-cols?] :as from}]
    (when project-all-cols?
      (throw (err/incorrect :xtql/invalid-from
                            "* is not a valid in from when inside a unify context"
                            {:from (str from)})))
    [[:from (plan-from from)]])

  DocsRelation
  (plan-unify-clause [{:keys [documents bindings]}]
    [[:from (plan-rel {:rows (mapv #(into {} (map (fn [[k v]] (MapEntry/create (util/kw->normal-form-kw (keyword k)) (plan-expr v)))) %)
                                   documents)}
                      (mapv plan-out-spec bindings))]])

  ParamRelation
  (plan-unify-clause [{:keys [param bindings]}]
    [[:from (plan-rel {:param (plan-expr param)}
                      (mapv plan-out-spec bindings))]])

  Where
  (plan-unify-clause [where]
    (for [planned-where-exprs (plan-where where)]
      [:where planned-where-exprs]))

  With
  (plan-unify-clause [{:keys [with-bindings]}]
    (for [binding with-bindings
          :let [{:keys [l r required-vars subqueries]} (plan-var-spec binding)]]
      [:with {:expr r
              :provided-vars #{l}
              :required-vars required-vars
              :subqueries subqueries}]))

  Unnest
  (plan-unify-clause [{:keys [unnest-binding]}]
    (let [{:keys [l r subqueries required-vars]} (plan-var-spec unnest-binding)]
      [[:unnest {:expr r
                 :required-vars required-vars
                 :provided-vars #{l}
                 :subqueries subqueries}]]))

  Join
  (plan-unify-clause [{:keys [join-query args bindings]}]
    (plan-join :inner-join join-query args bindings))

  LeftJoin
  (plan-unify-clause [{:keys [left-join-query args bindings]}]
    (plan-join :left-outer-join left-join-query args bindings)))

(defn wrap-wheres [{:keys [ra-plan provided-vars]} wheres]
  {:ra-plan (-> ra-plan
                (wrap-expr-subqueries provided-vars (mapcat :subqueries wheres))
                (wrap-select (map :expr wheres))
                (wrap-project (vec provided-vars)))
   :provided-vars provided-vars})

(defn wrap-withs [{:keys [ra-plan provided-vars]} withs]
  (let [renamed-withs (->> withs
                           (into [] (map-indexed
                                     (fn [idx with]
                                       (assoc
                                        with
                                        :renamed-provided-var
                                        (col-sym (str "_c" idx) (first (:provided-vars with))))))))

        var->cols (-> (concat (map (juxt identity identity) provided-vars)
                              (->> renamed-withs (map (juxt (comp first :provided-vars) :renamed-provided-var))))
                      (->> (group-by first))
                      (update-vals #(into #{} (map second) %)))]

    (-> {:ra-plan [:map {:projections (vec (for [{:keys [expr renamed-provided-var]} renamed-withs]
                                         {renamed-provided-var expr}))}
                   (-> ra-plan
                       (wrap-expr-subqueries provided-vars (mapcat :subqueries renamed-withs)))]}
        (wrap-unify var->cols))))

(defn wrap-unnest [{acc-provided-vars :provided-vars :as acc-plan} {:keys [provided-vars expr subqueries] :as _unnest}]
  (let [{:keys [rels var->cols]} (with-unique-cols [acc-plan]) ;;doesn't rename any cols, but creates var->cols
        [{acc-plan :ra-plan}] rels
        original-unnested-col (first provided-vars)
        pre-col (gen-col)
        unnested-col (col-sym (*gensym* original-unnested-col))]
    (wrap-unify
     {:ra-plan
      [:unnest {:columns {unnested-col pre-col}}
       [:map {:projections [{pre-col expr}]}
        (wrap-expr-subqueries acc-plan acc-provided-vars subqueries)]]}
     (update var->cols original-unnested-col (fnil conj #{}) unnested-col))))

(defn wrap-unnests [acc-plan unnests]
  (reduce wrap-unnest acc-plan unnests))

(defn wrap-inner-join [{acc-provided-vars :provided-vars :as acc-plan}
                       {:keys [arg-subqueries tmp-expr-sym->apply-param-sym tmp-expr-sym->expr-vec] :as join-plan}]
  (if (seq tmp-expr-sym->expr-vec)
    (let [{:keys [rels var->cols]} (with-unique-cols [acc-plan join-plan])
          [{acc-plan-with-unique-cols :ra-plan}
           {join-subquery-plan-with-unique-cols :ra-plan}] rels]
      (wrap-unify
       {:ra-plan [:apply {:mode :cross-join, :columns tmp-expr-sym->apply-param-sym}
                  [:map {:projections tmp-expr-sym->expr-vec}
                   (wrap-expr-subqueries acc-plan-with-unique-cols acc-provided-vars arg-subqueries)]
                  join-subquery-plan-with-unique-cols]}
       var->cols))
    (mega-join [acc-plan join-plan])))

(defn wrap-left-join [{acc-provided-vars :provided-vars :as acc-plan}
                      {:keys [provided-vars arg-subqueries tmp-expr-sym->apply-param-sym
                              tmp-expr-sym->expr-vec ra-plan] :as _join-plan}]
  ;; the join clause or select placed on the dependant side
  ;; is what provides the unification of the :bind vars with the outer plan
  (let [unifying-vars (->> provided-vars (filter acc-provided-vars))]
    {:ra-plan
     (if (seq tmp-expr-sym->expr-vec)
       (let [unifying-vars-apply-param-mapping (unifying-vars->apply-param-mapping unifying-vars)]
         (->
          [:apply {:mode :left-outer-join
                   :columns (merge
                             tmp-expr-sym->apply-param-sym
                             unifying-vars-apply-param-mapping)}
           [:map {:projections tmp-expr-sym->expr-vec}
            (wrap-expr-subqueries (:ra-plan acc-plan) acc-provided-vars arg-subqueries)]
           (->> unifying-vars-apply-param-mapping
                (map (fn [[var param-for-var]]
                       (list '== var param-for-var)))
                (wrap-select ra-plan))]
          (wrap-project (vec (into acc-provided-vars provided-vars)))))
       [:left-outer-join
        {:conditions (->> unifying-vars
                          (mapv (fn [v] {v v})))}
        (:ra-plan acc-plan)
        ra-plan])
     :provided-vars (set/union acc-provided-vars provided-vars)}))

(defn- wrap-joins [plan joins]
  (->> joins
       (reduce
        (fn [acc-plan {:keys [join-type] :as join-plan}]
          (if (= join-type :inner-join)
            (wrap-inner-join acc-plan join-plan)
            (wrap-left-join acc-plan join-plan)))
        plan)))

(extend-protocol PlanQuery
  Unify
  (plan-query [{:keys [unify-clauses]}]
    ;;TODO not all clauses can return entire plans (e.g. where-clauses),
    ;;they require an extra call to wrap should these still use the :ra-plan key.
    (let [{from-clauses :from, where-clauses :where with-clauses :with join-clauses :join
           unnests :unnest}
          (-> (mapcat plan-unify-clause unify-clauses)
              (->> (group-by first))
              (update-vals #(mapv second %)))]

      ;;TODO ideally plan should not start with an explicit mega-join of only from-clauses.
      ;;other relation producing clauses such as with could be included in the base mega-join
      ;;instead of at the next level if they have no required-vars
      ;;
      ;; Also may be better if this loop handles unification and inserting mega-joins where nececsary
      ;; rather than relying on each clause "wrapper" to do that.

      (loop [plan (mega-join from-clauses)
             wheres where-clauses
             withs with-clauses
             joins join-clauses
             unnests unnests]

        (if (and (empty? wheres) (empty? withs) (empty? joins) (empty? unnests))
          plan

          (let [available-vars (:provided-vars plan)]
            (letfn [(available? [clause]
                      (set/superset? available-vars (:required-vars clause)))]

              (let [{available-wheres true, unavailable-wheres false} (->> wheres (group-by available?))
                    {available-withs true, unavailable-withs false} (->> withs (group-by available?))
                    {available-joins true, unavailable-joins false} (->> joins (group-by available?))
                    {available-unnests true, unavailable-unnests false} (->> unnests (group-by available?))]

                (if (and (empty? available-wheres) (empty? available-withs) (empty? available-joins) (empty? available-unnests))
                  (throw (err/incorrect :no-available-clauses "No available clauses"
                                        {:available-vars available-vars
                                         :unavailable-wheres unavailable-wheres
                                         :unavailable-withs unavailable-withs
                                         :unavailable-joins unavailable-joins
                                         :unavailable-unnests unavailable-unnests}))

                  (recur (cond-> plan
                           available-wheres (wrap-wheres available-wheres)
                           available-withs (wrap-withs available-withs)
                           available-joins (wrap-joins available-joins)
                           available-unnests (wrap-unnests available-unnests))
                         unavailable-wheres
                         unavailable-withs
                         unavailable-joins
                         unavailable-unnests))))))))))

(defn- plan-order-spec [{:keys [expr direction nulls]}]
  {:order-spec [(if (instance? Expr$LogicVar expr)
                  (col-sym (.lv ^Expr$LogicVar expr))
                  (throw (UnsupportedOperationException. "TODO order-by spec can only take a column as val")))

                (cond-> {}
                  direction (assoc :direction (case direction
                                                :desc :desc
                                                :asc))
                  nulls (assoc :null-ordering (case nulls
                                                :last :nulls-last
                                                :nulls-first)))]})

(extend-protocol PlanQueryTail
  OrderBy
  (plan-query-tail [{:keys [order-specs]} {:keys [ra-plan provided-vars]}]
    ;;TODO Change order specs to use keywords
    (let [planned-specs (mapv plan-order-spec order-specs)]
      {:ra-plan [:order-by {:order-specs (mapv :order-spec planned-specs)}
                 ra-plan]
       :provided-vars provided-vars}))

  Aggregate
  (plan-query-tail [{:keys [agg-bindings]} {:keys [ra-plan provided-vars]}]
    (let [planned-specs (map #(plan-col-spec % provided-vars) agg-bindings)
          groups (group-by (comp boolean seq :agg-fns) planned-specs)

          aggregate-specs (mapcat (fn [{:keys [agg-fns]}]
                                    (for [{:keys [placeholder sub-expr agg-fn]} agg-fns
                                          :let [planned-sub-exprs
                                                (for [sub-e sub-expr]
                                                  (let [_ (required-vars-available? sub-e provided-vars)
                                                        {:keys [expr agg-fns subqueries]} (plan-expr-with-subqueries sub-e)]
                                                    (when (seq subqueries)
                                                      (throw
                                                       (UnsupportedOperationException. "TODO: Add support for subqueries in aggr expr")))
                                                    (when (seq agg-fns)
                                                      (throw (err/incorrect :xtql/invalid-expression
                                                                            "Aggregate functions cannot be nested"
                                                                            {:expr (str sub-expr)})))
                                                    {:sub-expr-placeholder (expr-subquery-placeholder)
                                                     :expr expr}))]]
                                      {:sub-exprs planned-sub-exprs
                                       :agg-fn (list* agg-fn (map :sub-expr-placeholder planned-sub-exprs))
                                       :placeholder placeholder}))
                                  (get groups true))
          grouping-cols (keep #(when (:logic-var? %) (:r %)) (get groups false))
          output-projections (for [{:keys [l r subqueries]} planned-specs]
                               (do (when (seq subqueries)
                                     (throw
                                      (UnsupportedOperationException. "TODO: Add support for subqueries in aggr output expr")))
                                   {l r}))
          grouping-cols-set (set grouping-cols)]

      (doseq [{:keys [required-vars expr]} planned-specs]
        (when-not (set/subset? required-vars grouping-cols-set)
          (throw (err/incorrect :xtql/invalid-expression
                                "Variables outside of aggregate expressions must be grouping columns"
                                {:expr (str expr), :required-vars required-vars, :grouping-cols grouping-cols-set}))))

      {:ra-plan  [:project {:projections (vec output-projections)}
                  [:group-by
                   {:columns (vec (concat
                                   grouping-cols
                                   (mapv (fn [{:keys [agg-fn placeholder]}]
                                           {placeholder agg-fn})
                                         aggregate-specs)))}
                   (if-let [sub-expr-projections ;;Handles agg-fns with no-sub-exprs such as row-count
                            (seq (mapcat #(for [{:keys [sub-expr-placeholder expr]} (:sub-exprs %)]
                                            {sub-expr-placeholder expr})
                                         aggregate-specs))]
                     [:map {:projections (vec sub-expr-projections)}
                      ra-plan]
                     ra-plan)]]
       :provided-vars (set (map :l planned-specs))}))

  Limit
  (plan-query-tail [{:keys [limit]} {:keys [ra-plan provided-vars]}]
    (when-not (or (instance? Expr$Long limit)
                  (instance? Expr$Param limit))
      (throw (err/incorrect
              :xtql/invalid-limit
              "Limit must be a non-negative integer literal or parameter"
              {:limit (str limit)})))
    {:ra-plan [:top {:limit (plan-expr limit)} ra-plan]
     :provided-vars provided-vars})

  Offset
  (plan-query-tail [{:keys [offset]} {:keys [ra-plan provided-vars]}]
    (when-not (or (instance? Expr$Long offset)
                  (instance? Expr$Param offset))
      (throw (err/incorrect
              :xtql/invalid-offset
              "Offset must be a non-negative integer literal or parameter"
              {:offset (str offset)})))
    {:ra-plan [:top {:skip (plan-expr offset)} ra-plan]
     :provided-vars provided-vars}))

(defn compile-query [query {:keys [table-info]}]
  (binding [*gensym* (util/seeded-gensym "_" 0)
            *table-info* table-info]
    (plan-query query)))

