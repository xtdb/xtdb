(ns xtdb.xtql
  (:require [clojure.set :as set]
            [xtdb.error :as err]
            [xtdb.logical-plan :as lp]
            [xtdb.operator :as op]
            [xtdb.util :as util]
            [xtdb.vector.writer :as vw]
            [xtdb.xtql.edn :as xt.edn])
  (:import (clojure.lang MapEntry)
           (org.apache.arrow.memory BufferAllocator)
           (xtdb.operator IRaQuerySource)
           (xtdb.query ArgSpec ColSpec DmlOps$AssertExists DmlOps$AssertNotExists DmlOps$Delete DmlOps$Erase DmlOps$Insert DmlOps$Update
                       Expr Expr$Bool Expr$Call Expr$Double Expr$LogicVar Expr$Long Expr$Obj Expr$Param OutSpec
                       Expr$Subquery Expr$Exists Expr$Pull Expr$PullMany Expr$Get
                       Query Query$Aggregate Query$From Query$Join Query$LeftJoin Query$Limit Query$Offset
                       Query$OrderBy Query$OrderDirection Query$OrderNulls Query$OrderSpec
                       Query$Pipeline Query$Return Query$Unify
                       Query$Where Query$With Query$WithCols Query$Without Query$DocsTable Query$ParamTable
                       Query$UnnestCol Query$UnnestVar
                       TemporalFilter$AllTime TemporalFilter$At TemporalFilter$In TemporalFilter$TemporalExtents VarSpec)))

;;TODO consider helper for [{sym expr} sym] -> provided vars set
;;TODO Should all user supplied lv be planned via plan-expr, rather than explicit calls to col-sym.
;;keeps the conversion to java AST to -> clojure sym in one place.
;;TODO Document var->cols purpose, and/or give it a more descriptive name


(defprotocol PlanQuery
  (plan-query [query]))

(defprotocol PlanQueryTail
  (plan-query-tail [query-tail plan]))

(defprotocol PlanUnifyClause
  (plan-unify-clause [clause]))

(defprotocol PlanTemporalFilter
  (plan-temporal-filter [temporal-filter]))

(defprotocol PlanDml
  (plan-dml [query tx-opts]))

(def ^:dynamic *gensym* gensym)

(defn seeded-gensym
  ([] (seeded-gensym "" 0))
  ([count-start] (seeded-gensym "" count-start))
  ([suffix count-start]
   (let [ctr (atom (dec count-start))]
     (fn gensym-seed
       ([] (symbol (str "gensym" suffix (swap! ctr inc))))
       ([prefix] (symbol (str prefix suffix (swap! ctr inc))))))))

(defprotocol ExprPlan
  (plan-expr [expr])
  (required-vars [expr]))

(defn- col-sym
  ([col]
   (-> (symbol col)
       util/symbol->normal-form-symbol
       (vary-meta assoc :column? true)))
  ([prefix col]
   (col-sym (str (format "%s_%s" prefix col)))))

(defn- param-sym [v]
  (-> (symbol (str "?" v))
      util/symbol->normal-form-symbol
      (with-meta {:param? true})))

(defn- apply-param-sym
  ([v]
   (apply-param-sym nil v))
  ([v prefix]
   (-> (symbol (str "?" prefix v))
       util/symbol->normal-form-symbol
       (with-meta {:correlated-column? true}))))

(defn- args->apply-params [args]
  (->> args
       (mapv (fn [{:keys [l r _required-vars]}]
               (MapEntry/create (apply-param-sym l) r)))
       (into {})
       (set/map-invert)))

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

(declare plan-arg-spec)

(def ^:dynamic *subqueries* nil)

(defn expr-subquery-required-vars [subquery-args]
  ;; planning arg-specs here to get required vars, could call required-vars on the exprs
  ;; directly, but wanted to avoid duplicating that code.
  (apply set/union (map (comp :required-vars plan-arg-spec) subquery-args)))

(extend-protocol ExprPlan
  Expr$LogicVar
  (plan-expr [this] (col-sym (.lv this)))
  (required-vars [this] #{(col-sym (.lv this))})

  Expr$Param ;;TODO need to differentiate between query params and subquery/apply params
  (plan-expr [this] (param-sym (subs (.v this) 1)))
  (required-vars [_this] #{})

  Expr$Obj
  (plan-expr [o]
    (let [obj (.obj o)]
      (cond (vector? obj) (into [] (map plan-expr) obj)
            (set? obj) (into #{} (map plan-expr) obj)
            (map? obj) (into {} (map (juxt (comp util/kw->normal-form-kw key) (comp plan-expr val))) obj)
            :else obj)))
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
  (plan-expr [call] (list* (symbol (.f call)) (mapv plan-expr (.args call))))
  (required-vars [call] (into #{} (mapcat required-vars) (.args call)))

  Expr$Get
  (plan-expr [this] (list '. (plan-expr (.expr this)) (keyword (.field this)))) ;;keywords are safer than symbols in the RA plan
  (required-vars [this] (required-vars (.expr this)))

  Expr$Exists
  (plan-expr [this]
    (when-not *subqueries*
      (throw (UnsupportedOperationException. "TODO subqueries not bound, subquery not allowed in expr here")))
    (let [placeholder (expr-subquery-placeholder)]
      (swap! *subqueries* conj
             {:type :exists
              :placeholder placeholder
              :subquery (plan-query (.query this))
              :args (mapv plan-arg-spec (.args this))})
      placeholder))
  (required-vars [this] (expr-subquery-required-vars (.args this)))
  Expr$Subquery
  (plan-expr [this]
    (when-not *subqueries*
      (throw (UnsupportedOperationException. "TODO subqueries not bound, subquery not allowed in expr here")))
    (let [placeholder (expr-subquery-placeholder)
          {:keys [ra-plan provided-vars]} (plan-query (.query this))]
      (when-not (= (count provided-vars) 1)
        (throw (err/illegal-arg
                :xtql/invalid-scalar-subquery
                {:subquery (.query this) :provided-vars provided-vars
                 ::err/message "Scalar subquery must only return a single column"})))
      (swap! *subqueries* conj
             {:type :scalar
              :subquery [:project [{placeholder (first provided-vars)}]
                         ra-plan]
              :args (mapv plan-arg-spec (.args this))})
      placeholder))
  (required-vars [this] (expr-subquery-required-vars (.args this)))
  Expr$Pull
  (plan-expr [this]
    (when-not *subqueries*
      (throw (UnsupportedOperationException. "TODO subqueries not bound, subquery not allowed in expr here")))
    (let [placeholder (expr-subquery-placeholder)]
      (swap! *subqueries* conj
             {:type :scalar
              :subquery [:project [{placeholder '*}]
                         (:ra-plan (plan-query (.query this)))]
              :args (mapv plan-arg-spec (.args this))})
      placeholder))
  (required-vars [this] (expr-subquery-required-vars (.args this)))
  Expr$PullMany
  (plan-expr [this]
    (when-not *subqueries*
      (throw (UnsupportedOperationException. "TODO subqueries not bound, subquery not allowed in expr here")))
    (let [placeholder (expr-subquery-placeholder)
          struct-col (expr-subquery-placeholder)]
      (swap! *subqueries* conj
             {:type :scalar
              :subquery [:group-by [{placeholder (list 'array-agg struct-col)}]
                         [:project [{struct-col '*}]
                          (:ra-plan (plan-query (.query this)))]]
              :args (mapv plan-arg-spec (.args this))})
      placeholder))
  (required-vars [this] (expr-subquery-required-vars (.args this)))
  nil
  (plan-expr [_] nil)
  (required-vars [_] nil))

(defn plan-expr-with-subqueries [expr]
  (binding [*subqueries* (atom [])]
    (let [planned-expr (plan-expr expr)]
      {:expr planned-expr
       :subqueries @*subqueries*})))

(defn- required-vars-available? [expr provided-vars]
  (let [required-vars (required-vars expr)]
    (when (not (set/subset? required-vars provided-vars))
      (throw (err/illegal-arg
              :xtql/invalid-expression
              {:expr expr :required-vars required-vars :available-vars provided-vars
               ::err/message "Not all variables in expression are in scope"})))))

(defn- wrap-select [ra-plan predicates]
  (case (count predicates)
    0 ra-plan
    1 [:select (first predicates) ra-plan]
    [:select (list* 'and predicates) ra-plan]))

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

(defn- wrap-unify [{:keys [ra-plan]} var->cols]
  ;; wrap-unify doesn't depend on provided/required vars it will
  ;; return provided-vars based on var->cols
  {:ra-plan [:project (vec (for [[lv cols] var->cols]
                             (or (cols lv)
                                 {(col-sym lv) (first cols)})))
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
                       {:ra-plan [:rename var->col
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
          0 {:ra-plan [:table [{}]]}
          1 (first rels)
          {:ra-plan [:mega-join [] (mapv :ra-plan rels)]})
        (wrap-unify var->cols))))

(defn- plan-out-spec [^OutSpec bind-spec]
  (let [col (col-sym (.attr bind-spec))
        expr (.expr bind-spec)]
    {:l col :r (plan-expr expr) :literal? (not (instance? Expr$LogicVar expr))}))
;;TODO defining literal as not an LV seems flakey, but might be okay?
;;this seems like the kind of thing the AST should encode as a type/interface?

(defn- plan-arg-spec [^ArgSpec bind-spec]
  ;;TODO expr here is far to permissive.
  ;;In the outer query this has to be a literal
  ;;in the subquery case it must be a col, as thats all apply supports
  ;;In reality we could support a full expr here, additionally top level query args perhaps should
  ;;use a different spec. Delaying decision here for now.
  (let [var (col-sym (.attr bind-spec))
        expr (.expr bind-spec)]
    {:l var :r (plan-expr expr) :required-vars (required-vars expr)}))

(def app-time-period-sym 'xt$valid_time)
(def app-time-from-sym 'xt$valid_from)
(def app-time-to-sym 'xt$valid_to)
(def app-temporal-cols {:period app-time-period-sym
                        :from app-time-from-sym
                        :to app-time-to-sym})


(def system-time-period-sym 'xt$system_time)
(def system-time-from-sym 'xt$system_from)
(def system-time-to-sym 'xt$system_to)
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
    (required-vars-available? (.at this) #{})
    [:at (plan-expr (.at this))])

  TemporalFilter$In
  (plan-temporal-filter [this]
    (required-vars-available? (.from this) #{})
    (required-vars-available? (.to this) #{})
    [:in (plan-expr (.from this)) (plan-expr (.to this))])

  nil
  (plan-temporal-filter [_this]
    nil))

(defn create-period-constructor [bindings {:keys [period from to]}]
  (when-let [{:keys [r literal?]} (first (filter #(= period (:l %)) bindings))]
    (if literal?
      {:type :selection
       :expr (list '= (list 'period (col-sym from) (col-sym to)) r)}
      {:type :projection
       :expr {(col-sym period) (list 'period (col-sym from) (col-sym to))}})))

(defn- wrap-map [plan projections]
  ;;currently callers job to handle updating :provided-vars etc. as this returns unested plan
  ;;decicsion based off wrap-select and current caller
  (if (seq projections)
    [:map (vec projections)
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

(defn- plan-from [^Query$From from]
  (let [planned-bind-specs (mapv plan-out-spec (.bindings from))
        distinct-scan-cols (distinct (replace-temporal-period-with-cols (mapv :l planned-bind-specs)))
        literal-preds-by-col (-> (->> planned-bind-specs
                                      (filter :literal?)
                                      (map #(assoc % :pred (list '= (:l %) (:r %))))
                                      (group-by :l))
                                 (update-vals #(map :pred %)))]
    (-> [:scan {:table (util/symbol->normal-form-symbol (symbol (.table from)))
                :for-valid-time (plan-temporal-filter (.forValidTime from))
                :for-system-time (plan-temporal-filter (.forSystemTime from))}
         (mapv #(wrap-scan-col-preds % (get literal-preds-by-col %)) distinct-scan-cols)]
        (wrap-with-period-projection-or-selection planned-bind-specs)
        (wrap-with-ra-plan)
        (wrap-unify (-> planned-bind-specs
                        (->> (remove :literal?)
                             (group-by :r))
                        (update-vals (comp set #(mapv :l %))))))))

(defn- plan-where [^Query$Where where]
  (for [pred (.preds where)
        :let [{:keys [expr subqueries]} (plan-expr-with-subqueries pred)]]
    {:expr expr
     :subqueries subqueries
     :required-vars (required-vars pred)}))

(defn wrap-out-binding-projection [{:keys [ra-plan _provided-vars]} out-bindings]
  ;;TODO check subquery provided vars line up with bindings (although maybe this isn't an error?)
  [:project (mapv (fn [{:keys [l r]}] {r l}) out-bindings)
   ra-plan])

(defn- plan-table [table-expr out-bindings]
  (when (some :literal? out-bindings)
    (throw (UnsupportedOperationException. "TODO what should literals in out specs do outside of scan")))
  {:ra-plan (wrap-out-binding-projection {:ra-plan [:table table-expr]} out-bindings)
   :provided-vars (set (map :r out-bindings))})

(extend-protocol PlanQuery
  Query$From
  (plan-query [from]
    (plan-from from))

  Query$Pipeline
  (plan-query [pipeline]
    (reduce (fn [plan query-tail]
              (plan-query-tail query-tail plan))
            (plan-query (.query pipeline))
            (.tails pipeline)))

  Query$DocsTable
  (plan-query [table]
    (plan-table (mapv #(into {} (map (fn [[k v]] (MapEntry/create (util/kw->normal-form-kw (keyword k)) (plan-expr v)))) %)
                      (.documents table))
                (mapv plan-out-spec (.bindings table))))

  Query$ParamTable
  (plan-query [table]
    (plan-table (plan-expr (.param table))
                (mapv plan-out-spec (.bindings table)))))

(defn- wrap-expr-subquery [plan {:keys [placeholder subquery args type]}]
  (case type
    :scalar
    (if (seq args)
      [:apply
       :single-join
       (args->apply-params args)
       plan
       subquery]
      [:single-join [true]
       plan
       subquery])
    :exists
    (if (seq args)
      [:apply {:mark-join {placeholder true}}
       (args->apply-params args)
       plan
       (:ra-plan subquery)]
      [:mark-join {placeholder [true]}
       plan
       (:ra-plan subquery)])))

(defn- wrap-expr-subqueries [plan subqueries]
  (reduce
   (fn [plan sq]
     (wrap-expr-subquery plan sq))
   plan
  subqueries))

(defn- wrap-project [plan projection]
  [:project projection plan])

(defn- plan-col-spec [^ColSpec col-spec provided-vars]
  (let [col (col-sym (.attr col-spec))
        spec-expr (.expr col-spec)
        _ (required-vars-available? spec-expr provided-vars)
        {:keys [subqueries expr]} (plan-expr-with-subqueries spec-expr)]
    {:l col :r expr :subqueries subqueries :logic-var? (instance? Expr$LogicVar spec-expr)}))

(defn- plan-var-spec [^VarSpec spec]
  (let [var (col-sym (.attr spec))
        spec-expr (.expr spec)
        {:keys [subqueries expr]} (plan-expr-with-subqueries spec-expr)]
    {:l var :r expr :required-vars (required-vars spec-expr)
     :subqueries subqueries :logic-var? (instance? Expr$LogicVar spec-expr)}))

(extend-protocol PlanQueryTail
  Query$Where
  (plan-query-tail [this {:keys [ra-plan provided-vars]}]

    (doseq [pred (.preds this)]
      (required-vars-available? pred provided-vars))

    (let [planned-where-exprs (plan-where this)]
      {:ra-plan (-> ra-plan
                    (wrap-expr-subqueries (mapcat :subqueries planned-where-exprs))
                    (wrap-select (map :expr planned-where-exprs))
                    (wrap-project (vec provided-vars)))
       :provided-vars provided-vars}))

  Query$WithCols
  (plan-query-tail [this {:keys [ra-plan provided-vars]}]
    (let [projections (map #(plan-col-spec % provided-vars) (.cols this))
          return-vars (set/union
                       provided-vars
                       (set (map :l projections)))]
      {:ra-plan
       [:project (vec return-vars)
        [:map (mapv (fn [{:keys [l r]}] {l r}) projections)
         (-> ra-plan
             (wrap-expr-subqueries (mapcat :subqueries projections)))]]
       :provided-vars return-vars}))

  Query$Without
  (plan-query-tail [without {:keys [ra-plan provided-vars]}]
    (let [cols-to-be-removed (into #{} (map col-sym) (.cols without))
          _ (when (not (set/subset? cols-to-be-removed provided-vars))
              (throw (err/illegal-arg
                      :xtql/invalid-without
                      {:without-cols cols-to-be-removed
                       :input-relation-cols provided-vars
                       ::err/message "All cols in without must be present in input relation"})))
          output-projection (set/difference provided-vars (into #{} (map symbol) (.cols without)))]
      {:ra-plan [:project (vec output-projection) ra-plan]
       :provided-vars output-projection}))

  Query$Return
  (plan-query-tail [this {:keys [ra-plan provided-vars]}]
    (let [projections
          (mapv
           (fn [col]
             (let [expr (.expr ^ColSpec col)]
               (required-vars-available? expr provided-vars)
               {(col-sym (.attr ^ColSpec col)) (plan-expr expr)}))
           (.cols this))]
      {:ra-plan [:project projections ra-plan]
       :provided-vars (set (map #(first (keys %)) projections))}))

  Query$UnnestCol
  (plan-query-tail [this {:keys [ra-plan provided-vars]}]
    (let [{:keys [l r subqueries logic-var?]} (plan-col-spec (.col this) provided-vars)]
      (when (seq subqueries)
        (throw (UnsupportedOperationException. "TODO: Add support for subqueries in unnest expr")))
      (when-not logic-var?
        (throw (UnsupportedOperationException. "TODO: Add support for expr in unnest")))
      {:ra-plan [:unwind {l r} ra-plan]
       :provided-vars (conj provided-vars l)})))

(defn plan-join [join-type query args binding]
  (let [out-bindings (mapv plan-out-spec binding) ;;TODO refelection (interface here?)
        arg-bindings (mapv plan-arg-spec args)
        subquery (plan-query query)]
    (when (some :literal? out-bindings)
      (throw (UnsupportedOperationException. "TODO what should literals in out specs do outside of scan")))
    [[:join {:ra-plan (wrap-out-binding-projection subquery out-bindings)
             :args (args->apply-params arg-bindings)
             :join-type join-type
             :provided-vars (set (map :r out-bindings))
             :required-vars (apply set/union (map :required-vars arg-bindings))}]]))

(extend-protocol PlanUnifyClause
  Query$From (plan-unify-clause [from] [[:from (plan-from from)]])

  Query$DocsTable
  (plan-unify-clause [table]
    [[:from (plan-table (mapv #(into {} (map (fn [[k v]] (MapEntry/create (util/kw->normal-form-kw (keyword k)) (plan-expr v)))) %)
                              (.documents table))
                        (mapv plan-out-spec (.bindings table)))]])

  Query$ParamTable
  (plan-unify-clause [table]
    [[:from (plan-table (plan-expr (.param table))
                        (mapv plan-out-spec (.bindings table)))]])

  Query$Where
  (plan-unify-clause [where]
    (for [planned-where-exprs (plan-where where)]
      [:where planned-where-exprs]))

  Query$With
  (plan-unify-clause [this]
    (for [binding (.vars this)
          :let [{:keys [l r required-vars subqueries]} (plan-var-spec binding)]]
      [:with {:expr r
              :provided-vars #{l}
              :required-vars required-vars
              :subqueries subqueries}]))

  Query$UnnestVar
  (plan-unify-clause [this]
    (let [{:keys [l r subqueries logic-var? required-vars]} (plan-var-spec (.var this))]
      (when (seq subqueries)
        (throw (UnsupportedOperationException. "TODO: Add support for subqueries in unnest expr")))
      (when-not logic-var?
        (throw (UnsupportedOperationException. "TODO: Add support for expr in unnest")))
      [[:unnest {:expr r
                 :required-vars required-vars
                 :provided-vars #{l}}]]))

  Query$Join
  (plan-unify-clause [this]
    (plan-join :inner-join (.query this) (.args this) (.bindings this)))

  Query$LeftJoin
  (plan-unify-clause [this]
    (plan-join :left-outer-join (.query this) (.args this) (.bindings this))))

(defn wrap-wheres [{:keys [ra-plan provided-vars]} wheres]
  {:ra-plan (-> ra-plan
                (wrap-expr-subqueries (mapcat :subqueries wheres))
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

    (-> {:ra-plan [:map (vec (for [{:keys [expr renamed-provided-var]} renamed-withs]
                               {renamed-provided-var expr}))
                   (-> ra-plan
                       (wrap-expr-subqueries (mapcat :subqueries renamed-withs)))]}
        (wrap-unify var->cols))))

(defn wrap-unnest [acc-plan {:keys [provided-vars expr] :as _unnest}]
  (let [{:keys [rels var->cols]} (with-unique-cols [acc-plan])
        ;;TODO is it safe to do unique-cols over a just the parent plan, it has no context of what var unnest
        ;; is going to introduce, maybe it renames a col to one this unnest introduces.
        ;; Behaviour should mirror with, could add a help fn.
        [{acc-plan-with-unique-cols :ra-plan}] rels
        original-unnested-col (first provided-vars)
        unnested-col (col-sym (*gensym* original-unnested-col))]
    (wrap-unify
     {:ra-plan [:unwind ;; confusingly the unwind operator takes it inverted
                {unnested-col (first (get var->cols expr))}
                acc-plan-with-unique-cols]}
     (update var->cols original-unnested-col (fnil conj #{}) unnested-col))))

(defn wrap-unnests [acc-plan unnests]
  (reduce wrap-unnest acc-plan unnests))

(defn wrap-inner-join [acc-plan {:keys [args] :as join-plan}]
  (if (seq args)
    (let [{:keys [rels var->cols]} (with-unique-cols [acc-plan join-plan])
          [{acc-plan-with-unique-cols :ra-plan}
           {join-subquery-plan-with-unique-cols :ra-plan}] rels]
      (wrap-unify
       {:ra-plan [:apply :cross-join args
                  acc-plan-with-unique-cols
                  join-subquery-plan-with-unique-cols]}
       var->cols))
    (mega-join [acc-plan join-plan])))

(defn wrap-left-join [acc-plan {:keys [args provided-vars ra-plan]}]
  ;; the join clause or select placed on the dependant side
  ;; is what provides the unification of the :bind vars with the outer plan
  (let [unifying-vars (->> provided-vars (filter (:provided-vars acc-plan)))]
    {:ra-plan
     (if (seq args)
       (let [unifying-vars-apply-param-mapping (unifying-vars->apply-param-mapping unifying-vars)]
         [:apply
          :left-outer-join
          (merge
           args
           unifying-vars-apply-param-mapping)
          (:ra-plan acc-plan)
          (->> unifying-vars-apply-param-mapping
               (map (fn [[var param-for-var]]
                      (list '= var param-for-var)))
               (wrap-select ra-plan))])
       [:left-outer-join
        (->> unifying-vars
             (mapv (fn [v] {v v})))
        (:ra-plan acc-plan)
        ra-plan])
     :provided-vars (set/union (:provided-vars acc-plan) provided-vars)}))

(defn- wrap-joins [plan joins]
  (->> joins
       (reduce
        (fn [acc-plan {:keys [join-type] :as join-plan}]
          (if (= join-type :inner-join)
            (wrap-inner-join acc-plan join-plan)
            (wrap-left-join acc-plan join-plan)))
        plan)))

(extend-protocol PlanQuery
  Query$Unify
  (plan-query [unify]
    ;;TODO not all clauses can return entire plans (e.g. where-clauses),
    ;;they require an extra call to wrap should these still use the :ra-plan key.
    (let [{from-clauses :from, where-clauses :where with-clauses :with join-clauses :join
           unnests :unnest}
          (-> (mapcat plan-unify-clause (.clauses unify))
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
                  (throw (err/illegal-arg :no-available-clauses
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

(defn- plan-order-spec [^Query$OrderSpec spec]
  (let [expr (.expr spec)]
    {:order-spec [(if (instance? Expr$LogicVar expr)
                    (col-sym (.lv ^Expr$LogicVar expr))
                    (throw (UnsupportedOperationException. "TODO order-by spec can only take a column as val")))

                  (cond-> {}
                    (.direction spec)
                    (assoc :direction (if (= Query$OrderDirection/DESC (.direction spec)) :desc :asc))
                    (.nulls spec)
                    (assoc :null-ordering (if (= Query$OrderNulls/LAST (.nulls spec)) :nulls-last :nulls-first)))]}))

(extend-protocol PlanQueryTail
  Query$OrderBy
  (plan-query-tail [order-by {:keys [ra-plan provided-vars]}]
    ;;TODO Change order specs to use keywords
    (let [planned-specs (mapv plan-order-spec (.orderSpecs order-by))]
      {:ra-plan [:order-by (mapv :order-spec planned-specs)
                 ra-plan]
       :provided-vars provided-vars}))

  Query$Aggregate
  (plan-query-tail [this {:keys [ra-plan _provided-vars]}]
    ;;TODO check provided vars for all vars in aggr specs
    ;;TODO check exprs are aggr exprs
    (let [planned-specs
          (mapv
           (fn [col]
             (if (instance? Expr$LogicVar (.expr ^ColSpec col))
               (plan-expr (.expr ^ColSpec col))
               {(col-sym (.attr ^ColSpec col)) (plan-expr (.expr ^ColSpec col))}))
           (.cols this))]
      {:ra-plan [:group-by planned-specs
                 ra-plan]
       :provided-vars (set (map #(if (map? %) (first (keys %)) %) planned-specs))}))

  Query$Limit
  (plan-query-tail [this {:keys [ra-plan provided-vars]}]
    {:ra-plan [:top {:limit (.length this)} ra-plan]
     :provided-vars provided-vars})

  Query$Offset
  (plan-query-tail [this {:keys [ra-plan provided-vars]}]
    {:ra-plan [:top {:skip (.length this)} ra-plan]
     :provided-vars provided-vars}))

(defn compile-query [query]
  (let [{:keys [ra-plan]} (binding [*gensym* (seeded-gensym "_" 0)]
                            (plan-query query))]

    (-> ra-plan
        #_(doto clojure.pprint/pprint)
        #_(->> (binding [*print-meta* true]))
        (lp/rewrite-plan {})
        #_(doto clojure.pprint/pprint)
        #_(->> (binding [*print-meta* true]))
        (doto (lp/validate-plan)))))

(def ^:private extra-dml-bind-specs
  [(OutSpec/of "xt$iid" (Expr/lVar "xt$dml$iid"))
   (OutSpec/of "xt$valid_from" (Expr/lVar "xt$dml$valid_from"))
   (OutSpec/of "xt$valid_to" (Expr/lVar "xt$dml$valid_to"))])

(defn- dml-colspecs [^TemporalFilter$TemporalExtents for-valid-time, {:keys [default-all-valid-time?]}]
  [(ColSpec/of "xt$iid" (Expr/lVar "xt$dml$iid"))

   (let [vf-var (Expr/lVar "xt$dml$valid_from")

         default-vf-expr (if default-all-valid-time?
                           vf-var
                           (Expr/call "current-timestamp" []))]

     (ColSpec/of "xt$valid_from"
                 (Expr/call "cast-tstz"
                            [(if-let [vf-expr (some-> for-valid-time .from)]
                               (Expr/call "greatest" [vf-var (Expr/call "coalesce" [vf-expr default-vf-expr])])
                               default-vf-expr)])))

   (ColSpec/of "xt$valid_to"
               (Expr/call "cast-tstz"
                          [(Expr/call "least"
                                      [(Expr/lVar "xt$dml$valid_to")
                                       (if-let [vt-expr (some-> for-valid-time .to)]
                                         (Expr/call "coalesce" [vt-expr (Expr/val 'xtdb/end-of-time)])
                                         (Expr/val 'xtdb/end-of-time))])]))])

(extend-protocol PlanDml
  DmlOps$Insert
  (plan-dml [insert-query _tx-opts]
    (let [{:keys [ra-plan provided-vars]} (plan-query (.query insert-query))]
      [:insert {:table (.table insert-query)}
       [:project (vec (for [col provided-vars]
                        (let [col-sym (util/symbol->normal-form-symbol col)]
                          {col-sym (if (contains? '#{xt$valid_from xt$valid_to} col-sym)
                                     `(~'cast-tstz ~col)
                                     col)})))
        ra-plan]]))

  DmlOps$Delete
  (plan-dml [delete-query tx-opts]
    (let [table-name (.table delete-query)
          target-query (Query/pipeline (Query/unify (into [(-> (Query/from table-name)
                                                               (.binding (concat (.bindSpecs delete-query)
                                                                                 extra-dml-bind-specs)))]
                                                          (.unifyClauses delete-query)))

                                       [(Query/ret (dml-colspecs (.forValidTime delete-query) tx-opts))])

          {target-plan :ra-plan} (plan-query target-query)]
      [:delete {:table table-name}
       target-plan]))

  DmlOps$Erase
  (plan-dml [erase-query _tx-opts]
    (let [table-name (.table erase-query)
          target-query (Query/pipeline (Query/unify (into [(-> (Query/from table-name)
                                                               (.binding (concat (.bindSpecs erase-query)
                                                                                 [(OutSpec/of "xt$iid" (Expr/lVar "xt$dml$iid"))])))]
                                                          (.unifyClauses erase-query)))

                                       [(Query/ret [(ColSpec/of "xt$iid" (Expr/lVar "xt$dml$iid"))])])

          {target-plan :ra-plan} (plan-query target-query)]
      [:erase {:table table-name}
       target-plan]))

  DmlOps$Update
  (plan-dml [update-query tx-opts]
    (let [table-name (.table update-query)
          set-specs (.setSpecs update-query)
          known-columns (set (get-in tx-opts [:table-info (util/str->normal-form-str table-name)]))
          unspecified-columns (-> (set/difference known-columns
                                                  (set (for [^ColSpec set-spec set-specs]
                                                         (util/str->normal-form-str (.attr set-spec)))))
                                  (disj "xt$id"))

          target-query (Query/pipeline (Query/unify (into [(-> (Query/from table-name)
                                                               (.binding (concat (.bindSpecs update-query)
                                                                                 [(OutSpec/of "xt$id" (Expr/lVar "xt$dml$id"))]
                                                                                 (for [col unspecified-columns]
                                                                                   (OutSpec/of col (Expr/lVar (str "xt$update$" col))))
                                                                                 extra-dml-bind-specs)))]
                                                          (.unifyClauses update-query)))

                                       [(Query/ret (concat (dml-colspecs (.forValidTime update-query) tx-opts)
                                                           [(ColSpec/of "xt$id" (Expr/lVar "xt$dml$id"))]
                                                           (for [col unspecified-columns]
                                                             (ColSpec/of col (Expr/lVar (str "xt$update$" col))))
                                                           set-specs))])

          {target-plan :ra-plan} (plan-query target-query)]
      [:update {:table table-name}
       target-plan]))

  DmlOps$AssertNotExists
  (plan-dml [query _tx-opts]
    [:assert-not-exists {} (:ra-plan (plan-query (.query query)))])

  DmlOps$AssertExists
  (plan-dml [query _tx-opts]
    [:assert-exists {} (:ra-plan (plan-query (.query query)))]))

(defn compile-dml [query tx-opts]
  (let [ra-plan (binding [*gensym* (seeded-gensym "_" 0)]
                  (plan-dml query tx-opts))
        [dml-op dml-op-opts plan] ra-plan]
    [dml-op dml-op-opts
     (-> plan
         #_(doto clojure.pprint/pprint)
         #_(->> (binding [*print-meta* true]))
         (lp/rewrite-plan {})
         #_(doto clojure.pprint/pprint)
         (doto (lp/validate-plan)))]))

(defn open-xtql-query ^xtdb.IResultSet [^BufferAllocator allocator, ^IRaQuerySource ra-src, wm-src,
                                        query {:keys [args default-all-valid-time? basis default-tz explain? key-fn]
                                               :or {key-fn :clojure}}]

  (let [plan (-> (xt.edn/parse-query query)
                 compile-query)]
    (if explain?
      (lp/explain-result plan)

      ;;TODO better error if args is a vector of maps, as this is supported in other places which take args (join etc.)
      (let [args (->> args
                      (into {} (map (fn [[k v]]
                                      (MapEntry/create (param-sym (str (symbol k))) v)))))]
        (util/with-close-on-catch [params (vw/open-params allocator args)
                                   cursor (-> (.prepareRaQuery ra-src plan)
                                              (.bind wm-src {:params params, :basis basis, :default-tz default-tz :default-all-valid-time? default-all-valid-time?})
                                              (.openCursor))]
          (op/cursor->result-set cursor params (util/parse-key-fn key-fn)))))))
