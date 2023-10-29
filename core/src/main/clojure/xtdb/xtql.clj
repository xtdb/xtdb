(ns xtdb.xtql
  (:require [clojure.set :as set]
            [xtdb.error :as err]
            [xtdb.logical-plan :as lp]
            [xtdb.operator :as op]
            [xtdb.util :as util]
            [xtdb.vector.writer :as vw])
  (:import (clojure.lang MapEntry)
           (org.apache.arrow.memory BufferAllocator)
           xtdb.operator.PreparedQuery
           (xtdb.operator PreparedQuery)
           (xtdb.query Expr$Call Expr$LogicVar Expr$Obj Expr$Long Query$From Query$Return
                       Query$OrderBy Query$OrderDirection Query$OrderSpec Query$Pipeline Query$With
                       OutSpec ArgSpec ColSpec VarSpec Query$WithCols Query$Join Expr$Param Query$LeftJoin
                       Query$Unify Query$Where Query$Without Query$Limit Query$Offset Query$Aggregate)))

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
   (-> (symbol col) (vary-meta assoc :column? true)))
  ([prefix col]
   (col-sym (str (format "%s_%s" prefix col)))))

(defn- param-sym [v]
  (-> (symbol (str "?" v))
      (with-meta {:param? true})))

(defn- apply-param-sym
  ([v]
   (apply-param-sym nil v))
  ([v prefix]
   (-> (symbol (str "?" prefix v))
       (with-meta {:correlated-column? true}))))

(defn- args->apply-params [args]
  (->> args
       (mapv (fn [{:keys [l r _required-vars]}]
               (MapEntry/create (apply-param-sym l) r)))
       (into {})))

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

;TODO fill out expr plan for other types
(extend-protocol ExprPlan
  Expr$LogicVar
  (plan-expr [this] (col-sym (.lv this)))
  (required-vars [this] #{(symbol (.lv this))})

  Expr$Param ;;TODO need to differentiate between query params and subquery/apply params
  (plan-expr [this] (param-sym (subs (.v this) 1)))
  (required-vars [_this] #{})

  Expr$Obj
  (plan-expr [o] (.obj o))
  (required-vars [_] #{})

  Expr$Long
  (plan-expr [this] (.lng this))
  (required-vars [_] #{})

  Expr$Call
  (plan-expr [call] (list* (symbol (.f call)) (mapv plan-expr (.args call))))
  (required-vars [call] (into #{} (mapcat required-vars) (.args call))))

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
                       ;;by not prefixing the leftmost rels columens, apply params need not be rewritten in the case its an apply binary join.
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

(defn- plan-from-bind-spec [^OutSpec bind-spec]
  (let [col (col-sym (.attr bind-spec))
        expr (.expr bind-spec)]
    (if (instance? Expr$LogicVar expr)
      {:var (symbol (.lv ^Expr$LogicVar expr))
       :col col
       :scan-col-spec col}
      {:scan-col-spec {col (list '= col (plan-expr expr))}})))

(defn- plan-out-spec [^OutSpec bind-spec]
  (let [col (col-sym (.attr bind-spec))
        expr (.expr bind-spec)]
    (if (instance? Expr$LogicVar expr)
      {:l col :r (plan-expr expr)}
      (throw (UnsupportedOperationException. "TODO: what should exprs in out specs do outside of scan")))))

(defn- plan-arg-spec [^ArgSpec bind-spec]
  ;;TODO expr here is far to pervasive.
  ;;In the outer query this has to be a literal
  ;;in the subquery case it must be a col, as thats all apply supports
  ;;In reality we could support a full expr here, additionally top level query args perhaps should
  ;;use a different spec. Delaying decision here for now.
  (let [var (col-sym (.attr bind-spec))
        expr (.expr bind-spec)]
    {:l var :r (plan-expr expr) :required-vars (required-vars expr)}))

(defn- plan-from [^Query$From from]
  (let [planned-bind-specs (mapv plan-from-bind-spec (.bindings from))]
      (-> {:ra-plan [:scan {:table (symbol (.table from))}
                     (mapv :scan-col-spec planned-bind-specs)]}
          (wrap-unify (-> planned-bind-specs
                          (->> (filter :var)
                               (group-by :var))
                          (update-vals (comp set #(mapv :col %))))))))

(defn- plan-where [^Query$Where where]
  (for [pred (.preds where)]
    {:ra-plan (plan-expr pred)
     :required-vars (required-vars pred)}))

(extend-protocol PlanQuery
  Query$From
  (plan-query [from]
    (plan-from from))

  Query$Pipeline
  (plan-query [pipeline]
    (reduce (fn [plan query-tail]
              (plan-query-tail query-tail plan))
            (plan-query (.query pipeline))
            (.tails pipeline))))

(defn- required-vars-available? [expr provided-vars]
  (let [required-vars (required-vars expr)]
    (when (not (set/subset? required-vars provided-vars))
      (throw (err/illegal-arg
              :xtql/invalid-expression
              {:expr expr :required-vars required-vars :available-vars provided-vars
               ::err/message "Not all variables in expression are in scope"})))))

(extend-protocol PlanQueryTail
  Query$Where
  (plan-query-tail [this {:keys [ra-plan provided-vars]}]
    {:ra-plan (wrap-select
               ra-plan
               (map
                #(do
                   (required-vars-available? % provided-vars)
                   (plan-expr %))
                (.preds this)))
     :provided-vars provided-vars})

  Query$WithCols
  (plan-query-tail [this {:keys [ra-plan provided-vars]}]
    (let [projections (for [binding (.cols this)
                            :let [var (col-sym (.attr ^ColSpec binding))
                                  expr (.expr ^ColSpec binding)
                                  _ (required-vars-available? expr provided-vars)
                                  planned-expr (plan-expr expr)]]
                        {var planned-expr})]
      {:ra-plan
       [:map (vec projections)
        ra-plan]
       :provided-vars
       (set/union
        provided-vars
        (set (map #(first (keys %)) projections)))}))

  Query$Without
  (plan-query-tail [without {:keys [ra-plan provided-vars]}]
    ;;TODO should this error if you remove vars that don't exist
    (let [provided-vars (set/difference provided-vars (into #{} (map symbol) (.cols without)))]
      {:ra-plan [:project (vec provided-vars) ra-plan]
       :provided-vars provided-vars}))

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
       :provided-vars (set (map #(first (keys %)) projections))})))

(defn wrap-out-binding-projection [{:keys [ra-plan _provided-vars]} out-bindings]
  ;;TODO check subquery provided vars line up with bindings (although maybe this isn't an error?)
  [:project (mapv (fn [{:keys [l r]}] {r l}) out-bindings)
   ra-plan])

(defn plan-join [join-type query args binding]
  (let [out-bindings (mapv plan-out-spec binding) ;;TODO refelection (interface here?)
        arg-bindings (mapv plan-arg-spec args)
        subquery (plan-query query)]
    [[:join {:ra-plan (wrap-out-binding-projection subquery out-bindings)
             :args (args->apply-params arg-bindings)
             :join-type join-type
             :provided-vars (set (map :r out-bindings))
             :required-vars (apply set/union (map :required-vars arg-bindings))}]]))

(extend-protocol PlanUnifyClause
  Query$From (plan-unify-clause [from] [[:from (plan-from from)]])

  Query$Where
  (plan-unify-clause [where]
    (for [pred (plan-where where)]
      [:where pred]))

  Query$With
  (plan-unify-clause [this]
    ;;TODO check for duplicate vars
    (for [binding (.vars this)
          :let [var (col-sym (.attr ^VarSpec binding))
                expr (.expr ^VarSpec binding)
                planned-expr (plan-expr expr)]]
      [:with {:ra-plan planned-expr
              :provided-vars #{var}
              :required-vars (required-vars expr)}]))

  Query$Join
  (plan-unify-clause [this]
    (plan-join :inner-join (.query this) (.args this) (.bindings this)))

  Query$LeftJoin
  (plan-unify-clause [this]
    (plan-join :left-outer-join (.query this) (.args this) (.bindings this))))

(defn wrap-wheres [plan wheres]
  (update plan :ra-plan wrap-select (map :ra-plan wheres)))

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

    (-> {:ra-plan [:map (vec (for [{:keys [ra-plan renamed-provided-var]} renamed-withs]
                               {renamed-provided-var ra-plan}))
                   ra-plan]}
        (wrap-unify var->cols))))

(defn wrap-inner-join [acc-plan {:keys [args] :as join-plan}]
  (if (seq args)
    (let [{:keys [rels var->cols]} (with-unique-cols [acc-plan join-plan])
          [{acc-plan-with-unique-cols :ra-plan}
           {join-subquery-plan-with-unique-cols :ra-plan}] rels]
      (wrap-unify
       {:ra-plan [:apply :cross-join (set/map-invert args) ;;TODO fix apply params, reverse map is unlike any other arg form
                  acc-plan-with-unique-cols
                  join-subquery-plan-with-unique-cols]}
       var->cols))
    (mega-join [acc-plan join-plan])))

(defn wrap-left-join [acc-plan {:keys [args provided-vars ra-plan] :as join-plan}]
  ;; the join clause or select placed on the dependant side
  ;; is what provides the unification of the :bind vars with the outer plan
  (let [unifying-vars (->> provided-vars (filter (:provided-vars acc-plan)))]
    {:ra-plan
     (if (seq args)
       (let [unifying-vars-apply-param-mapping (unifying-vars->apply-param-mapping unifying-vars)]
         [:apply
          :left-outer-join
          (merge
           (set/map-invert args) ;;TODO fix apply params, reverse map is unlike any other arg form
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
    (let [{from-clauses :from, where-clauses :where with-clauses :with join-clauses :join}
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
             joins join-clauses]

        (if (and (empty? wheres) (empty? withs) (empty? joins))

          plan

          (let [available-vars (:provided-vars plan)]
            (letfn [(available? [clause]
                      (set/superset? available-vars (:required-vars clause)))]

              (let [{available-wheres true, unavailable-wheres false} (->> wheres (group-by available?))
                    {available-withs true, unavailable-withs false} (->> withs (group-by available?))
                    {available-joins true, unavailable-joins false} (->> joins (group-by available?))]

                (if (and (empty? available-wheres) (empty? available-withs) (empty? available-joins))
                  (throw (err/illegal-arg :no-available-clauses
                                          {:available-vars available-vars
                                           :unavailable-wheres unavailable-wheres
                                           :unavailable-withs unavailable-withs
                                           :unavailable-joins unavailable-joins}))

                  (recur (cond-> plan
                           available-wheres (wrap-wheres available-wheres)
                           available-withs (wrap-withs available-withs)
                           available-joins (wrap-joins available-joins))
                         unavailable-wheres
                         unavailable-withs
                         unavailable-joins))))))))))

(defn- plan-order-spec [^Query$OrderSpec spec]
  (let [expr (.expr spec)]
    {:order-spec [(if (instance? Expr$LogicVar expr)
                    (col-sym (.lv ^Expr$LogicVar expr))
                    (throw (UnsupportedOperationException. "TODO")))

                  (if (= Query$OrderDirection/DESC (.direction spec))
                    {:direction :desc}
                    {:direction :asc})]}))

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
        (doto (lp/validate-plan)))))

(defn open-xtql-query ^xtdb.IResultSet [^BufferAllocator allocator, wm-src, ^PreparedQuery pq,
                                        {:keys [args basis default-tz default-all-valid-time?]}]
  (let [args (->> args
                  (into {} (map (fn [[k v]]
                                  (MapEntry/create (param-sym (str (symbol k))) v)))))]
    (util/with-close-on-catch [params (vw/open-params allocator args)]
      (-> (.bind pq wm-src {:params params, :basis basis, :default-tz default-tz :default-all-valid-time? default-all-valid-time?})
          (.openCursor)
          (op/cursor->result-set params)))))
