(ns xtdb.xtql
  (:require [clojure.set :as set]
            [xtdb.logical-plan :as lp]
            [xtdb.operator :as op]
            [xtdb.util :as util]
            [xtdb.vector.writer :as vw])
  (:import (org.apache.arrow.memory BufferAllocator)
           xtdb.IResultSet
           (xtdb.operator IRaQuerySource)
           (xtdb.operator.scan IScanEmitter)
           (xtdb.query BindingSpec Expr$Call Expr$LogicVar Expr$Obj Query$From Query$Return
                       Query$OrderBy Query$OrderDirection Query$OrderSpec Query$Pipeline
                       Query$Unify Query$Where Query$Without Query$Limit Query$Offset)))

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

(extend-protocol ExprPlan
  Expr$LogicVar
  (plan-expr [this] (col-sym (.lv this)))
  (required-vars [this] #{(symbol (.lv this))})

  Expr$Obj
  (plan-expr [o] (.obj o))
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
                                         (into {} (map (juxt col-sym (partial col-sym (str "_r" idx))))))]
                       {:ra-plan [:rename var->col
                                  ra-plan]
                        :provided-vars (set (vals var->col))
                        :var->col var->col})))))
    {:rels (mapv :ra-plan plans)
     :var->cols (-> plans
                    (->> (mapcat :var->col)
                         (group-by key))
                    (update-vals #(into #{} (map val) %)))}))

(defn- mega-join [plans]
  (let [{:keys [rels var->cols]} (with-unique-cols plans)]
    (-> (case (count rels)
          0 {:ra-plan [:table [{}]]}
          1 (first rels)
          {:ra-plan [:mega-join [] rels]})
        (wrap-unify var->cols))))

(defn- plan-from-bind-spec [^BindingSpec bind-spec]
  (let [col (symbol (.attr bind-spec))
        expr (.expr bind-spec)]
    (if (instance? Expr$LogicVar expr)
      {:var (symbol (.lv ^Expr$LogicVar expr))
       :col col
       :scan-col-spec col}
      {:scan-col-spec {col (list '= col (plan-expr expr))}})))

(defn- plan-from [^Query$From from]
  (let [planned-bind-specs (mapv plan-from-bind-spec (.bindSpecs from))]
      (-> {:ra-plan [:scan {:table (symbol (.table from))}
                     (mapv :scan-col-spec planned-bind-specs)]}
          (wrap-unify (-> planned-bind-specs
                          (->> (filter :var)
                               (group-by :var))
                          (update-vals (comp set #(mapv :col %))))))))

(defn- plan-where [^Query$Where where]
  (for [pred (.preds where)]
    {:pred (plan-expr pred)
     :required-vars (required-vars pred)}))

(extend-protocol PlanQuery
  Query$From (plan-query [from] (plan-from from))

  Query$Pipeline
  (plan-query [pipeline]
    (reduce (fn [plan query-tail]
              (plan-query-tail query-tail plan))
            (plan-query (.query pipeline))
            (.tails pipeline))))

(extend-protocol PlanQueryTail
  Query$Where
  (plan-query-tail [where plan]
    (throw (UnsupportedOperationException. "TODO")))

  Query$Without
  (plan-query-tail [without {:keys [ra-plan provided-vars]}]
    (let [provided-vars (set/difference provided-vars (into #{} (map symbol) (.cols without)))]
      {:ra-plan [:project (vec provided-vars) ra-plan]
       :provided-vars provided-vars}))
  Query$Return
  (plan-query-tail [this {:keys [ra-plan #_provided-vars]}]
    ;;TODO Check required vars for exprs (including col refs) are in the provided vars of prev step
    (let [planned-projections
          (mapv
           (fn [col]
             {(col-sym (.attr ^BindingSpec col)) (plan-expr (.expr ^BindingSpec col))})
           (.cols this))]
      {:ra-plan [:project planned-projections ra-plan]
       :provided-vars (set (map #(first (keys %)) planned-projections))})))

(extend-protocol PlanUnifyClause
  Query$From (plan-unify-clause [from] [[:from (plan-from from)]])

  Query$Where
  (plan-unify-clause [where]
    (for [pred (plan-where where)]
      [:where pred])))

(extend-protocol PlanQuery
  Query$Unify
  (plan-query [unify]
    (let [{from-clauses :from, where-clauses :where}
          (-> (mapcat plan-unify-clause (.clauses unify))
              (->> (group-by first))
              (update-vals #(mapv second %)))]
      (cond-> (mega-join from-clauses)
        where-clauses (update :ra-plan wrap-select (map :pred where-clauses))))))

(defn- plan-order-spec [^Query$OrderSpec spec]
  (let [expr (.expr spec)]
    {:order-spec [(if (instance? Expr$LogicVar expr)
                    (symbol (.lv ^Expr$LogicVar expr))
                    (throw (UnsupportedOperationException. "TODO")))

                  (if (= Query$OrderDirection/DESC (.direction spec))
                    {:direction :desc}
                    {:direction :asc})]}))

(extend-protocol PlanQueryTail
  Query$OrderBy
  (plan-query-tail [order-by {:keys [ra-plan provided-vars]}]
    (let [planned-specs (mapv plan-order-spec (.orderSpecs order-by))]
      {:ra-plan [:order-by (mapv :order-spec planned-specs)
                 ra-plan]
       :provided-vars provided-vars}))
  Query$Limit
  (plan-query-tail [this {:keys [ra-plan provided-vars]}]
    {:ra-plan [:top {:limit (.length this)} ra-plan]
     :provided-vars provided-vars})
  Query$Offset
  (plan-query-tail [this {:keys [ra-plan provided-vars]}]
    {:ra-plan [:top {:skip (.length this)} ra-plan]
     :provided-vars provided-vars}))

(defn- explain [plan]
  (let [col-types '{plan :clj-form}
        ^Iterable res [{:plan plan}]
        it (.iterator res)]
    (reify IResultSet
      (columnTypes [_] col-types)
      (hasNext [_] (.hasNext it))
      (next [_] (.next it))
      (close [_]))))

(defn open-xtql-query ^xtdb.IResultSet [^BufferAllocator allocator, ^IRaQuerySource ra-src, wm-src, ^IScanEmitter _scan-emitter
                                        query {:keys [args default-all-valid-time? basis default-tz explain?]}]
  (let [{:keys [ra-plan]} (binding [*gensym* (seeded-gensym "_" 0)]
                            (plan-query query))

        ra-plan (-> ra-plan
                    #_(doto clojure.pprint/pprint)
                    #_(->> (binding [*print-meta* true]))
                    (lp/rewrite-plan {})
                    #_(doto clojure.pprint/pprint)
                    (doto (lp/validate-plan)))]

    (if explain?
      (explain ra-plan)

      (let [^xtdb.operator.PreparedQuery pq (.prepareRaQuery ra-src ra-plan)]
        (util/with-close-on-catch [params (vw/open-params allocator {} #_(args->params args in-bindings))]
          (-> (.bind pq wm-src {; :params params, :table-args (args->tables args in-bindings),
                                :basis basis, :default-tz default-tz :default-all-valid-time? default-all-valid-time?})
              (.openCursor)
              (op/cursor->result-set params)))))))
