(ns xtdb.sql.plan
  (:require [clojure.set :as set]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [xtdb.antlr :as antlr]
            [xtdb.api :as xt]
            [xtdb.error :as err]
            [xtdb.information-schema :as info-schema]
            [xtdb.logical-plan :as lp]
            [xtdb.time :as time]
            [xtdb.tx-ops :as tx-ops]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.writer :as vw])
  (:import clojure.lang.MapEntry
           [java.net URI]
           (java.time Duration LocalDate LocalDateTime LocalTime OffsetTime Period ZoneOffset ZonedDateTime)
           (java.util Collection HashMap HashSet LinkedHashSet Map SequencedSet Set UUID)
           java.util.function.Function
           (org.antlr.v4.runtime ParserRuleContext)
           (org.apache.arrow.vector.types.pojo Field)
           [org.apache.commons.codec.binary Hex]
           (xtdb.antlr Sql$BaseTableContext Sql$DirectlyExecutableStatementContext Sql$GroupByClauseContext Sql$HavingClauseContext Sql$IntervalQualifierContext Sql$JoinSpecificationContext Sql$JoinTypeContext Sql$ObjectNameAndValueContext Sql$OrderByClauseContext Sql$QualifiedRenameColumnContext Sql$QueryBodyTermContext Sql$QuerySpecificationContext Sql$QueryTailContext Sql$RenameColumnContext Sql$SearchedWhenClauseContext Sql$SelectClauseContext Sql$SetClauseContext Sql$SimpleWhenClauseContext Sql$SortSpecificationContext Sql$SortSpecificationListContext Sql$WhenOperandContext Sql$WhereClauseContext Sql$WithTimeZoneContext SqlVisitor)
           (xtdb.types IntervalMonthDayNano)
           xtdb.util.StringUtil))

(defn- ->insertion-ordered-set [coll]
  (LinkedHashSet. ^Collection (vec coll)))

(defprotocol PlanError
  (error-string [err]))

(defn- add-err! [{:keys [!errors]} err]
  (swap! !errors conj err)
  nil)

(defn- add-warning! [{:keys [!warnings]} err]
  (swap! !warnings conj err)
  nil)

(declare ->ExprPlanVisitor map->ExprPlanVisitor ->QueryPlanVisitor)

(defn accept-visitor [visitor ^ParserRuleContext ctx]
  (.accept ctx visitor))

(defn ->col-sym
  ([n]
   (cond
     (string? n) (recur (symbol n))
     (symbol? n) (-> n (vary-meta assoc :column? true))))

  ([ns n]
   (-> (symbol (str ns) (str n))
       (vary-meta assoc :column? true))))

(defn identifier-sym [^ParserRuleContext ctx]
  (some-> ctx
          (.accept (reify SqlVisitor
                     (visitSchemaName [this ctx] (-> (.identifier ctx) (.accept this)))
                     (visitAsClause [this ctx] (-> (.columnName ctx) (.accept this)))
                     (visitQueryName [this ctx] (-> (.identifier ctx) (.accept this)))

                     (visitTableName [this ctx]
                       (let [tn (-> (.identifier ctx) (.accept this))]
                         (if-let [sn (some-> (.schemaName ctx) (.accept this))]
                           (symbol (str sn) (str tn))
                           tn)))

                     (visitTableAlias [this ctx] (-> (.correlationName ctx) (.accept this)))
                     (visitColumnName [this ctx] (-> (.identifier ctx) (.accept this)))
                     (visitFieldName [this ctx] (-> (.identifier ctx) (.accept this)))
                     (visitCorrelationName [this ctx] (-> (.identifier ctx) (.accept this)))
                     (visitObjectName [this ctx] (-> (.identifier ctx) (.accept this)))

                     (visitRegularIdentifier [_ ctx] (symbol (util/str->normal-form-str (.getText ctx))))

                     (visitDelimitedIdentifier [_ ctx]
                       (let [di-str (.getText ctx)]
                         (symbol (subs di-str 1 (dec (count di-str))))))))))

(defprotocol OptimiseStatement
  (optimise-stmt [stmt]))

(defrecord QueryExpr [plan col-syms]
  OptimiseStatement (optimise-stmt [this] (update this :plan lp/rewrite-plan)))

(defprotocol Scope
  (available-cols [scope])
  (-find-cols [scope chain excl-cols]))

(defprotocol PlanRelation
  (plan-rel [scope]))

(extend-protocol Scope
  nil
  (available-cols [_])
  (-find-cols [_ _ _]))

(extend-protocol PlanRelation
  nil
  (plan-rel [_]
    [:table [{}]]))

(defn- find-cols
  ([scope chain] (find-cols scope chain #{}))
  ([scope chain excl-cols] (-find-cols scope chain excl-cols)))

(defn- find-col [scope chain]
  (let [[match & more-matches] (find-cols scope chain)]
    (assert (nil? more-matches) (str "multiple decls: " {:matches (cons match more-matches), :chain chain}))
    match))

(defrecord AmbiguousColumnReference [chain]
  PlanError
  (error-string [_] (format "Ambiguous column reference: %s" (str/join "." (reverse chain)))))

(defrecord BaseTableNotFound [schema-name table-name]
  PlanError
  (error-string [_] (format "Table not found: %s" (str/join "." (filter some? [schema-name table-name])))))

(defrecord ColumnNotFound [chain]
  PlanError
  (error-string [_] (format "Column not found: %s" (str/join "." (reverse chain)))))

(defrecord SubqueryDisallowed []
  PlanError
  (error-string [_] "Subqueries are not allowed in this context"))

(defn- ->sq-sym [sym {:keys [!id-count]} ^Map !sq-refs]
  (when sym
    (if (:correlated-column? (meta sym))
      sym
      (.computeIfAbsent !sq-refs sym
                        (reify Function
                          (apply [_ sym]
                            (-> (symbol (format "?_sq_%s_%d" (name sym) (dec (swap! !id-count inc))))
                                (vary-meta assoc :correlated-column? true))))))))

(defrecord SubqueryScope [env, scope, ^Map !sq-refs]
  Scope
  (available-cols [_] (available-cols scope))

  (-find-cols [_ chain excl-cols]
    (not-empty (->> (find-cols scope chain excl-cols)
                    (mapv #(->sq-sym % env !sq-refs))))))

(defn- plan-sq [^ParserRuleContext sq-ctx, {:keys [!id-count] :as env}, scope, ^Map !subqs, sq-opts]
  (if-not !subqs
    (add-err! env (->SubqueryDisallowed))

    (let [sq-sym (-> (->col-sym (str "_sq_" (swap! !id-count inc)))
                     (vary-meta assoc :sq-out-sym? true))
          !sq-refs (HashMap.)
          query-plan (-> sq-ctx (.accept (->QueryPlanVisitor env (->SubqueryScope env scope !sq-refs))))]

      (.put !subqs sq-sym (-> sq-opts
                              (assoc :query-plan query-plan
                                     :sq-refs (into {} !sq-refs))))

      sq-sym)))

(defn- apply-sqs [plan subqs]
  (reduce-kv (fn [plan sq-sym {:keys [query-plan sq-type sq-refs] :as sq}]
               (case sq-type
                 :scalar [:apply :single-join sq-refs
                          plan
                          [:project [{sq-sym (first (:col-syms query-plan))}]
                           (:plan query-plan)]]

                 :nest-one [:apply :single-join sq-refs
                            plan
                            [:project [{sq-sym (->> (for [col-sym (:col-syms query-plan)]
                                                      (MapEntry/create (keyword col-sym) col-sym))
                                                    (into {}))}]
                             (:plan query-plan)]]

                 :nest-many [:apply :single-join sq-refs
                             plan
                             [:group-by [{sq-sym (list 'array_agg sq-sym)}]
                              [:project [{sq-sym (->> (for [col-sym (:col-syms query-plan)]
                                                        (MapEntry/create (keyword col-sym) col-sym))
                                                      (into {}))}]
                               (:plan query-plan)]]]

                 :array-by-query [:apply :single-join sq-refs
                                  plan
                                  [:group-by [{sq-sym (list 'vec_agg (first (:col-syms query-plan)))}]
                                   (:plan query-plan)]]


                 :exists [:apply {:mark-join {sq-sym true}} sq-refs
                          plan
                          (:plan query-plan)]

                 :quantified-comparison (let [{:keys [expr op]} sq
                                              needle-param (vary-meta '?_needle assoc :correlated-column? true)]

                                          (if (and (:column? (meta expr))
                                                   (not (get sq-refs expr)))
                                              ;;can't apply shortcut if column is already mapped to existing param due to limitation
                                              ;;of input col as key in sq-refs param map
                                            [:apply
                                             {:mark-join {sq-sym (list op needle-param (first (:col-syms query-plan)))}}
                                             (assoc sq-refs expr needle-param)
                                             plan
                                             (:plan query-plan)]

                                            [:apply
                                             {:mark-join {sq-sym (list op needle-param (first (:col-syms query-plan)))}}
                                             (assoc sq-refs (->col-sym '_needle) needle-param)
                                             [:map [{'_needle expr}]
                                              plan]
                                             (:plan query-plan)]))))
             plan
             subqs))

(defrecord CTE [plan col-syms])

(defrecord WithVisitor [env scope]
  SqlVisitor
  (visitWithClause [{{:keys [ctes]} :env, :as this} ctx]
    (assert (not (.RECURSIVE ctx)) "Recursive CTEs are not supported yet")
    (->> (.withListElement ctx)
         (reduce (fn [ctes ^ParserRuleContext wle]
                   (conj ctes (.accept wle (assoc-in this [:env :ctes] ctes))))
                 (or ctes {}))))

  (visitWithListElement [_ ctx]
    (let [query-name (identifier-sym (.queryName ctx))]
      (assert (nil? (.columnNameList ctx)) "Column aliases are not supported yet")

      (let [{:keys [plan col-syms]} (-> (.subquery ctx)
                                        (.accept (->QueryPlanVisitor env scope)))]
        (MapEntry/create query-name (->CTE plan col-syms))))))

(defrecord TableTimePeriodSpecificationVisitor [expr-visitor]
  SqlVisitor
  (visitQueryValidTimePeriodSpecification [this ctx]
    (if (.ALL ctx)
      :all-time
      (-> (.tableTimePeriodSpecification ctx)
          (.accept this))))

  (visitQuerySystemTimePeriodSpecification [this ctx]
    (if (.ALL ctx)
      :all-time
      (-> (.tableTimePeriodSpecification ctx)
          (.accept this))))

  (visitTableAllTime [_ _] :all-time)

  (visitTableAsOf [this ctx]
    [:at (-> ctx (.periodSpecificationExpr) (.accept this))])

  (visitTableBetween [this ctx]
    [:between
     (-> ctx (.periodSpecificationExpr 0) (.accept this))
     (-> ctx (.periodSpecificationExpr 1) (.accept this))])

  (visitTableFromTo [this ctx]
    [:in
     (-> ctx (.periodSpecificationExpr 0) (.accept this))
     (-> ctx (.periodSpecificationExpr 1) (.accept this))])

  (visitPeriodSpecLiteral [_ ctx] (-> (.literal ctx) (.accept expr-visitor)))
  (visitPeriodSpecParam [_ ctx] (-> (.parameterSpecification ctx) (.accept expr-visitor)))
  (visitPeriodSpecNow [_ _] :now))

(defrecord MultipleTimePeriodSpecifications []
  PlanError
  (error-string [_] "Multiple time period specifications were specified"))

(defrecord PeriodSpecificationDisallowedOnCte [table-name]
  PlanError
  (error-string [_] (format "Period specifications not allowed on CTE reference: %s" table-name)))


(def ^:private temporal-period-columns #{'_valid_time '_system_time})

(defn temporal-period-column? [col]
  (contains? temporal-period-columns col))

(defn wrap-temporal-periods [plan scan-cols valid-time-col? sys-time-col?]
  [:project
   (cond-> scan-cols
     valid-time-col? (conj {(->col-sym '_valid_time) (list 'period (->col-sym '_valid_from) (->col-sym '_valid_to))})
     sys-time-col? (conj {(->col-sym '_system_time) (list 'period (->col-sym '_system_from) (->col-sym '_system_to))}))
   plan])

(defrecord BaseTable [env, ^Sql$BaseTableContext ctx
                      schema-name table-name table-alias unique-table-alias cols
                      ^Map !reqd-cols]
  Scope
  (available-cols [_] cols)

  (-find-cols [this [col-name table-name schema-name] excl-cols]
    (when (and (or (nil? table-name) (= table-name table-alias))
               (or (nil? schema-name) (= schema-name (:schema-name this))))
      (for [col (if col-name
                  (when (or (contains? cols col-name) (types/temporal-column? col-name)
                            (temporal-period-column? col-name))
                    [col-name])
                  (available-cols this))
            :when (not (contains? excl-cols col))]
        (.computeIfAbsent !reqd-cols col
                          (fn [col]
                            (->col-sym (str unique-table-alias) (str col)))))))

  PlanRelation
  (plan-rel [{{:keys [valid-time-default sys-time-default]} :env, :as this}]
    (let [reqd-cols (set (.keySet !reqd-cols))
          valid-time-col? (contains? reqd-cols '_valid_time)
          sys-time-col? (contains? reqd-cols '_system_time)
          scan-cols (cond-> (vec (disj reqd-cols '_valid_time '_system_time))
                      valid-time-col? (into ['_valid_from '_valid_to])
                      sys-time-col? (into ['_system_from '_system_to]))
          expr-visitor (->ExprPlanVisitor env this)]
      (letfn [(<-table-time-period-specification [specs]
                (case (count specs)
                  0 nil
                  1 (.accept ^ParserRuleContext (first specs) (->TableTimePeriodSpecificationVisitor expr-visitor))
                  :else (add-err! env (->MultipleTimePeriodSpecifications))))]
        (let [for-vt (or (<-table-time-period-specification (.queryValidTimePeriodSpecification ctx))
                         valid-time-default)
              for-st (or (<-table-time-period-specification (.querySystemTimePeriodSpecification ctx))
                         sys-time-default)]

          [:rename unique-table-alias
           (cond-> [:scan (cond-> {:table (symbol (if schema-name
                                                    (str schema-name)
                                                    "public")
                                                  (str table-name))}
                            for-vt (assoc :for-valid-time for-vt)
                            for-st (assoc :for-system-time for-st))
                    scan-cols]
             (or valid-time-col? sys-time-col?) (wrap-temporal-periods scan-cols valid-time-col? sys-time-col?))])))))

(defrecord JoinConditionScope [env l r]
  Scope
  (available-cols [_]
    (->> [l r]
         (into [] (comp (mapcat available-cols)
                        (distinct)))))

  (-find-cols [_ chain excl-cols]
    (->> [l r]
         (mapcat #(find-cols % chain excl-cols)))))

(defrecord JoinTable [env l r
                      ^Sql$JoinTypeContext join-type-ctx
                      ^Sql$JoinSpecificationContext join-spec-ctx
                      common-cols]
  Scope
  (available-cols [_]
    (->> [l r]
         (into [] (comp (mapcat available-cols)
                        (distinct)))))

  (-find-cols [_ chain excl-cols]
    (->> (if (and (= 1 (count chain))
                  (get common-cols (first chain)))
           [l] [l r])
         (mapcat #(find-cols % chain excl-cols))))

  PlanRelation
  (plan-rel [_]
    (let [join-type (case (some-> join-type-ctx
                                  (.outerJoinType)
                                  (.getText)
                                  (str/upper-case))
                      "LEFT" :left-outer-join
                      "RIGHT" :right-outer-join
                      ;; "FULL" :full-outer-join
                      :join)

          [join-type l r] (if (= join-type :right-outer-join)
                            [:left-outer-join r l]
                            [join-type l r])]

      (if common-cols
        [join-type (vec (for [col-name common-cols]
                          {(find-col l [col-name])
                           (find-col r [col-name])}))
         (plan-rel l)
         (plan-rel r)]

        (some-> join-spec-ctx
                (.accept
                 (reify SqlVisitor
                   (visitJoinCondition [_ ctx]
                     (let [!join-cond-subqs (HashMap.)
                           !lhs-refs (HashMap.)
                           join-pred (-> (.expr ctx)
                                         (.accept (map->ExprPlanVisitor {:env env,
                                                                         :scope (->JoinConditionScope env (->SubqueryScope env l !lhs-refs) r)
                                                                         :!subqs !join-cond-subqs})))]

                       [:apply (if (= join-type :join)
                                 :cross-join
                                 join-type)
                        (into {} !lhs-refs)
                        (plan-rel l)
                        [:select join-pred
                         (-> (plan-rel r)
                             (apply-sqs !join-cond-subqs))]])))))))))

(defrecord CrossJoinTable [env !sq-refs l r]
  Scope
  (available-cols [_]
    (->> [l r]
         (into [] (comp (mapcat available-cols)
                        (distinct)))))

  (-find-cols [_ chain excl-cols]
    (->> [l r]
         (mapcat #(-find-cols % chain excl-cols))))

  PlanRelation
  (plan-rel [_]
    (let [planned-l (plan-rel l)
          planned-r (plan-rel r)]
      [:apply :cross-join (into {} !sq-refs)
       planned-l planned-r])))

(defrecord DerivedTable [plan table-alias unique-table-alias, ^SequencedSet available-cols]
  Scope
  (available-cols [_] available-cols)

  (-find-cols [_ [col-name table-name] excl-cols]
    (when (or (nil? table-name) (= table-name table-alias))
      (for [col (if col-name
                  (when (.contains available-cols col-name)
                    [col-name])
                  available-cols)
            :when (not (contains? excl-cols col))]
        (->col-sym (str unique-table-alias) (str col)))))

  PlanRelation
  (plan-rel [_]
    [:rename unique-table-alias
     plan]))

(defrecord UnnestTable [env table-alias unique-table-alias unnest-col unnest-expr ordinality-col]
  Scope
  (available-cols [_]
    (->insertion-ordered-set (cond-> [unnest-col]
                               ordinality-col (conj ordinality-col))))

  (-find-cols [this [col-name table-name] excl-cols]
    (when (or (nil? table-name) (= table-name table-alias))
      (for [col (available-cols this)
            :when (or (nil? col-name) (= col-name col))
            :when (not (contains? excl-cols col))]
        (-> (->col-sym (str unique-table-alias) (str col))
            (with-meta (meta col))))))

  PlanRelation
  (plan-rel [_]
    (as-> [:table {(-> (->col-sym (str unique-table-alias) (str unnest-col))
                       (with-meta (meta unnest-col)))
                   unnest-expr}]
        plan

      (if ordinality-col
        [:map [{(-> (->col-sym (str unique-table-alias) (str ordinality-col))
                    (with-meta (meta ordinality-col)))
                '(row-number)}]
         plan]
        plan))))

(defrecord ArrowTable [env url table-alias unique-table-alias ^SequencedSet !table-cols]
  Scope
  (available-cols [_]
    !table-cols)

  (-find-cols [this [col-name table-name] excl-cols]
    (when (or (nil? table-name) (= table-name table-alias))
      (for [col (if col-name
                  (when (.contains !table-cols col-name)
                    [col-name])
                  (available-cols this))
            :when (not (contains? excl-cols col))]
        (->col-sym (str unique-table-alias) (str col)))))

  PlanRelation
  (plan-rel [_]
    [:rename unique-table-alias
     [:project (vec !table-cols)
      [:arrow url]]]))

(defn- ->table-projection [^ParserRuleContext ctx]
  (some-> ctx
          (.accept
           (reify SqlVisitor
             (visitTableProjection [_ ctx]
               (some->> (.columnNameList ctx) (.columnName)
                        (mapv (comp ->col-sym identifier-sym))))))))

(defrecord ProjectedCol [projection col-sym])

(defn- negate-op [op]
  (case op
    = '<>, <> '=
    < '>=, > '<=
    <= '>, >= '<))

(defrecord InvalidOrderByOrdinal [out-cols ordinal]
  PlanError
  (error-string [_] (format "Invalid order by ordinal: %s - out-cols: %s" ordinal out-cols)))

(defrecord MissingGroupingColumns [missing-grouping-cols]
  PlanError
  (error-string [_] (format "Missing grouping columns: %s" missing-grouping-cols)))

(defrecord GroupInvariantColsTracker [env scope, ^Set !implied-gicrs ^Set !unresolved-cr]
  SqlVisitor
  (visitSelectClause [this ctx] (.accept (.getParent ctx) this))

  (visitGroupByClause [_ gbc]
    (let [grouping-cols (vec (for [^ParserRuleContext grp-el (.groupingElement gbc)]
                               (.accept grp-el
                                        (reify SqlVisitor
                                          (visitOrdinaryGroupingSet [_ ctx]
                                            (.accept (.columnReference ctx)
                                                     (map->ExprPlanVisitor {:env env :scope scope
                                                                            :!unresolved-cr !unresolved-cr})))))))]

      (if-let [missing-grouping-cols (not-empty (set/difference (set !implied-gicrs) (set grouping-cols)))]
        (add-err! env (->MissingGroupingColumns missing-grouping-cols))
        grouping-cols)))

  Scope 
  (available-cols [_] (available-cols scope))

  (-find-cols [_ chain excl-cols]
    (for [sym (-find-cols scope chain excl-cols)]
      (do
        (some-> !implied-gicrs (.add sym))
        sym))))

(defn- wrap-aggs [plan aggs group-invariant-cols]
  (let [in-projs (not-empty (into [] (keep (comp :projection :in-projection)) (vals aggs)))]
    (as-> plan plan
      (if in-projs
        [:map in-projs plan]
        plan)

      [:group-by (vec (concat group-invariant-cols
                              (for [[agg-sym {:keys [agg-expr]}] aggs]
                                {agg-sym agg-expr})))
       plan]

      (if in-projs
        [:project (concat group-invariant-cols (vec (keys aggs)))
         plan]
        plan))))

(defrecord TableRefVisitor [env scope left-scope]
  SqlVisitor
  (visitBaseTable [{{:keys [!id-count table-info ctes] :as env} :env} ctx]
    (let [tn (some-> (.tableOrQueryName ctx) (.tableName))
          sn (identifier-sym (.schemaName tn))
          tn (identifier-sym (.identifier tn))
          table-alias (or (identifier-sym (.tableAlias ctx)) tn)
          unique-table-alias (symbol (str table-alias "." (swap! !id-count inc)))
          cols (some-> (.tableProjection ctx) (->table-projection))]
      (or (when-not sn
            (when-let [{:keys [plan], cte-cols :col-syms} (get ctes tn)]
              (when (or (seq (.querySystemTimePeriodSpecification ctx))
                        (seq (.queryValidTimePeriodSpecification ctx)))
                (add-err! env (->PeriodSpecificationDisallowedOnCte tn)))
              (->DerivedTable plan table-alias unique-table-alias
                              (->insertion-ordered-set (or cols cte-cols)))))

          (let [[sn table-cols] (or (when-let [table-cols (get table-info (symbol (or (some-> sn str) "public") (str tn)))]
                                      [sn table-cols])

                                    (when-not sn
                                      (when-let [table-cols (get info-schema/unq-pg-catalog tn)]
                                        ['pg_catalog table-cols]))

                                    (add-warning! env (->BaseTableNotFound sn tn)))]
            (->BaseTable env ctx sn tn table-alias unique-table-alias
                         (->insertion-ordered-set (or cols table-cols))
                         (HashMap.))))))

  (visitJoinTable [this ctx]
    (let [l (-> (.tableReference ctx 0) (.accept this))
          r (-> (.tableReference ctx 1) (.accept this))
          common-cols (.accept (.joinSpecification ctx)
                               (reify SqlVisitor
                                 (visitJoinCondition [_ _] nil)
                                 (visitNamedColumnsJoin [_ ctx]
                                   (->> (.columnNameList ctx) (.columnName)
                                        (into #{} (map (comp ->col-sym identifier-sym)))))))]

      (->JoinTable env l r (.joinType ctx) (.joinSpecification ctx) common-cols)))

  (visitCrossJoinTable [this ctx]
    (->CrossJoinTable env
                      {}
                      (-> (.tableReference ctx 0) (.accept this))
                      (-> (.tableReference ctx 1) (.accept this))))

  (visitNaturalJoinTable [this ctx]
    (let [l (-> (.tableReference ctx 0) (.accept this))
          r (-> (.tableReference ctx 1) (.accept this))
          common-cols (set/intersection (set (available-cols l)) (set (available-cols r)))]

      (->JoinTable env l r (.joinType ctx) nil common-cols)))

  (visitDerivedTable [{{:keys [!id-count]} :env} ctx]
    (let [{:keys [plan col-syms]} (-> (.subquery ctx) (.queryExpression)
                                      (.accept (-> (->QueryPlanVisitor env scope)
                                                   (assoc :out-col-syms (->table-projection (.tableProjection ctx))))))

          table-alias (identifier-sym (.tableAlias ctx))]

      (->DerivedTable plan table-alias
                      (symbol (str table-alias "." (swap! !id-count inc)))
                      (->insertion-ordered-set col-syms))))

  (visitLateralDerivedTable [{{:keys [!id-count]} :env} ctx]
    (let [{:keys [plan col-syms]} (-> (.subquery ctx) (.queryExpression)
                                      (.accept (-> (->QueryPlanVisitor env (or left-scope scope))
                                                   (assoc :out-col-syms (->table-projection (.tableProjection ctx))))))

          table-alias (identifier-sym (.tableAlias ctx))]

      (->DerivedTable plan table-alias (symbol (str table-alias "." (swap! !id-count inc)))
                      (->insertion-ordered-set col-syms))))

  (visitCollectionDerivedTable [{{:keys [!id-count]} :env} ctx]
    (let [expr (-> (.expr ctx)
                   (.accept (->ExprPlanVisitor env (or left-scope scope))))
          table-projection (->table-projection (.tableProjection ctx))
          table-alias (identifier-sym (.tableAlias ctx))
          with-ordinality? (boolean (.withOrdinality ctx))]

      (assert (or (nil? table-projection)
                  (= (+ 1 (if with-ordinality? 1 0)) (count table-projection))))

      (->UnnestTable env table-alias
                     (symbol (str table-alias "." (swap! !id-count inc)))
                     (or (->col-sym (first table-projection))
                         (-> (->col-sym (str "_unnest." (swap! !id-count inc)))
                             (vary-meta assoc :unnamed-unnest-col? true)))
                     expr
                     (when with-ordinality?
                       (or (->col-sym (second table-projection))
                           (-> (->col-sym (str "_ordinal." (swap! !id-count inc)))
                               (vary-meta assoc :unnamed-unnest-col? true)))))))

  (visitGenerateSeriesTable [{{:keys [!id-count]} :env} ctx]
    (let [expr (-> (.generateSeries ctx)
                   (.accept (->ExprPlanVisitor env (or left-scope scope))))

          table-projection (->table-projection (.tableProjection ctx))
          table-alias (identifier-sym (.tableAlias ctx))
          with-ordinality? (boolean (.withOrdinality ctx))]

      (assert (or (nil? table-projection)
                  (= (+ 1 (if with-ordinality? 1 0))
                     (count table-projection))))

      (->UnnestTable env table-alias
                     (symbol (str table-alias "." (swap! !id-count inc)))
                     (or (->col-sym (first table-projection))
                         (-> (->col-sym (str "_genseries." (swap! !id-count inc)))
                             (vary-meta assoc :unnamed-unnest-col? true)))
                     expr
                     (when with-ordinality?
                       (or (->col-sym (second table-projection))
                           (-> (->col-sym (str "_ordinal." (swap! !id-count inc)))
                               (vary-meta assoc :unnamed-unnest-col? true)))))))

  (visitWrappedTableReference [this ctx] (-> (.tableReference ctx) (.accept this)))

  (visitArrowTable [{{:keys [!id-count]} :env} ctx]
    (let [table-alias (identifier-sym (.tableAlias ctx))
          cols (->table-projection (.tableProjection ctx))
          url (some-> (.characterString ctx) (.accept (->ExprPlanVisitor env scope)))]
      (->ArrowTable env url table-alias
                    (symbol (str table-alias "." (swap! !id-count inc)))
                    (->insertion-ordered-set cols)))))

(defrecord QuerySpecificationScope [outer-scope from-rel]
  Scope
  (available-cols [_] (available-cols from-rel))

  (-find-cols [_ chain excl-cols]
    (or (not-empty (-find-cols from-rel chain excl-cols))
        (-find-cols outer-scope chain excl-cols)))

  PlanRelation
  (plan-rel [_]
    (plan-rel from-rel)))

(defn- ->projected-col-expr [col-idx expr]
  (let [{:keys [column? sq-out-sym? agg-out-sym? window-out-sym? unnamed-unnest-col? identifier]} (meta expr)]
    (if (and column? (not sq-out-sym?) (not agg-out-sym?) (not window-out-sym?) (not unnamed-unnest-col?))
      (->ProjectedCol expr expr)
      (let [col-name (or identifier (->col-sym (str "_column_" (inc col-idx))))]
        (->ProjectedCol {col-name expr} col-name)))))

(defrecord SelectClauseProjectedCols [env scope]
  SqlVisitor
  (visitSelectClause [_ ctx]
    (let [sl-ctx (.selectList ctx)
          !subqs (HashMap.)
          !aggs (HashMap.)
          !agg-subqs (HashMap.)
          !windows (HashMap.)

          explicitly-projected-cols (->> (.selectSublist sl-ctx)
                                         (into [] (comp (map-indexed
                                                         (fn [col-idx ^ParserRuleContext sl-elem]
                                                           (.accept (.getChild sl-elem 0)
                                                                    (reify SqlVisitor
                                                                      (visitDerivedColumn [_ ctx]
                                                                        (let [expr-ctx (.expr ctx)]
                                                                          [(let [expr (.accept expr-ctx
                                                                                               (map->ExprPlanVisitor {:env env, :scope scope, :!subqs !subqs, :!aggs !aggs :!agg-subqs !agg-subqs :!windows !windows}))]
                                                                             (if-let [as-clause (.asClause ctx)]
                                                                               (let [col-name (->col-sym (identifier-sym as-clause))]
                                                                                 (->ProjectedCol {col-name expr} col-name))

                                                                               (->projected-col-expr col-idx expr)))]))

                                                                      (visitQualifiedAsterisk [_ ctx]
                                                                        (let [[table-name schema-name] (rseq (mapv identifier-sym (.identifier (.identifierChain ctx))))]
                                                                          (when schema-name
                                                                            (throw (UnsupportedOperationException. "schema not supported")))

                                                                          (let [excludes (when-let [exclude-ctx (.excludeClause ctx)]
                                                                                           (into #{} (map identifier-sym) (.identifier exclude-ctx)))]

                                                                            (if-let [table-cols (not-empty (find-cols scope [nil table-name] excludes))]
                                                                              (let [renames (->> (for [^Sql$QualifiedRenameColumnContext rename-pair (some-> (.qualifiedRenameClause ctx)
                                                                                                                                                             (.qualifiedRenameColumn))]
                                                                                                   (let [sym (find-col scope [(identifier-sym (.identifier rename-pair)) table-name])
                                                                                                         out-col-name (.columnName (.asClause rename-pair))]
                                                                                                     (MapEntry/create sym (->col-sym (identifier-sym out-col-name)))))
                                                                                                 (into {}))]
                                                                                (->> table-cols
                                                                                     (into [] (map-indexed (fn [col-idx sym]
                                                                                                             (if-let [renamed-col (get renames sym)]
                                                                                                               (->ProjectedCol {renamed-col sym} renamed-col)
                                                                                                               (->projected-col-expr col-idx sym)))))))

                                                                              (throw (UnsupportedOperationException. (str "Table not found: " table-name)))))))))))
                                                        cat)))]

      {:projected-cols (vec (concat (when-let [star-ctx (.selectListAsterisk sl-ctx)]
                                      (let [renames (->> (for [^Sql$RenameColumnContext rename-pair (some-> (.renameClause star-ctx)
                                                                                                            (.renameColumn))]
                                                           (let [chain (rseq (mapv identifier-sym (.identifier (.identifierChain (.columnReference rename-pair)))))
                                                                 out-col-name (.columnName (.asClause rename-pair))
                                                                 sym (find-col scope chain)]

                                                             (MapEntry/create sym (->col-sym (identifier-sym out-col-name)))))
                                                         (into {}))

                                            excludes (set/union (into #{} (map (comp symbol name :col-sym)) explicitly-projected-cols)
                                                                (when-let [exclude-ctx (.excludeClause star-ctx)]
                                                                  (into #{} (map identifier-sym) (.identifier exclude-ctx))))]

                                        (->> (find-cols scope nil excludes)
                                             (map-indexed (fn [col-idx sym]
                                                            (if-let [renamed-col (get renames sym)]
                                                              (->ProjectedCol {renamed-col sym} renamed-col)
                                                              (->projected-col-expr col-idx sym))))
                                             (into []))))

                                    explicitly-projected-cols))
       :subqs (not-empty (into {} !subqs))
       :agg-subqs (not-empty (into {} !agg-subqs))
       :aggs (not-empty (into {} !aggs))
       :windows (not-empty (into {} !windows))})))

(defrecord CannotParseInteger[s]
  PlanError
  (error-string [_] (format "Cannot parse integer: %s" s)))

(defrecord CannotParseDate [d-str msg]
  PlanError
  (error-string [_] (format "Cannot parse date: %s - failed with message %s" d-str msg)))

(defn parse-date-literal [d-str env]
  (try
    (LocalDate/parse d-str)
    (catch Exception e
      (add-err! env (->CannotParseDate d-str (.getMessage e))))))

(defrecord CannotParseTime [t-str msg]
  PlanError
  (error-string [_] (format "Cannot parse time: %s - failed with message %s" t-str msg)))

(defn parse-time-literal [t-str env]
  (if-let [[_ h m s sf offset-str] (re-matches #"(\d{1,2}):(\d{1,2}):(\d{1,2})(?:\.(\d+))?([+-]\d{2}:\d{2})?" t-str)]
    (try
      (let [local-time (LocalTime/of (parse-long h) (parse-long m) (parse-long s) (time/seconds-fraction->nanos sf))]
        (if offset-str
          (OffsetTime/of local-time (ZoneOffset/of ^String offset-str))
          local-time))
      (catch Exception e
        (add-err! env (->CannotParseTime t-str (.getMessage e)))))

    (add-err! env (->CannotParseTime t-str nil))))

(defrecord CannotParseTimestamp [ts-str msg]
  PlanError
  (error-string [_] (format "Cannot parse timestamp: %s - failed with message %s" ts-str msg)))

(defn parse-timestamp-literal [ts-str env]
  (try
    (or (time/parse-sql-timestamp-literal ts-str)
        (add-err! env (->CannotParseTimestamp ts-str nil)))
    (catch Exception e
      (add-err! env (->CannotParseTimestamp ts-str (.getMessage e))))))

(defrecord CannotParseInterval [i-str msg]
  PlanError
  (error-string [_] (format "Cannot parse interval: %s - failed with message %s" i-str msg)))

(defn parse-iso-interval-literal [i-str env]
  (if-let [[_ p-str d-str] (re-matches #"P([-\dYMWD]+)?(?:T([-\dHMS\.]+)?)?" i-str)]
    (try
      (IntervalMonthDayNano. (if p-str
                               (Period/parse (str "P" p-str))
                               Period/ZERO)
                             (if d-str
                               (Duration/parse (str "PT" d-str))
                               Duration/ZERO))
      (catch Exception e
        (add-err! env (->CannotParseInterval i-str (.getMessage e)))))

    (add-err! env (->CannotParseInterval i-str nil))))

(defrecord CannotParseDuration [d-str msg]
  PlanError
  (error-string [_] (format "Cannot parse duration: %s - failed with message %s" d-str msg)))

(defn- parse-duration-literal [d-str env]
  (try
    (Duration/parse d-str)
    (catch Exception e
      (add-err! env (->CannotParseDuration d-str (.getMessage e))))))

(defrecord CannotParseBinaryHexString [bin-str-expr msg]
  PlanError
  (error-string [_] (format "Cannot parse binary string expr: %s - failed with message %s" bin-str-expr msg)))

(defn- parse-hex-string [bin-str env]
  (try
    (Hex/decodeHex (subs bin-str 3 (- (count bin-str) 2)))
    (catch Exception e
      (add-err! env (->CannotParseBinaryHexString bin-str (.getMessage e))))))

(defrecord CannotParseUUID [u-str msg]
  PlanError
  (error-string [_] (format "Cannot parse UUID: %s - failed with message %s" u-str msg)))

(defn- parse-uuid-literal [u-str env]
  (try
    (UUID/fromString u-str)
    (catch Exception e
      (add-err! env (->CannotParseUUID u-str (.getMessage e))))))

(defrecord CannotParseURI [u-str msg]
  PlanError
  (error-string [_] (format "Cannot parse uri: %s - failed with message %s" u-str msg)))

(defn- parse-uri-literal [u-str env]
  (try
    (URI. u-str)
    (catch Exception e
      (add-err! env (->CannotParseURI u-str (.getMessage e))))))

(defn fn-with-precision [fn-symbol ^ParserRuleContext precision-ctx]
  (if-let [precision (some-> precision-ctx (.getText) (parse-long))]
    (list fn-symbol precision)
    (list fn-symbol)))

(defn ->interval-expr [ve {:keys [start-field end-field leading-precision fractional-precision]}]
  (if end-field
    (list 'multi-field-interval ve start-field leading-precision end-field fractional-precision)
    (list 'single-field-interval ve start-field leading-precision fractional-precision)))

(defn iq-context->iq-map [^Sql$IntervalQualifierContext ctx]
  (if-let [sdf (.singleDatetimeField ctx)]
    (let [field (-> (.getChild sdf 0) (.getText) (str/upper-case))
          fp (some-> (.intervalFractionalSecondsPrecision sdf) (.getText) (parse-long))]
      {:start-field field
       :end-field nil :leading-precision 2
       :fractional-precision (or fp 6)})

    (let [start-field (-> (.startField ctx) (.nonSecondPrimaryDatetimeField) (.getText) (str/upper-case))
          ef (-> (.endField ctx) (.singleDatetimeField))
          end-field (if-let [non-sec-ef (.nonSecondPrimaryDatetimeField ef)]
                      (-> (.getText non-sec-ef) (str/upper-case))
                      "SECOND")
          fp (some-> (.intervalFractionalSecondsPrecision ef) (.getText) (parse-long))]
      {:start-field start-field
       :end-field end-field
       :leading-precision 2
       :fractional-precision (or fp 6)})))

(defrecord CastArgsVisitor [env]
  SqlVisitor
  (visitBooleanType [_ _] {:cast-type :bool})
  (visitIntegerType [_ ctx]
    {:cast-type (case (str/lower-case (.getText ctx))
                  "smallint" :i16
                  ("int" "integer") :i32
                  "bigint" :i64)})

  (visitFloatType [_ _] {:cast-type :f32})
  (visitRealType [_ _] {:cast-type :f32})
  (visitDoubleType [_ _] {:cast-type :f64})

  (visitDateType [_ _] {:cast-type [:date :day]})
  (visitTimeType [_ ctx]
    (let [precision (some-> (.precision ctx) (.getText) (parse-long))
          time-unit (if precision
                      (if (<= precision 6) :micro :nano)
                      :micro)]
      (if (instance? Sql$WithTimeZoneContext
                     (.withOrWithoutTimeZone ctx))
        {:->cast-fn (fn [ve]
                      (list* 'cast-tstz ve
                             (when precision
                               [{:precision precision :unit time-unit}])))}

        {:cast-type [:time-local time-unit]
         :cast-opts (when precision
                      {:precision precision})})))

  (visitTimestampType [_ ctx]
    (let [precision (some-> (.precision ctx) (.getText) (parse-long))
          time-unit (if precision
                      (if (<= precision 6) :micro :nano)
                      :micro)]
      (if (instance? Sql$WithTimeZoneContext
                     (.withOrWithoutTimeZone ctx))
        {:->cast-fn (fn [ve]
                      (list 'cast-tstz ve
                             {:precision precision, :unit time-unit}))}

        {:cast-type [:timestamp-local time-unit]
         :cast-opts (when precision
                      {:precision precision})})))

  (visitTimestampTzType [_ _]
    {:->cast-fn (fn [ve]
                  (list 'cast-tstz ve {:unit :micro}))})

  (visitDurationType [_ ctx]
    (let [precision (some-> (.precision ctx) (.getText) (parse-long))
          time-unit (if precision
                      (if (<= precision 6) :micro :nano)
                      :micro)]
      {:cast-type [:duration time-unit]
       :cast-opts (when precision {:precision precision})}))

  (visitIntervalType [_ ctx]
    (let [interval-qualifier (.intervalQualifier ctx)]
      {:cast-type :interval
       :cast-opts (when interval-qualifier (iq-context->iq-map interval-qualifier))}))

  (visitKeywordType [_ _] {:cast-type :keyword})
  (visitUuidType [_ _] {:cast-type :uuid})
  (visitVarbinaryType [_ _] {:cast-type :varbinary})

  (visitRegClassType [_ _] {:cast-type :regclass, :cast-opts {}})
  (visitRegProcType [_ _] {:cast-type :regproc, :cast-opts {}})

  (visitCharacterStringType [_ _] {:cast-type :utf8}))

(defn handle-cast-expr [ve {:keys [cast-type cast-opts ->cast-fn]}] 
  (if ->cast-fn
    (->cast-fn ve)
    (cond-> (list 'cast ve cast-type)
      (not-empty cast-opts) (concat [cast-opts]))))

(defrecord AggregatesDisallowed []
  PlanError
  (error-string [_] "Aggregates are not allowed in this context"))

(defrecord WindowFunctionsDisallowed []
  PlanError
  (error-string [_] "Window functions are not allowed in this context"))

(def ^xtdb.antlr.SqlVisitor string-literal-visitor
  (reify SqlVisitor
    (visitCharacterStringLiteral [this ctx] (-> (.characterString ctx) (.accept this)))

    (visitSqlStandardString [_ ctx]
      (let [text (.getText ctx)]
        (-> text
            (subs 1 (dec (count text)))
            (str/replace #"''" "'"))))

    (visitCEscapesString [_ ctx]
      (let [str (.getText ctx)]
        (StringUtil/parseCString (subs str 2 (dec (count str))))))

    (visitDollarString [_ ctx]
      (or (some-> (.dollarStringText ctx) (.getText))
          ""))))

(declare plan-sort-specification-list)

(defrecord ExprPlanVisitor [env scope]
  SqlVisitor
  (visitSearchCondition [this ctx] (list* 'and (mapv (partial accept-visitor this) (.expr ctx))))
  (visitExprPrimary1 [this ctx] (-> (.exprPrimary ctx) (.accept this)))
  (visitNumericExpr0 [this ctx] (-> (.numericExpr ctx) (.accept this)))
  (visitWrappedExpr [this ctx] (-> (.expr ctx) (.accept this)))

  (visitLiteralExpr [this ctx] (-> (.literal ctx) (.accept this)))
  (visitFloatLiteral [_ ctx] (parse-double (.getText ctx)))
  (visitIntegerLiteral [_ ctx]
    (or (parse-long (.getText ctx))
        (add-err! env (->CannotParseInteger (.getText ctx)))))

  (visitCharacterStringLiteral [_ ctx] (.accept ctx string-literal-visitor))
  (visitSqlStandardString [_ ctx] (.accept ctx string-literal-visitor))
  (visitCEscapesString [_ ctx] (.accept ctx string-literal-visitor))

  (visitDateTimeLiteral0 [this ctx] (-> (.dateTimeLiteral ctx) (.accept this)))

  (visitDateLiteral [this ctx] (parse-date-literal (.accept (.characterString ctx) this) env))
  (visitTimeLiteral [this ctx] (parse-time-literal (.accept (.characterString ctx) this) env))
  (visitTimestampLiteral [this ctx] (parse-timestamp-literal (.accept (.characterString ctx) this) env))

  (visitIntervalLiteral0 [this ctx] (.accept (.intervalLiteral ctx) this))

  (visitIntervalLiteral [this ctx]
    (let [csl (some-> (.characterString ctx) (.accept this))
          iq-map (some-> (.intervalQualifier ctx) (iq-context->iq-map))
          interval-expr (if iq-map
                          (->interval-expr csl iq-map)
                          (parse-iso-interval-literal csl env))]
      (if (.MINUS ctx)
        (list '- interval-expr)
        interval-expr)))

  (visitDurationLiteral [this ctx] (parse-duration-literal (.accept (.characterString ctx) this) env))

  (visitBinaryStringLiteral [_ ctx] (parse-hex-string (.getText ctx) env))

  (visitBooleanLiteral [_ ctx]
    (case (-> (.getText ctx) str/lower-case)
      ("true" "on") true
      ("false" "off") false
      "unknown" nil))

  (visitKeywordLiteral [this ctx]
    (let [s (.accept (.characterString ctx) this)]
      (keyword (if (str/starts-with? s ":") (subs s 1) s))))

  (visitUUIDLiteral [this ctx] (parse-uuid-literal (.accept (.characterString ctx) this) env))
  (visitURILiteral [this ctx] (parse-uri-literal (.accept (.characterString ctx) this) env))

  (visitNullLiteral [_ _ctx] nil)

  (visitColumnExpr [this ctx] (-> (.columnReference ctx) (.accept this)))

  (visitColumnReference [{{:keys [!id-count]} :env :keys [^Set !ob-col-refs ^Set !unresolved-cr]} ctx]
    (let [chain (rseq (mapv identifier-sym (.identifier (.identifierChain ctx))))
          matches (find-cols scope chain)]
      (when-let [sym (case (count matches)
                       0 (do (add-warning! env (->ColumnNotFound chain))
                             (when !unresolved-cr
                               (let [sym (->col-sym (str "xt$missing_column" (swap! !id-count inc)))]
                                 (.add !unresolved-cr sym)
                                 sym)))
                       1 (first matches)
                       (add-err! env (->AmbiguousColumnReference chain))) ]
        (some-> !ob-col-refs (.add sym))

        sym)))

  (visitParamExpr [this ctx] (-> (.parameterSpecification ctx) (.accept this)))

  (visitDynamicParameter [{{:keys [!param-count]} :env} _]
    (-> (symbol (str "?_" (dec (swap! !param-count inc))))
        (vary-meta assoc :param? true)))

  (visitPostgresParameter [{{:keys [!param-count]} :env} ctx]
    (-> (symbol (str "?_" (let [param-idx (parse-long (subs (.getText ctx) 1))]
                            (swap! !param-count max param-idx)
                            (dec param-idx))))
        (vary-meta assoc :param? true)))

  (visitStaticLiteral [this ctx] (-> (.literal ctx) (.accept this)))
  (visitStaticParam [this ctx] (-> (.parameterSpecification ctx) (.accept this)))

  (visitFieldAccess [this ctx]
    (let [ve (-> (.exprPrimary ctx) (.accept this))
          field-name (identifier-sym (.fieldName ctx))]
      (-> (list '. ve (keyword field-name))
          (vary-meta assoc :identifier (->col-sym field-name)))))

  (visitArrayAccess [this ctx]
    (let [ve (-> (.exprPrimary ctx) (.accept this))
          n (-> (.expr ctx) (.accept this))]
      (list 'nth ve (if (integer? n)
                      (dec n)
                      (list '- n 1)))))

  (visitUnaryPlusExpr [this ctx] (-> (.numericExpr ctx) (.accept this)))

  (visitUnaryMinusExpr [this ctx]
    (if (= (.getText ctx) (str Long/MIN_VALUE))
      Long/MIN_VALUE

      (let [expr (-> (.numericExpr ctx)
                     (.accept this))]
        (if (number? expr)
          (- expr)
          (list '- expr)))))

  (visitNumericTermExpr [this ctx]
    (list (cond
            (.PLUS ctx) '+
            (.MINUS ctx) '-
            :else (throw (IllegalStateException.)))
          (-> (.numericExpr ctx 0) (.accept this))
          (-> (.numericExpr ctx 1) (.accept this))))

  (visitNumericFactorExpr [this ctx]
    (list (cond
            (.ASTERISK ctx) '*
            (.SOLIDUS ctx) '/
            :else (throw (IllegalStateException.)))
          (-> (.numericExpr ctx 0) (.accept this))
          (-> (.numericExpr ctx 1) (.accept this))))

  (visitNumericBitwiseNotExpr [this ctx]
    (list 'bit_not
          (-> (.numericExpr ctx) (.accept this))))

  (visitNumericBitwiseAndExpr [this ctx]
    (list 'bit_and
          (-> (.numericExpr ctx 0) (.accept this))
          (-> (.numericExpr ctx 1) (.accept this))))

  (visitNumericBitwiseOrExpr [this ctx]
    (list (cond
            (.BITWISE_OR ctx) 'bit_or
            (.BITWISE_XOR ctx) 'bit_xor
            :else (throw (IllegalStateException.)))
          (-> (.numericExpr ctx 0) (.accept this))
          (-> (.numericExpr ctx 1) (.accept this))))

  (visitNumericBitwiseShiftExpr [this ctx]
    (list (cond
            (.BITWISE_SHIFT_LEFT ctx) 'bit_shift_left
            (.BITWISE_SHIFT_RIGHT ctx) 'bit_shift_right
            :else (throw (IllegalStateException.)))
          (-> (.numericExpr ctx 0) (.accept this))
          (-> (.numericExpr ctx 1) (.accept this))))

  (visitConcatExpr [this ctx]
    (list 'concat
          (-> (.exprPrimary ctx 0) (.accept this))
          (-> (.exprPrimary ctx 1) (.accept this))))

  (visitStrFunction [this ctx]
    (xt/template (str ~@(->> (.expr ctx)
                             (mapv (partial accept-visitor this))))))

  (visitIsBooleanValueExpr [this ctx]
    (let [boolean-value (-> (.booleanValue ctx) (.getText) (str/upper-case))
          expr (-> (.expr ctx) (.accept this))
          boolean-fn (case boolean-value
                       "TRUE" (list 'true? expr)
                       "FALSE" (list 'false? expr)
                       "UNKNOWN" (list 'nil? expr))]
      (if (.NOT ctx)
        (list 'not boolean-fn)
        boolean-fn)))
  
  (visitExtractFunction [this ctx]
    (let [extract-field (-> (.extractField ctx) (.getText) (str/upper-case))
          extract-source (-> (.extractSource ctx) (.expr) (.accept this))]
      (list 'extract extract-field extract-source)))

  (visitPositionFunction [this ctx]
    (let [needle (-> (.expr ctx 0) (.accept this))
          haystack (-> (.expr ctx 1) (.accept this))
          units (or (some-> (.charLengthUnits ctx) (.getText)) "CHARACTERS")]
      (list (case units
              "CHARACTERS" 'position
              "OCTETS" 'octet-position)
            needle haystack)))

  (visitCharacterLengthFunction [this ctx]
    (let [nve (-> (.expr ctx) (.accept this))
          units (or (some-> (.charLengthUnits ctx) (.getText)) "CHARACTERS")]
      (list (case units
              "CHARACTERS" 'character-length
              "OCTETS" 'octet-length)
            nve)))

  (visitOctetLengthFunction [this ctx]
    (let [nve (-> (.expr ctx) (.accept this))]
      (list 'octet-length nve)))

  (visitLengthFunction [this ctx]
    (let [nve (-> (.expr ctx) (.getChild 0) (.accept this))]
      (list 'length nve)))

  (visitCardinalityFunction [this ctx]
    (let [nve (-> (.expr ctx) (.accept this))]
      (list 'cardinality nve)))

  (visitArrayUpperFunction [this ctx]
    (xt/template (array-upper ~(-> (.expr ctx 0) (.accept this))
                              ~(-> (.expr ctx 1) (.accept this)))))

  (visitAbsFunction [this ctx]
    (let [nve (-> (.expr ctx) (.accept this))]
      (list 'abs nve)))

  (visitModFunction [this ctx]
    (let [nve1 (-> (.expr ctx 0) (.accept this))
          nve2 (-> (.expr ctx 1) (.accept this))]
      (list 'mod nve1 nve2)))

  (visitTrigonometricFunction [this ctx]
    (let [nve (-> (.expr ctx) (.accept this))
          fn-name (-> (.trigonometricFunctionName ctx) (.getText) (str/lower-case))]
      (list (symbol fn-name) nve)))

  (visitLogFunction [this ctx]
    (let [nve1 (-> (.generalLogarithmBase ctx) (.expr) (.accept this))
          nve2 (-> (.generalLogarithmArgument ctx) (.expr) (.accept this))]
      (list 'log nve1 nve2)))

  (visitLog10Function [this ctx]
    (let [nve (-> (.expr ctx) (.accept this))]
      (list 'log10 nve)))

  (visitLnFunction [this ctx]
    (let [nve (-> (.expr ctx) (.accept this))]
      (list 'ln nve)))

  (visitExpFunction [this ctx]
    (let [nve (-> (.expr ctx) (.accept this))]
      (list 'exp nve)))

  (visitPowerFunction [this ctx]
    (let [nve1 (-> (.expr ctx 0) (.accept this))
          nve2 (-> (.expr ctx 1) (.accept this))]
      (list 'power nve1 nve2)))

  (visitSqrtFunction [this ctx]
    (let [nve (-> (.expr ctx) (.accept this))]
      (list 'sqrt nve)))

  (visitFloorFunction [this ctx]
    (let [nve (-> (.expr ctx) (.accept this))]
      (list 'floor nve)))

  (visitCeilingFunction [this ctx]
    (let [nve (-> (.expr ctx) (.accept this))]
      (list 'ceil nve)))

  (visitLeastFunction [this ctx]
    (let [nves (mapv #(.accept ^ParserRuleContext % this) (.expr ctx))]
      (list* 'least nves)))

  (visitGreatestFunction [this ctx]
    (let [nves (mapv #(.accept ^ParserRuleContext % this) (.expr ctx))]
      (list* 'greatest nves)))

  (visitOrExpr [this ctx]
    (list 'or
          (-> (.expr ctx 0) (.accept this))
          (-> (.expr ctx 1) (.accept this))))

  (visitAndExpr [this ctx]
    (list 'and
          (-> (.expr ctx 0) (.accept this))
          (-> (.expr ctx 1) (.accept this))))

  (visitUnaryNotExpr [this ctx] (list 'not (-> (.expr ctx) (.accept this))))

  (visitComparisonPredicate [this ctx]
    (list ((some-fn {'!= '<>} identity)
           (symbol (.getText (.compOp ctx))))
          (-> (.expr ctx 0) (.accept this))
          (-> (.expr ctx 1) (.accept this))))

  (visitComparisonPredicatePart2 [{:keys [pt1] :as this} ctx]
    (list ((some-fn {'!= '<>} identity)
           (symbol (.getText (.compOp ctx))))
          pt1
          (-> (.expr ctx) (.accept (dissoc this :pt1)))))

  (visitNullPredicate [this ctx]
    (let [expr (list 'nil? (-> (.expr ctx) (.accept this)))]
      (if (.NOT ctx)
        (list 'not expr)
        expr)))

  (visitNullPredicatePart2 [{:keys [pt1]} ctx]
    (let [expr (list 'nil? pt1)]
      (if (.NOT ctx)
        (list 'not expr)
        expr)))

  (visitBetweenPredicate [this ctx]
    (let [between-expr (list (cond
                               (.SYMMETRIC ctx) 'between-symmetric
                               (.ASYMMETRIC ctx) 'between
                               :else 'between)
                             (-> (.numericExpr ctx 0) (.accept this))
                             (-> (.numericExpr ctx 1) (.accept this))
                             (-> (.numericExpr ctx 2) (.accept this)))]
      (if (.NOT ctx)
        (list 'not between-expr)
        between-expr)))

  (visitBetweenPredicatePart2 [{:keys [pt1] :as this} ctx]
    (let [between-expr (list (cond
                               (.SYMMETRIC ctx) 'between-symmetric
                               (.ASYMMETRIC ctx) 'between
                               :else 'between)
                             pt1
                             (-> (.expr ctx 0) (.accept this))
                             (-> (.expr ctx 1) (.accept this)))]
      (if (.NOT ctx)
        (list 'not between-expr)
        between-expr)))

  (visitLikePredicate [this ctx]
    (let [like-expr (list 'like
                          (-> (.expr ctx) (.accept this))
                          (-> (.likePattern ctx) (.exprPrimary) (.accept this)))]
      (if (.NOT ctx)
        (list 'not like-expr)
        like-expr)))

  (visitLikePredicatePart2 [{:keys [pt1] :as this} ctx]
    (let [cp (-> (.likePattern ctx) (.exprPrimary) (.accept (dissoc this :pt1)))]
      (if (.NOT ctx)
        (list 'not (list 'like pt1 cp))
        (list 'like pt1 cp))))

  (visitLikeRegexPredicate [this ctx]
    (let [like-expr (list 'like-regex (.accept (.expr ctx) this)
                          (-> (.xqueryPattern ctx) (.exprPrimary) (.accept this))
                          (or (some-> (.xqueryOptionFlag ctx) (.exprPrimary) (.accept this)) ""))]
      (if (.NOT ctx)
        (list 'not like-expr)
        like-expr)))

  (visitLikeRegexPredicatePart2 [{:keys [pt1] :as this} ctx]
    (let [like-expr (list 'like-regex pt1
                          (-> (.xqueryPattern ctx) (.exprPrimary) (.accept this))
                          (or (some-> (.xqueryOptionFlag ctx) (.exprPrimary) (.accept this)) ""))]
      (if (.NOT ctx)
        (list 'not like-expr)
        like-expr)))

  (visitPostgresRegexPredicate [this ctx]
    (let [pro (-> (.postgresRegexOperator ctx) (.getText))
          expr (list 'like-regex (.accept (.expr ctx) this)
                     (-> (.xqueryPattern ctx) (.exprPrimary) (.accept this))
                     (if (#{"~*" "!~*"} pro) "i" ""))]
      (if (#{"!~" "!~*"} pro)
        (list 'not expr)
        expr)))

  (visitPostgresRegexPredicatePart2 [{:keys [pt1] :as this} ctx]
    (let [pro (-> (.postgresRegexOperator ctx) (.getText))
          expr (list 'like-regex pt1
                     (-> (.xqueryPattern ctx) (.exprPrimary) (.accept this))
                     (if (#{"~*" "!~*"} pro) "i" ""))]
      (if (#{"!~" "!~*"} pro)
        (list 'not expr)
        expr)))

  (visitPeriodOverlapsPredicate [this ctx]
    (let [p1 (-> (.expr ctx 0) (.accept this))
          p2 (-> (.expr ctx 1) (.accept this))]
      (xt/template
       (and (< (lower ~p1) (coalesce (upper ~p2) xtdb/end-of-time))
            (> (coalesce (upper ~p1) xtdb/end-of-time) (lower ~p2))))))

  (visitOverlapsFunction [this ctx]
    (let [exprs (mapv #(.accept ^ParserRuleContext % this) (.expr ctx))]
      (xt/template
       (< (greatest
           ~@(map
              #(xt/template (lower ~%)) exprs))
          (least
           ~@(map #(xt/template (coalesce (upper ~%) xtdb/end-of-time))
                  exprs))))))

  (visitPeriodEqualsPredicate [this ctx]
    (let [p1 (-> (.expr ctx 0) (.accept this))
          p2 (-> (.expr ctx 1) (.accept this))]
      (xt/template
       (and (= (lower ~p1) (lower ~p2))
            (null-eq (upper ~p1) (upper ~p2))))))

  (visitPeriodContainsPredicate [this ctx]
    (let [p1 (-> (.expr ctx 0) (.accept this))
          p2 (-> (.expr ctx 1) (.accept this))]
      (xt/template (contains? ~p1 ~p2))))

  (visitPeriodPrecedesPredicate [this ctx]
    (let [p1 (-> (.expr ctx 0) (.accept this))
          p2 (-> (.expr ctx 1) (.accept this))]
      (xt/template
       (<= (coalesce (upper ~p1) xtdb/end-of-time) (lower ~p2)))))

  (visitPeriodSucceedsPredicate [this ctx]
    (let [p1 (-> (.expr ctx 0) (.accept this))
          p2 (-> (.expr ctx 1) (.accept this))]
      (xt/template
       (>= (lower ~p1) (coalesce (upper ~p2) xtdb/end-of-time)))))

  (visitPeriodImmediatelyPrecedesPredicate [this ctx]
    (let [p1 (-> (.expr ctx 0) (.accept this))
          p2 (-> (.expr ctx 1) (.accept this))]
      (xt/template
       (= (coalesce (upper ~p1) xtdb/end-of-time) (lower ~p2)))))

  (visitPeriodImmediatelySucceedsPredicate [this ctx]
    (let [p1 (-> (.expr ctx 0) (.accept this))
          p2 (-> (.expr ctx 1) (.accept this))]
      (xt/template
       (= (lower ~p1) (coalesce (upper ~p2) xtdb/end-of-time)))))

  (visitTsTzRangeConstructor [this ctx]
    (xt/template
     (period ~(some-> (.expr ctx 0) (.accept this))
             ~(some-> (.expr ctx 1) (.accept this)))))

  (visitHasAnyColumnPrivilegePredicate [_ _] true)
  (visitHasTablePrivilegePredicate [_ _] true)
  (visitHasSchemaPrivilegePredicate [_ _] true)
  (visitPgExpandArrayFunction [_ _] nil)
  (visitPgGetExprFunction [_ _] nil)
  (visitPgGetIndexdefFunction [_ _] nil)
  (visitPgSleepFunction [this ctx]
    (list 'sleep (list 'cast (.accept (.sleepSeconds ctx) this) [:duration :milli])))

  (visitPgSleepForFunction [this ctx]
    (list 'sleep (list 'cast (.accept (.sleepPeriod ctx) this) [:duration :milli])))

  (visitCurrentDateFunction [_ _] '(current-date))
  (visitCurrentTimeFunction [_ ctx] (fn-with-precision 'current-time (.precision ctx)))
  (visitCurrentTimestampFunction [_ ctx] (fn-with-precision 'current-timestamp (.precision ctx)))

  (visitSnapshotTimeFunction [_ _]
    (-> '(snapshot_time)
        (vary-meta assoc :identifier 'snapshot_time)))

  (visitLocalTimeFunction [_ ctx] (fn-with-precision 'local-time (.precision ctx)))
  (visitLocalTimestampFunction [_ ctx] (fn-with-precision 'local-timestamp (.precision ctx)))

  (visitCurrentInstantFunction0 [this ctx] (-> (.currentInstantFunction ctx) (.accept this)))
  (visitCurrentSettingFunction [this ctx]
    (xt/template (current-setting ~(-> (.expr ctx) (.accept this)))))

  (visitDateTruncFunction [this ctx]
    (let [dtp (-> (.dateTruncPrecision ctx) (.getText) (str/upper-case))
          dts (-> (.dateTruncSource ctx) (.expr) (.accept this))
          dt-tz (some-> (.dateTruncTimeZone ctx) (.characterString) (.accept this))]
      (if dt-tz
        (list 'date_trunc dtp dts dt-tz)
        (list 'date_trunc dtp dts))))

  (visitDateBinFunction [this ctx]
    (xt/template
     (date-bin ~(-> (.intervalLiteral ctx) (.accept this))
               ~(-> (.expr (.dateBinSource ctx)) (.accept this))
               ~@(some-> (.dateBinOrigin ctx)
                         .expr
                         (.accept this)
                         vector))))

  (visitRangeBinsFunction [this ctx]
    (let [p (-> (.rangeBinsSource ctx) (.expr) (.accept this))]
      (xt/template
       (range-bins ~(-> (.intervalLiteral ctx) (.accept this))
                   (lower ~p)
                   (upper ~p)
                   ~@(some-> (.dateBinOrigin ctx)
                             .expr
                             (.accept this)
                             vector)))))

  (visitAgeFunction [this ctx]
    (let [ve1 (-> (.expr ctx 0) (.accept this))
          ve2 (-> (.expr ctx 1) (.accept this))]
      (list 'age ve1 ve2)))

  (visitGenerateSeriesFunction [this ctx]
    (.accept (.generateSeries ctx) this))

  (visitGenerateSeries [this ctx]
    (xt/template (generate_series ~(.accept (.expr (.seriesStart ctx)) this)
                                  ~(.accept (.expr (.seriesEnd ctx)) this)
                                  ~(or (some-> (.seriesStep ctx) (.expr) (.accept this))
                                       1))))

  (visitObjectExpr [this ctx] (.accept (.objectConstructor ctx) this))

  (visitObjectConstructor [this ctx]
    (->> (for [^Sql$ObjectNameAndValueContext kv (.objectNameAndValue ctx)]
           (MapEntry/create (keyword (-> (.objectName kv) (.accept this)))
                            (-> (.expr kv) (.accept this))))
         (into {})))

  (visitObjectName [_ ctx] (identifier-sym (.identifier ctx)))

  (visitArrayExpr [this ctx] (.accept (.arrayValueConstructor ctx) this))

  (visitArrayValueConstructorByEnumeration [this ctx]
    (mapv #(.accept ^ParserRuleContext % this) (.expr ctx)))

  (visitTrimArrayFunction [this ctx]
    (let [ve-1 (-> (.expr ctx 0) (.accept this))
          ve-2 (-> (.expr ctx 1) (.accept this))]
      (list 'trim-array ve-1 ve-2)))

  (visitCharacterSubstringFunction [this ctx]
    (let [cve (-> (.expr ctx) (.accept this))
          sp (-> (.startPosition ctx) (.expr) (.accept this))
          sl (some-> (.stringLength ctx) (.expr) (.accept this))]
      (if sl
        (list 'substring cve sp sl)
        (list 'substring cve sp))))

  (visitLowerFunction [this ctx] (list 'lower (-> (.expr ctx) (.accept this))))
  (visitLowerInfFunction [this ctx] (list 'lower_inf (-> (.expr ctx) (.accept this))))

  (visitUpperFunction [this ctx] (list 'upper (-> (.expr ctx) (.accept this))))
  (visitUpperInfFunction [this ctx] (list 'upper_inf (-> (.expr ctx) (.accept this))))

  (visitLocalNameFunction [this ctx] (list 'local_name (-> (.expr ctx) (.accept this))))
  (visitNamespaceFunction [this ctx] (list 'namespace (-> (.expr ctx) (.accept this))))

  (visitTrimFunction [this ctx]
    (let [trim-fn (case (some-> (.trimSpecification ctx) (.getText) (str/upper-case))
                    "LEADING" 'trim-leading
                    "TRAILING" 'trim-trailing
                    'trim)
          trim-char (some-> (.trimCharacter ctx) (.expr) (.accept this))
          nve (-> (.trimSource ctx) (.expr) (.accept this))]
      (list trim-fn nve (or trim-char " "))))

  (visitOverlayFunction [this ctx]
    (let [target (-> (.expr ctx 0) (.accept this))
          placing (-> (.expr ctx 1) (.accept this))
          pos (-> (.startPosition ctx) (.expr) (.accept this))
          len (some-> (.stringLength ctx) (.expr) (.accept this))]
      (if len
        (list 'overlay target placing pos len)
        (list 'overlay target placing pos (list 'default-overlay-length placing)))))

  (visitReplaceFunction [this ctx]
    (xt/template
     #_{:clj-kondo/ignore [:invalid-arity]}
     (replace ~(.accept (.expr ctx) this)
              ~(.accept (.expr (.replaceTarget ctx)) this)
              ~(.accept (.expr (.replacement ctx)) this))))

  (visitCurrentUserFunction [_ _] '(current-user))
  (visitCurrentSchemaFunction [_ _] '(current-schema))
  (visitCurrentSchemasFunction [this ctx] (list 'current-schemas (-> (.expr ctx) (.accept this))))
  (visitCurrentDatabaseFunction [_ _] '(current-database))

  (visitSimpleCaseExpr [this ctx]
    (let [case-operand (-> (.expr ctx) (.accept this))
          when-clauses (->> (.simpleWhenClause ctx)
                            (mapv #(.accept ^Sql$SimpleWhenClauseContext % this))
                            (reduce into []))
          else-clause (some-> (.elseClause ctx) (.accept this))]
      (list* 'case case-operand (cond-> when-clauses
                                  else-clause (conj else-clause)))))

  (visitSearchedCaseExpr [this ctx]
    (let [when-clauses (->> (.searchedWhenClause ctx)
                            (mapv #(.accept ^Sql$SearchedWhenClauseContext % this))
                            (reduce into []))
          else-clause (some-> (.elseClause ctx) (.accept this))]
      (list* 'cond (cond-> when-clauses
                     else-clause (conj else-clause)))))

  (visitSimpleWhenClause [this ctx]
    (let [when-operands (-> (.whenOperandList ctx) (.whenOperand))
          when-exprs (mapv #(.accept (.getChild ^Sql$WhenOperandContext % 0) this) when-operands)
          then-expr (-> (.expr ctx) (.accept this))]
      (->> (for [when-expr when-exprs]
             [when-expr then-expr])
           (reduce into []))))

  (visitSearchedWhenClause [this ctx]
    (let [expr1 (-> (.expr ctx 0) (.accept this))
          expr2 (-> (.expr ctx 1) (.accept this))]
      [expr1 expr2]))

  (visitElseClause [this ctx] (-> (.expr ctx) (.accept this)))

  (visitNullIfExpr [this ctx]
    (list 'nullif
          (-> (.expr ctx 0) (.accept this))
          (-> (.expr ctx 1) (.accept this))))

  (visitCoalesceExpr [this ctx]
    (list* 'coalesce (mapv #(.accept ^ParserRuleContext % this) (.expr ctx))))

  (visitCastExpr [this ctx]
    (let [ve (-> (.expr ctx) (.accept this))
          data-type (-> (.dataType ctx) (.accept (->CastArgsVisitor env)))]
      (handle-cast-expr ve data-type)))

  (visitPostgresCastExpr [this ctx]
    (let [ve (-> (.exprPrimary ctx) (.accept this))
          data-type (-> (.dataType ctx) (.accept (->CastArgsVisitor env)))]
      (handle-cast-expr ve data-type)))

  (visitAggregateFunctionExpr [{:keys [!aggs !agg-subqs] :as this} ctx]
    (if-not !aggs
      (add-err! env (->AggregatesDisallowed))
      (-> (.aggregateFunction ctx) (.accept (assoc this :!subqs !agg-subqs)))))

  (visitCountStarFunction [{{:keys [!id-count]} :env, :keys [^Map !aggs]} _ctx]
    (let [agg-sym (-> (->col-sym (str "_row_count_" (swap! !id-count inc)))
                      (vary-meta assoc :agg-out-sym? true))]
      (.put !aggs agg-sym {:agg-expr '(row-count)})
      agg-sym))

  (visitArrayAggFunction [{{:keys [!id-count]} :env, :keys [^Map !aggs], :as this} ctx]
    (if (.sortSpecificationList ctx)
      (throw (UnsupportedOperationException. "array-agg sort-spec"))

      (let [agg-sym (-> (->col-sym (str "_array_agg_out" (swap! !id-count inc)))
                        (vary-meta assoc :agg-out-sym? true))
            expr (-> (.expr ctx)
                     (.accept (assoc this :!aggs nil, :scope (assoc scope :!implied-gicrs nil))))]
        (.put !aggs agg-sym
              (if (:column? (meta expr))
                {:agg-expr (list 'array-agg expr)}
                (let [in-sym (-> (->col-sym (str "_array_agg_in" (swap! !id-count inc)))
                                 (vary-meta assoc :agg-in-sym? true))]
                  {:agg-expr (list 'array-agg in-sym)
                   :in-projection (->ProjectedCol {in-sym expr} in-sym)})))

        agg-sym)))

  (visitSetFunction [{{:keys [!id-count]} :env, :keys [^Map !aggs], :as this} ctx]
    (let [set-fn (symbol (str/lower-case (cond-> (.getText (.setFunctionType ctx))
                                           (= "distinct" (some-> (.setQuantifier ctx) (.getText) (str/lower-case)))
                                           (str "_distinct"))))
          agg-sym (-> (->col-sym (str "_" set-fn "_out_" (swap! !id-count inc)))
                      (vary-meta assoc :agg-out-sym? true))
          expr (-> (.expr ctx)
                   (.accept (assoc this :!aggs nil, :scope (assoc scope :!implied-gicrs nil))))]
      (.put !aggs agg-sym
            (if (:column? (meta expr))
              {:agg-expr (list set-fn expr)}

              (let [in-sym (-> (->col-sym (str "_" set-fn "_in_" (swap! !id-count inc)))
                               (vary-meta assoc :agg-in-sym? true))]
                {:agg-expr (list set-fn in-sym)
                 :in-projection (->ProjectedCol {in-sym expr} in-sym)})))

      agg-sym))

  (visitWindowFunctionExpr [{:keys [^Map !windows] :as this} ctx]
    (if-not !windows
      (add-err! env (->WindowFunctionsDisallowed))
      (let [[window-sym window-projection] (-> (.windowFunctionType ctx) (.accept this))
            [window-name window-spec] (-> (.windowNameOrSpecification ctx) (.accept this))]
        (.put !windows window-sym {:windows {window-name window-spec}
                                   :projections [{window-sym (-> window-projection
                                                                 (assoc :window-name window-name))}]})

        window-sym)))

  (visitWindowNameOrSpecification [this ctx]
    (if-let [_window-name-ctx (.windowName ctx)]
      (throw (UnsupportedOperationException. "TODO: Window names currently not supported!"))
      (-> (.windowSpecification ctx) (.accept this))))

  (visitRowNumberWindowFunction [{{:keys [!id-count]} :env} _]
    (let [window-sym (-> (->col-sym (str "xt$row_number_" (swap! !id-count inc)))
                         (vary-meta assoc :window-out-sym? true))]
      [window-sym {:window-agg '(row-number)}]))

  (visitWindowSpecification [this ctx]
    (-> (.windowSpecificationDetails ctx) (.accept this)))

  (visitWindowSpecificationDetails [{{:keys [!id-count]} :env :as this} ctx]
    (let [window-name (symbol (str "window-name" (swap! !id-count inc)))
          partition-cols (some-> (.windowPartitionClause ctx) (.accept this))
          order-specs (some-> (.windowOrderClause ctx) (.accept this))
          _frame (some-> (.windowFrameClause ctx) (.accept this))]
      [window-name (cond-> {}
                     partition-cols (assoc :partition-cols partition-cols)
                     order-specs (assoc :order-specs order-specs))]))

  (visitWindowPartitionClause [this ctx]
    (-> (.windowPartitionColumnReferenceList ctx) (.accept this)))

  (visitWindowPartitionColumnReferenceList [this ctx]
    (mapv #(.accept ^ParserRuleContext % this) (.windowPartitionColumnReference ctx)))

  (visitWindowPartitionColumnReference [this ctx]
    (-> (.columnReference ctx) (.accept this)))

  (visitWindowOrderClause [_this ctx]
    (plan-sort-specification-list (.sortSpecificationList ctx) env scope nil))

  (visitWindowFrameClause [_this _ctx]
    (throw (UnsupportedOperationException. "TODO: Window frames currently not supported!")))

  (visitScalarSubqueryExpr [{:keys [!subqs]} ctx]
    (plan-sq (.subquery ctx) env scope !subqs
             {:sq-type :scalar}))

  (visitNestOneSubqueryExpr [{:keys [!subqs]} ctx]
    (plan-sq (.subquery ctx) env scope !subqs
             {:sq-type :nest-one}))

  (visitNestManySubqueryExpr [{:keys [!subqs]} ctx]
    (plan-sq (.subquery ctx) env scope !subqs
             {:sq-type :nest-many}))

  (visitArrayValueConstructorByQuery [{:keys [!subqs]} ctx]
    (plan-sq (.subquery ctx) env scope !subqs
             {:sq-type :array-by-query}))

  (visitExistsPredicate [{:keys [!subqs]} ctx]
    (plan-sq (.subquery ctx) env scope !subqs
             {:sq-type :exists}))

  (visitQuantifiedComparisonPredicate [this ctx]
    (let [quantifier (case (str/lower-case (.getText (.quantifier ctx)))
                       "all" :all
                       ("some" "any") :any)
          op (symbol (.getText (.compOp ctx)))]
      (.accept (.quantifiedComparisonPredicatePart3 ctx)
               (assoc this :qc-pt2 {:expr (.accept (.expr ctx) this)
                                    :op (cond-> op
                                          (= quantifier :all) negate-op)}))))

  (visitQuantifiedComparisonPredicatePart2 [{:keys [pt1] :as this} ctx]
    (let [quantifier (case (str/lower-case (.getText (.quantifier ctx)))
                       "all" :all
                       ("some" "any") :any)
          op (symbol (.getText (.compOp ctx)))]
      (.accept (.quantifiedComparisonPredicatePart3 ctx)
               (assoc this :qc-pt2 {:expr pt1
                                    :op (cond-> op
                                          (= quantifier :all) negate-op)}))))

  (visitQuantifiedComparisonSubquery [{:keys [!subqs qc-pt2]} ctx]
    (plan-sq (.subquery ctx) env scope !subqs
             (into {:sq-type :quantified-comparison}
                   qc-pt2)))

  (visitQuantifiedComparisonExpr [{{:keys [!id-count]} :env, :keys [qc-pt2, ^Map !subqs], :as this} ctx]
    (let [sq-sym (-> (->col-sym (str "_sq_" (swap! !id-count inc)))
                     (vary-meta assoc :sq-out-sym? true))

          ;; HACK: removing the scope. will unblock #3539,
          ;; but I wasn't sure of the semantics in the general case
          expr (.accept (.expr ctx) (assoc this :scope nil))

          expr-sym (->col-sym (str "_qc_expr_" (swap! !id-count inc)))
          query-plan (->QueryExpr [:table {expr-sym expr}]
                                  [expr-sym])]

      (.put !subqs sq-sym (into {:sq-type :quantified-comparison
                                 :query-plan query-plan}
                                qc-pt2))

      sq-sym))

  (visitInPredicate [{:keys [!subqs] :as this} ctx]
    (let [^ParserRuleContext
          sq-ctx (.accept (.inPredicateValue ctx)
                          (reify SqlVisitor
                            (visitInSubquery [_ ctx] (.subquery ctx))
                            (visitInRowValueList [_ ctx] (.rowValueList ctx))))]

      (cond->> (plan-sq sq-ctx env scope !subqs
                        {:sq-type :quantified-comparison
                         :expr (.accept (.expr ctx) this)
                         :op '=})
        (boolean (.NOT ctx)) (list 'not))))

  (visitInPredicatePart2 [{:keys [pt1 !subqs]} ctx]
    (let [^ParserRuleContext
          sq-ctx (.accept (.inPredicateValue ctx)
                          (reify SqlVisitor
                            (visitInSubquery [_ ctx] (.subquery ctx))
                            (visitInRowValueList [_ ctx] (.rowValueList ctx))))]

      (cond->> (plan-sq sq-ctx env scope !subqs
                        {:sq-type :quantified-comparison
                         :expr pt1
                         :op '=})
        (boolean (.NOT ctx)) (list 'not))))

  (visitFetchFirstRowCount [this ctx]
    (if-let [ps (.parameterSpecification ctx)]
      (.accept ps this)
      (parse-long (.getText ctx))))

  (visitOffsetRowCount [this ctx]
    (if-let [ps (.parameterSpecification ctx)]
      (.accept ps this)
      (parse-long (.getText ctx))))

  (visitPostgresVersionFunction [_ ctx]
    (-> (or (when-let [schema-name (identifier-sym (.schemaName ctx))]
              (when (= schema-name 'xt)
                'xtdb/xtdb-server-version))
            'xtdb/postgres-server-version)
        (vary-meta assoc :identifier (->col-sym 'version))))

  (visitPostgresTableIsVisibleFunction [_ _ctx]
    ;; FIXME - when we have authorization - need to check permissions
    true)

  (visitPostgresGetUserByIdFunction [_ _ctx]
    ;; FIXME - when we have ownership - need to check owner
    "xtdb"))

(defn- wrap-predicates [plan predicate]
  (or (when (seq? predicate)
        (let [[f & args] predicate]
          (when (= 'and f)
            (reduce wrap-predicates plan args))))

      [:select predicate
       plan]))

(defrecord ColumnCountMismatch [expected given]
  PlanError
  (error-string [_] (format "Column count mismatch: expected %s, given %s" expected given)))

(defn- plan-sort-specification-list [^Sql$SortSpecificationListContext ctx
                                     {:keys [!id-count] :as env} inner-scope outer-col-syms]
  (let [outer-cols (->> outer-col-syms
                        (into {} (mapcat (juxt (juxt identity identity)
                                               (juxt (comp symbol name) identity)))))
        ob-expr-visitor (->ExprPlanVisitor env
                                           (reify Scope
                                             (available-cols [_]
                                               (set/union (available-cols inner-scope) (set (keys outer-cols))))

                                             (-find-cols [_ [col-name :as chain] excl-cols]
                                               (assert col-name)
                                               (or (when (= 1 (count chain))
                                                     (when-let [sym (get outer-cols col-name)]
                                                       (when-not (contains? excl-cols col-name)
                                                         [sym])))
                                                   (find-cols inner-scope chain excl-cols)))))]

    (-> (.sortSpecification ctx)
        (->> (mapv (fn [^Sql$SortSpecificationContext sort-spec-ctx]
                     (let [expr (-> (.expr sort-spec-ctx) (.accept ob-expr-visitor))
                           dir (or (some-> (.orderingSpecification sort-spec-ctx)
                                           (.getText)
                                           str/lower-case
                                           keyword)
                                   :asc)

                           ob-opts {:direction dir

                                    :null-ordering (or (when-let [null-order (.nullOrdering sort-spec-ctx)]
                                                         (case (-> (.getChild null-order 1)
                                                                   (.getText)
                                                                   str/lower-case)
                                                           "first" :nulls-first
                                                           "last" :nulls-last))

                                                       ;; postgres sorts nulls high - so last on asc and first on desc
                                                       (case dir :asc :nulls-last, :desc :nulls-first))}]

                       ;; we could potentially try to avoid this extra projection for simple cols
                       (cond
                         (integer? expr) (if (<= 1 (long expr) (count outer-col-syms))
                                           {:order-by-spec [(nth outer-col-syms (dec expr)) ob-opts]}
                                           (add-err! env (->InvalidOrderByOrdinal outer-col-syms expr)))

                         (contains? outer-cols expr) {:order-by-spec [expr ob-opts]}

                         :else (let [in-sym (->col-sym (str "_ob" (swap! !id-count inc)))]
                                 {:order-by-spec [in-sym ob-opts]
                                  :in-projection {in-sym expr}})))))))))

(defn- plan-order-by [^Sql$OrderByClauseContext ctx env inner-scope outer-col-syms]
  (plan-sort-specification-list (.sortSpecificationList ctx) env inner-scope outer-col-syms))

(defn- wrap-isolated-ob [plan outer-col-syms ob-specs]
  (let [in-projs (not-empty (into [] (keep :in-projection) ob-specs))]
    (as-> plan plan
      (if in-projs
        [:map in-projs plan]
        plan)

      [:order-by (mapv :order-by-spec ob-specs)
       plan]

      (if in-projs
        [:project outer-col-syms plan]
        plan))))

(defn- wrap-integrated-ob [plan projected-cols ob-specs]
  (let [in-projs (not-empty (into [] (keep :in-projection) ob-specs))]
    (as-> plan plan
      [:project (into (mapv :projection projected-cols)
                      in-projs)
       plan]

      [:order-by (mapv :order-by-spec ob-specs)
       plan]

      (if in-projs
        [:project (mapv :col-sym projected-cols) plan]
        plan))))

(defn- wrap-windows [plan windows]
  (when (> (count windows) 1)
    (throw (UnsupportedOperationException. "TODO: only one window function supported at the moment!")))

  (let [{:keys [windows] :as window-opts} (apply merge-with into (vals windows))
        in-projs (not-empty (->> (mapcat :order-specs (vals windows))
                                 (into [] (keep :in-projection))))
        window-opts (->> (update-vals windows (fn [window-name->window-spec]
                                                (update window-name->window-spec :order-specs #(mapv :order-by-spec %))))
                         (assoc window-opts :windows))]

    (as-> plan plan
      (if in-projs
        [:map in-projs plan]
        plan)

      [:window window-opts
       plan]

      ;; hygienics happen in the order by planning
      )))

(defrecord DuplicateColumnProjection [col-sym]
  PlanError
  (error-string [_] (str "Duplicate column projection: " col-sym)))

(defn dups [seq]
  (for [[id freq] (frequencies seq)
        :when (> freq 1)]
    id))

(defn- remove-ns-qualifiers [{:keys [plan col-syms]} env]
  (let [out-projections (->> col-syms
                             (into [] (map (fn [col-sym]
                                             (if (namespace col-sym)
                                               (let [out-sym (-> (->col-sym (name col-sym))
                                                                 (with-meta (meta col-sym)))]
                                                 (->ProjectedCol {out-sym col-sym}
                                                                 out-sym))
                                               (->ProjectedCol col-sym col-sym))))))
        out-col-syms (mapv :col-sym out-projections)
        duplicate-col-syms (not-empty (dups out-col-syms))]
    (if duplicate-col-syms
      (doseq [sym duplicate-col-syms]
        (add-err! env (->DuplicateColumnProjection sym)))
      (->QueryExpr [:project (mapv :projection out-projections) plan] out-col-syms))))

(defrecord SetOperationColumnCountMismatch [operation-type lhs-count rhs-count]
  PlanError
  (error-string [_] 
    (format "Column count mismatch on %s set operation: lhs column count %s, rhs column count %s" operation-type lhs-count rhs-count)))

(defrecord InsertWithoutXtId []
  PlanError
  (error-string [_] "INSERT does not contain mandatory _id column"))

(defrecord InvalidParamIndex [arg-count param-idx]
  PlanError
  (error-string [_] (format "Invalid parameter index: %d (%d arguments provided)" param-idx arg-count)))

(defrecord MissingColumnNameList []
  PlanError
  (error-string [_ ] "INSERT with VALUES needs column name list"))

(defrecord TableRowsVisitor [env scope out-col-syms]
  SqlVisitor
  (visitTableValueConstructor [this ctx] (.accept (.rowValueList ctx) this))
  (visitRecordsValueConstructor [this ctx] (.accept (.recordsValueList ctx) this))

  (visitRowValueList [_ ctx]
    (let [expr-plan-visitor (->ExprPlanVisitor env scope)
          col-syms (or out-col-syms
                       (-> (.rowValueConstructor ctx 0)
                           (.accept
                            (reify SqlVisitor
                              (visitSingleExprRowConstructor [_ _ctx]
                                '[_column_1])

                              (visitMultiExprRowConstructor [_ ctx]
                                (->> (.expr ctx)
                                     (into [] (map-indexed (fn [idx _]
                                                             (->col-sym (str "_column_" (inc idx))))))))))))

          col-keys (mapv keyword col-syms)
          col-count (count col-keys)

          row-visitor (reify SqlVisitor
                        (visitSingleExprRowConstructor [_ ctx]
                          (let [expr (.expr ctx)]
                            (if (not= 1 col-count)
                              (add-err! env (->ColumnCountMismatch col-count 1))
                              {(first col-keys) (.accept expr expr-plan-visitor)})))

                        (visitMultiExprRowConstructor [_ ctx]
                          (let [exprs (.expr ctx)]
                            (if (not= (count exprs) col-count)
                              (add-err! env (->ColumnCountMismatch col-count (count exprs)))
                              (->> (map (fn [col ^ParserRuleContext expr]
                                          (MapEntry/create col
                                                           (.accept expr expr-plan-visitor)))
                                        col-keys
                                        exprs)
                                   (into {}))))))]

      {:rows (->> (.rowValueConstructor ctx)
                  (mapv #(.accept ^ParserRuleContext % row-visitor)))
       :col-syms col-syms}))

  (visitRecordsValueList [_ ctx]
    (let [expr-plan-visitor (->ExprPlanVisitor env scope)
          arg-fields (:arg-fields env)

          col-syms (or out-col-syms
                       (letfn [(emit-param [param-idx]
                                 (when-not arg-fields
                                   (throw (err/illegal-arg ::records-param-outside-DML
                                                           {::err/message "RECORDS ? not supported outside of DML"})))

                                 (if-let [arg-field (nth arg-fields param-idx nil)]
                                   (vec (for [^Field child-field (types/flatten-union-field arg-field)
                                              :when (= #xt.arrow/type :struct (.getType child-field))
                                              ^Field struct-key (.getChildren child-field)]
                                          (symbol (.getName struct-key))))

                                   (add-err! env (->InvalidParamIndex param-idx (count arg-fields)))))]

                         (->> (.recordValueConstructor ctx)
                              (into [] (comp (mapcat (partial accept-visitor
                                                              (reify SqlVisitor
                                                                (visitParameterRecord [this ctx] (.accept (.parameterSpecification ctx) this))

                                                                (visitDynamicParameter [_ _] (emit-param @(:!param-count env)))
                                                                (visitPostgresParameter [_ ctx]
                                                                  (emit-param (dec (parse-long (subs (.getText ctx) 1)))))

                                                                (visitObjectRecord [this ctx]
                                                                  (->> (.objectNameAndValue (.objectConstructor ctx))
                                                                       (into #{} (map (partial accept-visitor this)))))

                                                                (visitObjectNameAndValue [_ ctx]
                                                                  (identifier-sym (.objectName ctx))))))
                                             (distinct))))))

          col-keys (mapv keyword col-syms)

          row-visitor (reify SqlVisitor
                        (visitParameterRecord [_ ctx]
                          (.accept (.parameterSpecification ctx) expr-plan-visitor))
                        (visitObjectRecord [_ ctx]
                          (-> (.accept (.objectConstructor ctx) expr-plan-visitor)
                              (select-keys col-keys))))]

      {:rows (->> (.recordValueConstructor ctx)
                  (mapv (partial accept-visitor row-visitor)))
       :col-syms col-syms})))

(defn- ->qs-scope [{:keys [env scope]} table-refs]
  (->QuerySpecificationScope scope
                             (reduce (fn [left-table-ref ^ParserRuleContext table-ref]
                                       (let [!sq-refs (HashMap.)
                                             left-sq-scope (->SubqueryScope env left-table-ref !sq-refs)
                                             right-table-ref (.accept table-ref (->TableRefVisitor env scope (when left-table-ref
                                                                                                               left-sq-scope)))]
                                         (if left-table-ref
                                           (->CrossJoinTable env !sq-refs left-table-ref right-table-ref)
                                           right-table-ref)))
                                     nil
                                     table-refs)))

(defn- wrap-where [scope, {:keys [env]}, ^Sql$WhereClauseContext where-clause]
  (let [!subqs (HashMap.)
        predicate (-> (.searchCondition where-clause)
                      (.accept (map->ExprPlanVisitor {:env env, :scope scope, :!subqs !subqs})))]
    (reify
      Scope
      (available-cols [_] (available-cols scope))
      (-find-cols [_ chain excl-cols] (-find-cols scope chain excl-cols))

      PlanRelation
      (plan-rel [_]
        (-> (plan-rel scope)
            (apply-sqs !subqs)
            (wrap-predicates predicate))))))

(defn- wrap-query-tail [scope {:keys [env]}
                        ^Sql$GroupByClauseContext group-by-clause
                        ^Sql$HavingClauseContext having-clause
                        ^Sql$SelectClauseContext select-clause
                        order-by-clause]
  (let [!unresolved-cr (HashSet.)
        !implied-gicrs (HashSet.)
        group-invar-col-tracker (->GroupInvariantColsTracker env scope !implied-gicrs !unresolved-cr)

        having-plan (when having-clause
                      (let [!subqs (HashMap.)
                            !aggs (HashMap.)]
                        {:predicate (-> (.searchCondition having-clause)
                                        (.accept (map->ExprPlanVisitor {:env env, :scope group-invar-col-tracker, :!subqs !subqs, :!aggs !aggs})))
                         :subqs (not-empty (into {} !subqs))
                         :aggs (not-empty (into {} !aggs))}))


        {:keys [projected-cols windows agg-subqs] :as select-plan} (.accept select-clause (->SelectClauseProjectedCols env group-invar-col-tracker))
        aggs (not-empty (merge (:aggs select-plan) (:aggs having-plan)))
        grouped-table? (boolean (or aggs group-by-clause))
        group-invariant-cols (when grouped-table?
                               (if group-by-clause
                                 (.accept group-by-clause group-invar-col-tracker)
                                 (for [col-ref !implied-gicrs]
                                   col-ref)))

        ob-plan (some-> order-by-clause
                        (plan-order-by env scope
                                       (mapv :col-sym projected-cols)))]
    (reify
      Scope
      (available-cols [_]
        (->insertion-ordered-set (mapv :col-sym (:projected-cols select-plan))))

      (-find-cols [this [col-name table-name] excl-cols]
        (when (nil? table-name)
          (let [cols (available-cols this)
                cols-by-name (group-by (comp symbol name) cols)]
            (->> (if col-name
                   (cols-by-name (symbol (name col-name)))
                   cols)
                 (into [] (remove (set excl-cols)))))))

      PlanRelation
      (plan-rel [_]
        (as-> (plan-rel scope) plan
          (if-let [unresolved-cr (not-empty (into #{} !unresolved-cr))]
            [:map (mapv #(hash-map % nil) unresolved-cr)
             plan]
            plan)

          (cond-> plan
            agg-subqs (apply-sqs agg-subqs))

          (cond-> plan
            grouped-table? (wrap-aggs aggs group-invariant-cols))

          (if-let [{:keys [predicate subqs]} having-plan]
            (-> plan
                (apply-sqs subqs)
                (wrap-predicates predicate))
            plan)

          (-> plan (apply-sqs (:subqs select-plan)))

          (cond-> plan
            windows (wrap-windows windows))

          (if ob-plan
            (-> plan
                (wrap-integrated-ob projected-cols ob-plan))

            [:project (mapv :projection projected-cols)
             plan])

          (if (some-> select-clause .setQuantifier (.getText) (str/upper-case) (= "DISTINCT"))
            [:distinct plan]
            plan))))))

(defrecord QueryPlanVisitor [env scope]
  SqlVisitor
  (visitWrappedQuery [this ctx] (-> (.queryExpressionNoWith ctx) (.accept this)))

  (visitQueryExpression [this ctx]
    (.accept (.queryExpressionNoWith ctx)
             (-> this
                 (assoc-in [:env :ctes] (or (some-> (.withClause ctx)
                                                    (.accept (->WithVisitor env scope)))
                                            (:ctes env))))))

  (visitQueryExpressionNoWith [{:keys [env] :as this} ctx]
    (let [order-by-ctx (.orderByClause ctx)

          qeb-ctx (.queryExpressionBody ctx)

          ;; see SQL:2011 §7.13, syntax rule 28c
          simple-table-query? (when (instance? Sql$QueryBodyTermContext qeb-ctx)
                                (let [term (.queryTerm ^Sql$QueryBodyTermContext qeb-ctx)]
                                  (when (instance? Sql$QuerySpecificationContext term)
                                    (let [^Sql$QuerySpecificationContext qs-ctx term]
                                      (or (.selectClause qs-ctx)
                                          (seq (.queryTail qs-ctx)))))))]

      (as-> (.accept qeb-ctx (cond-> (assoc this :scope scope)
                               simple-table-query? (assoc :order-by-ctx order-by-ctx)))
          {:keys [plan col-syms] :as query-expr}

        (if (and order-by-ctx (not simple-table-query?))
          (->QueryExpr (-> plan
                           (wrap-isolated-ob col-syms (plan-order-by order-by-ctx env nil col-syms)))
                       col-syms)

          query-expr)

        (remove-ns-qualifiers query-expr env)

        (let [offset+limit (.offsetAndLimit ctx)
              offset-clause (some-> offset+limit .resultOffsetClause)
              limit-clause (some-> offset+limit .fetchFirstClause)]
          (cond-> query-expr
            (or offset-clause limit-clause)
            (update :plan (fn [plan]
                            (let [expr-visitor (->ExprPlanVisitor env scope)]
                              [:top {:skip (some-> offset-clause
                                                   (.offsetRowCount)
                                                   (.accept expr-visitor))

                                     :limit (when limit-clause
                                              (or (some-> (.fetchFirstRowCount limit-clause)
                                                          (.accept expr-visitor))
                                                  1))}
                               plan]))))))))

  (visitQueryBodyTerm [this ctx] (.accept (.queryTerm ctx) this))

  (visitUnionQuery [this ctx]
    (let [{l-plan :plan, l-col-syms :col-syms} (-> (.queryExpressionBody ctx) (.accept this)
                                                   (remove-ns-qualifiers env))

          {r-plan :plan, r-col-syms :col-syms} (-> (.queryTerm ctx) (.accept this)
                                                   (remove-ns-qualifiers env))

          _ (when-not (= (count l-col-syms) (count r-col-syms))
              (add-err! env (->SetOperationColumnCountMismatch "UNION" (count l-col-syms) (count r-col-syms))))
          
          rename-col-syms (fn [plan]
                            (if (not= l-col-syms r-col-syms)
                              [:rename (zipmap r-col-syms l-col-syms) plan]
                              plan)) 
          
          plan [:union-all l-plan (rename-col-syms r-plan)]]
      (->QueryExpr (if-not (.ALL ctx) [:distinct plan] plan) l-col-syms)))

  (visitExceptQuery [this ctx]
    (let [{l-plan :plan, l-col-syms :col-syms} (-> (.queryExpressionBody ctx) (.accept this)
                                                   (remove-ns-qualifiers env))

          {r-plan :plan, r-col-syms :col-syms} (-> (.queryTerm ctx) (.accept this)
                                                   (remove-ns-qualifiers env))

          _ (when-not (= (count l-col-syms) (count r-col-syms))
              (add-err! env (->SetOperationColumnCountMismatch "EXCEPT" (count l-col-syms) (count r-col-syms))))

          rename-col-syms (fn [plan] 
                            (if (not= l-col-syms r-col-syms)
                              [:rename (zipmap r-col-syms l-col-syms) plan]
                              plan))
          
          wrap-distinct (fn [plan]
                          (if (not (.ALL ctx))
                            [:distinct plan]
                            plan))]
      
      (->QueryExpr [:difference
                    (wrap-distinct l-plan)
                    (rename-col-syms (wrap-distinct r-plan))]
                   l-col-syms)))

  (visitIntersectQuery [this ctx]
    (let [{l-plan :plan, l-col-syms :col-syms} (-> (.queryTerm ctx 0) (.accept this)
                                                   (remove-ns-qualifiers env))

          {r-plan :plan, r-col-syms :col-syms} (-> (.queryTerm ctx 1) (.accept this)
                                                   (remove-ns-qualifiers env))

          _ (when-not (= (count l-col-syms) (count r-col-syms))
              (add-err! env (->SetOperationColumnCountMismatch "INTERSECT" (count l-col-syms) (count r-col-syms))))
          
          rename-col-syms (fn [plan]
                            (if (not= l-col-syms r-col-syms)
                              [:rename (zipmap r-col-syms l-col-syms) plan]
                              plan))
          
          plan [:intersect l-plan (rename-col-syms r-plan)]]
      (->QueryExpr (if-not (.ALL ctx)
                     [:distinct plan]
                     plan)
                   l-col-syms)))

  (visitQuerySpecification [{:keys [out-col-syms order-by-ctx] :as this} ctx]
    (let [qs-scope (->qs-scope this (some->> (.fromClause ctx) (.tableReference)))
          rel (if-let [select-clause (.selectClause ctx)]
                (let [where-clause (.whereClause ctx)]
                  (-> qs-scope
                      (cond-> where-clause (wrap-where this where-clause)
                              select-clause (wrap-query-tail this (.groupByClause ctx) (.havingClause ctx) select-clause order-by-ctx))))

                (letfn [(wrap-tail [order-by-ctx, rel, ^Sql$QueryTailContext tail]
                          (let [where-clause (some-> tail .whereClause)
                                select-clause (some-> tail .selectClause)]
                            (-> rel
                                (cond-> where-clause (wrap-where this where-clause)
                                        select-clause (wrap-query-tail this (.groupByClause tail) (.havingClause tail) select-clause order-by-ctx)))))]
                  (let [tails (.queryTail ctx)]
                    (as-> qs-scope rel
                      (reduce (partial wrap-tail nil) rel (butlast tails))
                      (wrap-tail order-by-ctx rel (last tails))))))

          projections (->> (find-cols rel nil)
                           (into [] (map-indexed ->projected-col-expr)))

          plan [:project (mapv :projection projections)
                (plan-rel rel)]

          col-syms (mapv :col-sym projections)]

      (if out-col-syms
        (let [out-count (count out-col-syms)
              in-count (count col-syms)]
          (if (not= out-count in-count)
            (add-err! env (->ColumnCountMismatch out-count in-count))

            (->QueryExpr [:rename (zipmap col-syms out-col-syms)
                          plan]
                         out-col-syms)))
        (->QueryExpr plan col-syms))))

  (visitValuesQuery [this ctx] (-> (.tableValueConstructor ctx) (.accept this)))
  (visitTableValueConstructor [this ctx] (-> (.rowValueList ctx) (.accept this)))

  (visitRowValueList [{{:keys [!id-count]} :env, :keys [out-col-syms]} ctx]
    (let [unique-table-alias (symbol (str "xt.values." (swap! !id-count inc)))
          {:keys [rows col-syms]} (.accept ctx (->TableRowsVisitor env scope out-col-syms))]
      (->QueryExpr [:rename unique-table-alias
                    [:table col-syms
                     rows]]

                   (->> col-syms
                        (mapv #(->col-sym (str unique-table-alias) (str %)))))))

  (visitRecordsQuery [this ctx] (-> (.recordsValueConstructor ctx) (.accept this)))
  (visitRecordsValueConstructor [this ctx] (-> (.recordsValueList ctx) (.accept this)))

  (visitRecordsValueList [{{:keys [!id-count]} :env, :keys [out-col-syms]} ctx]
    (let [unique-table-alias (symbol (str "xt.values." (swap! !id-count inc)))
          {:keys [rows col-syms]} (.accept ctx (->TableRowsVisitor env scope out-col-syms))]
      (->QueryExpr [:rename unique-table-alias
                    [:table col-syms
                     rows]]

                   (->> col-syms
                        (mapv #(->col-sym (str unique-table-alias) (str %)))))))

  (visitSubquery [this ctx] (-> (.queryExpression ctx) (.accept this)))

  (visitInsertRecords [this ctx]
    (as-> (-> (.recordsValueConstructor ctx)
              (.accept (assoc this :out-col-syms (some->> (.columnNameList ctx)
                                                          (.columnName)
                                                          (mapv identifier-sym)))))
        {:keys [plan col-syms] :as query-expr}

      (remove-ns-qualifiers query-expr env)

      (if (some (comp types/temporal-column? str) col-syms)
        (->QueryExpr [:project (mapv (fn [col-sym]
                                       {col-sym
                                        (if (types/temporal-column? (str col-sym))
                                          (list 'cast col-sym types/temporal-col-type)
                                          col-sym)})
                                     col-syms)
                      plan]
                     col-syms)

        query-expr)))

  (visitInsertValues [this ctx]
    (if-let [out-col-syms (some->> (.columnNameList ctx)
                                   (.columnName)
                                   (mapv identifier-sym))]
      (as-> (-> (.tableValueConstructor ctx)
                (.accept (assoc this :out-col-syms out-col-syms)))
          {:keys [plan col-syms] :as query-expr}

        (remove-ns-qualifiers query-expr env)

        (if (some (comp types/temporal-column? str) col-syms)
          (->QueryExpr [:project (mapv (fn [col-sym]
                                         {col-sym
                                          (if (types/temporal-column? (str col-sym))
                                            (list 'cast col-sym types/temporal-col-type)
                                            col-sym)})
                                       col-syms)
                        plan]
                       col-syms)

          query-expr))
      (add-err! env (->MissingColumnNameList))))

  (visitInsertFromSubquery [this ctx]
    (let [out-col-syms (some->> (.columnNameList ctx) .columnName
                                (mapv identifier-sym))
          {:keys [plan col-syms] :as inner} (-> (.queryExpression ctx)
                                                (.accept (cond-> this
                                                           out-col-syms (assoc :out-col-syms out-col-syms))))]
      (if (some (comp types/temporal-column? str) col-syms)
        (->QueryExpr [:project (mapv (fn [col-sym]
                                       {col-sym
                                        (if (types/temporal-column? (str col-sym))
                                          (list 'cast col-sym types/temporal-col-type)
                                          col-sym)})
                                     col-syms)
                      plan]
                     col-syms)
        inner))))

(defrecord DmlTableRef [env table-name table-alias unique-table-alias for-valid-time cols ^Map !reqd-cols]
  Scope
  (available-cols [_] cols)

  (-find-cols [this [col-name table-name] excl-cols]
    (when (or (nil? table-name) (= table-name table-alias))
      (for [col (if col-name
                  (when (or (contains? cols col-name) (types/temporal-column? col-name)
                            (temporal-period-column? col-name))
                    [col-name])
                  (available-cols this))
            :when (not (contains? excl-cols col))]
        (.computeIfAbsent !reqd-cols (->col-sym col)
                          (fn [col]
                            (->col-sym (str unique-table-alias) (str col)))))))

  PlanRelation
  (plan-rel [_]
    [:rename unique-table-alias
     [:scan (cond-> {:table (symbol table-name)}
              for-valid-time (assoc :for-valid-time for-valid-time))
      (vec (.keySet !reqd-cols))]]))

(def ^:private vf-col (->col-sym "_valid_from"))
(def ^:private vt-col (->col-sym "_valid_to"))

(defn- dml-stmt-valid-time-portion [from-expr to-expr]
  {:for-valid-time [:in from-expr (when-not (= to-expr 'xtdb/end-of-time) to-expr)]
   :projection [{vf-col (xt/template
                         (greatest ~vf-col (cast ~(or from-expr '(current-timestamp)) ~types/temporal-col-type)))}

                {vt-col (if to-expr
                          (xt/template
                           (least (coalesce ~vt-col xtdb/end-of-time)
                                  (coalesce (cast ~to-expr ~types/temporal-col-type) xtdb/end-of-time)))

                          vt-col)}]})

(defrecord DmlValidTimeExtentsVisitor [env scope]
  SqlVisitor
  (visitDmlStatementValidTimeAll [_ _]
    {:for-valid-time :all-time,
     :projection [vf-col vt-col]})

  (visitDmlStatementValidTimePortion [_ ctx]
    (let [expr-visitor (->ExprPlanVisitor env scope)]
      (dml-stmt-valid-time-portion (-> (.from ctx) (.accept expr-visitor))
                                   (some-> (.to ctx) (.accept expr-visitor))))))

(def ^:private default-vt-extents-projection
  [{vf-col (list 'greatest vf-col (list 'cast '(current-timestamp) types/temporal-col-type))}
   vt-col])

(defrecord EraseTableRef [env table-name table-alias unique-table-alias cols ^Map !reqd-cols]
  Scope
  (available-cols [_] cols)

  (-find-cols [this [col-name table-name :as _chain] excl-cols]
    (when (or (nil? table-name) (= table-name table-alias))
      (for [col (if col-name
                  (when (or (contains? cols col-name) (types/temporal-column? col-name))
                    [col-name])
                  (available-cols this))
            :when (not (contains? excl-cols col))]
        (.computeIfAbsent !reqd-cols (->col-sym col)
                          (reify Function
                            (apply [_ col]
                              (->col-sym (str unique-table-alias) (str col))))))))

  PlanRelation
  (plan-rel [_]
    [:rename unique-table-alias
     [:scan {:table (symbol table-name)
             :for-system-time :all-time
             :for-valid-time :all-time}
      (vec (.keySet !reqd-cols))]]))

(defn forbidden-update-col? [col]
  (str/starts-with? (str col) "_"))

(def forbidden-insert-col?
  (every-pred forbidden-update-col?
              (complement '#{_id _valid_from _valid_to})))

(def forbidden-patch-col?
  (every-pred forbidden-update-col?
              (complement '#{_id})))

(defrecord ForbiddenColumnUpdate [col]
  PlanError
  (error-string [_] (format "Cannot UPDATE %s column" col)))

(defrecord ForbiddenColumnInsert [col]
  PlanError
  (error-string [_] (format "Cannot INSERT %s column" col)))

(defrecord ForbiddenColumnPatch [col]
  PlanError
  (error-string [_] (format "Cannot PATCH %s column" col)))

(defn plan-patch [{:keys [table-info]} {:keys [table valid-from valid-to patch-rel]}]
  (let [known-cols (mapv symbol (get table-info table))]
    (xt/template
     [:project [{_iid new/_iid}
                {_valid_from (coalesce old/_valid_from ~valid-from (current-timestamp))}
                {_valid_to (coalesce old/_valid_to ~valid-to xtdb/end-of-time)}
                {doc (_patch old/doc new/doc)}]
      [:left-outer-join [{new/_iid old/_iid}]
       [:rename new
        ~(:plan patch-rel)]
       [:rename old
        [:patch-gaps {:valid-from ~valid-from, :valid-to ~valid-to}
         [:project [_iid _valid_from _valid_to
                    {doc ~(into {} (map (juxt keyword identity)) known-cols)}]
          [:order-by [[_iid] [_valid_from]]
           [:scan {:table ~table,
                   :for-valid-time [:in ~valid-from ~valid-to]}
            [_iid _valid_from _valid_to
             ~@known-cols]]]]]]]])))

(defrecord InsertStmt [table query-plan]
  OptimiseStatement (optimise-stmt [this] (update-in this [:query-plan :plan] lp/rewrite-plan)))

(defrecord PatchStmt [table query-plan]
  OptimiseStatement (optimise-stmt [this] (update-in this [:query-plan :plan] lp/rewrite-plan)))

(defrecord UpdateStmt [table query-plan]
  OptimiseStatement (optimise-stmt [this] (update-in this [:query-plan :plan] lp/rewrite-plan)))

(defrecord DeleteStmt [table query-plan]
  OptimiseStatement (optimise-stmt [this] (update-in this [:query-plan :plan] lp/rewrite-plan)))

(defrecord EraseStmt [table query-plan]
  OptimiseStatement (optimise-stmt [this] (update-in this [:query-plan :plan] lp/rewrite-plan)))

(defrecord AssertStmt [query-plan]
  OptimiseStatement (optimise-stmt [this] (update-in this [:query-plan :plan] lp/rewrite-plan)))

(defrecord UserStmt [stmt]
  OptimiseStatement (optimise-stmt [this] this))

(defrecord StmtVisitor [env scope]
  SqlVisitor
  (visitQueryExpr [this ctx]
    (let [env (->> (some-> (.settingQueryVariables ctx)
                           (.settingQueryVariable))
                   (transduce (keep (partial accept-visitor this))
                              conj env))]

      (-> (.queryExpression ctx)
          (.accept (->QueryPlanVisitor env scope)))))

  (visitShowSnapshotTimeStatement [_ _]
    (->QueryExpr '[:table [snapshot_time]
                   [{:snapshot_time (snapshot_time)}]]
                 '[snapshot_time]))

  (visitShowClockTimeStatement [_ _]
    (->QueryExpr '[:table [clock_time]
                   [{:clock_time (current_timestamp)}]]
                 '[clock_time]))

  (visitSettingDefaultSystemTime [_ ctx]
    [:sys-time-default
     (.accept (.tableTimePeriodSpecification ctx)
              (->TableTimePeriodSpecificationVisitor (->ExprPlanVisitor env scope)))])

  (visitSettingDefaultValidTime [_ ctx]
    [:valid-time-default
     (.accept (.tableTimePeriodSpecification ctx)
              (->TableTimePeriodSpecificationVisitor (->ExprPlanVisitor env scope)))])

  ;; dealt with earlier
  (visitSettingSnapshotTime [_ _ctx])
  (visitSettingClockTime [_ _ctx])

  (visitInsertStmt [this ctx] (-> (.insertStatement ctx) (.accept this)))

  (visitInsertStatement [_ ctx]
    (let [{:keys [col-syms] :as insert-plan} (-> (.insertColumnsAndSource ctx)
                                                 (.accept (->QueryPlanVisitor env scope)))]
      (when-not (contains? (set col-syms) '_id)
        (add-err! env (->InsertWithoutXtId)))

      (doseq [col (into #{} (filter forbidden-insert-col?) col-syms)]
        (add-err! env (->ForbiddenColumnInsert col)))

      (->InsertStmt (-> (identifier-sym (.tableName ctx)) util/with-default-schema)
                    insert-plan)))

  (visitPatchStmt [this ctx] (-> (.patchStatement ctx) (.accept this)))

  (visitPatchStatement [this ctx]
    (let [table-name (-> (identifier-sym (.tableName ctx))
                         util/with-default-schema)
          expr-visitor (->ExprPlanVisitor env scope)]
      (->PatchStmt table-name
                   (->QueryExpr (plan-patch env {:table table-name
                                                 :valid-from (some-> (.validFrom ctx) (.accept expr-visitor))
                                                 :valid-to (some-> (.validTo ctx) (.accept expr-visitor))
                                                 :patch-rel (.accept (.patchSource ctx) this)})
                                '[_iid _valid_from _valid_to doc]))))

  (visitPatchRecords [_ ctx]
    (let [{:keys [plan col-syms]} (-> (.recordsValueConstructor ctx)
                                      (.accept (->QueryPlanVisitor env scope))
                                      (remove-ns-qualifiers env))]
      (if-let [forbidden-cols (seq (filter forbidden-patch-col? col-syms))]
        (add-err! env (->ForbiddenColumnPatch forbidden-cols))
        (->QueryExpr [:project ['{_iid (_iid _id)}
                                {'doc (into {} (map (juxt keyword identity)) col-syms)}]
                      plan]
                     '[_iid doc]))))

  (visitUpdateStmt [this ctx] (-> (.updateStatementSearched ctx) (.accept this)))

  (visitUpdateStatementSearched [{{:keys [!id-count table-info]} :env} ctx]
    (let [internal-cols (mapv ->col-sym '[_iid _valid_from _valid_to])
          table-name (identifier-sym (.tableName ctx))
          table-alias (or (identifier-sym (.correlationName ctx)) (-> table-name name symbol))
          table-name (util/with-default-schema table-name)
          unique-table-alias (symbol (str table-alias "." (swap! !id-count inc)))
          aliased-cols (mapv (fn [col] {col (->col-sym (str unique-table-alias) (str col))}) internal-cols)

          {:keys [for-valid-time], vt-projection :projection} (some-> (.dmlStatementValidTimeExtents ctx)
                                                                      (.accept (->DmlValidTimeExtentsVisitor env scope)))

          table-cols (if-let [cols (get table-info table-name)]
                       cols
                       (do
                         (add-warning! env (->BaseTableNotFound nil table-name))
                         #{}))

          dml-scope (->DmlTableRef env table-name table-alias unique-table-alias for-valid-time table-cols
                                   (HashMap. ^Map (apply merge aliased-cols)))

          expr-visitor (->ExprPlanVisitor env dml-scope)

          set-clauses (for [^Sql$SetClauseContext set-clause (->> (.setClauseList ctx) (.setClause))
                            :let [set-target (.setTarget set-clause)]]
                        {(identifier-sym (.columnName set-target))
                         (.accept (.expr (.updateSource set-clause)) expr-visitor)})

          _ (doseq [forbidden-col (->> set-clauses
                                       (into #{} (comp (map (comp key first))
                                                       (filter forbidden-update-col?))))]
              (add-err! env (->ForbiddenColumnUpdate forbidden-col)))

          set-clauses-cols (set (mapv ffirst set-clauses))

          where-selection (when-let [search-clause (.searchCondition ctx)]
                            (let [!subqs (HashMap.)]
                              {:predicate (.accept search-clause (map->ExprPlanVisitor {:env env, :scope dml-scope, :!subqs !subqs}))
                               :subqs (not-empty (into {} !subqs))}))

          all-non-set-cols (for [col (find-cols dml-scope nil set-clauses-cols)
                                 :let [col-sym (->col-sym (name col))]]
                             (->ProjectedCol {col-sym col} col-sym))

          outer-projection (vec (concat '[_iid]
                                        (or vt-projection default-vt-extents-projection)
                                        set-clauses-cols
                                        (mapv :col-sym all-non-set-cols)))]

      (->UpdateStmt (symbol table-name)
                    (->QueryExpr [:project outer-projection
                                  (as-> (plan-rel dml-scope) plan
                    
                                    (if-let [{:keys [predicate subqs]} where-selection]
                                      (-> plan
                                          (apply-sqs subqs)
                                          (wrap-predicates predicate))
                                      plan)
                    
                                    [:project (concat aliased-cols set-clauses (mapv :projection all-non-set-cols)) plan])]
                                 (vec (concat internal-cols set-clauses-cols all-non-set-cols))))))

  (visitDeleteStmt [this ctx] (-> (.deleteStatementSearched ctx) (.accept this)))

  (visitDeleteStatementSearched [{{:keys [!id-count table-info]} :env} ctx]
    (let [internal-cols (mapv ->col-sym '[_iid _valid_from _valid_to])
          table-name (identifier-sym (.tableName ctx))
          table-alias (or (identifier-sym (.correlationName ctx)) (-> table-name name symbol))
          table-name (util/with-default-schema table-name)
          unique-table-alias (symbol (str table-alias "." (swap! !id-count inc)))
          aliased-cols (mapv (fn [col] {col (->col-sym (str unique-table-alias) (str col))}) internal-cols)

          {:keys [for-valid-time], vt-projection :projection} (some-> (.dmlStatementValidTimeExtents ctx)
                                                                      (.accept (->DmlValidTimeExtentsVisitor env scope)))

          table-cols (if-let [cols (get table-info table-name)]
                       cols
                       (do
                         (add-warning! env (->BaseTableNotFound nil table-name))
                         #{}))

          dml-scope (->DmlTableRef env table-name table-alias unique-table-alias for-valid-time table-cols
                                   (HashMap. ^Map (into {} aliased-cols)))

          where-selection (when-let [search-clause (.searchCondition ctx)]
                            (let [!subqs (HashMap.)]
                              {:predicate (.accept search-clause (map->ExprPlanVisitor {:env env, :scope dml-scope, :!subqs !subqs}))
                               :subqs (not-empty (into {} !subqs))}))

          projection (into '[_iid] (or vt-projection default-vt-extents-projection))]
      (->DeleteStmt table-name
                    (->QueryExpr [:project projection
                                  (as-> (plan-rel dml-scope) plan

                                    (if-let [{:keys [predicate subqs]} where-selection]
                                      (-> plan
                                          (apply-sqs subqs)
                                          (wrap-predicates predicate))
                                      plan)

                                    [:project aliased-cols plan])]
                                 internal-cols))))

  (visitEraseStmt [this ctx] (-> (.eraseStatementSearched ctx) (.accept this)))

  (visitEraseStatementSearched [{{:keys [!id-count table-info]} :env} ctx]
    (let [internal-cols '[_iid]
          table-name (identifier-sym (.tableName ctx))
          table-alias (or (identifier-sym (.correlationName ctx)) (-> table-name name symbol))
          unique-table-alias (symbol (str table-alias "." (swap! !id-count inc)))
          table-name (util/with-default-schema table-name)
          aliased-cols (mapv (fn [col] {col (->col-sym (str unique-table-alias) (str col))}) internal-cols)

          table-cols (if-let [cols (get table-info table-name)]
                       cols
                       (do
                         (add-warning! env (->BaseTableNotFound nil table-name))
                         #{}))

          dml-scope (->EraseTableRef env table-name table-alias unique-table-alias table-cols
                                     (HashMap. ^Map (apply merge aliased-cols)))

          where-selection (when-let [search-clause (.searchCondition ctx)]
                            (let [!subqs (HashMap.)]
                              {:predicate (.accept search-clause (map->ExprPlanVisitor {:env env, :scope dml-scope, :!subqs !subqs}))
                               :subqs (not-empty (into {} !subqs))}))]

      (->EraseStmt (symbol table-name)
                   (->QueryExpr [:distinct
                                 [:project internal-cols
                                  (as-> (plan-rel dml-scope) plan
                                  
                                    (if-let [{:keys [predicate subqs]} where-selection]
                                      (-> plan
                                          (apply-sqs subqs)
                                          (wrap-predicates predicate))
                                      plan)
                                  
                                    [:project aliased-cols plan])]]
                                internal-cols))))

  (visitAssertStatement [_ ctx]
    (let [!subqs (HashMap.)
          predicate (.accept (.searchCondition ctx)
                             (map->ExprPlanVisitor {:env env, :scope nil, :!subqs !subqs}))]
      (->AssertStmt (->QueryExpr (-> [:table [{}]]
                                     (apply-sqs (not-empty (into {} !subqs)))
                                     (wrap-predicates predicate))
                                 []))))

  (visitShowVariableStatement [this ctx] (.accept (.showVariable ctx) this))

  (visitShowTransactionIsolationLevel [_ _]
    (->QueryExpr [:table [{:transaction_isolation "read committed"}]]
                 [(->col-sym 'transaction_isolation)]))

  (visitShowTimeZone [_ _]
    (->QueryExpr [:table [{:timezone '(current-timezone)}]]
                 [(->col-sym 'timezone)]))

  (visitCreateUserStatement [_ ctx]
    (->UserStmt [:create-user (-> (.userName ctx) (.getText)) (.accept (.password ctx) string-literal-visitor)]))

  (visitAlterUserStatement [_ ctx]
    (->UserStmt [:alter-user (-> (.userName ctx) (.getText)) (.accept (.password ctx) string-literal-visitor)]))

  (visitExecuteStatement [_ ctx]
    ;; this is only planning a SQL query for the _args_ of the execute statement
    (let [arg-row (->> (.executeArgs ctx) (.expr)
                       (into [] (comp (map (partial accept-visitor (->ExprPlanVisitor env nil)))
                                      (map-indexed (fn [idx expr]
                                                     (MapEntry/create (symbol (str "?_" idx)) expr))))))]
      (->QueryExpr [:table [(into {} arg-row)]]
                   (vec (keys arg-row)))))

  (visitExecuteStmt [this ctx]
    (.accept (.executeStatement ctx) this)))

(defn xform-table-info [table-info]
  (into {}
        (for [[tn cns] (merge info-schema/table-info
                              '{xt/txs #{_id committed error system_time}}
                              table-info)]
          [(symbol tn) (->> cns
                            (map ->col-sym)
                            ^Collection
                            (sort-by identity (fn [s1 s2]
                                                (cond
                                                  (= '_id s1) -1
                                                  (= '_id s2) 1
                                                  :else (compare s1 s2))))
                            ->insertion-ordered-set)])))

(defn log-warnings [!warnings]
  (doseq [warning @!warnings]
    (log/warn (error-string warning))))

(defn- ->env [{:keys [table-info default-tz]}]
  {:!errors (atom [])
   :!warnings (atom [])
   :!id-count (atom 0)
   :!param-count (atom 0)
   :table-info (xform-table-info table-info)
   :default-tz default-tz})

(defprotocol PlanExpr
  (-plan-expr [sql opts]))

(extend-protocol PlanExpr
  ParserRuleContext
  (-plan-expr [ast {:keys [scope], :as opts}]
    (let [{:keys [!errors !warnings] :as env} (->env opts)

          plan (-> ast
                   #_(doto (-> (.toStringTree parser) read-string (clojure.pprint/pprint))) ; <<no-commit>>
                   (.accept (->ExprPlanVisitor env scope)))]

      (if-let [errs (not-empty @!errors)]
        (throw (err/illegal-arg :xtdb/sql-error
                                {::err/message (str "Errors planning SQL statement:\n  - "
                                                    (str/join "\n  - " (map #(error-string %) errs)))
                                 :errors errs}))
        (do
          (log-warnings !warnings)
          plan))))

  String
  (-plan-expr [sql {:keys [ast-type], :or {ast-type :expr}, :as opts}]
    (let [parser (antlr/->parser sql)]
      (-plan-expr (case ast-type
                    :expr (.expr parser)
                    :where (.searchCondition (.whereClause parser)))
                  opts))))

(defn plan-expr
  ([sql] (plan-expr sql {}))
  ([sql opts] (-plan-expr sql opts)))

;; eventually these data structures will be used as logical plans,
;; we won't need an adapter
(defprotocol AdaptPlan
  (->logical-plan [stmt]))

(extend-protocol AdaptPlan
  QueryExpr (->logical-plan [{:keys [plan]}] plan)

  InsertStmt
  (->logical-plan [{:keys [table query-plan]}]
    [:insert {:table table}
     (->logical-plan query-plan)])

  PatchStmt
  (->logical-plan [{:keys [table query-plan]}]
    [:patch {:table table}
     (->logical-plan query-plan)])

  UpdateStmt
  (->logical-plan [{:keys [table query-plan]}]
    [:update {:table table}
     (->logical-plan query-plan)])

  DeleteStmt
  (->logical-plan [{:keys [table query-plan]}]
    [:delete {:table table}
     (->logical-plan query-plan)])

  EraseStmt
  (->logical-plan [{:keys [table query-plan]}]
    [:erase {:table table}
     (->logical-plan query-plan)])

  AssertStmt
  (->logical-plan [{:keys [query-plan]}]
    [:assert {} (->logical-plan query-plan)])

  UserStmt
  (->logical-plan [{:keys [stmt]}] stmt))

(defprotocol PlanStatement
  (-plan-statement [query opts]))

(extend-protocol PlanStatement
  String
  (-plan-statement [sql opts]
    (-> (antlr/parse-statement sql)
        (-plan-statement opts)))

  Sql$DirectlyExecutableStatementContext
  (-plan-statement [ctx {:keys [scope table-info arg-fields]}]
    (let [!errors (atom [])
         !warnings (atom [])
         !param-count (atom 0)
         env {:!errors !errors
              :!warnings !warnings
              :!id-count (atom 0)
              :!param-count !param-count
              :table-info (xform-table-info table-info)
              ;; NOTE this may not necessarily be provided
              ;; we get it through SQL DML, which is the main case we need it for #3656
              :arg-fields arg-fields}

          stmt (.accept ctx (->StmtVisitor env scope))]
     (if-let [errs (not-empty @!errors)]
       (throw (err/illegal-arg :xtdb/sql-error
                               {::err/message (str "Errors planning SQL statement:\n  - "
                                                   (str/join "\n  - " (map #(error-string %) errs)))
                                :errors errs}))
       (do
         (log-warnings !warnings)
         (-> stmt
             #_(doto clojure.pprint/pprint) ;; <<no-commit>>
             (optimise-stmt) ;; <<no-commit>>
             #_(doto clojure.pprint/pprint) ;; <<no-commit>>
             (vary-meta assoc
                        :param-count @!param-count
                        :warnings @!warnings)))))))

(defn plan-statement
  ([sql] (plan-statement sql {}))
  ([sql opts] (-plan-statement sql opts)))

(comment
  (plan-statement "WITH foo AS (SELECT id FROM bar WHERE id = 5)
                   SELECT foo.id, baz.id
                   FROM foo, foo AS baz"
                  {:table-info {"public/bar" #{"id"}}}))

(defrecord SqlToStaticOpsVisitor [env scope arg-rows]
  SqlVisitor
  (visitInsertStmt [this ctx] (-> (.insertStatement ctx) (.accept this)))
  (visitUpdateStmt [_ _])
  (visitDeleteStmt [_ _])
  (visitEraseStmt [_ _])
  (visitAssertStatement [_ _])
  (visitQueryExpr [_ _])
  (visitShowVariableStatement [_ _])
  (visitCreateUserStatement [_ _])
  (visitAlterUserStatement [_ _])

  (visitInsertStatement [this ctx]
    (let [table (-> (identifier-sym (.tableName ctx)) (util/with-default-schema))]
      (when-let [rows (-> (.insertColumnsAndSource ctx) (.accept this))]
        (letfn [(->const [v arg-row]
                  (letfn [(->const* [obj]
                            (cond
                              (symbol? obj) (->const* (val (or (find arg-row obj) (throw (RuntimeException.)))))
                              (seq? obj) (throw (RuntimeException.))
                              (vector? obj) (mapv ->const* obj)
                              (set? obj) (into #{} (map ->const*) obj)
                              (instance? Map obj) (->> obj
                                                       (into {} (map (fn [[k v]]
                                                                       (MapEntry/create (str (symbol k))
                                                                                        (->const* v))))))
                              :else obj))]
                    (->const* v)))

                (->inst [v]
                  (when v
                    (cond
                      (instance? LocalDate v) (->inst (.atStartOfDay ^LocalDate v))
                      (instance? LocalDateTime v) (-> ^LocalDateTime v
                                                      (ZonedDateTime/of (or (:default-tz env)
                                                                            ZoneOffset/UTC))
                                                      ->inst)
                      :else (time/->instant v))))]
          (->> (for [arg-row (or arg-rows [[]])
                     row rows]
                 (->const row (->> arg-row
                                   (into {} (map-indexed (fn [idx v]
                                                           (MapEntry/create (symbol (str "?_" idx)) v)))))))

               (group-by (juxt (comp ->inst #(get % "_valid_from"))
                               (comp ->inst #(get % "_valid_to"))))

               (into [] (map (fn [[[vf vt] rows]]
                               (tx-ops/->PutDocs (str table) (mapv #(dissoc % "_valid_from" "_valid_to") rows)
                                                 vf vt)))))))))

  (visitInsertFromSubquery [_ _])

  (visitInsertValues [_ ctx]
    (let [out-col-syms (->> (.columnName (.columnNameList ctx))
                            (mapv identifier-sym))
          {:keys [rows]} (-> (.tableValueConstructor ctx)
                             (.accept (->TableRowsVisitor env scope out-col-syms)))]
      rows))

  (visitInsertRecords [_ ctx]
    (let [out-col-syms (some->> (.columnNameList ctx)
                                .columnName
                                (mapv identifier-sym))
          {:keys [rows]} (-> (.recordsValueConstructor ctx)
                             (.accept (->TableRowsVisitor env scope out-col-syms)))]
      rows))

  ;; TODO these can be made static ops too
  (visitPatchStmt [_ _ctx]))

(defn sql->static-ops
  ([sql arg-rows] (sql->static-ops sql arg-rows {}))

  ([sql arg-rows {:keys [scope] :as opts}]
   (try
     (let [{:keys [!errors !warnings] :as env} (-> (->env opts)
                                                   (assoc :arg-fields (for [arg-idx (range (count (first arg-rows)))]
                                                                        (->> (for [arg-row arg-rows]
                                                                               (vw/value->col-type (nth arg-row arg-idx)))
                                                                             (apply types/merge-col-types)
                                                                             types/col-type->field))))
           tx-ops (-> (antlr/parse-statement sql)
                      (.accept (->SqlToStaticOpsVisitor env scope arg-rows)))]
       (when (and (empty? @!errors) (empty? @!warnings))
         tx-ops))

     ;; eventually we could deal with these on pre-submit
     (catch IllegalArgumentException _)
     (catch RuntimeException _))))
