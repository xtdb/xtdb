(ns xtdb.sql.plan
  (:require [clojure.set :as set]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [xtdb.error :as err]
            [xtdb.information-schema :as info-schema]
            [xtdb.logical-plan :as lp]
            [xtdb.types :as types]
            [xtdb.util :as util])
  (:import clojure.lang.MapEntry
           (java.time Duration LocalDate LocalDateTime LocalTime OffsetTime Period ZoneId ZoneOffset ZonedDateTime)
           (java.util Collection HashMap HashSet LinkedHashSet Map SequencedSet Set UUID)
           java.util.function.Function
           (org.antlr.v4.runtime BaseErrorListener CharStreams CommonTokenStream ParserRuleContext Recognizer)
           (xtdb.antlr SqlLexer SqlParser SqlParser$BaseTableContext SqlParser$DirectSqlStatementContext SqlParser$IntervalQualifierContext SqlParser$JoinSpecificationContext SqlParser$JoinTypeContext SqlParser$ObjectNameAndValueContext SqlParser$OrderByClauseContext SqlParser$QualifiedRenameColumnContext SqlParser$QueryBodyTermContext SqlParser$QuerySpecificationContext SqlParser$RenameColumnContext SqlParser$SearchedWhenClauseContext SqlParser$SetClauseContext SqlParser$SimpleWhenClauseContext SqlParser$SortSpecificationContext SqlParser$WhenOperandContext SqlParser$WithTimeZoneContext SqlVisitor)
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

(defn- ->col-sym
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
                     (visitSchemaName [_ ctx] (symbol (.getText ctx)))
                     (visitAsClause [this ctx] (-> (.columnName ctx) (.accept this)))
                     (visitQueryName [this ctx] (-> (.identifier ctx) (.accept this)))
                     (visitTableName [this ctx] (-> (.identifier ctx) (.accept this)))
                     (visitTableAlias [this ctx] (-> (.correlationName ctx) (.accept this)))
                     (visitColumnName [this ctx] (-> (.identifier ctx) (.accept this)))
                     (visitFieldName [this ctx] (-> (.identifier ctx) (.accept this)))
                     (visitCorrelationName [this ctx] (-> (.identifier ctx) (.accept this)))

                     (visitRegularIdentifier [_ ctx] (symbol (util/str->normal-form-str (.getText ctx))))
                     (visitDelimitedIdentifier [_ ctx]
                       (let [di-str (.getText ctx)]
                         (symbol (subs di-str 1 (dec (count di-str))))))))))

(defprotocol Scope
  (available-cols [scope chain])
  (available-tables [scope])
  (find-decls [scope chain]))

(defprotocol TableRef
  (plan-table-ref [scope]))

(extend-protocol Scope
  nil
  (available-cols [_ _])
  (available-tables [_])
  (find-decls [_ _]))

(extend-protocol TableRef
  nil
  (plan-table-ref [_]
    [:table [{}]]))

(defn- find-decl [scope chain]
  (let [[match & more-matches] (find-decls scope chain)]
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
                            (-> (symbol (format "?xt$sq_%s_%d" (name sym) (dec (swap! !id-count inc))))
                                (vary-meta assoc :correlated-column? true))))))))

(defrecord SubqueryScope [env, scope, ^Map !sq-refs]
  Scope
  (available-cols [_ chain] (available-cols scope chain))
  (available-tables [_] (available-tables scope))

  (find-decls [_ chain]
    (not-empty (->> (find-decls scope chain)
                    (mapv #(->sq-sym % env !sq-refs))))))

(defn- plan-sq [^ParserRuleContext sq-ctx, {:keys [!id-count] :as env}, scope, ^Map !subqs, sq-opts]
  (if-not !subqs
    (add-err! env (->SubqueryDisallowed))

    (let [sq-sym (-> (->col-sym (str "xt$sq_" (swap! !id-count inc)))
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
                                  [:group-by [{sq-sym (list 'array_agg (first (:col-syms query-plan)))}]
                                   (:plan query-plan)]]


                 :exists [:apply {:mark-join {sq-sym true}} sq-refs
                          plan
                          (:plan query-plan)]

                 :quantified-comparison (let [{:keys [expr op]} sq
                                              needle-param (vary-meta '?xt$needle assoc :correlated-column? true)]

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
                                             (assoc sq-refs (->col-sym 'xt$needle) needle-param)
                                             [:map [{'xt$needle expr}]
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

  (visitTableAsOf [_ ctx]
    [:at (-> ctx (.periodSpecificationExpr) (.accept expr-visitor))])

  (visitTableBetween [_ ctx]
    [:between
     (-> ctx (.periodSpecificationExpr 0) (.accept expr-visitor))
     (-> ctx (.periodSpecificationExpr 1) (.accept expr-visitor))])

  (visitTableFromTo [_ ctx]
    [:in
     (-> ctx (.periodSpecificationExpr 0) (.accept expr-visitor))
     (-> ctx (.periodSpecificationExpr 1) (.accept expr-visitor))]))

(defrecord MultipleTimePeriodSpecifications []
  PlanError
  (error-string [_] "Multiple time period specifications were specified"))

(defrecord PeriodSpecificationDisallowedOnCte [table-name]
  PlanError
  (error-string [_] (format "Period specifications not allowed on CTE reference: %s" table-name)))

(defrecord BaseTable [env, ^SqlParser$BaseTableContext ctx
                      schema-name table-name table-alias unique-table-alias cols
                      ^Map !reqd-cols]
  Scope
  (available-cols [_ chain]
    (when-not (and chain (not= chain [table-alias]))
      cols))

  (available-tables [_] [table-alias])

  (find-decls [this [col-name table-name]]
    (when (and (or (nil? table-name) (= table-name table-alias))
               (or (nil? schema-name) (= schema-name (:schema-name this)))
               (or (contains? cols col-name) (types/temporal-column? col-name)))
      [(.computeIfAbsent !reqd-cols col-name
                         (reify Function
                           (apply [_ col]
                             (->col-sym (str unique-table-alias) (str col)))))]))

  TableRef
  (plan-table-ref [{{:keys [default-all-valid-time?]} :env, :as this}]
    (let [expr-visitor (->ExprPlanVisitor env this)]
      (letfn [(<-table-time-period-specification [specs]
                (case (count specs)
                  0 nil
                  1 (.accept ^ParserRuleContext (first specs) (->TableTimePeriodSpecificationVisitor expr-visitor))
                  :else (add-err! env (->MultipleTimePeriodSpecifications))))]
        (let [for-vt (or (<-table-time-period-specification (.queryValidTimePeriodSpecification ctx))
                         (when default-all-valid-time? :all-time))
              for-st (<-table-time-period-specification (.querySystemTimePeriodSpecification ctx))]

          [:rename unique-table-alias
           [:scan (cond-> {:table (if schema-name
                                    (symbol (str schema-name) (str table-name))
                                    table-name)}
                    for-vt (assoc :for-valid-time for-vt)
                    for-st (assoc :for-system-time for-st))
            (vec (.keySet !reqd-cols))]])))))

(defrecord JoinConditionScope [env l r]
  Scope
  (available-cols [_ chain]
    (->> [l r]
         (into [] (comp (mapcat #(available-cols % chain))
                        (distinct)))))

  (available-tables [_]
    (into (available-tables l)
          (available-tables r)))

  (find-decls [_ chain]
    (->> [l r]
         (mapcat #(find-decls % chain)))))

(defrecord JoinTable [env l r
                      ^SqlParser$JoinTypeContext join-type-ctx
                      ^SqlParser$JoinSpecificationContext join-spec-ctx
                      common-cols]
  Scope
  (available-cols [_ chain]
    (->> [l r]
         (into [] (comp (mapcat #(available-cols % chain))
                        (distinct)))))

  (available-tables [_]
    (into (available-tables l)
          (available-tables r)))

  (find-decls [_ chain]
    (->> (if (and (= 1 (count chain))
                  (get common-cols (first chain)))
           [l] [l r])
         (mapcat #(find-decls % chain))))

  TableRef
  (plan-table-ref [_]
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
                          {(find-decl l [col-name])
                           (find-decl r [col-name])}))
         (plan-table-ref l)
         (plan-table-ref r)]

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
                        (plan-table-ref l)
                        [:select join-pred
                         (-> (plan-table-ref r)
                             (apply-sqs !join-cond-subqs))]])))))))))

(defrecord CrossJoinTable [env !sq-refs l r]
  Scope
  (available-cols [_ chain]
    (->> [l r]
         (into [] (comp (mapcat #(available-cols % chain))
                        (distinct)))))

  (available-tables [_]
    (into (available-tables l)
          (available-tables r)))

  (find-decls [_ chain]
    (->> [l r]
         (mapcat #(find-decls % chain))))

  TableRef
  (plan-table-ref [_]
    (let [planned-l (plan-table-ref l)
          planned-r (plan-table-ref r)]
      [:apply :cross-join (into {} !sq-refs)
       planned-l planned-r])))

(defrecord DerivedTable [plan table-alias unique-table-alias, ^SequencedSet available-cols]
  Scope
  (available-cols [_ chain]
    (when-not (and chain (not= chain [table-alias]))
      available-cols))

  (available-tables [_] [table-alias])

  (find-decls [_ [col-name table-name]]
    (when (or (nil? table-name) (= table-name table-alias))
      (when (.contains available-cols col-name)
        [(->col-sym (str unique-table-alias) (str col-name))])))

  TableRef
  (plan-table-ref [_]
    [:rename unique-table-alias
     plan]))

(defrecord UnnestTable [env table-alias unique-table-alias unnest-col unnest-expr ordinality-col]
  Scope
  (available-cols [_ chain]
    (when-not (and chain (not= chain [table-alias]))
      (->insertion-ordered-set (cond-> [unnest-col]
                                 ordinality-col (conj ordinality-col)))))

  (available-tables [_] [table-alias])

  (find-decls [_ [col-name table-name]]
    (when (or (nil? table-name) (= table-name table-alias))
      (condp = col-name
        unnest-col [(-> (->col-sym (str unique-table-alias) (str col-name))
                        (with-meta (meta unnest-col)))]
        ordinality-col [(-> (->col-sym (str unique-table-alias) (str ordinality-col))
                            (with-meta (meta ordinality-col)))]
        nil)))

  TableRef
  (plan-table-ref [_]
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
  (available-cols [_ chain]
    (when-not (and chain (not= chain [table-alias]))
      !table-cols))

  (available-tables [_] [table-alias])

  (find-decls [_ [col-name table-name]]
    (when (and (or (nil? table-name) (= table-name table-alias))
               (.contains !table-cols col-name))
      [(->col-sym (str unique-table-alias) (str col-name))]))

  TableRef
  (plan-table-ref [_]
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

(defrecord GroupInvariantColsTracker [env scope, ^Set !implied-gicrs]
  SqlVisitor
  (visitSelectClause [this ctx] (.accept (.getParent ctx) this))

  (visitQuerySpecification [_ ctx]
    (if-let [gbc (.groupByClause ctx)]
      (let [grouping-cols (vec (for [^ParserRuleContext grp-el (.groupingElement gbc)]
                                 (.accept grp-el
                                          (reify SqlVisitor
                                            (visitOrdinaryGroupingSet [_ ctx]
                                              (.accept (.columnReference ctx) (->ExprPlanVisitor env scope)))))))]

        (if-let [missing-grouping-cols (not-empty (set/difference (set !implied-gicrs) (set grouping-cols)))]
          (add-err! env (->MissingGroupingColumns missing-grouping-cols))
          grouping-cols))

      (for [col-ref !implied-gicrs]
        col-ref)))

  Scope 
  (available-cols [_ table-name] (available-cols scope table-name))

  (available-tables [_] (available-tables scope))

  (find-decls [_ chain]
    (for [sym (find-decls scope chain)]
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

          (let [[sn table-cols] (or (when-let [table-cols (get table-info (if sn
                                                                            (symbol (str sn) (str tn))
                                                                            tn))]
                                      [sn table-cols])

                                    (when-not sn
                                      (when-let [table-cols (get info-schema/unq-pg-catalog tn)]
                                        ['pg_catalog table-cols]))

                                    (add-warning! env (->BaseTableNotFound sn tn)))
                [sn tn] (if (= sn 'xt)
                          [nil (symbol (str "xt$" tn))]
                          [sn tn])]
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
          common-cols (set/intersection (set (available-cols l nil)) (set (available-cols r nil)))]

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
                         (-> (->col-sym (str "xt$unnest." (swap! !id-count inc)))
                             (vary-meta assoc :unnamed-unnest-col? true)))
                     expr
                     (when with-ordinality?
                       (or (->col-sym (second table-projection))
                           (-> (->col-sym (str "xt$ordinal." (swap! !id-count inc)))
                               (vary-meta assoc :unnamed-unnest-col? true)))))))

  (visitWrappedTableReference [this ctx] (-> (.tableReference ctx) (.accept this)))

  (visitArrowTable [{{:keys [!id-count]} :env} ctx]
    (let [table-alias (identifier-sym (.tableAlias ctx))
          cols (->table-projection (.tableProjection ctx))
          url (some-> (.characterString ctx) (.accept (->ExprPlanVisitor env scope)))]
      (->ArrowTable env url table-alias
                    (symbol (str table-alias "." (swap! !id-count inc)))
                    (->insertion-ordered-set cols)))))

(defrecord QuerySpecificationScope [outer-scope from-table-ref]
  Scope
  (available-cols [_ chain] (available-cols from-table-ref chain))

  (available-tables [_]
    (into (available-tables outer-scope)
          (available-tables from-table-ref)))

  (find-decls [_ chain]
    (or (not-empty (find-decls from-table-ref chain))
        (find-decls outer-scope chain)))

  TableRef
  (plan-table-ref [_]
    (if from-table-ref
      (plan-table-ref from-table-ref)
      [:table [{}]])))

(defn- ->projected-col-expr [col-idx expr]
  (let [{:keys [column? sq-out-sym? agg-out-sym? unnamed-unnest-col? identifier]} (meta expr)]
    (if (and column? (not sq-out-sym?) (not agg-out-sym?) (not unnamed-unnest-col?))
      (->ProjectedCol expr expr)
      (let [col-name (or identifier (->col-sym (str "xt$column_" (inc col-idx))))]
        (->ProjectedCol {col-name expr} col-name)))))

(defrecord SelectClauseProjectedCols [env scope]
  SqlVisitor
  (visitSelectClause [_ ctx]
    (let [sl-ctx (.selectList ctx)
          !subqs (HashMap.)
          !aggs (HashMap.)]

      {:projected-cols (vec (concat (when-let [star-ctx (.selectListAsterisk sl-ctx)]
                                      (let [renames (->> (for [^SqlParser$RenameColumnContext rename-pair (some-> (.renameClause star-ctx)
                                                                                                                  (.renameColumn))]
                                                           (let [chain (rseq (mapv identifier-sym (.identifier (.identifierChain (.columnReference rename-pair)))))
                                                                 out-col-name (.columnName (.asClause rename-pair))
                                                                 sym (find-decl scope chain)]

                                                             (MapEntry/create sym (->col-sym (identifier-sym out-col-name)))))
                                                         (into {}))

                                            excludes (when-let [exclude-ctx (.excludeClause star-ctx)]
                                                       (into #{} (map identifier-sym) (.identifier exclude-ctx)))]
                                        (->> (for [table-name (available-tables scope)
                                                   col-name (available-cols scope [table-name])
                                                   :when (not (contains? excludes col-name))
                                                   :let [sym (find-decl scope [col-name table-name])]
                                                   :when (not (contains? excludes sym))]
                                               sym)
                                             (map-indexed (fn [col-idx sym]
                                                            (if-let [renamed-col (get renames sym)]
                                                              (->ProjectedCol {renamed-col sym} renamed-col)
                                                              (->projected-col-expr col-idx sym))))
                                             (into []))))
                                    (->> (.selectSublist sl-ctx)
                                         (into [] (comp (map-indexed
                                                         (fn [col-idx ^ParserRuleContext sl-elem]
                                                           (.accept (.getChild sl-elem 0)
                                                                    (reify SqlVisitor
                                                                      (visitDerivedColumn [_ ctx]
                                                                        [(let [expr (.accept (.expr ctx)
                                                                                             (map->ExprPlanVisitor {:env env, :scope scope, :!subqs !subqs, :!aggs !aggs}))]
                                                                           (if-let [as-clause (.asClause ctx)]
                                                                             (let [col-name (->col-sym (identifier-sym as-clause))]
                                                                               (->ProjectedCol {col-name expr} col-name))

                                                                             (->projected-col-expr col-idx expr)))])

                                                                      (visitQualifiedAsterisk [_ ctx]
                                                                        (let [[table-name schema-name] (rseq (mapv identifier-sym (.identifier (.identifierChain ctx))))]
                                                                          (when schema-name
                                                                            (throw (UnsupportedOperationException. "schema not supported")))

                                                                          (if-let [table-cols (available-cols scope [table-name])]
                                                                            (let [renames (->> (for [^SqlParser$QualifiedRenameColumnContext rename-pair (some-> (.qualifiedRenameClause ctx)
                                                                                                                                                                 (.qualifiedRenameColumn))]
                                                                                                 (let [sym (find-decl scope [(identifier-sym (.identifier rename-pair)) table-name])
                                                                                                       out-col-name (.columnName (.asClause rename-pair))]
                                                                                                   (MapEntry/create sym (identifier-sym out-col-name))))
                                                                                               (into {}))
                                                                                  excludes (when-let [exclude-ctx (.excludeClause ctx)]
                                                                                             (into #{} (map identifier-sym) (.identifier exclude-ctx)))]
                                                                              (->> (for [col-name table-cols
                                                                                         :when (not (contains? excludes col-name))
                                                                                         :let [sym (find-decl scope [col-name table-name])]
                                                                                         :when (not (contains? excludes sym))]
                                                                                     sym)
                                                                                   (into [] (map-indexed (fn [col-idx sym]
                                                                                                           (if-let [renamed-col (get renames sym)]
                                                                                                             (->ProjectedCol {renamed-col sym} renamed-col)
                                                                                                             (->projected-col-expr col-idx sym)))))))

                                                                            (throw (UnsupportedOperationException. (str "Table not found: " table-name))))))))))
                                                        cat)))))
       :subqs (not-empty (into {} !subqs))
       :aggs (not-empty (into {} !aggs))})))

(defn- project-all-cols [scope]
  ;; duplicated from the ASTERISK case above
  {:projected-cols (->> (for [table-name (available-tables scope)
                              col-name (available-cols scope [table-name])
                              :let [sym (find-decl scope [col-name table-name])]]
                          sym)
                        (into [] (map-indexed ->projected-col-expr)))})

(defn seconds-fraction->nanos ^long [seconds-fraction]
  (if seconds-fraction
    (* (Long/parseLong seconds-fraction)
       (long (Math/pow 10 (- 9 (count seconds-fraction)))))
    0))

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
      (let [local-time (LocalTime/of (parse-long h) (parse-long m) (parse-long s) (seconds-fraction->nanos sf))]
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
  (if-let [[_ y mons d h mins s sf ^String offset zone] (re-matches #"(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2}):(\d{2})(?:\.(\d+))?(Z|[+-]\d{2}:\d{2})?(?:\[([\w\/]+)\])?" ts-str)]
    (try
      (let [ldt (LocalDateTime/of (parse-long y) (parse-long mons) (parse-long d)
                                  (parse-long h) (parse-long mins) (parse-long s) (seconds-fraction->nanos sf))]
        (cond
          zone (ZonedDateTime/ofLocal ldt (ZoneId/of zone) (some-> offset ZoneOffset/of))
          offset (ZonedDateTime/of ldt (ZoneOffset/of offset))
          :else ldt))
      (catch Exception e
        (add-err! env (->CannotParseTimestamp ts-str (.getMessage e)))))

    (add-err! env (->CannotParseTimestamp ts-str nil))))

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

(defrecord CannotParseUUID [u-str msg]
  PlanError
  (error-string [_] (format "Cannot parse UUID: %s - failed with message %s" u-str msg)))

(defn- parse-uuid-literal [u-str env]
  (try
    (UUID/fromString u-str)
    (catch Exception e
      (add-err! env (->CannotParseUUID u-str (.getMessage e))))))

(defn fn-with-precision [fn-symbol ^ParserRuleContext precision-ctx]
  (if-let [precision (some-> precision-ctx (.getText) (parse-long))]
    (list fn-symbol precision)
    (list fn-symbol)))

(defn ->interval-expr [ve {:keys [start-field end-field leading-precision fractional-precision]}]
  (if end-field
    (list 'multi-field-interval ve start-field leading-precision end-field fractional-precision)
    (list 'single-field-interval ve start-field leading-precision fractional-precision)))

(defn iq-context->iq-map [^SqlParser$IntervalQualifierContext ctx]
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

(defn- trim-quotes-from-string [string]
  (subs string 1 (dec (count string))))

(defrecord CastArgsVisitor [env]
  SqlVisitor
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
      (if (instance? SqlParser$WithTimeZoneContext
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
      (if (instance? SqlParser$WithTimeZoneContext
                     (.withOrWithoutTimeZone ctx))
        {:->cast-fn (fn [ve]
                      (list* 'cast-tstz ve
                             [{:precision precision :unit time-unit}]))}

        {:cast-type [:timestamp-local time-unit]
         :cast-opts (when precision
                      {:precision precision})})))

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

  (visitCharacterStringType [_ _] {:cast-type :utf8}))

(defn handle-cast-expr [ve {:keys [cast-type cast-opts ->cast-fn]}] 
  (if ->cast-fn
    (->cast-fn ve)
    (cond-> (list 'cast ve cast-type)
      (not-empty cast-opts) (concat [cast-opts]))))

(defrecord AggregatesDisallowed []
  PlanError
  (error-string [_] "Aggregates are not allowed in this context"))

(defrecord ExprPlanVisitor [env scope]
  SqlVisitor
  (visitSearchCondition [this ctx] (-> (.expr ctx) (.accept this)))
  (visitExprPrimary1 [this ctx] (-> (.exprPrimary ctx) (.accept this)))
  (visitNumericExpr0 [this ctx] (-> (.numericExpr ctx) (.accept this)))
  (visitWrappedExpr [this ctx] (-> (.expr ctx) (.accept this)))

  (visitLiteralExpr [this ctx] (-> (.literal ctx) (.accept this)))
  (visitFloatLiteral [_ ctx] (parse-double (.getText ctx)))
  (visitIntegerLiteral [_ ctx] (parse-long (.getText ctx)))

  (visitCharacterStringLiteral [this ctx] (-> (.characterString ctx) (.accept this)))

  (visitSqlStandardString [_ ctx]
    (trim-quotes-from-string (.getText ctx)))

  (visitCEscapesString [_ ctx]
    (let [str (.getText ctx)]
      (StringUtil/parseCString (subs str 2 (dec (count str))))))

  (visitDateLiteral [this ctx] (parse-date-literal (.accept (.characterString ctx) this) env))
  (visitTimeLiteral [this ctx] (parse-time-literal (.accept (.characterString ctx) this) env))
  (visitTimestampLiteral [this ctx] (parse-timestamp-literal (.accept (.characterString ctx) this) env))

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

  (visitBooleanLiteral [_ ctx]
    (case (-> (.getText ctx) str/lower-case)
      "true" true
      "false" false
      "unknown" nil))
  
  (visitUUIDLiteral [this ctx] (parse-uuid-literal (.accept (.characterString ctx) this) env))

  (visitNullLiteral [_ _ctx] nil)

  (visitColumnExpr [this ctx] (-> (.columnReference ctx) (.accept this)))

  (visitColumnReference [{:keys [^Set !ob-col-refs]} ctx]
    (let [chain (rseq (mapv identifier-sym (.identifier (.identifierChain ctx))))
          matches (find-decls scope chain)]
      (when-let [sym (case (count matches)
                       0 (add-warning! env (->ColumnNotFound chain))
                       1 (first matches)
                       (add-err! env (->AmbiguousColumnReference chain)))]
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

  (visitPeriodSpecLiteral [this ctx] (-> (.literal ctx) (.accept this)))
  (visitPeriodSpecParam [this ctx] (-> (.parameterSpecification ctx) (.accept this)))

  (visitFieldAccess [this ctx]
    (let [ve (-> (.exprPrimary ctx) (.accept this))
          field-name (identifier-sym (.fieldName ctx))]
      (-> (list '. ve (keyword field-name))
          (vary-meta assoc :identifier field-name))))

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

  (visitConcatExpr [this ctx]
    (list 'concat
          (-> (.exprPrimary ctx 0) (.accept this))
          (-> (.exprPrimary ctx 1) (.accept this))))

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
    (list (symbol (.getText (.compOp ctx)))
          (-> (.expr ctx 0) (.accept this))
          (-> (.expr ctx 1) (.accept this))))

  (visitComparisonPredicatePart2 [{:keys [pt1] :as this} ctx]
    (list (symbol (.getText (.compOp ctx)))
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
    (let [p1 (-> (.periodPredicand ctx 0) (.accept this))
          p2 (-> (.periodPredicand ctx 1) (.accept this))]
      (list 'and (list '< (:from p1) (:to p2)) (list '> (:to p1) (:from p2)))))

  (visitPeriodEqualsPredicate [this ctx]
    (let [p1 (-> (.periodPredicand ctx 0) (.accept this))
          p2 (-> (.periodPredicand ctx 1) (.accept this))]
      (list 'and (list '= (:from p1) (:from p2)) (list '= (:to p1) (:to p2)))))

  (visitPeriodContainsPredicate [this ctx]
    (let [p1 (-> (.periodPredicand ctx) (.accept this))
          p2 (-> (.periodOrPointInTimePredicand ctx) (.accept this))]
      (list 'and (list '<= (:from p1) (:from p2)) (list '>= (:to p1) (:to p2)))))

  (visitPeriodPrecedesPredicate [this ctx]
    (let [p1 (-> (.periodPredicand ctx 0) (.accept this))
          p2 (-> (.periodPredicand ctx 1) (.accept this))]
      (list '<= (:to p1) (:from p2))))

  (visitPeriodSucceedsPredicate [this ctx]
    (let [p1 (-> (.periodPredicand ctx 0) (.accept this))
          p2 (-> (.periodPredicand ctx 1) (.accept this))]
      (list '>= (:from p1) (:to p2))))

  (visitPeriodImmediatelyPrecedesPredicate [this ctx]
    (let [p1 (-> (.periodPredicand ctx 0) (.accept this))
          p2 (-> (.periodPredicand ctx 1) (.accept this))]
      (list '= (:to p1) (:from p2))))

  (visitPeriodImmediatelySucceedsPredicate [this ctx]
    (let [p1 (-> (.periodPredicand ctx 0) (.accept this))
          p2 (-> (.periodPredicand ctx 1) (.accept this))]
      (list '= (:from p1) (:to p2))))

  (visitPeriodColumnReference [_ ctx]
    (let [tn (identifier-sym (.tableName ctx))
          pcn (-> (.periodColumnName ctx) (.getText) (str/upper-case))]
      (case pcn
        "VALID_TIME" {:from (find-decl scope ['xt$valid_from tn])
                      :to (find-decl scope ['xt$valid_to tn])}
        "SYSTEM_TIME" {:from (find-decl scope ['xt$system_from tn])
                       :to (find-decl scope ['xt$system_to tn])})))

  (visitPeriodValueConstructor [this ctx]
    (let [sv (some-> (.periodStartValue ctx) (.expr) (.accept this))
          ev (some-> (.periodEndValue ctx) (.expr) (.accept this))]
      {:from sv :to ev}))

  (visitPeriodOrPointInTimePredicand [this ctx] (.accept (.getChild ctx 0) this))

  (visitPointInTimePredicand [this ctx]
    (let [pit (-> (.expr ctx) (.accept this))]
      {:from pit :to pit}))

  (visitHasTablePrivilegePredicate [_ _] true)
  (visitHasSchemaPrivilegePredicate [_ _] true)

  (visitCurrentDateFunction [_ _] '(current-date))
  (visitCurrentTimeFunction [_ ctx] (fn-with-precision 'current-time (.precision ctx)))
  (visitCurrentTimestampFunction [_ ctx] (fn-with-precision 'current-timestamp (.precision ctx)))
  (visitLocalTimeFunction [_ ctx] (fn-with-precision 'local-time (.precision ctx)))
  (visitLocalTimestampFunction [_ ctx] (fn-with-precision 'local-timestamp (.precision ctx)))
  (visitEndOfTimeFunction [_ _] 'xtdb/end-of-time)

  (visitCurrentInstantFunction0 [this ctx] (-> (.currentInstantFunction ctx) (.accept this)))
  (visitEndOfTimeFunction0 [this ctx] (-> (.endOfTimeFunction ctx) (.accept this)))
  (visitPeriodSpecCurrentInstant [this ctx] (-> (.currentInstantFunction ctx) (.accept this)))
  (visitPeriodSpecEndOfTime [this ctx] (-> (.endOfTimeFunction ctx) (.accept this)))

  (visitDateTruncFunction [this ctx]
    (let [dtp (-> (.dateTruncPrecision ctx) (.getText) (str/upper-case))
          dts (-> (.dateTruncSource ctx) (.expr) (.accept this))
          dt-tz (some-> (.dateTruncTimeZone ctx) (.characterString) (.accept this))]
      (if dt-tz
        (list 'date_trunc dtp dts dt-tz)
        (list 'date_trunc dtp dts))))

  (visitAgeFunction [this ctx]
    (let [ve1 (-> (.expr ctx 0) (.accept this))
          ve2 (-> (.expr ctx 1) (.accept this))]
      (list 'age ve1 ve2)))

  (visitObjectExpr [this ctx] (.accept (.objectConstructor ctx) this))

  (visitObjectConstructor [this ctx]
    (->> (for [^SqlParser$ObjectNameAndValueContext kv (.objectNameAndValue ctx)]
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
  (visitUpperFunction [this ctx] (list 'upper (-> (.expr ctx) (.accept this))))

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

  (visitCurrentUserFunction [_ _] '(current-user))
  (visitCurrentSchemaFunction [_ _] '(current-schema))
  (visitCurrentDatabaseFunction [_ _] '(current-database))

  (visitSimpleCaseExpr [this ctx]
    (let [case-operand (-> (.expr ctx) (.accept this))
          when-clauses (->> (.simpleWhenClause ctx)
                            (mapv #(.accept ^SqlParser$SimpleWhenClauseContext % this))
                            (reduce into []))
          else-clause (some-> (.elseClause ctx) (.accept this))]
      (list* 'case case-operand (cond-> when-clauses
                                  else-clause (conj else-clause)))))

  (visitSearchedCaseExpr [this ctx]
    (let [when-clauses (->> (.searchedWhenClause ctx)
                            (mapv #(.accept ^SqlParser$SearchedWhenClauseContext % this))
                            (reduce into []))
          else-clause (some-> (.elseClause ctx) (.accept this))]
      (list* 'cond (cond-> when-clauses
                     else-clause (conj else-clause)))))

  (visitSimpleWhenClause [this ctx]
    (let [when-operands (-> (.whenOperandList ctx) (.whenOperand))
          when-exprs (mapv #(.accept (.getChild ^SqlParser$WhenOperandContext % 0) this) when-operands)
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
  
  (visitAggregateFunctionExpr [{:keys [!aggs] :as this} ctx]
    (if-not !aggs
      (add-err! env (->AggregatesDisallowed))
      (-> (.aggregateFunction ctx) (.accept this))))

  (visitCountStarFunction [{{:keys [!id-count]} :env, :keys [^Map !aggs]} ctx]
    (let [agg-sym (-> (->col-sym (str "xt$row_count_" (swap! !id-count inc)))
                      (vary-meta assoc :agg-out-sym? true))]
      (.put !aggs agg-sym {:agg-expr '(row-count)})
      agg-sym))

  (visitArrayAggFunction [{{:keys [!id-count]} :env, :keys [^Map !aggs], :as this} ctx]
    (if (.sortSpecificationList ctx)
      (throw (UnsupportedOperationException. "array-agg sort-spec"))

      (let [agg-sym (-> (->col-sym (str "xt$array_agg_out" (swap! !id-count inc)))
                        (vary-meta assoc :agg-out-sym? true))
            expr (-> (.expr ctx)
                     (.accept (assoc this :!aggs nil, :scope (assoc scope :!implied-gicrs nil))))]
        (.put !aggs agg-sym
              (if (:column? (meta expr))
                {:agg-expr (list 'array-agg expr)}
                (let [in-sym (-> (->col-sym (str "xt$array_agg_in" (swap! !id-count inc)))
                                 (vary-meta assoc :agg-in-sym? true))]
                  {:agg-expr (list 'array-agg in-sym)
                   :in-projection (->ProjectedCol {in-sym expr} in-sym)})))

        agg-sym)))

  (visitSetFunction [{{:keys [!id-count]} :env, :keys [^Map !aggs], :as this} ctx]
    (let [set-fn (symbol (str/lower-case (cond-> (.getText (.setFunctionType ctx))
                                           (= "distinct" (some-> (.setQuantifier ctx) (.getText) (str/lower-case)))
                                           (str "_distinct"))))
          agg-sym (-> (->col-sym (str "xt$" set-fn "_out_" (swap! !id-count inc)))
                      (vary-meta assoc :agg-out-sym? true))
          expr (-> (.expr ctx)
                   (.accept (assoc this :!aggs nil, :scope (assoc scope :!implied-gicrs nil))))]
      (.put !aggs agg-sym
            (if (:column? (meta expr))
              {:agg-expr (list set-fn expr)}

              (let [in-sym (-> (->col-sym (str "xt$" set-fn "_in_" (swap! !id-count inc)))
                               (vary-meta assoc :agg-in-sym? true))]
                {:agg-expr (list set-fn in-sym)
                 :in-projection (->ProjectedCol {in-sym expr} in-sym)})))

      agg-sym))

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

  (visitQuantifiedComparisonPredicate [{:keys [!subqs] :as this} ctx]
    (let [quantifier (case (str/lower-case (.getText (.quantifier ctx)))
                       "all" :all
                       ("some" "any") :any)
          op (symbol (.getText (.compOp ctx)))]
      (plan-sq (.subquery ctx) env scope !subqs
               {:sq-type :quantified-comparison
                :expr (.accept (.expr ctx) this)
                :op (cond-> op
                      (= quantifier :all) negate-op)})))

  (visitQuantifiedComparisonPredicatePart2 [{:keys [pt1 !subqs]} ctx]
    (let [quantifier (case (str/lower-case (.getText (.quantifier ctx)))
                       "all" :all
                       ("some" "any") :any)
          op (symbol (.getText (.compOp ctx)))]
      (plan-sq (.subquery ctx) env scope !subqs
               {:sq-type :quantified-comparison
                :expr pt1
                :op (cond-> op
                      (= quantifier :all) negate-op)})))

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
      (parse-long (.getText ctx)))))

(defn- wrap-predicates [plan predicate]
  (or (when (list? predicate)
        (let [[f & args] predicate]
          (when (= 'and f)
            (reduce wrap-predicates plan args))))

      [:select predicate
       plan]))

(defrecord ColumnCountMismatch [expected given]
  PlanError
  (error-string [_] (format "Column count mismatch: expected %s, given %s" expected given)))

(defprotocol OptimiseStatement
  (optimise-stmt [stmt]))

(defrecord QueryExpr [plan col-syms]
  OptimiseStatement (optimise-stmt [this] (update this :plan lp/rewrite-plan)))

(defn- plan-order-by [^SqlParser$OrderByClauseContext ctx
                      {:keys [!id-count] :as env} inner-scope outer-col-syms]
  (let [available-cols (set outer-col-syms)
        ob-expr-visitor (->ExprPlanVisitor env
                                           (reify Scope
                                             (available-cols [_ chain]
                                               (set/union (available-cols inner-scope chain)
                                                          (when (empty? chain) available-cols)))

                                             (find-decls [_ [col-name :as chain]]
                                               (or (when (= 1 (count chain))
                                                     (when-let [sym (get available-cols col-name)]
                                                       [sym]))
                                                   (find-decls inner-scope chain)))))]

    (-> (.sortSpecificationList ctx)
        (.sortSpecification)
        (->> (mapv (fn [^SqlParser$SortSpecificationContext sort-spec-ctx]
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

                         (contains? available-cols expr) {:order-by-spec [expr ob-opts]}

                         :else (let [in-sym (->col-sym (str "xt$ob" (swap! !id-count inc)))]
                                 {:order-by-spec [in-sym ob-opts]
                                  :in-projection {in-sym expr}})))))))))

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

(defrecord QueryPlanVisitor [env scope]
  SqlVisitor
  (visitWrappedQuery [this ctx] (-> (.queryExpressionBody ctx) (.accept this)))

  (visitQueryExpression [this ctx]
    (let [{:keys [env] :as this} (-> this
                                     (assoc-in [:env :ctes] (or (some-> (.withClause ctx)
                                                                        (.accept (->WithVisitor env scope)))
                                                                (:ctes env))))

          order-by-ctx (.orderByClause ctx)

          qeb-ctx (.queryExpressionBody ctx)

          ;; see SQL:2011 §7.13, syntax rule 28c
          simple-table-query? (when (instance? SqlParser$QueryBodyTermContext qeb-ctx)
                                (instance? SqlParser$QuerySpecificationContext (.queryTerm ^SqlParser$QueryBodyTermContext qeb-ctx)))]

      (as-> (.accept qeb-ctx (cond-> (assoc this :scope scope)
                               simple-table-query? (assoc :order-by-ctx order-by-ctx)))
          {:keys [plan col-syms] :as query-expr}

        (if (and order-by-ctx (not simple-table-query?))
          (->QueryExpr (-> plan
                           (wrap-isolated-ob col-syms (plan-order-by order-by-ctx env nil col-syms)))
                       col-syms)

          query-expr)

        (remove-ns-qualifiers query-expr env)

        (let [offset-clause (.resultOffsetClause ctx)
              limit-clause (.fetchFirstClause ctx)]
          (cond-> query-expr
            (or offset-clause limit-clause)
            (update :plan (fn [plan]
                            (let [expr-visitor (->ExprPlanVisitor env scope)]
                              [:top {:offset (some-> offset-clause
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

  (visitQuerySpecification [{:keys [out-col-syms order-by-ctx]} ctx]
    (let [qs-scope (->QuerySpecificationScope scope
                                              (when-let [from (.fromClause ctx)]
                                                (reduce (fn [left-table-ref ^ParserRuleContext table-ref]
                                                          (let [!sq-refs (HashMap.)
                                                                left-sq-scope (->SubqueryScope env left-table-ref !sq-refs)
                                                                right-table-ref (.accept table-ref (->TableRefVisitor env scope left-sq-scope))]
                                                            (if left-table-ref
                                                              (->CrossJoinTable env !sq-refs left-table-ref right-table-ref)
                                                              right-table-ref)))
                                                        nil
                                                        (.tableReference from))))

          where-plan (when-let [where-clause (.whereClause ctx)]
                       (let [!subqs (HashMap.)]
                         {:predicate (.accept (.expr where-clause) (map->ExprPlanVisitor {:env env, :scope qs-scope, :!subqs !subqs}))
                          :subqs (not-empty (into {} !subqs))}))

          group-invar-col-tracker (->GroupInvariantColsTracker env qs-scope (HashSet.))

          having-plan (when-let [having-clause (.havingClause ctx)]
                        (let [!subqs (HashMap.)
                              !aggs (HashMap.)]
                          {:predicate (.accept (.expr having-clause) (map->ExprPlanVisitor {:env env, :scope qs-scope, :!subqs !subqs, :!aggs !aggs}))
                           :subqs (not-empty (into {} !subqs))
                           :aggs (not-empty (into {} !aggs))}))

          select-clause (.selectClause ctx)

          {:keys [projected-cols] :as select-plan} (if select-clause
                                                     (.accept select-clause (->SelectClauseProjectedCols env group-invar-col-tracker))
                                                     (project-all-cols group-invar-col-tracker))

          aggs (not-empty (merge (:aggs select-plan) (:aggs having-plan)))
          grouped-table? (boolean (or aggs (.groupByClause ctx)))
          group-invariant-cols (when grouped-table?
                                 (.accept ctx group-invar-col-tracker))

          distinct? (some-> select-clause .setQuantifier (.getText) (str/upper-case) (= "DISTINCT"))

          ob-specs (some-> order-by-ctx
                           (plan-order-by env qs-scope (mapv :col-sym projected-cols)))

          plan (as-> (plan-table-ref qs-scope) plan
                 (if-let [{:keys [predicate subqs]} where-plan]
                   (-> plan
                       (apply-sqs subqs)
                       (wrap-predicates predicate))
                   plan)

                 (cond-> plan
                   grouped-table? (wrap-aggs aggs group-invariant-cols))

                 (if-let [{:keys [predicate subqs]} having-plan]
                   (-> plan
                       (apply-sqs subqs)
                       (wrap-predicates predicate))
                   plan)

                 (-> plan (apply-sqs (:subqs select-plan)))

                 (if order-by-ctx
                   (-> plan
                       (wrap-integrated-ob projected-cols ob-specs))

                   [:project (mapv :projection projected-cols)
                    plan]))]

      (as-> (->QueryExpr plan (mapv :col-sym (:projected-cols select-plan)))
          {:keys [plan col-syms] :as query-expr}

        (if out-col-syms
          (->QueryExpr [:rename (zipmap col-syms out-col-syms)
                        plan]
                       out-col-syms)
          query-expr)

        (if distinct?
          (->QueryExpr [:distinct plan]
                       col-syms)
          query-expr))))

  (visitValuesQuery [this ctx] (-> (.tableValueConstructor ctx) (.accept this)))
  (visitTableValueConstructor [this ctx] (-> (.rowValueList ctx) (.accept this)))

  (visitRowValueList [{{:keys [!id-count]} :env, :keys [out-col-syms]} ctx]
    (let [expr-plan-visitor (->ExprPlanVisitor env scope)
          col-syms (or out-col-syms
                       (-> (.rowValueConstructor ctx 0)
                           (.accept
                            (reify SqlVisitor
                              (visitSingleExprRowConstructor [_ ctx]
                                '[xt$column_1])

                              (visitMultiExprRowConstructor [_ ctx]
                                (->> (.expr ctx)
                                     (into [] (map-indexed (fn [idx _]
                                                             (->col-sym (str "xt$column_" (inc idx))))))))))))

          col-keys (mapv keyword col-syms)

          unique-table-alias (symbol (str "xt.values." (swap! !id-count inc)))

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

      (->QueryExpr [:rename unique-table-alias
                    [:table col-syms
                     (->> (.rowValueConstructor ctx)
                          (mapv #(.accept ^ParserRuleContext % row-visitor)))]]

                   (->> col-syms
                        (mapv #(->col-sym (str unique-table-alias) (str %)))))))

  (visitSubquery [this ctx] (-> (.queryExpression ctx) (.accept this)))

  (visitInsertValues [this ctx]
    (let [out-col-syms (->> (.columnName (.columnNameList ctx))
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

          query-expr))))

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
  (available-cols [_ chain]
    (when-not (and chain (not= chain [table-alias]))
      cols))
  
  (available-tables [_] [table-alias])

  (find-decls [_ [col-name table-name]]
    (when (and (or (nil? table-name) (= table-name table-alias))
               (or (contains? cols col-name) (types/temporal-column? col-name)))
      [(.computeIfAbsent !reqd-cols (->col-sym col-name)
                         (reify Function
                           (apply [_ col]
                             (->col-sym (str unique-table-alias) (str col)))))]))

  TableRef
  (plan-table-ref [_]
    [:rename unique-table-alias
     [:scan (cond-> {:table (symbol table-name)}
              for-valid-time (assoc :for-valid-time for-valid-time))
      (vec (.keySet !reqd-cols))]]))

(def ^:private vf-col (->col-sym "xt$valid_from"))
(def ^:private vt-col (->col-sym "xt$valid_to"))

(defrecord DmlValidTimeExtentsVisitor [env scope]
  SqlVisitor
  (visitDmlStatementValidTimeExtents [this ctx] (-> (.getChild ctx 0) (.accept this)))

  (visitDmlStatementValidTimeAll [_ _]
    {:for-valid-time :all-time,
     :projection [vf-col vt-col]})

  (visitDmlStatementValidTimePortion [{{:keys [default-all-valid-time?]} :env} ctx]
    (let [expr-visitor (->ExprPlanVisitor env scope)
          from-expr (-> (.expr ctx 0) (.accept expr-visitor))
          to-expr (-> (.expr ctx 1) (.accept expr-visitor))]
      {:for-valid-time [:between from-expr (when-not (= to-expr 'xtdb/end-of-time) to-expr)]
       :projection [{vf-col (cond
                              from-expr (list 'greatest vf-col (list 'cast from-expr types/temporal-col-type))
                              default-all-valid-time? vf-col
                              :else (list 'greatest vf-col (list 'cast '(current-timestamp) types/temporal-col-type)))}

                    {vt-col (if to-expr
                              (list 'least vt-col (list 'cast to-expr types/temporal-col-type))
                              vt-col)}]})))

(defn- default-vt-extents-projection [{:keys [default-all-valid-time?]}]
  [(if default-all-valid-time?
     vf-col
     {vf-col (list 'greatest vf-col (list 'cast '(current-timestamp) types/temporal-col-type))})
   vt-col])

(defrecord EraseTableRef [env table-name table-alias unique-table-alias cols ^Map !reqd-cols]
  Scope
  (available-cols [_ chain]
    (when-not (and chain (not= chain [table-alias]))
      cols))
  
  (available-tables [_] [table-alias])

  (find-decls [_ [col-name table-name :as chain]]
    (when (and (or (nil? table-name) (= table-name table-alias))
               (or (contains? cols col-name) (types/temporal-column? col-name)))
      [(.computeIfAbsent !reqd-cols (->col-sym col-name)
                         (reify Function
                           (apply [_ col]
                             (->col-sym (str unique-table-alias) (str col)))))]))

  TableRef
  (plan-table-ref [_]
    [:rename unique-table-alias
     [:scan {:table (symbol table-name)
             :for-system-time :all-time
             :for-valid-time :all-time}
      (vec (.keySet !reqd-cols))]]))

(def ^:private forbidden-update-cols
  (into #{'xt$id} (map symbol) (keys types/temporal-col-types)))

(defrecord ForbiddenColumnUpdate [col]
  PlanError
  (error-string [_] (format "Cannot UPDATE %s column" (subs (str col) 3))))

(defrecord InsertStmt [table query-plan]
  OptimiseStatement (optimise-stmt [this] (update-in this [:query-plan :plan] lp/rewrite-plan)))

(defrecord UpdateStmt [table query-plan]
  OptimiseStatement (optimise-stmt [this] (update-in this [:query-plan :plan] lp/rewrite-plan)))

(defrecord DeleteStmt [table query-plan]
  OptimiseStatement (optimise-stmt [this] (update-in this [:query-plan :plan] lp/rewrite-plan)))

(defrecord EraseStmt [table query-plan]
  OptimiseStatement (optimise-stmt [this] (update-in this [:query-plan :plan] lp/rewrite-plan)))

(defrecord AssertStmt [query-plan]
  OptimiseStatement (optimise-stmt [this] (update-in this [:query-plan :plan] lp/rewrite-plan)))

(defrecord StmtVisitor [env scope]
  SqlVisitor
  (visitDirectSqlStatement [this ctx] (-> (.directlyExecutableStatement ctx) (.accept this)))
  (visitDirectlyExecutableStatement [this ctx] (-> (.getChild ctx 0) (.accept this)))

  (visitQueryExpression [_ ctx] (-> ctx (.accept (->QueryPlanVisitor env scope))))

  (visitInsertStatement [_ ctx]
    (let [{:keys [col-syms] :as insert-plan} (-> (.insertColumnsAndSource ctx)
                                                 (.accept (->QueryPlanVisitor env scope)))]
      (when-not (contains? (set col-syms) 'xt$id)
        (add-err! env (->InsertWithoutXtId)))

      (->InsertStmt (identifier-sym (.tableName ctx)) insert-plan)))

  (visitUpdateStatementSearched [{{:keys [!id-count table-info]} :env} ctx]
    (let [internal-cols (mapv ->col-sym '[xt$iid xt$valid_from xt$valid_to])
          table-name (identifier-sym (.tableName ctx))
          table-alias (or (identifier-sym (.correlationName ctx)) table-name)
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

          set-clauses (for [^SqlParser$SetClauseContext set-clause (->> (.setClauseList ctx) (.setClause))
                            :let [set-target (.setTarget set-clause)]]
                        {(identifier-sym (.columnName set-target))
                         (.accept (.expr (.updateSource set-clause)) expr-visitor)})

          _ (doseq [forbidden-col (set/intersection (set (map (comp key first) set-clauses)) forbidden-update-cols)]
              (add-err! env (->ForbiddenColumnUpdate forbidden-col)))

          set-clauses-cols (set (mapv ffirst set-clauses))

          where-selection (when-let [search-clause (.searchCondition ctx)]
                            (let [!subqs (HashMap.)]
                              {:predicate (.accept search-clause (map->ExprPlanVisitor {:env env, :scope dml-scope, :!subqs !subqs}))
                               :subqs (not-empty (into {} !subqs))}))

          all-non-set-cols (filter (fn [col] 
                                     (not (contains? set-clauses-cols col))) 
                                   (available-cols dml-scope nil))

          col-projections (for [col all-non-set-cols
                                :let [col-sym (->col-sym col)]]
                            {col-sym (find-decl dml-scope [col-sym])})

          outer-projection (vec (concat '[xt$iid]
                                        (or vt-projection (default-vt-extents-projection env))
                                        set-clauses-cols
                                        (map ffirst col-projections)))]

      (->UpdateStmt (symbol table-name)
                    (->QueryExpr [:project outer-projection
                                  (as-> (plan-table-ref dml-scope) plan
                    
                                    (if-let [{:keys [predicate subqs]} where-selection]
                                      (-> plan
                                          (apply-sqs subqs)
                                          (wrap-predicates predicate))
                                      plan)
                    
                                    [:project (concat aliased-cols set-clauses col-projections) plan])]
                                 (vec (concat internal-cols set-clauses-cols all-non-set-cols))))))

  (visitDeleteStatementSearched [{{:keys [!id-count table-info]} :env} ctx]
    (let [internal-cols (mapv ->col-sym '[xt$iid xt$valid_from xt$valid_to])
          table-name (identifier-sym (.tableName ctx))
          table-alias (or (identifier-sym (.correlationName ctx)) table-name)
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

          projection (into '[xt$iid] (or vt-projection (default-vt-extents-projection env)))]
      (->DeleteStmt (symbol table-name)
                    (->QueryExpr [:project projection
                                  (as-> (plan-table-ref dml-scope) plan
                                    
                                    (if-let [{:keys [predicate subqs]} where-selection]
                                      (-> plan
                                          (apply-sqs subqs)
                                          (wrap-predicates predicate))
                                      plan)
                                    
                                    [:project aliased-cols plan])]
                                 internal-cols)))) 

  (visitEraseStatementSearched [{{:keys [!id-count table-info]} :env} ctx]
    (let [internal-cols '[xt$iid]
          table-name (identifier-sym (.tableName ctx))
          table-alias (or (identifier-sym (.correlationName ctx)) table-name)
          unique-table-alias (symbol (str table-alias "." (swap! !id-count inc)))
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
                                  (as-> (plan-table-ref dml-scope) plan
                                  
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
                                 [])))))

(defn add-throwing-error-listener [^Recognizer x]
  (doto x
    (.removeErrorListeners)
    (.addErrorListener 
     (proxy 
      [BaseErrorListener] []
       (syntaxError [_ _ line char-position-in-line msg _]
         (throw 
          (err/illegal-arg :xtdb/sql-error
                           {::err/message (str "Errors parsing SQL statement:\n  - "
                                               (format "line %s:%s %s" line char-position-in-line msg))})))))))

(defn ->parser ^xtdb.antlr.SqlParser [sql]
  (-> (SqlLexer. (CharStreams/fromString sql))
      (add-throwing-error-listener)
      (CommonTokenStream.)
      (SqlParser.)
      (add-throwing-error-listener)))

(defn- xform-table-info [table-info]
  (into {}
        (for [[tn cns] (merge info-schema/table-info
                              {'xt/txs #{"xt$id" "committed" "error" "tx_time"}}
                              table-info)]
          [(symbol tn) (->> cns
                            (map ->col-sym)
                            ^Collection
                            (sort-by identity (fn [s1 s2]
                                                (cond
                                                  (= 'xt$id s1) -1
                                                  (= 'xt$id s2) 1
                                                  :else (compare s1 s2))))
                            ->insertion-ordered-set)])))

(defn log-warnings [!warnings]
  (doseq [warning @!warnings]
    (log/warn (error-string warning))))

(defn plan-expr
  ([sql] (plan-expr sql {}))

  ([sql {:keys [scope table-info default-all-valid-time?]}]
   (let [!errors (atom [])
         !warnings (atom [])
         env {:!errors !errors
              :!warnings !warnings
              :!id-count (atom 0)
              :!param-count (atom 0)
              :table-info (xform-table-info table-info)
              :default-all-valid-time? (boolean default-all-valid-time?)}
         parser (->parser sql)
         plan (-> (.expr parser)
                  #_(doto (-> (.toStringTree parser) read-string (clojure.pprint/pprint))) ; <<no-commit>>
                  (.accept (->ExprPlanVisitor env scope)))]

     (if-let [errs (not-empty @!errors)]
       (throw (err/illegal-arg :xtdb/sql-error
                               {::err/message (str "Errors planning SQL statement:\n  - "
                                                   (str/join "\n  - " (map #(error-string %) errs)))
                                :errors errs}))
       (do
         (log-warnings !warnings)
         plan)))))

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
    [:assert-exists {} (->logical-plan query-plan)]))

(defn parse-statement ^SqlParser$DirectSqlStatementContext [sql]
  (let [parser (->parser sql)]
    (-> (.directSqlStatement parser)
        #_(doto (-> (.toStringTree parser) read-string (clojure.pprint/pprint))) ; <<no-commit>>
        )))

(defn plan-statement
  ([sql] (plan-statement sql {}))

  ([sql {:keys [scope table-info default-all-valid-time?]}]
   (let [!errors (atom [])
         !warnings (atom [])
         !param-count (atom 0)
         env {:!errors !errors
              :!warnings !warnings
              :!id-count (atom 0)
              :!param-count !param-count
              :table-info (xform-table-info table-info)
              :default-all-valid-time? (boolean default-all-valid-time?)}
         stmt (-> (parse-statement sql)
                  (.accept (->StmtVisitor env scope)))]
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
             (vary-meta assoc :param-count @!param-count)))))))

(comment
  (plan-statement "WITH foo AS (SELECT id FROM bar WHERE id = 5)
                   SELECT foo.id, baz.id
                   FROM foo, foo AS baz"
                  {:table-info {"bar" #{"id"}}}))
