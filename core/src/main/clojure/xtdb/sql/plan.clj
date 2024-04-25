(ns xtdb.sql.plan
  (:require [clojure.set :as set]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [xtdb.error :as err]
            [xtdb.logical-plan :as lp]
            [xtdb.types :as types]
            [xtdb.util :as util])
  (:import clojure.lang.MapEntry
           (java.util Collection HashMap LinkedHashSet Map SequencedSet)
           java.util.function.Function
           (org.antlr.v4.runtime BaseErrorListener CharStreams CommonTokenStream ParserRuleContext Recognizer)
           (xtdb.antlr SqlLexer SqlParser SqlParser$BaseTableContext SqlParser$DirectSqlStatementContext SqlParser$JoinSpecificationContext SqlParser$JoinTypeContext SqlVisitor)))

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

(declare ->ExprPlanVisitor ->QueryPlanVisitor)

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
                     (visitTableName [this ctx] (-> (.identifier ctx) (.accept this)))
                     (visitTableAlias [this ctx] (-> (.correlationName ctx) (.accept this)))
                     (visitColumnName [this ctx] (-> (.identifier ctx) (.accept this)))
                     (visitCorrelationName [this ctx] (-> (.identifier ctx) (.accept this)))

                     (visitRegularIdentifier [_ ctx] (symbol (util/str->normal-form-str (.getText ctx))))
                     (visitDelimitedIdentifier [_ ctx]
                       (let [di-str (.getText ctx)]
                         (symbol (subs di-str 1 (dec (count di-str))))))))))

(defprotocol Scope
  (available-cols [scope chain])
  (find-decls [scope chain]))

(defprotocol TableRef
  (plan-table-ref [scope]))

(extend-protocol Scope
  nil
  (available-cols [_ _])
  (find-decls [_ _]))

(extend-protocol TableRef
  nil
  (plan-table-ref [_]
    [:table [{}]]))

(defn- find-decl [scope chain]
  (let [[match & more-matches] (find-decls scope chain)]
    (assert (nil? more-matches) (str "multiple decls: " {:matches (cons match more-matches)}))
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
    [:at (-> ctx (.expr) (.accept expr-visitor))])

  (visitTableBetween [_ ctx]
    [:between
     (-> ctx (.expr 0) (.accept expr-visitor))
     (-> ctx (.expr 1) (.accept expr-visitor))])

  (visitTableFromTo [_ ctx]
    [:in
     (-> ctx (.expr 0) (.accept expr-visitor))
     (-> ctx (.expr 1) (.accept expr-visitor))]))

(defrecord MultipleTimePeriodSpecifications []
  PlanError
  (error-string [_] "Multiple time period specifications were specified"))

(defrecord BaseTable [env, ^SqlParser$BaseTableContext ctx
                      schema-name table-name table-alias unique-table-alias cols
                      ^Map !reqd-cols]
  Scope
  (available-cols [_ chain]
    (when-not (and chain (not= chain [table-alias]))
      cols))

  (find-decls [_ [col-name table-name]]
    (when (or (nil? table-name) (= table-name table-alias))
      (when (or (contains? cols col-name) (types/temporal-column? col-name))
        [(.computeIfAbsent !reqd-cols col-name
                           (reify Function
                             (apply [_ col]
                               (->col-sym (str unique-table-alias) (str col)))))])))

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
           [:scan (cond-> {:table table-name}
                    for-vt (assoc :for-valid-time for-vt)
                    for-st (assoc :for-system-time for-st))
            (vec (.keySet !reqd-cols))]])))))

(defrecord JoinTable [env l r
                      ^SqlParser$JoinTypeContext join-type-ctx
                      ^SqlParser$JoinSpecificationContext join-spec-ctx
                      common-cols]
  Scope
  (available-cols [_ chain]
    (->> [l r]
         (into [] (comp (mapcat #(available-cols % chain))
                        (distinct)))))

  (find-decls [_ chain]
    (->> (if (and (= 1 (count chain))
                  (get common-cols (first chain)))
           [l] [l r])
         (mapcat #(find-decls % chain))))

  TableRef
  (plan-table-ref [this-scope]
    (let [join-type (case (some-> join-type-ctx
                                  (.outerJoinType)
                                  (.getText)
                                  (str/upper-case))
                      "LEFT" :left-outer-join
                      "RIGHT" :right-outer-join
                      "FULL" :full-outer-join
                      :join)

          [join-type l r] (if (= join-type :right-outer-join)
                            [:left-outer-join r l]
                            [join-type l r])

          join-cond (or (if common-cols
                          (vec (for [col-name common-cols]
                                 {(find-decl l [col-name])
                                  (find-decl r [col-name])}))

                          (some-> join-spec-ctx
                                  (.accept
                                   (reify SqlVisitor
                                     (visitJoinCondition [_ ctx]
                                       (let [expr-visitor (->ExprPlanVisitor env this-scope)]
                                         [(-> (.expr ctx)
                                              (.accept expr-visitor))]))))))
                        [])
          planned-l (plan-table-ref l)
          planned-r (plan-table-ref r)]

      [join-type join-cond planned-l planned-r])))

(defrecord CrossJoinTable [env l r]
  Scope
  (available-cols [_ chain]
    (->> [l r]
         (into [] (comp (mapcat #(available-cols % chain))
                        (distinct)))))

  (find-decls [_ chain]
    (->> [l r]
         (mapcat #(find-decls % chain))))

  TableRef
  (plan-table-ref [_]
    (let [planned-l (plan-table-ref l)
          planned-r (plan-table-ref r)]
      [:cross-join planned-l planned-r])))

(defrecord DerivedTable [plan table-alias unique-table-alias, ^SequencedSet available-cols]
  Scope
  (available-cols [_ chain]
    (when-not (and chain (not= chain [table-alias]))
      available-cols))

  (find-decls [_ [col-name table-name]]
    (when (or (nil? table-name) (= table-name table-alias))
      (when-let [col (get available-cols col-name)]
        [(->col-sym (str unique-table-alias) (str col))])))

  TableRef
  (plan-table-ref [_]
    [:rename unique-table-alias
     plan]))

(defn- ->table-projection [^ParserRuleContext ctx]
  (some-> ctx
          (.accept
           (reify SqlVisitor
             (visitTableProjection [_ ctx]
               (some->> (.columnNameList ctx) (.columnName)
                        (mapv (comp ->col-sym identifier-sym))))))))

(defrecord ProjectedCol [projection col-sym])

(defrecord TableRefVisitor [env scope]
  SqlVisitor
  (visitBaseTable [{{:keys [!id-count table-info]} :env} ctx]
    (let [tn (some-> (.tableOrQueryName ctx) (.tableName))
          sn (identifier-sym (.schemaName tn))
          tn (identifier-sym (.identifier tn))
          table-alias (or (identifier-sym (.tableAlias ctx)) tn)
          unique-table-alias (symbol (str table-alias "." (swap! !id-count inc)))
          cols (some-> (.tableProjection ctx) (->table-projection))]
      (->BaseTable env ctx sn tn table-alias unique-table-alias
                   (->insertion-ordered-set (or cols (get table-info tn)))
                   (HashMap.))))

  (visitJoinTable [this ctx]
    (let [l (-> (.tableReference ctx 0) (.accept this))
          r (-> (.tableReference ctx 1) (.accept this))
          common-cols (.accept (.joinSpecification ctx)
                            (reify SqlVisitor
                              (visitJoinCondition [_ _] nil)
                              (visitNamedColumnsJoin [_ ctx]
                                (->> (.columnNameList ctx) (.columnName)
                                     (into #{} (map (comp ->col-sym identifier-sym)))))))]

      (->JoinTable env l r (.joinType ctx) (.joinSpecification ctx)
                   common-cols)))

  (visitCrossJoinTable [this ctx]
    (->CrossJoinTable env
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

  (visitWrappedTableReference [this ctx] (-> (.tableReference ctx) (.accept this))))

(defrecord QuerySpecificationScope [outer-scope from-table-ref]
  Scope
  (available-cols [_ chain] (available-cols from-table-ref chain))

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
    (let [sl-ctx (.selectList ctx)]
      (if (.ASTERISK sl-ctx)
        (vec (for [col-name (available-cols scope nil)
                   :let [sym (find-decl scope [col-name])]]
               (->ProjectedCol sym sym)))

        (->> (.selectSublist sl-ctx)
             (into [] (comp (map-indexed
                             (fn [col-idx ^ParserRuleContext sl-elem]
                               (.accept (.getChild sl-elem 0)
                                        (reify SqlVisitor
                                          (visitDerivedColumn [_ ctx]
                                            [(let [expr (.accept (.expr ctx) (->ExprPlanVisitor env scope))]
                                               (if-let [as-clause (.asClause ctx)]
                                                 (let [col-name (->col-sym (identifier-sym as-clause))]
                                                   (->ProjectedCol {col-name expr} col-name))

                                                 (->projected-col-expr col-idx expr)))])

                                          (visitQualifiedAsterisk [_ ctx]
                                            (let [[table-name schema-name] (rseq (mapv identifier-sym (.identifier (.identifierChain ctx))))]
                                              (when schema-name
                                                (throw (UnsupportedOperationException. "schema not supported")))

                                              (if-let [table-cols (available-cols scope [table-name])]
                                                (for [col-name table-cols
                                                      :let [sym (find-decl scope [col-name table-name])]]
                                                  (->ProjectedCol sym sym))
                                                (throw (UnsupportedOperationException. (str "Table not found: " table-name))))))))))
                            cat)))))))

(defn- project-all-cols [scope]
  ;; duplicated from the ASTERISK case above
  (vec (for [col-name (available-cols scope nil)
             :let [sym (find-decl scope [col-name])]]
         (->ProjectedCol sym sym))))

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

  (visitCharacterString [_ ctx]
    (let [str (.getText ctx)]
      (subs str 1 (dec (count str)))))

  (visitBooleanLiteral [_ ctx]
    (case (-> (.getText ctx) str/lower-case)
      "true" true
      "false" false
      "unknown" nil))

  (visitNullLiteral [_ _ctx] nil)

  (visitColumnExpr [this ctx] (-> (.columnReference ctx) (.accept this)))

  (visitColumnReference [_ ctx]
    (let [chain (rseq (mapv identifier-sym (.identifier (.identifierChain ctx))))
          matches (find-decls scope chain)]
      (case (count matches)
        0 (add-warning! env (->ColumnNotFound chain))
        1 (first matches)
        (add-err! env (->AmbiguousColumnReference chain)))))

  (visitParamExpr [this ctx] (-> (.parameterSpecification ctx) (.accept this)))

  (visitDynamicParameter [{{:keys [!param-count]} :env} _]
    (-> (symbol (str "?_" (dec (swap! !param-count inc))))
        (vary-meta assoc :param? true)))

  (visitPostgresParameter [{{:keys [!param-count]} :env} ctx]
    (-> (symbol (str "?_" (let [param-idx (parse-long (subs (.getText ctx) 1))]
                            (swap! !param-count max param-idx)
                            (dec param-idx))))
        (vary-meta assoc :param? true)))

  (visitUnaryPlusExpr [this ctx] (-> (.numericExpr ctx) (.accept this)))
  (visitUnaryMinusExpr [this ctx] (list '- (-> (.numericExpr ctx) (.accept this))))

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

  (visitNullPredicate [this ctx]
    (as-> (list 'nil? (-> (.expr ctx) (.accept this))) expr
       (if (.NOT ctx)
        (list 'not expr)
        expr))))

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

(defn- remove-ns-qualifiers [{:keys [plan col-syms]}]
  (let [out-projections (->> col-syms
                             (into [] (map (fn [col-sym]
                                             (if (namespace col-sym)
                                               (let [out-sym (-> (->col-sym (name col-sym))
                                                                 (with-meta (meta col-sym)))]
                                                 (->ProjectedCol {out-sym col-sym}
                                                                 out-sym))
                                               (->ProjectedCol col-sym col-sym))))))]
    (->QueryExpr [:project (mapv :projection out-projections)
                  plan]
                 (mapv :col-sym out-projections))))

(defrecord SetOperationColumnCountMismatch [operation-type lhs-count rhs-count]
  PlanError
  (error-string [_] 
    (format "Column count mismatch on %s set operation: lhs column count %s, rhs column count %s" operation-type lhs-count rhs-count)))

(defrecord QueryPlanVisitor [env scope]
  SqlVisitor
  (visitWrappedQuery [this ctx] (-> (.queryExpressionBody ctx) (.accept this)))

  (visitQueryExpression [this ctx]
    (-> (.accept (.queryExpressionBody ctx) this)
        (remove-ns-qualifiers)))

  (visitQueryBodyTerm [this ctx] (.accept (.queryTerm ctx) this))

  (visitUnionQuery [this ctx]
    (let [{l-plan :plan, l-col-syms :col-syms} (-> (.queryExpressionBody ctx) (.accept this)
                                                   (remove-ns-qualifiers))

          {r-plan :plan, r-col-syms :col-syms} (-> (.queryTerm ctx) (.accept this)
                                                   (remove-ns-qualifiers))

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
                                                   (remove-ns-qualifiers))

          {r-plan :plan, r-col-syms :col-syms} (-> (.queryTerm ctx) (.accept this)
                                                   (remove-ns-qualifiers))

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
                                                   (remove-ns-qualifiers))

          {r-plan :plan, r-col-syms :col-syms} (-> (.queryTerm ctx 1) (.accept this)
                                                   (remove-ns-qualifiers))

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

  (visitQuerySpecification [{:keys [out-col-syms]} ctx]
    (let [qs-scope (->QuerySpecificationScope scope
                                              (when-let [from (.fromClause ctx)]
                                                (reduce (fn [left-table-ref ^ParserRuleContext table-ref]
                                                          (let [right-table-ref (.accept table-ref (->TableRefVisitor env scope))]
                                                            (if left-table-ref
                                                              (->CrossJoinTable env left-table-ref right-table-ref)
                                                              right-table-ref)))
                                                        nil
                                                        (.tableReference from))))

          where-pred (when-let [where-clause (.whereClause ctx)]
                       (.accept (.expr where-clause) (->ExprPlanVisitor env qs-scope)))

          select-clause (.selectClause ctx)

          select-projected-cols (if select-clause
                                  (.accept select-clause (->SelectClauseProjectedCols env qs-scope))
                                  (project-all-cols qs-scope))

          plan (as-> (plan-table-ref qs-scope) plan
                 (cond-> plan
                   where-pred (wrap-predicates where-pred))

                 [:project (mapv :projection select-projected-cols)
                  plan])]

      (as-> (->QueryExpr plan (mapv :col-sym select-projected-cols))
          {:keys [plan col-syms] :as query-expr}

        (if out-col-syms
          (->QueryExpr [:rename (zipmap out-col-syms col-syms)
                        plan]
                       out-col-syms)
          query-expr)

        (if (some-> select-clause .setQuantifier (.getText) (str/upper-case) (= "DISTINCT"))
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
                        (mapv #(symbol (str unique-table-alias) (str %))))))))

(defrecord StmtVisitor [env scope]
  SqlVisitor
  (visitDirectSqlStatement [this ctx] (-> (.directlyExecutableStatement ctx) (.accept this)))
  (visitDirectlyExecutableStatement [this ctx] (-> (.getChild ctx 0) (.accept this)))

  (visitQueryExpression [_ ctx] (-> ctx (.accept (->QueryPlanVisitor env scope)))))

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
  (->> (for [[tn cns] table-info]
         [(symbol tn) (->> cns
                           (map ->col-sym)
                           ^Collection
                           (sort-by identity (fn [s1 s2]
                                               (cond
                                                 (= 'xt$id s1) -1
                                                 (= 'xt$id s2) 1
                                                 :else (compare s1 s2))))
                           ->insertion-ordered-set)])
       (into {})))

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
  QueryExpr (->logical-plan [{:keys [plan]}] plan))

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
  (plan-statement "SELECT * FROM foo JOIN bar USING (baz)"
                  {:table-info {"foo" #{"bar" "baz"}
                                "bar" #{"baz" "quux"}}}))
