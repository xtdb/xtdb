(ns xtdb.xtql
  (:require [clojure.set :as set]
            [xtdb.error :as err]
            [xtdb.table :as table])
  (:import (clojure.lang MapEntry)
           (java.util List)
           (xtdb.api.query Binding Expr Expr$Bool Expr$Call Expr$Double Expr$Exists Expr$Get Expr$ListExpr Expr$LogicVar Expr$Long Expr$MapExpr Expr$Null Expr$Obj Expr$Param Expr$Pull Expr$PullMany Expr$SetExpr Expr$Subquery Exprs TemporalFilter$AllTime TemporalFilter$At TemporalFilter$In TemporalFilters XtqlQuery XtqlQuery$QueryTail XtqlQuery$UnifyClause)
           (xtdb.table TableRef)))

(defn check-opt-keys [valid-keys opts]
  (when-let [invalid-opt-keys (not-empty (set/difference (set (keys opts)) valid-keys))]
    (throw (err/incorrect :invalid-opt-keys "Invalid keys provided to option map"
                          {:opts opts, :valid-keys valid-keys, :invalid-keys invalid-opt-keys}))))

(defn- query-type? [q] (seq? q))

(defmulti ^xtdb.api.query.XtqlQuery parse-query
  #_{:clj-kondo/ignore [:unused-binding]}
  (fn [query env]
    (when-not (query-type? query)
      (throw (err/incorrect :xtql/malformed-query "Malformed query" {:query query})))

    (let [[op] query]
      (when-not (symbol? op)
        (throw (err/incorrect :xtql/malformed-query "Malformed query" {:query query})))

      (case op
        fn 'fn*
        op))))

(defmethod parse-query :default [[op] _env]
  (throw (err/incorrect :xtql/unknown-query-op "Unknown query op" {:op op})))

(defn- parse-subquery [query env]
  (parse-query query (assoc env :subq? true)))

(defmulti parse-query-tail
  #_{:clj-kondo/ignore [:unused-binding]}
  (fn [query-tail env]
    (when-not (query-type? query-tail)
      (throw (err/incorrect :xtql/malformed-query-tail "Malformed query tail" {:query-tail query-tail})))

    (let [[op] query-tail]
      (when-not (symbol? op)
        (throw (err/incorrect :xtql/malformed-query-tail "Malformed query tail" {:query-tail query-tail})))

      op)))

(defmethod parse-query-tail :default [[op] _env]
  (throw (err/incorrect :xtql/unknown-query-tail "Unknown query tail" {:op op})))

(defmulti parse-unify-clause
  #_{:clj-kondo/ignore [:unused-binding]}
  (fn [clause env]
    (when-not (query-type? clause)
      (throw (err/incorrect :xtql/malformed-unify-clause "Malformed unify clause" {:clause clause})))

    (let [[op] clause]
      (when-not (symbol? op)
        (throw (err/incorrect :xtql/malformed-unify-clause "Malformed unify clause" {:clause clause})))

      op)))

(defmethod parse-unify-clause :default [[op] _env]
  (throw (err/incorrect :xtql/unknown-unify-clause "Unknown unify clause" {:op op})))

(defprotocol Unparse (unparse [this]))
(defprotocol UnparseQuery (unparse-query [this]))
(defprotocol UnparseQueryTail (unparse-query-tail [this]))
(defprotocol UnparseUnifyClause (unparse-unify-clause [this]))

(defrecord QueryWithParams [params query]
  XtqlQuery
  UnparseQuery
  (unparse-query [_] (list 'fn params (unparse-query query))))

(declare parse-expr)

(defn- sq-arg-bindings [parsed-query args env]
  (let [params (when (instance? QueryWithParams parsed-query)
                 (:params parsed-query))
        arg-exprs (or (some->> args (mapv #(parse-expr % env)))
                        (for [param params]
                          (Exprs/param (str param))))]

    (when-not (= (count params) (count arg-exprs))
      (throw (err/incorrect :xtql/subquery-arity-error "Subquery arity error" {:params params, :args args})))

    (when (seq params)
      (mapv (fn [param ^Expr arg-expr]
              (Binding. (str param) arg-expr))
            params arg-exprs))))

(defn parse-expr ^xtdb.api.query.Expr [expr {:keys [params] :as env}]
  (cond
    (nil? expr) Expr$Null/INSTANCE
    (true? expr) Expr$Bool/TRUE
    (false? expr) Expr$Bool/FALSE
    (int? expr) (Exprs/val (long expr))
    (double? expr) (Exprs/val (double expr))
    (symbol? expr) (if (= 'xtdb/end-of-time expr)
                     (Exprs/val 'xtdb/end-of-time)
                     (if-let [param (get params expr)]
                       (Exprs/param (str param))
                       (Exprs/lVar (str expr))))
    (keyword? expr) (Exprs/val expr)
    (vector? expr) (Exprs/list ^List (mapv #(parse-expr % env) expr))
    (set? expr) (Exprs/set ^List (mapv #(parse-expr % env) expr))
    (map? expr) (Exprs/map (into {} (map (juxt (comp #(subs % 1) str key) (comp #(parse-expr % env) val))) expr))

    (seq? expr) (do
                  (when (empty? expr)
                    (throw (err/incorrect :xtql/malformed-call "Malformed call" {:call expr})))

                  (let [[f & args] expr]
                    (when-not (symbol? f)
                      (throw (err/incorrect :xtql/malformed-call "Malformed call" {:call expr})))

                    (case f
                      .
                      (do
                        (when-not (and (= (count args) 2)
                                       (first args)
                                       (symbol? (second args)))
                          (throw (err/incorrect :xtql/malformed-get "Malformed get" {:expr expr})))

                        (Exprs/get (parse-expr (first args) env) (str (second args))))

                      (exists? q pull pull*)
                      (do
                        (when-not (and (<= 1 (count args) 2)
                                       (or (nil? (second args))
                                           (map? (second args))))
                          (throw (err/incorrect :xtql/malformed-subquery "Malformed subquery" {:expr expr})))

                        (let [[query+args] args
                              [query args] (if (vector? query+args)
                                             [(first query+args) (rest query+args)]
                                             [query+args nil])
                              parsed-query (parse-subquery query env)
                              arg-bindings (sq-arg-bindings parsed-query args env)]

                          (case f
                            exists? (Exprs/exists parsed-query arg-bindings)
                            q (Exprs/q parsed-query arg-bindings)
                            pull (Exprs/pull parsed-query arg-bindings)
                            pull* (Exprs/pullMany parsed-query arg-bindings))))

                      (Exprs/call (str f) ^List (mapv #(parse-expr % env) args)))))

    :else (Exprs/val expr)))

;;NOTE out-specs and arg-specs are currently indentical structurally, but one is an input binding,
;;the other an output binding.
;;I could see remerging these into a single binding-spec,
;;that being said, its possible we don't want arbitrary exprs in the from of arg specs, only plain vars
;;
;;TODO binding-spec-errs
(defn parse-out-specs
  "[{:from to-var} from-col-to-var {:col (pred)}]"
  ^List [specs _query env]
  (letfn [(parse-out-spec [[attr expr]]
            (when-not (keyword? attr)
              (throw (err/incorrect :xtql/malformed-bind-spec "Attribute in bind spec must be keyword"
                                    {:attr attr, :expr expr})))

            (Binding. (str (symbol attr)) (parse-expr expr env)))]

    (if (vector? specs)
      (->> specs
           (into [] (mapcat (fn [spec]
                              (cond
                                (symbol? spec) [(Binding. (str spec))]
                                (map? spec) (map parse-out-spec spec))))))

      (throw (UnsupportedOperationException.)))))

(defn- parse-var-specs
  "[{to-var (from-expr)}]"
  [specs _query env]
  (letfn [(parse-var-spec [[attr expr]]
            (when-not (symbol? attr)
              (throw (err/incorrect :xtql/malformed-var-spec "Attribute in var spec must be symbol"
                                    {:attr attr, :expr expr})))


            (Binding. (str attr) (parse-expr expr env)))]

    (cond
      (map? specs) (mapv parse-var-spec specs)
      (sequential? specs) (->> specs
                               (into [] (mapcat (fn [spec]
                                                  (if (map? spec) (map parse-var-spec spec)
                                                      (throw (err/incorrect :xtql/malformed-var-spec "Var specs must be pairs of bindings"
                                                                          {:spec spec})))))))
      :else (throw (UnsupportedOperationException.)))))

(defn parse-col-specs
  "[{:to-col (from-expr)} :col ...]"
  [specs _query env]
  (letfn [(parse-col-spec [[attr expr]]
            (when-not (keyword? attr)
              (throw (err/incorrect :xtql/malformed-col-spec "Attribute in col spec must be keyword"
                                    {:attr attr, :expr expr})))

            (Binding. (str (symbol attr)) (parse-expr expr env)))]

    (cond
      (map? specs) (mapv parse-col-spec specs)
      (sequential? specs) (->> specs
                               (into [] (mapcat (fn [spec]
                                                  (cond
                                                    (symbol? spec) [(Binding. (str spec))]
                                                    (map? spec) (map parse-col-spec spec)

                                                    :else
                                                    (throw (err/incorrect :xtql/malformed-col-spec "Short form of col spec must be a symbol"
                                                                        {:spec spec})))))))
      :else (throw (UnsupportedOperationException.)))))

(defn parse-temporal-filter [v k query env]
  (let [ctx {:v v, :filter k, :query query}]
    (if (= :all-time v)
      TemporalFilters/allTime

      (do
        (when-not (and (seq? v) (not-empty v))
          (throw (err/incorrect :xtql/malformed-temporal-filter "Malformed temporal filter" ctx)))

        (let [[tag & args] v]
          (when-not (symbol? tag)
            (throw (err/incorrect :xtql/malformed-temporal-filter "Malformed temporal filter" ctx)))

          (letfn [(assert-arg-count [expected args]
                    (when-not (= expected (count args))
                      (throw (err/incorrect :xtql/malformed-temporal-filter "Malformed temporal filter" (into ctx {:tag tag, :at args}))))

                    args)]
            (case tag
              at (let [[at] (assert-arg-count 1 args)]
                   (TemporalFilters/at (parse-expr at env)))

              in (let [[from to] (assert-arg-count 2 args)]
                   (TemporalFilters/in (parse-expr from env) (parse-expr to env)))

              from (let [[from] (assert-arg-count 1 args)]
                     (TemporalFilters/from (parse-expr from env)))

              to (let [[to] (assert-arg-count 1 args)]
                   (TemporalFilters/to (parse-expr to env)))

              (throw (err/incorrect :xtql/malformed-temporal-filter "Malformed temporal filter" (into ctx {:tag tag}))))))))))

(extend-protocol Unparse
  TemporalFilter$AllTime (unparse [_] :all-time)
  TemporalFilter$At (unparse [at] (list 'at (unparse (.getAt at))))
  TemporalFilter$In (unparse [in] (list 'in (some-> (.getFrom in) unparse) (some-> (.getTo in) unparse))))

(defn unparse-binding [base-type nested-type, ^Binding binding]
  (let [attr (.getBinding binding)
        expr (.getExpr binding)]
    (if base-type
      (if (and (instance? Expr$LogicVar expr)
               (= (.lv ^Expr$LogicVar expr) attr))
        (base-type attr)
        {(nested-type attr) (unparse expr)})
      {(nested-type attr) (unparse expr)})))

(def unparse-out-spec (partial unparse-binding symbol keyword))
(def unparse-col-spec (partial unparse-binding symbol keyword))
(def unparse-var-spec (partial unparse-binding nil symbol))

(defn- unparse-arg-binding [^Binding binding]
  (unparse (.getExpr binding)))

(defmethod parse-query 'fn* [[_fn params body] {:keys [!param-count subq?] :as env}]
  (when-not (and (vector? params)
                 (every? symbol? params))
    (throw (err/incorrect :xtql/malformed-fn-params "Malformed fn params" {:params params})))

  (let [env (-> env
                (update :params (fnil into {})
                        (cond
                          subq? (map (fn [sym]
                                       [sym (symbol (str "?_sq_" sym))]))
                          !param-count (map (fn [sym]
                                              [sym (symbol (str "?_" (dec (swap! !param-count inc))))]))
                          :else (map-indexed (fn [idx sym]
                                               [sym (symbol (str "?_" idx))])))
                        params))]
    (->QueryWithParams params (parse-query body env))))

(defrecord Pipeline [head tails]
  XtqlQuery
  UnparseQuery (unparse-query [_] (list* '-> (unparse-query head) (mapv unparse-query-tail tails))))

(defmethod parse-query '-> [[_ head & tails :as this] env]
  (when-not head
    (throw (err/incorrect :xtql/malformed-pipeline "Pipeline must contain at least one operator" {:pipeline this})))

  (->Pipeline (parse-query head env)
              (mapv #(parse-query-tail % env) tails)))

(defrecord Unify [unify-clauses]
  XtqlQuery
  UnparseQuery (unparse-query [_] (list* 'unify (mapv unparse-unify-clause unify-clauses))))

(defmethod parse-query 'unify [[_ & clauses :as this] env]
  (when (> 1 (count clauses))
    (throw (err/incorrect :xtql/malformed-unify "Unify must contain at least one sub clause" {:unify this})))
  (->Unify (mapv #(parse-unify-clause % env) clauses)))

(defrecord From [^TableRef table for-valid-time for-system-time bindings project-all-cols?]
  XtqlQuery
  XtqlQuery$UnifyClause

  UnparseQuery
  (unparse-query [_]
    (let [bind (mapv unparse-out-spec bindings)
          bind (if project-all-cols? (vec (cons '* bind)) bind)]
      (list 'from table
            (if (or for-valid-time for-system-time)
              (cond-> {:bind bind}
                for-valid-time (assoc :for-valid-time (unparse for-valid-time))
                for-system-time (assoc :for-system-time (unparse for-system-time)))
              bind))))

  UnparseUnifyClause (unparse-unify-clause [from] (unparse-query from)))

(def from-opt-keys #{:bind :for-valid-time :for-system-time})

(defn find-star-projection [star-ident bindings]
  ;;TODO worth checking here or in parse-out-specs for * in col/attr position? {* var}??
  ;;Arguably not checking for this could allow * to be a valid col name?
  (reduce
   (fn [acc binding]
     (if (= binding star-ident)
       (assoc acc :project-all-cols? true)
       (update acc :bind conj binding)))
   {:project-all-cols? false
    :bind []}
   bindings))

(defn parse-from [[_ table-or-opts opts :as this] {:keys [default-db] :as env}]
  (let [table-ref (cond
                    (instance? TableRef table-or-opts) table-or-opts
                    (map? table-or-opts) (let [{:keys [db table]} table-or-opts]
                                           (when-not table
                                             (throw (err/incorrect :xtql/malformed-from "`table` key must be present in `table` map"
                                                                   {:from this})))

                                           (table/->ref (or db default-db) table))
                    (keyword? table-or-opts) (table/->ref default-db table-or-opts)
                    :else (throw (err/incorrect :xtql/malformed-from "`table` must be a keyword or map"
                                                {:from this})))]

    (cond
      (or (nil? opts) (map? opts))

      (do
        (check-opt-keys from-opt-keys opts)

        (let [{:keys [for-valid-time for-system-time bind]} opts]
          (cond
            (nil? bind)
            (throw (err/incorrect :xtql/missing-bind "Missing bind" {:opts opts, :from this}))

            (not (vector? bind))
            (throw (err/incorrect :xtql/malformed-bind "Malformed bind" {:opts opts, :from this}))

            :else
            (let [{:keys [bind project-all-cols?]} (find-star-projection '* bind)]
              (->From table-ref
                      (some-> for-valid-time (parse-temporal-filter :for-valid-time this env))
                      (some-> for-system-time (parse-temporal-filter :for-system-time this env))
                      (parse-out-specs bind this env)
                      project-all-cols?)))))

      (vector? opts)
      (let [{:keys [bind project-all-cols?]} (find-star-projection '* opts)]
        (map->From {:table table-ref
                    :bindings (parse-out-specs bind this env)
                    :project-all-cols? project-all-cols?}))

      :else (throw (err/incorrect :xtql/malformed-table-opts "Malformed table opts" {:opts opts, :from this})))))

(defmethod parse-query 'from [this env] (parse-from this env))
(defmethod parse-unify-clause 'from [this env] (parse-from this env))

(defrecord Where [preds]
  XtqlQuery$QueryTail
  XtqlQuery$UnifyClause
  UnparseQueryTail (unparse-query-tail [_] (list* 'where (mapv unparse preds)))
  UnparseUnifyClause (unparse-unify-clause [this] (unparse-query-tail this)))

(defn parse-where [[_ & preds :as this] env]
  (when (> 1 (count preds))
    (throw (err/incorrect :xtql/malformed-where "Where must contain at least one predicate" {:where this})))

  (->Where (mapv #(parse-expr % env) preds)))

(defmethod parse-query-tail 'where [this env] (parse-where this env))
(defmethod parse-unify-clause 'where [this env] (parse-where this env))

(defrecord With [with-bindings]
  XtqlQuery$QueryTail
  XtqlQuery$UnifyClause
  UnparseQueryTail (unparse-query-tail [_] (list* 'with (mapv unparse-col-spec with-bindings)))
  UnparseUnifyClause (unparse-unify-clause [_] (list* 'with (mapv unparse-var-spec with-bindings))))

(defmethod parse-query-tail 'with [[_ & cols :as this] env]
  ;;TODO with uses col-specs but doesn't support short form, this needs handling
  (->With (parse-col-specs cols this env)))

(defmethod parse-unify-clause 'with [[_ & vars :as this] env]
  (->With (parse-var-specs vars this env)))

(defrecord Without [without-cols]
  XtqlQuery$QueryTail
  XtqlQuery$UnifyClause
  UnparseQueryTail (unparse-query-tail [_] (list* 'without without-cols)))

(defmethod parse-query-tail 'without [[_ & cols :as this] _env]
  (when-not (every? keyword? cols)
    (throw (err/incorrect :xtql/malformed-without "Columns must be keywords in without" {:without this})))
  (->Without cols))

(defrecord Return [return-bindings]
  XtqlQuery$QueryTail
  UnparseQueryTail (unparse-query-tail [_] (list* 'return (mapv unparse-col-spec return-bindings))))

(defmethod parse-query-tail 'return [[_ & cols :as this] env]
  (->Return (parse-col-specs cols this env)))

(defrecord Join [join-query args bindings]
  XtqlQuery$UnifyClause
  UnparseUnifyClause
  (unparse-unify-clause [_]
    (let [args args
          bind (mapv unparse-col-spec bindings)]
      (list 'join (unparse-query join-query)
            (if args
              (cond-> {:bind bind}
                (seq args) (assoc :args (mapv unparse-arg-binding args)))
              bind)))))

(defmethod parse-unify-clause 'join [[_ query+args bind :as join] env]
  (when (and bind (not (vector? bind)))
    (throw (err/incorrect :xtql/malformed-bind "Malformed bind" {:bind bind, :join join})))

  (let [[query args] (if (vector? query+args)
                       [(first query+args) (rest query+args)]
                       [query+args nil])
        parsed-query (parse-subquery query env)
        arg-bindings (sq-arg-bindings parsed-query args env)]
    (->Join parsed-query arg-bindings
            (some-> bind (parse-out-specs join env)))))

(defrecord LeftJoin [left-join-query args bindings]
  XtqlQuery$UnifyClause
  UnparseUnifyClause
  (unparse-unify-clause [_]
    (let [bind (mapv unparse-col-spec bindings)]
      (list 'left-join (unparse-query left-join-query)
            (if args
              (cond-> {:bind bind}
                (seq args) (assoc :args (mapv unparse-arg-binding args)))
              bind)))))

(defmethod parse-unify-clause 'left-join [[_ query+args bind :as left-join] env]
  (when (and bind (not (vector? bind)))
    (throw (err/incorrect :xtql/malformed-bind "Malformed bind" {:bind bind, :left-join left-join})))

  (let [[query args] (if (vector? query+args)
                       [(first query+args) (rest query+args)]
                       [query+args nil])
        parsed-query (parse-subquery query env)
        arg-bindings (sq-arg-bindings parsed-query args env)]
    (->LeftJoin parsed-query arg-bindings
                (some-> bind (parse-out-specs left-join env)))) )

(defrecord Aggregate [agg-bindings]
  XtqlQuery$QueryTail
  UnparseQueryTail (unparse-query-tail [_] (list* 'aggregate (mapv unparse-col-spec agg-bindings))))

(defmethod parse-query-tail 'aggregate [[_ & cols :as this] env]
  (->Aggregate (parse-col-specs cols this env)))

(defrecord OrderSpec [expr direction nulls]
  Unparse
  (unparse [_]
    (let [expr (unparse expr)]
      (if (and (not direction) (not nulls))
        expr
        (cond-> {:val expr}
          direction (assoc :dir direction)
          nulls (assoc :nulls nulls))))))

(def order-spec-opt-keys #{:val :dir :nulls})

(defn- parse-order-spec [order-spec this env]
  (if (map? order-spec)
    (let [{:keys [val dir nulls]} order-spec]
      (check-opt-keys order-spec-opt-keys order-spec)

      (when-not (contains? order-spec :val)
        (throw (err/incorrect :xtql/order-by-val-missing "order-by :val missing"
                              {:order-spec order-spec, :query this})))

      (->OrderSpec (parse-expr val env)
                   (case dir
                     (nil :asc :desc) dir

                     (throw (err/incorrect :xtql/malformed-order-by-direction "Malformed order-by direction"
                                           {:direction dir, :order-spec order-spec, :query this})))
                   (case nulls
                     (nil :first :last) nulls

                     (throw (err/incorrect :xtql/malformed-order-by-nulls "Malformed order-by nulls"
                                           {:nulls nulls, :order-spec order-spec, :query this})))))

    (->OrderSpec (parse-expr order-spec env) nil nil)))

(defrecord OrderBy [order-specs]
  XtqlQuery$QueryTail
  UnparseQueryTail (unparse-query-tail [_] (list* 'order-by (mapv unparse order-specs))))

(defmethod parse-query-tail 'order-by [[_ & order-specs :as this] env]
  (->OrderBy (mapv #(parse-order-spec % this env) order-specs)))

(defrecord Limit [limit]
  XtqlQuery$QueryTail
  UnparseQueryTail (unparse-query-tail [_] (list 'limit (unparse limit))))

(defmethod parse-query-tail 'limit [[_ length :as this] env]
  (when-not (= 2 (count this))
    (throw (err/incorrect :xtql/limit "Limit can only take a single value" {:limit this})))
  (->Limit (parse-expr length env)))

(defrecord Offset [offset]
  XtqlQuery$QueryTail
  UnparseQueryTail (unparse-query-tail [_] (list 'offset (unparse offset))))

(defmethod parse-query-tail 'offset [[_ length :as this] env]
  (when-not (= 2 (count this))
    (throw (err/incorrect :xtql/offset "Offset can only take a single value" {:offset this})))
  (->Offset (parse-expr length env)))

(defrecord DocsRelation [documents bindings]
  XtqlQuery
  XtqlQuery$UnifyClause

  UnparseQuery
  (unparse-query [_]
    (list 'rel
          (mapv #(into {} (map (fn [[k v]] (MapEntry/create (keyword k) (unparse v)))) %) documents)
          (mapv unparse-out-spec bindings)))

  UnparseUnifyClause (unparse-unify-clause [this] (unparse-query this)))

(defrecord ParamRelation [^Expr$Param param, bindings]
  XtqlQuery
  XtqlQuery$UnifyClause

  UnparseQuery
  (unparse-query [_]
    (list 'rel (symbol (.v param)) (mapv unparse-out-spec bindings)))

  UnparseUnifyClause (unparse-unify-clause [this] (unparse-query this)))

(defn- keyword-map? [m]
  (every? keyword? (keys m)))

(defn parse-rel [[_ param-or-docs bind :as this] env]
  (when-not (= 3 (count this))
    (throw (err/incorrect :xtql/rel "`rel` takes exactly 3 arguments" {:rel this})))

  (when-not (or (symbol? param-or-docs)
                (and (vector? param-or-docs) (every? keyword-map? param-or-docs)))
    (throw (err/incorrect :xtql/rel "`rel` takes a param or an explicit relation" {:rel this})))

  (let [parsed-bind (parse-out-specs bind this env)]
    (if (symbol? param-or-docs)
      (let [parsed-expr (parse-expr param-or-docs env)]
        (if (instance? Expr$Param parsed-expr)
          (->ParamRelation parsed-expr parsed-bind)
          (throw (err/incorrect :xtql/rel "Illegal second argument to `rel`" {:arg param-or-docs}))))
      (->DocsRelation (vec
                       (for [doc param-or-docs]
                         (->> doc
                              (into {} (map (fn [[k v]]
                                              (MapEntry/create k (parse-expr v env))))))))
                      parsed-bind))))

(defmethod parse-query 'rel [this env] (parse-rel this env))
(defmethod parse-unify-clause 'rel [this env] (parse-rel this env))

(defrecord Unnest [unnest-binding]
  XtqlQuery$QueryTail
  XtqlQuery$UnifyClause
  UnparseQueryTail (unparse-query-tail [_] (list 'unnest (unparse-col-spec unnest-binding)))
  UnparseUnifyClause (unparse-unify-clause [_] (list 'unnest (unparse-var-spec unnest-binding))))

(defn check-unnest [binding unnest]
  (when-not (and (= 2 (count unnest))
                 (map? binding)
                 (= 1 (count binding)))
    (throw (err/incorrect :xtql/unnest "Unnest takes only a single binding" {:unnest unnest}))))

(defmethod parse-query-tail 'unnest [[_ binding :as this] env]
  (check-unnest binding this)
  (->Unnest (first (parse-col-specs binding this env))))

(defmethod parse-unify-clause 'unnest [[_ binding :as this] env]
  (check-unnest binding this)
  (->Unnest (first (parse-var-specs binding this env))))

(defrecord UnionAll [union-all-queries]
  XtqlQuery
  UnparseQuery (unparse-query [_] (list* 'union-all (mapv unparse-query union-all-queries))))

(defmethod parse-query 'union-all [[_ & queries :as this] env]
  (when (> 1 (count queries))
    (throw (err/incorrect :xtql/malformed-union "Union must contain at least one sub query" {:union this})))
  (->UnionAll (mapv #(parse-query % env) queries)))

(extend-protocol Unparse
  Expr$Null (unparse [_] nil)
  Expr$LogicVar (unparse [e] (symbol (.lv e)))
  Expr$Param (unparse [e] (symbol (.v e)))
  Expr$Call (unparse [e] (list* (symbol (.f e)) (mapv unparse (.args e))))
  Expr$Bool (unparse [e] (.bool e))
  Expr$Double (unparse [e] (.dbl e))
  Expr$Long (unparse [e] (.lng e))
  Expr$ListExpr (unparse [v] (mapv unparse (.elements v)))
  Expr$SetExpr (unparse [s] (into #{} (map unparse (.elements s))))
  Expr$MapExpr (unparse [m] (-> (.elements m)
                                (update-keys keyword)
                                (update-vals unparse)))

  Expr$Obj
  (unparse [e] (.obj e))

  Expr$Get
  (unparse [e]
    (list '. (unparse (.expr e)) (symbol (.field e))))

  Expr$Exists
  (unparse [e]
    (let [q (unparse-query (.query e))]
      (list 'exists?
            (if-let [args (.args e)]
              (into [q] (map unparse-arg-binding) args)
              q))))

  Expr$Subquery
  (unparse [e]
    (let [q (unparse-query (.query e))]
      (list 'q
            (if-let [args (.args e)]
              (into [q] (map unparse-arg-binding) args)
              q))))

  Expr$Pull
  (unparse [e]
    (let [q (unparse-query (.query e))]
      (list 'pull (if-let [args (.args e)]
                    (into [q] (map unparse-arg-binding) args)
                    q))))

  Expr$PullMany
  (unparse [e]
    (let [q (unparse-query (.query e))]
      (list 'pull* (if-let [args (.args e)]
                     (into [q] (map unparse-arg-binding) args)
                     q)))))
