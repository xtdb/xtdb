(ns xtdb.xtql.edn
  (:require [xtdb.error :as err])
  (:import (xtdb.query Expr Expr$Bool Expr$Call Expr$Double Expr$Exists
                       OutSpec ArgSpec ColSpec VarSpec
                       Expr$LogicVar Expr$Long Expr$Obj Expr$NotExists Expr$Subquery
                       Query Query$Aggregate Query$From Query$LeftJoin Query$Join Query$Limit
                       Query$OrderBy Query$OrderDirection Query$OrderSpec Query$Pipeline Query$Offset
                      Query$Return Query$Unify Query$UnionAll Query$Where Query$With Query$WithCols Query$Without
                       TemporalFilter TemporalFilter$AllTime TemporalFilter$At TemporalFilter$In)))

(defmulti parse-query
  (fn [query]
    (when-not (list? query)
      (throw (err/illegal-arg :xtql/malformed-query {:query query})))

    (let [[op] query]
      (when-not (symbol? op)
        (throw (err/illegal-arg :xtql/malformed-query {:query query})))

      op)))

(defmethod parse-query :default [[op]]
  (throw (err/illegal-arg :xtql/unknown-query-op {:op op})))

(defmulti parse-query-tail
  (fn [query-tail]
    (when-not (list? query-tail)
      (throw (err/illegal-arg :xtql/malformed-query-tail {:query-tail query-tail})))

    (let [[op] query-tail]
      (when-not (symbol? op)
        (throw (err/illegal-arg :xtql/malformed-query-tail {:query-tail query-tail})))

      op)))

(defmethod parse-query-tail :default [[op]]
  (throw (err/illegal-arg :xtql/unknown-query-tail {:op op})))

(defmulti parse-unify-clause
  (fn [clause]
    (when-not (list? clause)
      (throw (err/illegal-arg :xtql/malformed-unify-clause {:clause clause})))

    (let [[op] clause]
      (when-not (symbol? op)
        (throw (err/illegal-arg :xtql/malformed-unify-clause {:clause clause})))

      op)))

(defmethod parse-unify-clause :default [[op]]
  (throw (err/illegal-arg :xtql/unknown-unify-clause {:op op})))

(declare parse-arg-specs)

(defn parse-expr [expr]
  (cond
    (true? expr) Expr/TRUE
    (false? expr) Expr/FALSE
    (int? expr) (Expr/val (long expr))
    (double? expr) (Expr/val (double expr))
    (symbol? expr) (Expr/lVar (str expr))
    (keyword? expr) (Expr/val expr)
    (vector? expr) (Expr/val (mapv parse-expr expr))
    (set? expr) (Expr/val (into #{} (map parse-expr) expr))

    (list? expr) (do
                   (when (empty? expr)
                     (throw (err/illegal-arg :xtql/malformed-call {:call expr})))

                   (let [[f & args] expr]
                     (when-not (symbol? f)
                       (throw (err/illegal-arg :xtql/malformed-call {:call expr})))

                     (case f
                       (exists? not-exists? q)
                       (do
                         (when-not (and (<= 1 (count args) 2)
                                        (or (nil? (second args))
                                            (map? (second args))))
                           (throw (err/illegal-arg :xtql/malformed-subquery {:expr expr})))

                         (let [[query {:keys [args]}] args
                               parsed-query (parse-query query)
                               parsed-args (some-> args (parse-arg-specs expr))]
                           (case f
                             exists? (Expr/exists parsed-query parsed-args)
                             not-exists? (Expr/notExists parsed-query parsed-args)
                             q (Expr/q parsed-query parsed-args))))

                       (Expr/call (str f) (mapv parse-expr args)))))

    :else (Expr/val expr)))

(defprotocol Unparse
  (unparse [this]))

(extend-protocol Unparse
  Expr$LogicVar (unparse [e] (symbol (.lv e)))
  Expr$Call (unparse [e] (list* (symbol (.f e)) (mapv unparse (.args e))))
  Expr$Bool (unparse [e] (.bool e))
  Expr$Double (unparse [e] (.dbl e))
  Expr$Long (unparse [e] (.lng e))

  Expr$Obj
  (unparse [e]
    (let [obj (.obj e)]
      (cond
        (vector? obj) (mapv unparse obj)
        (set? obj) (into #{} (map unparse) obj)
        :else obj)))

  Expr$Exists
  (unparse [e]
    (list* 'exists? (unparse (.query e))
           (when-let [args (.args e)]
             [{:args (mapv unparse args)}])))

  Expr$NotExists
  (unparse [e]
    (list* 'not-exists? (unparse (.query e))
           (when-let [args (.args e)]
             [{:args (mapv unparse args)}])))

  Expr$Subquery
  (unparse [e]
    (list* 'q (unparse (.query e))
           (when-let [args (.args e)]
             [{:args (mapv unparse args)}]))))

(defn- parse-temporal-filter [v k query]
  (let [ctx {:v v, :filter k, :query query}]
    (if (= :all-time v)
      TemporalFilter/ALL_TIME

      (do
        (when-not (and (list? v) (not-empty v))
          (throw (err/illegal-arg :xtql/malformed-temporal-filter ctx)))

        (let [[tag & args] v]
          (when-not (symbol? tag)
            (throw (err/illegal-arg :xtql/malformed-temporal-filter ctx)))

          (letfn [(assert-arg-count [expected args]
                    (when-not (= expected (count args))
                      (throw (err/illegal-arg :xtql/malformed-temporal-filter (into ctx {:tag tag, :at args}))))

                    args)]
            (case tag
              at (let [[at] (assert-arg-count 1 args)]
                   (TemporalFilter/at (parse-expr at)))

              in (let [[from to] (assert-arg-count 2 args)]
                   (TemporalFilter/in (parse-expr from) (parse-expr to)))

              from (let [[from] (assert-arg-count 1 args)]
                     (TemporalFilter/from (parse-expr from)))

              to (let [[to] (assert-arg-count 1 args)]
                   (TemporalFilter/to (parse-expr to)))

              (throw (err/illegal-arg :xtql/malformed-temporal-filter (into ctx {:tag tag}))))))))))

(extend-protocol Unparse
  TemporalFilter$AllTime (unparse [_] :all-time)
  TemporalFilter$At (unparse [at] (list 'at (unparse (.at at))))
  TemporalFilter$In (unparse [in] (list 'in (some-> (.from in) unparse) (some-> (.to in) unparse))))

;;NOTE out-specs and arg-specs are currently indentical structurally, but one is an input binding,
;;the other an output binding.
;;I could see remerging these into a single binding-spec,
;;that being said, its possible we don't want arbitrary exprs in the from of arg specs, only plain vars
;;
;;TODO binding-spec-errs
(defn- parse-out-specs
  "[{:from to-var} from-col-to-var {:col (pred)}]"
  [specs _query]
  (letfn [(parse-out-spec [[attr expr]]
            (when-not (keyword? attr)
              ;; TODO error
              )

            (OutSpec/of (str (symbol attr)) (parse-expr expr)))]

    (cond
      (map? specs) (mapv parse-out-spec specs)
      (vector? specs) (->> specs
                           (into [] (mapcat (fn [spec]
                                              (cond
                                                (symbol? spec) (let [attr (str spec)]
                                                                 [(OutSpec/of attr (Expr/lVar attr))])
                                                (map? spec) (map parse-out-spec spec))))))
      ;; TODO error
      :else (throw (UnsupportedOperationException.)))))

(defn- parse-arg-specs
  "[{:to-var (from-expr)} to-var-from-var]"
  [specs _query]
  (->> specs
       (into [] (mapcat (fn [spec]
                          (cond
                            (symbol? spec) (let [attr (str spec)]
                                             [(ArgSpec/of attr (Expr/lVar attr))])
                            (map? spec) (for [[attr expr] spec]
                                          (do
                                            (when-not (keyword? attr)
                                              ;; TODO error
                                              )
                                            (ArgSpec/of (str (symbol attr)) (parse-expr expr))))))))))
(defn- parse-var-specs
  "[{to-var (from-expr)}]"
  [specs _query]
  (->> specs
       (into [] (mapcat (fn [spec]
                          (if (map? spec)
                            (for [[attr expr] spec]
                              (do
                                (when-not (symbol? attr)
                                  (throw (err/illegal-arg :xtql/malformed-var-spec)))
                                (VarSpec/of (str attr) (parse-expr expr))))
                            (throw (err/illegal-arg :xtql/malformed-var-spec))))))))

(defn- parse-col-specs
  "[{:to-col (from-expr)} :col ...]"
  [specs _query]
  (->> specs
       (into [] (mapcat (fn [spec]
                          (cond
                            (keyword? spec) (let [attr (str (symbol spec))]
                                             [(ColSpec/of attr (Expr/lVar attr))])
                            (map? spec) (for [[attr expr] spec]
                                          (do
                                            (when-not (keyword? attr)
                                              ;; TODO error
                                              )
                                            (ColSpec/of (str (symbol attr)) (parse-expr expr))))))))))

(defn parse-from [[_ table opts :as this]]
  (cond
    (not (keyword? table))
    (throw (err/illegal-arg :xtql/malformed-table {:table table, :from this}))

    (and opts (not (map? opts)))
    (throw (err/illegal-arg :xtql/malformed-table-opts {:opts opts, :from this}))

    :else (let [{:keys [for-valid-time for-system-time bind]} opts]
            (cond-> (Query/from (str (symbol table)))
              for-valid-time (.forValidTime (parse-temporal-filter for-valid-time :for-valid-time this))
              for-system-time (.forSystemTime (parse-temporal-filter for-system-time :for-system-time this))
              bind (.binding (parse-out-specs bind this))))))

(defmethod parse-query 'from [this] (parse-from this))
(defmethod parse-unify-clause 'from [this] (parse-from this))

(defmethod parse-unify-clause 'join [[_ query opts :as join]]
  (when-not (or (nil? opts) (map? opts))
    (throw (err/illegal-arg :malformed-join-opts {:opts opts, :join join})))

  (let [{:keys [args bind]} opts]
    (-> (Query/join (parse-query query) args)
        (.binding (some-> bind (parse-out-specs join))))))

(defmethod parse-unify-clause 'left-join [[_ query opts :as left-join]]
  (when-not (or (nil? opts) (map? opts))
    (throw (err/illegal-arg :malformed-join-opts {:opts opts, :left-join left-join})))

  (let [{:keys [args bind]} opts]
    (-> (Query/leftJoin (parse-query query) args)
        (.binding (some-> bind (parse-out-specs left-join))))))

(defn- unparse-binding-spec [attr expr base-type nested-type]
  (if base-type
    (if (and (instance? Expr$LogicVar expr)
             (= (.lv ^Expr$LogicVar expr) attr))
      (base-type attr)
      {(nested-type attr) (unparse expr)})
    {(nested-type attr) (unparse expr)}))

(extend-protocol Unparse
  OutSpec (unparse [spec] (unparse-binding-spec (.attr spec) (.expr spec) symbol keyword))
  ArgSpec (unparse [spec] (unparse-binding-spec (.attr spec) (.expr spec) symbol keyword))
  VarSpec (unparse [spec] (unparse-binding-spec (.attr spec) (.expr spec) false symbol))
  ColSpec (unparse [spec] (unparse-binding-spec (.attr spec) (.expr spec) keyword keyword))

  Query$From
  (unparse [from]
    (let [for-valid-time (.forValidTime from)
          for-sys-time (.forSystemTime from)
          bind (.bindings from)]
      (list* 'from (keyword (.table from))
             (when (or for-valid-time for-sys-time bind)
               [(cond-> {}
                  for-valid-time (assoc :for-valid-time (unparse for-valid-time))
                  for-sys-time (assoc :for-system-time (unparse for-sys-time))
                  bind (assoc :bind (mapv unparse bind)))]))))

  Query$Join
  (unparse [join]
    (let [args (.args join)
          bind (.bindings join)]
      (list* 'join (unparse (.query join))
             (when (or args bind)
               [(cond-> {}
                  args (assoc :args (mapv unparse args))
                  bind (assoc :bind (mapv unparse bind)))]))))

  Query$LeftJoin
  (unparse [left-join]
    (let [args (.args left-join)
          bind (.bindings left-join)]
      (list* 'left-join (unparse (.query left-join))
             (when (or args bind)
               [(cond-> {}
                  args (assoc :args (mapv unparse args))
                  bind (assoc :bind (mapv unparse bind)))])))))

(extend-protocol Unparse
  Query$Pipeline (unparse [query] (list* '-> (unparse (.query query)) (mapv unparse (.tails query))))
  Query$Where (unparse [query] (list* 'where (mapv unparse (.preds query))))
  Query$With (unparse [query] (list* 'with (mapv unparse (.vars query))))
  Query$WithCols (unparse [query] (list* 'with (mapv unparse (.cols query))))
  Query$Without (unparse [query] (list* 'without (map keyword (.cols query))))
  Query$Return (unparse [query] (list* 'return (mapv unparse (.cols query))))
  Query$Aggregate (unparse [query] (list* 'aggregate (mapv unparse (.cols query))))
  Query$Unify (unparse [query] (list* 'unify (mapv unparse (.clauses query))))
  Query$UnionAll (unparse [query] (list* 'union-all (mapv unparse (.queries query))))
  Query$Limit (unparse [this] (list 'limit (.length this)))
  Query$Offset (unparse [this] (list 'offset (.length this))))

(defmethod parse-query 'unify [[_ & clauses :as this]]
  (when (> 1 (count clauses))
    (throw (err/illegal-arg :xtql/malformed-unify {:unify this
                                                   :message "Unify most contain at least one sub clause"})))
  (->> clauses
       (mapv parse-unify-clause)
       (Query/unify)))

(defmethod parse-query 'union-all [[_ & queries :as this]]
  (when (> 1 (count queries))
    (throw (err/illegal-arg :xtql/malformed-union {:union this
                                                   :message "Union must contain a least one sub query"})))
  (->> queries
       (mapv parse-query)
       (Query/unionAll)))

(defn parse-where [[_ & preds :as this]]
  (when (> 1 (count preds))
    (throw (err/illegal-arg :xtql/malformed-where {:where this
                                                   :message "Where most contain at least one predicate"})))
  (Query/where (mapv parse-expr preds)))

(defmethod parse-query-tail 'where [this] (parse-where this))
(defmethod parse-unify-clause 'where [this] (parse-where this))

(defmethod parse-query '-> [[_ head & tails :as this]]
  (when-not head
    (throw (err/illegal-arg :xtql/malformed-pipeline {:pipeline this
                                                      :message "Pipeline most contain at least one operator"})))
  (Query/pipeline (parse-query head) (mapv parse-query-tail tails)))

;TODO Align errors with json ones where appropriate.

(defmethod parse-query-tail 'with [[_ & cols :as this]]
  (Query/withCols (parse-col-specs cols this)))

(defmethod parse-unify-clause 'with [[_ & vars :as this]]
  (Query/with (parse-var-specs vars this)))

(defmethod parse-query-tail 'without [[_ & cols :as this]]
  (when-not (every? keyword? cols)
    (throw (err/illegal-arg :xtql/malformed-without {:without this
                                                     :message "Columns must be keywords in without"})))
  (Query/without (map name cols)))

(defmethod parse-query-tail 'return [[_ & cols :as this]]
  (Query/ret (parse-col-specs cols this)))

(defmethod parse-query-tail 'aggregate [[_ & cols :as this]]
  (Query/aggregate (parse-col-specs cols this)))

(defmethod parse-query-tail 'limit [[_ length :as this]]
  (when-not (= 2 (count this))
    (throw (err/illegal-arg :xtql/limit {:limit this :message "Limit can only take a single value"})))
  (Query/limit length))

(defmethod parse-query-tail 'offset [[_ length :as this]]
  (when-not (= 2 (count this))
    (throw (err/illegal-arg :xtql/offset {:offset this :message "Offset can only take a single value"})))
  (Query/offset length))

(defn- parse-order-spec [order-spec this]
  (if (vector? order-spec)
    (do
      (when-not (= 2 (count order-spec))
        (throw (err/illegal-arg :xtql/malformed-order-spec {:order-spec order-spec, :query this})))

      (let [[expr opts] order-spec
            parsed-expr (parse-expr expr)]
        (when-not (map? opts)
          (throw (err/illegal-arg :xtql/malformed-order-spec {:order-spec order-spec, :query this})))

        (let [{:keys [dir]} opts]
          (case dir
            :asc (Query/asc parsed-expr)
            :desc (Query/desc parsed-expr)

            (throw (err/illegal-arg :xtql/malformed-order-by-direction
                                    {:direction dir, :order-spec order-spec, :query this}))))))

    (Query/asc (parse-expr order-spec))))

(defmethod parse-query-tail 'order-by [[_ & order-specs :as this]]
  (Query/orderBy (mapv #(parse-order-spec % this) order-specs)))

(extend-protocol Unparse
  Query$OrderSpec
  (unparse [spec]
    (let [expr (unparse (.expr spec))
          dir (.direction spec)]
      (if (= Query$OrderDirection/DESC dir)
        [expr {:dir :desc}]
        expr)))

  Query$OrderBy
  (unparse [query]
    (list* 'order-by (mapv unparse (.orderSpecs query)))))
