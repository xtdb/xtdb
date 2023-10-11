(ns xtdb.xtql.edn
  (:require [xtdb.error :as err])
  (:import (xtdb.query BindingSpec Expr Expr$Bool Expr$Call Expr$Double Expr$LogicVar Expr$Long Expr$Obj
                       Query Query$Aggregate Query$From Query$LeftJoin Query$Join Query$OrderBy Query$OrderDirection Query$OrderSpec Query$Pipeline Query$Return Query$Unify Query$UnionAll Query$Where Query$With Query$Without
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
                     (Expr/call (str f) (mapv parse-expr args))))

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
        :else obj))))

(defn- parse-temporal-filter [v k query]
  (let [ctx {:v v, :filter k, :query query}]
    (if (= :all-time v)
      TemporalFilter/ALL_TIME

      (do
        (when-not (and (vector? v) (not-empty v))
          (throw (err/illegal-arg :xtql/malformed-temporal-filter ctx)))

        (let [[tag & args] v]
          (when-not (keyword? tag)
            (throw (err/illegal-arg :xtql/malformed-temporal-filter ctx)))

          (letfn [(assert-arg-count [expected args]
                    (when-not (= expected (count args))
                      (throw (err/illegal-arg :xtql/malformed-temporal-filter (into ctx {:tag tag, :at args}))))

                    args)]
            (case tag
              :at (let [[at] (assert-arg-count 1 args)]
                    (TemporalFilter/at (parse-expr at)))

              :in (let [[from to] (assert-arg-count 2 args)]
                    (TemporalFilter/in (parse-expr from) (parse-expr to)))

              :from (let [[from] (assert-arg-count 1 args)]
                      (TemporalFilter/from (parse-expr from)))

              :to (let [[to] (assert-arg-count 1 args)]
                    (TemporalFilter/to (parse-expr to)))

              (throw (err/illegal-arg :xtql/malformed-temporal-filter (into ctx {:tag tag}))))))))))

(extend-protocol Unparse
  TemporalFilter$AllTime (unparse [_] :all-time)
  TemporalFilter$At (unparse [at] [:at (unparse (.at at))])
  TemporalFilter$In (unparse [in] [:in (some-> (.from in) unparse) (some-> (.to in) unparse)]))

(defn- parse-table+opts [table+opts from]
  (cond
    (keyword? table+opts) {:table (str (symbol table+opts))}

    (and (vector? table+opts) (= 2 (count table+opts)))
    (let [[table opts] table+opts]
      (when-not (keyword? table)
        (throw (err/illegal-arg :xtql/malformed-table {:table table, :from from})))

      (when-not (map? opts)
        (throw (err/illegal-arg :xtql/malformed-table-opts {:opts opts, :from from})))

      (let [{:keys [for-valid-time for-system-time]} opts]
        {:table (str (symbol table))
         :for-valid-time (some-> for-valid-time (parse-temporal-filter :for-valid-time from))
         :for-system-time (some-> for-system-time (parse-temporal-filter :for-system-time from))}))

    :else (throw (err/illegal-arg :xtql/malformed-from {:from from}))))

(defn- parse-binding-specs [binding-specs _query]
  (->> binding-specs
       (into [] (mapcat (fn [binding-spec]
                          (cond
                            (symbol? binding-spec) (let [attr (str binding-spec)]
                                                     [(BindingSpec/of attr (Expr/lVar attr))])
                            (map? binding-spec) (for [[attr expr] binding-spec]
                                                  (do
                                                    (when-not (keyword? attr)
                                                      ;; TODO error
                                                      )
                                                    (BindingSpec/of (str (symbol attr)) (parse-expr expr))))))))))

(defn parse-from [[_ table+opts & binding-specs :as this]]
  (let [{:keys [table for-valid-time for-system-time]} (parse-table+opts table+opts this)]
    (-> (Query/from table)
        (cond-> for-valid-time (.forValidTime for-valid-time)
                for-system-time (.forSystemTime for-system-time))
        (.binding (parse-binding-specs binding-specs this)))))

(defmethod parse-query 'from [this] (parse-from this))
(defmethod parse-unify-clause 'from [this] (parse-from this))

(defn- parse-join-query [query join]
  (if (vector? query)
    (let [[query & args] query]
      [(parse-query query) (parse-binding-specs args join)])
    [(parse-query query)]))

(defmethod parse-unify-clause 'join [[_ query & binding-specs :as join]]
  (let [[parsed-query args] (parse-join-query query join)]
    (-> (Query/join parsed-query args)
        (.binding (parse-binding-specs binding-specs join)))))


(extend-protocol Unparse
  BindingSpec
  (unparse [binding-spec]
    (let [attr (.attr binding-spec)
          expr (.expr binding-spec)]
      (if (and (instance? Expr$LogicVar expr)
               (= (.lv ^Expr$LogicVar expr) attr))
        (symbol attr)
        ;; TODO not sure if this should always return `(keyword attr)`,
        ;; because when its used in `unify` it's expected to be symbols?
        {(keyword attr) (unparse expr)})))

  Query$From
  (unparse [from]
    (let [table (keyword (.table from))
          for-valid-time (.forValidTime from)
          for-sys-time (.forSystemTime from)]
      (list* 'from (if (or for-valid-time for-sys-time)
                     [table (cond-> {}
                              for-valid-time (assoc :for-valid-time (unparse for-valid-time))
                              for-sys-time (assoc :for-system-time (unparse for-sys-time)))]
                     table)
             (map unparse (.bindSpecs from)))))

  Query$Join
  (unparse [join]
    (let [query (unparse (.query join))
          args (.args join)]
      (list* 'join
             (if args
               (into [query] (map unparse) args)
               query)

             (map unparse (.bindings join)))))

 Query$LeftJoin
  (unparse [join]
    (let [query (unparse (.query join))
          args (.args join)]
      (list* 'left-join
             (if args
               (into [query] (map unparse) args)
               query)

             (map unparse (.bindings join))))))

(extend-protocol Unparse
  Query$Pipeline (unparse [query] (list* '-> (unparse (.query query)) (mapv unparse (.tails query))))
  Query$Where (unparse [query] (list* 'where (mapv unparse (.preds query))))
  Query$With (unparse [query] (list* 'with (mapv unparse (.cols query))))
  Query$Without (unparse [query] (list* 'without (map keyword (.cols query))))
  Query$Return (unparse [query] (list* 'return (mapv unparse (.cols query))))
  Query$Aggregate (unparse [query] (list* 'aggregate (mapv unparse (.cols query))))
  Query$Unify (unparse [query] (list* 'unify (mapv unparse (.clauses query))))
  Query$UnionAll (unparse [query] (list* 'union-all (mapv unparse (.queries query)))))

(defmethod parse-query 'unify [[_ & clauses :as this]]
  (when (> 1 (count clauses))
    (throw (err/illegal-arg :xtql/malformed-unify {:unify this
                                                   :message "Unify most contain at least one sub clause"})))
  (->> clauses
       (mapv parse-unify-clause)
       (Query/unify)))

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

(defn parse-with [[_ & cols :as this]]
  (Query/with (parse-binding-specs cols this)))

(defmethod parse-query-tail 'with [this] (parse-with this))
(defmethod parse-unify-clause 'with [this] (parse-with this))

(defmethod parse-query-tail 'without [[_ & cols :as this]]
  (when-not (every? keyword? cols)
    (throw (err/illegal-arg :xtql/malformed-without {:without this
                                                     :message "Columns must be keywords in without"})))
  (Query/without (map name cols)))

(defmethod parse-query-tail 'return [[_ & cols :as this]]
  (Query/ret (parse-binding-specs cols this)))

(defmethod parse-query-tail 'aggregate [[_ & cols :as this]]
  (Query/aggregate (parse-binding-specs cols this)))

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
