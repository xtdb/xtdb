(ns xtdb.xtql.edn
  (:require [xtdb.error :as err])
  (:import (xtdb.query BindingSpec Expr Expr$Bool Expr$Call Expr$Double Expr$LogicVar Expr$Long Expr$Obj
                       Query Query$Aggregate Query$From Query$Pipeline Query$Return Query$Unify Query$Where Query$With Query$Without
                       TemporalFilter TemporalFilter$AllTime TemporalFilter$At TemporalFilter$In)))

(defmulti parse-query
  (fn [query]
    (when-not (list? query)
      (throw (err/illegal-arg :xtql/malformed-query {:query query})))

    (let [[op] query]
      (when-not (symbol? op)
        (throw (err/illegal-arg :xtql/malformed-query {:query query})))

      op)))

(defn parse-expr [expr]
  (cond
    (boolean? expr) (if expr Expr/TRUE Expr/FALSE)
    (int? expr) (Expr/val (long expr))
    (double? expr) (Expr/val (double expr))
    (symbol? expr) (Expr/lVar (str expr))
    (keyword? expr) (Expr/val expr)
    (vector? expr) (Expr/val (mapv parse-expr expr))
    (set? expr) (Expr/val (into #{} (map parse-expr) expr))
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

(defmethod parse-query :default [[op]]
  (throw (err/illegal-arg :xtql/unknown-query-op {:op op})))

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

(defn- parse-table+opts [table+opts query]
  (cond
    (keyword? table+opts) {:table (str (symbol table+opts))}

    (and (vector? table+opts) (= 2 (count table+opts)))
    (let [[table opts] table+opts]
      (when-not (keyword? table)
        (throw (err/illegal-arg :xtql/malformed-table {:table table, :from query})))

      (when-not (map? opts)
        (throw (err/illegal-arg :xtql/malformed-table-opts {:opts opts, :from query})))

      (let [{:keys [for-valid-time for-system-time]} opts]
        {:table (str (symbol table))
         :for-valid-time (some-> for-valid-time (parse-temporal-filter :for-valid-time query))
         :for-system-time (some-> for-system-time (parse-temporal-filter :for-system-time query))}))

    :else (throw (err/illegal-arg :xtql/malformed-from {:from query}))))

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

(defmethod parse-query 'from [[_ table+opts & binding-specs :as query]]
  (let [{:keys [table for-valid-time for-system-time]} (parse-table+opts table+opts query)]
    (-> (Query/from table)
        (cond-> for-valid-time (.forValidTime for-valid-time)
                for-system-time (.forSystemTime for-system-time))
        (.binding (parse-binding-specs binding-specs query)))))

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
             (map unparse (.bindSpecs from))))))

(extend-protocol Unparse
  Query$Pipeline (unparse [query] (list* '-> (unparse (.query query)) (mapv unparse (.tails query))))
  Query$Where (unparse [query] (list* 'where (mapv unparse (.preds query))))
  Query$With (unparse [query] (list* 'with (mapv unparse (.cols query))))
  Query$Without (unparse [query] (list* 'without (map keyword (.cols query))))
  Query$Return (unparse [query] (list* 'return (mapv unparse (.cols query))))
  Query$Aggregate (unparse [query] (list* 'aggregate (mapv unparse (.cols query))))
  Query$Unify (unparse [query] (list* 'unify (mapv unparse (.clauses query)))))
