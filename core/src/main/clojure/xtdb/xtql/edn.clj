(ns xtdb.xtql.edn
  (:require [xtdb.error :as err]
            [xtdb.util :as util])
  (:import (xtdb.query Expr Expr$LogicVar Expr$Obj
                       QueryStep QueryStep$BindingSpec QueryStep$From
                       TemporalFilter TemporalFilter$At TemporalFilter$In)))

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
    (symbol? expr) (Expr/lVar (str expr))
    (keyword? expr) (Expr/val expr)
    :else (throw (UnsupportedOperationException. (format "expr: %s" (pr-str expr))))))

(defprotocol Unparse
  (unparse [this]))

(extend-protocol Unparse
  Expr$LogicVar (unparse [lv] (symbol (.lv lv)))
  Expr$Obj (unparse [obj] (.obj obj)))

(defmethod parse-query :default [[op]]
  (throw (err/illegal-arg :xtql/unknown-query-op {:op op})))

(defn- parse-temporal-filter [v k step]
  (let [ctx {:v v, :filter k, :step step}]
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

                    args)

                  (->instant [v]
                    (try
                      (util/->instant v)
                      (catch Exception _
                        (throw (err/illegal-arg :xtql/malformed-temporal-filter (into ctx {:tag tag, :v v}))))))]
            (case tag
              :at (let [[at] (assert-arg-count 1 args)]
                    (TemporalFilter/at (->instant at)))

              :in (let [[from to] (assert-arg-count 2 args)]
                    (TemporalFilter/in (some-> from ->instant) (some-> to ->instant)))

              :from (let [[from] (assert-arg-count 1 args)]
                      (TemporalFilter/from (->instant from)))

              :to (let [[to] (assert-arg-count 1 args)]
                    (TemporalFilter/to (->instant to)))

              (throw (err/illegal-arg :xtql/malformed-temporal-filter (into ctx {:tag tag}))))))))))

(extend-protocol Unparse
  TemporalFilter$At (unparse [at] [:at (.at at)])
  TemporalFilter$In (unparse [in] [:in (.from in) (.to in)]))

(defn- parse-table+opts [table+opts step]
  (cond
    (keyword? table+opts) {:table (str (symbol table+opts))}

    (and (vector? table+opts) (= 2 (count table+opts)))
    (let [[table opts] table+opts]
      (when-not (keyword? table)
        (throw (err/illegal-arg :xtql/malformed-table {:table table, :from step})))

      (when-not (map? opts)
        (throw (err/illegal-arg :xtql/malformed-table-opts {:opts opts, :from step})))

      (let [{:keys [for-valid-time for-system-time]} opts]
        {:table (str (symbol table))
         :for-valid-time (some-> for-valid-time (parse-temporal-filter :for-valid-time step))
         :for-system-time (some-> for-system-time (parse-temporal-filter :for-system-time step))}))

    :else (throw (err/illegal-arg :xtql/malformed-from {:from step}))))

(defn- parse-binding-specs [binding-specs step]
  (->> binding-specs
       (into [] (mapcat (fn [binding-spec]
                          (cond
                            (symbol? binding-spec) (let [attr (str binding-spec)]
                                                     [(QueryStep/bindSpec attr (Expr/lVar attr))])
                            (map? binding-spec) (for [[attr expr] binding-spec]
                                                  (do
                                                    (when-not (keyword? attr)
                                                      ;; TODO error
                                                      )
                                                    (QueryStep/bindSpec (str (symbol attr))
                                                                        (parse-expr expr))))))))))

(defmethod parse-query 'from [[_ table+opts & binding-specs :as step]]
  (let [{:keys [table for-valid-time for-system-time]} (parse-table+opts table+opts step)]
    (-> (QueryStep/from table)
        (cond-> for-valid-time (.forValidTime for-valid-time)
                for-system-time (.forSystemTime for-system-time))
        (.binding (parse-binding-specs binding-specs step)))))

(extend-protocol Unparse
  QueryStep$From
  (unparse [from]
    (let [table (keyword (.table from))
          for-valid-time (.forValidTime from)
          for-sys-time (.forSystemTime from)]
      (list* 'from (if (or for-valid-time for-sys-time)
                     [table (cond-> {}
                              for-valid-time (assoc :for-valid-time (unparse for-valid-time))
                              for-sys-time (assoc :for-system-time (unparse for-sys-time)))]
                     table)
             (for [^QueryStep$BindingSpec binding-spec (.bindSpecs from)
                   :let [attr (.attr binding-spec)
                         expr (.expr binding-spec)]]
               (if (and (instance? Expr$LogicVar expr)
                        (= (.lv ^Expr$LogicVar expr) attr))
                 (symbol attr)
                 {(keyword attr) (unparse expr)}))))))
