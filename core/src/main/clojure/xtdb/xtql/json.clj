(ns xtdb.xtql.json
  (:require [xtdb.error :as err])
  (:import [java.time Duration LocalDate LocalDateTime ZonedDateTime]
           (xtdb.query Expr Expr$Bool Expr$Call Expr$Double Expr$Exists Expr$LogicVar Expr$Long Expr$NotExists Expr$Obj Expr$Subquery
                       Query Query$Aggregate Query$From Query$LeftJoin Query$Join Query$Pipeline Query$OrderBy Query$OrderDirection Query$OrderSpec Query$Return Query$Unify Query$UnionAll Query$Where Query$With Query$Without
                       OutSpec ArgSpec ColSpec VarSpec Query$WithCols
                       TemporalFilter TemporalFilter$AllTime TemporalFilter$At TemporalFilter$In)))

(defmulti parse-query
  (fn [query]
    (when-not (and (map? query) (= 1 (count query)))
      (throw (err/illegal-arg :xtql/malformed-query {:query query})))

    (symbol (key (first query)))))

(defmethod parse-query :default [q]
  (throw (err/illegal-arg :xtql/unknown-query-op {:op (key (first q))})))

(defmulti parse-query-tail
  (fn [query]
    (when-not (and (map? query) (= 1 (count query)))
      (throw (err/illegal-arg :xtql/malformed-query {:query query})))

    (symbol (key (first query)))))

(defmethod parse-query-tail :default [q]
  (throw (err/illegal-arg :xtql/unknown-query-tail {:op (key (first q))})))

(defmulti parse-unify-clause
  (fn [query]
    (when-not (and (map? query) (= 1 (count query)))
      (throw (err/illegal-arg :xtql/malformed-query {:query query})))

    (symbol (key (first query)))))

(defmethod parse-unify-clause :default [q]
  (throw (err/illegal-arg :xtql/unknown-unify-clause {:op (key (first q))})))

(defprotocol Unparse
  (unparse [this]))

(declare parse-expr parse-arg-specs)

(defn- parse-literal [{v "@value", t "@type" :as l}]
  (letfn [(bad-literal [l]
            (throw (err/illegal-arg :xtql/malformed-literal {:literal l})))

          (try-parse [v f]
            (try
              (f v)
              (catch Exception e
                (throw (err/illegal-arg :xtql/malformed-literal {:literal l, :error (.getMessage e)})))))]
    (cond
      (nil? v) (Expr/val nil)

      (nil? t) (cond
                 (map? v) (Expr/val (into {} (map (juxt key (comp parse-expr val))) v))
                 (vector? v) (Expr/val (mapv parse-expr v))
                 (string? v) (Expr/val v)
                 :else (bad-literal l))

      :else (if (= "xt:set" t)
              (if-not (vector? v)
                (bad-literal l)

                (Expr/val (into #{} (map parse-expr) v)))

              (if-not (string? v)
                (bad-literal l)

                (case t
                  "xt:keyword" (Expr/val (keyword v))
                  "xt:date" (Expr/val (try-parse v #(LocalDate/parse %)))
                  "xt:duration" (Expr/val (try-parse v #(Duration/parse %)))
                  "xt:timestamp" (Expr/val (try-parse v #(LocalDateTime/parse %)))
                  "xt:timestamptz" (Expr/val (try-parse v #(ZonedDateTime/parse %)))
                  (throw (err/illegal-arg :xtql/unknown-type {:value v, :type t}))))))))

(defn parse-subquery-args [args expr]
  (cond
    (map? args) [(parse-query args) nil]

    (and (vector? args) (not (empty? args)))
    (let [[query & bind-specs] args]
      [(parse-query query) (parse-arg-specs bind-specs expr)])

    :else (throw (err/illegal-arg :xtql/malformed-subquery {:expr expr}))))

(defn parse-expr [expr]
  (letfn [(bad-expr [expr]
            (throw (err/illegal-arg :xtql/malformed-expr {:expr expr})))]
    (cond
      (nil? expr) (Expr/val nil)
      (int? expr) (Expr/val (long expr))
      (double? expr) (Expr/val (double expr))
      (boolean? expr) (if expr Expr/TRUE Expr/FALSE)
      (string? expr) (Expr/lVar expr)
      (vector? expr) (parse-literal {"@value" expr})

      (map? expr) (cond
                    (contains? expr "@value") (parse-literal expr)

                    (= 1 (count expr))
                    (let [[f args] (first expr)]
                      (case f
                        ("exists" "notExists" "q")
                        (let [[query args] (parse-subquery-args args expr)]
                          (case f
                            "exists" (Expr/exists query args)
                            "notExists" (Expr/notExists query args)
                            "q" (Expr/q query args)))

                        (do
                          (when-not (and (string? f) (vector? args))
                            (bad-expr expr))

                          (Expr/call f (mapv parse-expr args)))))

                    :else (bad-expr expr))

      :else (bad-expr expr))))

(extend-protocol Unparse
  Expr$LogicVar (unparse [lv] (.lv lv))
  Expr$Bool (unparse [b] (.bool b))
  Expr$Long (unparse [l] (.lng l))
  Expr$Double (unparse [d] (.dbl d))

  Expr$Call (unparse [c] {(.f c) (mapv unparse (.args c))})

  Expr$Obj
  (unparse [obj]
    (let [obj (.obj obj)]
      (cond
        (nil? obj) nil
        (vector? obj) (mapv unparse obj)
        (string? obj) {"@value" obj}
        (keyword? obj) {"@value" (str (symbol obj)), "@type" "xt:keyword"}
        (set? obj) {"@value" (mapv unparse obj), "@type" "xt:set"}
        (instance? LocalDate obj) {"@value" (str obj), "@type" "xt:date"}
        (instance? Duration obj) {"@value" (str obj), "@type" "xt:duration"}
        (instance? LocalDateTime obj) {"@value" (str obj), "@type" "xt:timestamp"}
        (instance? ZonedDateTime obj) {"@value" (str obj), "@type" "xt:timestamptz"}
        :else (throw (UnsupportedOperationException. (format "obj: %s" (pr-str obj)))))))

  Expr$Exists
  (unparse [e]
    (let [q (unparse (.query e))]
      {"exists" (if-let [args (.args e)]
                  (into [q] (mapv unparse args))
                  q)}))

  Expr$NotExists
  (unparse [e]
    (let [q (unparse (.query e))]
      {"notExists" (if-let [args (.args e)]
                     (into [q] (mapv unparse args))
                     q)}))

  Expr$Subquery
  (unparse [e]
    (let [q (unparse (.query e))]
      {"q" (if-let [args (.args e)]
             (into [q] (mapv unparse args))
             q)})))

(defn- parse-temporal-filter [v k query]
  (let [ctx {:v v, :filter k, :query query}]
    (if (= "allTime" v)
      TemporalFilter/ALL_TIME

      (do
        (when-not (and (map? v) (= 1 (count v)))
          (throw (err/illegal-arg :xtql/malformed-temporal-filter ctx)))

        (let [[tag arg] (first v)]
          (case tag
            "at" (TemporalFilter/at (parse-expr arg))

            "in" (if-not (and (vector? arg) (= 2 (count arg)))
                   (throw (err/illegal-arg :xtql/malformed-temporal-filter (into ctx {:tag tag, :in arg})))

                   (let [[from to] arg]
                     (TemporalFilter/in (parse-expr from) (parse-expr to))))

            "from" (TemporalFilter/from (parse-expr arg))

            "to" (TemporalFilter/to (parse-expr arg))

            (throw (err/illegal-arg :xtql/malformed-temporal-filter (into ctx {:tag tag})))))))))

(extend-protocol Unparse
  TemporalFilter$AllTime (unparse [_] "allTime")
  TemporalFilter$At (unparse [at] {"at" (unparse (.at at))})
  TemporalFilter$In (unparse [in] {"in" [(unparse (.from in)) (unparse (.to in))]}))

(defn- with-parsed-temporal-filters ^xtdb.query.Query$From [^Query$From from, temporal-filters, query]
  (when-not (map? temporal-filters)
    (throw (err/illegal-arg :xtql/malformed-table-opts {:filters temporal-filters, :from query})))

  (let [{for-valid-time "forValidTime", for-system-time "forSystemTime"} temporal-filters]
    (cond-> from
      for-valid-time (.forValidTime (parse-temporal-filter for-valid-time :forValidTime query))
      for-system-time (.forSystemTime (parse-temporal-filter for-system-time :forSystemTime query)))))

(defn- parse-from-source ^xtdb.query.Query$From [source query]
  (cond
    (string? source) (Query/from source)

    (and (map? source) (= 1 (count source)))
    (let [[table temporal-filters] (first source)]
      (when-not (string? table)
        (throw (err/illegal-arg :xtql/malformed-table {:table table, :from query})))

      (-> (Query/from table)
          (with-parsed-temporal-filters temporal-filters query)))

    :else (throw (err/illegal-arg :xtql/malformed-from {:from query}))))

(defn- parse-binding-specs [spec-of binding-specs _query]
  (->> binding-specs
       (into [] (mapcat (fn [binding-spec]
                          (cond
                            (string? binding-spec) [(spec-of binding-spec (Expr/lVar binding-spec))]
                            (map? binding-spec) (for [[attr expr] binding-spec]
                                                  (do
                                                    (when-not (string? attr)
                                                      ;; TODO error
                                                      )
                                                    (spec-of attr (parse-expr expr))))))))))

(def parse-out-specs (partial parse-binding-specs #(OutSpec/of %1 %2)))
(def parse-arg-specs (partial parse-binding-specs #(ArgSpec/of %1 %2)))
(def parse-col-specs (partial parse-binding-specs #(ColSpec/of %1 %2)))

(defn- parse-var-specs
  [specs _query]
  (->> specs
       (into [] (mapcat (fn [spec]
                          (if (map? spec)
                            (for [[attr expr] spec]
                              (do
                                (when-not (string? attr)
                                  (throw (err/illegal-arg :xtql/malformed-var-spec)))
                                (VarSpec/of (str attr) (parse-expr expr))))
                            (throw (err/illegal-arg :xtql/malformed-var-spec))))))))

(defn- parse-from [from]
  (let [v (val (first from))]
    (when-not (and (vector? v) (not-empty v))
      (throw (err/illegal-arg :xtql/malformed-from {:from from})))

    (let [[source & binding-specs] v]
      (-> (parse-from-source source from)
          (.binding (parse-out-specs binding-specs from))))))

(defmethod parse-query 'from [from] (parse-from from))
(defmethod parse-unify-clause 'from [from] (parse-from from))

(defn- parse-join-query [query join]
  (cond
    (map? query) [(parse-query query)]

    (vector? query) (let [[query & args] query]
                      [(parse-query query) (parse-arg-specs args join)])

    :else (throw (err/illegal-arg :xtql/malformed-join {:join join}))))

(defmethod parse-unify-clause 'join [join]
  (let [v (val (first join))]
    (when-not (and (vector? v) (not-empty v))
      (throw (err/illegal-arg :xtql/malformed-join {:join join})))

    (let [[query & binding-specs] v
          [parsed-query args] (parse-join-query query join)]
      (-> (Query/join parsed-query args)
          (.binding (parse-out-specs binding-specs join))))))

(defmethod parse-unify-clause 'leftJoin [left-join]
  (let [v (val (first left-join))]
    (when-not (and (vector? v) (not-empty v))
      (throw (err/illegal-arg :xtql/malformed-join {:join left-join})))

    (let [[query & binding-specs] v
          [parsed-query args] (parse-join-query query left-join)]
      (-> (Query/leftJoin parsed-query args)
          (.binding (parse-out-specs binding-specs left-join))))))

(defn unparse-binding-spec [attr expr]
  (if (and (instance? Expr$LogicVar expr)
           (= (.lv ^Expr$LogicVar expr) attr))
    attr
    {attr (unparse expr)}))

(extend-protocol Unparse
  OutSpec (unparse [spec] (unparse-binding-spec (.attr spec) (.expr spec)))
  ArgSpec (unparse [spec] (unparse-binding-spec (.attr spec) (.expr spec)))
  VarSpec (unparse [spec] (unparse-binding-spec (.attr spec) (.expr spec)))
  ColSpec (unparse [spec] (unparse-binding-spec (.attr spec) (.expr spec)))

  Query$From
  (unparse [from]
    (let [table (.table from)
          for-valid-time (.forValidTime from)
          for-sys-time (.forSystemTime from)]
      {"from" (into [(if (or for-valid-time for-sys-time)
                       {table (cond-> {}
                                for-valid-time (assoc "forValidTime" (unparse for-valid-time))
                                for-sys-time (assoc "forSystemTime" (unparse for-sys-time)))}
                       table)]

                    (map unparse (.bindings from)))}))

  Query$Join
  (unparse [join]
    (let [query (unparse (.query join))
          args (.args join)]
      {"join" (into [(if args
                       (into [query] (map unparse) args)
                       query)]

                    (map unparse (.bindings join)))}))

  Query$LeftJoin
  (unparse [join]
    (let [query (unparse (.query join))
          args (.args join)]
      {"leftJoin" (into [(if args
                           (into [query] (map unparse) args)
                           query)]

                        (map unparse (.bindings join)))})))

(defmethod parse-query '-> [query]
  (let [v (val (first query))]
    (when-not (and (vector? v) (not-empty v))
      (throw (err/illegal-arg :xtql/malformed-pipeline {:pipeline query})))

    (let [[head & tails] v]
      (Query/pipeline (parse-query head) (mapv parse-query-tail tails)))))

(defmethod parse-query 'unify [query]
  (let [v (val (first query))]
    (when-not (vector? v)
      (throw (err/illegal-arg :xtql/malformed-unify {:unify query})))

    (Query/unify (mapv parse-unify-clause v))))

(defn- parse-where [where]
  (let [v (val (first where))]
    (when-not (vector? v)
      (throw (err/illegal-arg :xtql/malformed-where {:where where})))

    (Query/where (mapv parse-expr v))))

(defmethod parse-query-tail 'where [where] (parse-where where))
(defmethod parse-unify-clause 'where [where] (parse-where where))



(defmethod parse-query-tail 'with [with]
  (let [v (val (first with))]
    (when-not (vector? v)
      (throw (err/illegal-arg :xtql/malformed-with {:with with})))

    (Query/withCols (parse-col-specs v with))))

(defmethod parse-unify-clause 'with [with]
  (let [v (val (first with))]
    (when-not (vector? v)
      (throw (err/illegal-arg :xtql/malformed-with {:with with})))

    (Query/with (parse-var-specs v with))))

(defmethod parse-query-tail 'without [query]
  (let [v (val (first query))]
    (when-not (and (vector? v) (every? string? v))
      (throw (err/illegal-arg :xtql/malformed-without {:without query})))

    (Query/without v)))

(defmethod parse-query-tail 'return [query]
  (let [v (val (first query))]
    (when-not (vector? v)
      (throw (err/illegal-arg :xtql/malformed-return {:return query})))

    (Query/ret (parse-col-specs v query))))

(defmethod parse-query-tail 'aggregate [query]
  (let [v (val (first query))]
    (when-not (vector? v)
      (throw (err/illegal-arg :xtql/malformed-aggregate {:aggregate query})))

    (Query/aggregate (parse-col-specs v query))))

(defmethod parse-query 'unionAll [query]
  (let [v (val (first query))]
    (when-not (vector? v)
      (throw (err/illegal-arg :xtql/malformed-union-all {:union-all query})))

    (Query/unionAll (mapv parse-query v))))

(extend-protocol Unparse
  Query$Pipeline (unparse [q] {"->" (into [(unparse (.query q))] (mapv unparse (.tails q)))})
  Query$Where (unparse [q] {"where" (mapv unparse (.preds q))})
  Query$With (unparse [q] {"with" (mapv unparse (.vars q))})
  Query$WithCols (unparse [q] {"with" (mapv unparse (.cols q))})
  Query$Without (unparse [q] {"without" (.cols q)})
  Query$Return (unparse [q] {"return" (mapv unparse (.cols q))})
  Query$Aggregate (unparse [q] {"aggregate" (mapv unparse (.cols q))})
  Query$Unify (unparse [q] {"unify" (mapv unparse (.clauses q))})
  Query$UnionAll (unparse [q] {"unionAll" (mapv unparse (.queries q))}))

(defn- parse-order-spec [order-spec query]
  (if (vector? order-spec)
    (do
      (when-not (= 2 (count order-spec))
        (throw (err/illegal-arg :xtql/malformed-order-spec {:order-spec order-spec, :query query})))

      (let [[expr opts] order-spec
            parsed-expr (parse-expr expr)]
        (when-not (map? opts)
          (throw (err/illegal-arg :xtql/malformed-order-spec {:order-spec order-spec, :query query})))

        (let [{dir "dir"} opts]
          (case dir
            "asc" (Query/asc parsed-expr)
            "desc" (Query/desc parsed-expr)

            (throw (err/illegal-arg :xtql/malformed-order-by-direction
                                    {:direction dir, :order-spec order-spec, :query query}))))))

    (Query/asc (parse-expr order-spec))))

(defmethod parse-query-tail 'orderBy [query]
  (let [v (val (first query))]
    (when-not (vector? v)
      (throw (err/illegal-arg :xtql/malformed-order-by {:order-by query})))

    (Query/orderBy (mapv #(parse-order-spec % query) v))))

(extend-protocol Unparse
  Query$OrderSpec
  (unparse [spec]
    (let [expr (unparse (.expr spec))
          dir (.direction spec)]
      (if (= Query$OrderDirection/DESC dir)
        [expr {"dir" "desc"}]
        expr)))

  Query$OrderBy
  (unparse [q]
    {"orderBy" (mapv unparse (.orderSpecs q))}))
