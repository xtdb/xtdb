(ns xtdb.xtql.json
  (:require [xtdb.xtql.edn :as xtql.edn]
            [xtdb.error :as err])
  (:import [java.time Duration LocalDate LocalDateTime ZonedDateTime]
           (java.util Date List)
           (xtdb.query Expr Expr$Bool Expr$Call Expr$Double Expr$Exists Expr$LogicVar Expr$Long Expr$Obj Expr$Subquery
                       Query Query$Aggregate Query$From Query$LeftJoin Query$Limit Query$Join Query$Limit
                       Query$Offset Query$Pipeline Query$OrderBy Query$OrderDirection Query$OrderSpec Query$OrderNulls
                       Query$Return Query$Unify Query$UnionAll Query$Where Query$With Query$Without
                       OutSpec ArgSpec ColSpec VarSpec Query$WithCols Query$DocsTable Query$ParamTable
                       Query$UnnestVar Query$UnnestCol
                       TemporalFilter TemporalFilter$AllTime TemporalFilter$At TemporalFilter$In)))

(defn- query-type [query]
  (cond
    (vector? query) '->

    (map? query) (let [query (dissoc query "args" "bind" "forValidTime" "forSystemTime")]
                   (if-not (= 1 (count query))
                     (throw (err/illegal-arg :xtql/malformed-query {:query query}))
                     (symbol (key (first query)))))

    :else (throw (err/illegal-arg :xtql/malformed-query {:query query}))))

(defmulti parse-query query-type)

(defmethod parse-query :default [q]
  (throw (err/illegal-arg :xtql/unknown-query-op {:op (key (first q))})))

(defmulti parse-query-tail query-type)

(defmethod parse-query-tail :default [q]
  (throw (err/illegal-arg :xtql/unknown-query-tail {:op (key (first q))})))

(defmulti parse-unify-clause query-type)

(defmethod parse-unify-clause :default [q]
  (throw (err/illegal-arg :xtql/unknown-unify-clause {:op (key (first q))})))

(defprotocol Unparse
  (unparse [this]))

(declare parse-expr parse-arg-specs)

(defn- bad-literal [l]
  (throw (err/illegal-arg :xtql/malformed-literal {:literal l})))

(defn- try-parse [v f l]
  (try
    (f v)
    (catch Exception e
      (throw (err/illegal-arg :xtql/malformed-literal {:literal l, :error (.getMessage e)})))))

(defn json-value->object [{v "@value", t "@type" :as l}]
  (cond (or (nil? v) (nil? t)) l
        (= "xt:set" t) (if-not (vector? v)
                         (bad-literal l)
                         (into #{} (map json-value->object) v))

        (not (string? v)) (bad-literal l)

        :else (case t
                "xt:keyword" (keyword v)
                "xt:date" (try-parse v #(LocalDate/parse %) l)
                "xt:duration" (try-parse v #(Duration/parse %) l)
                "xt:timestamp" (try-parse v #(LocalDateTime/parse %) l)
                "xt:timestamptz" (try-parse v #(ZonedDateTime/parse %) l)
                (throw (err/illegal-arg :xtql/unknown-type {:value v, :type t})))))

(defn object->json-value [obj]
  (cond
    (keyword? obj) {"@value" (str (symbol obj)), "@type" "xt:keyword"}
    (set? obj) {"@value" (mapv object->json-value obj), "@type" "xt:set"}
    (instance? Date obj) {"@value" (str (.toInstant ^Date obj),) "@type" "xt:timestamp"}
    (instance? LocalDate obj) {"@value" (str obj), "@type" "xt:date"}
    (instance? Duration obj) {"@value" (str obj), "@type" "xt:duration"}
    (instance? LocalDateTime obj) {"@value" (str obj), "@type" "xt:timestamp"}
    (instance? ZonedDateTime obj) {"@value" (str obj), "@type" "xt:timestamptz"}
    :else obj))

(defn- parse-literal [{v "@value", t "@type" :as l}]
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
                "xt:date" (Expr/val (try-parse v #(LocalDate/parse %) l))
                "xt:duration" (Expr/val (try-parse v #(Duration/parse %) l))
                "xt:timestamp" (Expr/val (try-parse v #(LocalDateTime/parse %) l))
                "xt:timestamptz" (Expr/val (try-parse v #(ZonedDateTime/parse %) l))
                (throw (err/illegal-arg :xtql/unknown-type {:value v, :type t})))))))

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

      (map? expr) (if (contains? expr "@value")
                    (parse-literal expr)

                    (let [{:strs [exists q]} expr]
                      (letfn [(parse-args [{:strs [args]}]
                                (some-> args (parse-arg-specs expr)))]
                        (cond
                          exists (Expr/exists (parse-query exists) (parse-args expr))
                          q (Expr/q (parse-query q) (parse-args expr))

                          (not= 1 (count expr)) (bad-expr expr)

                          :else (let [[f args] (first expr)]
                                  (if-not (vector? args)
                                    (bad-expr expr)
                                    (Expr/call f (mapv parse-expr args))))))))

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
        (map? obj) {"@value" (update-vals obj unparse)}
        (keyword? obj) {"@value" (str (symbol obj)), "@type" "xt:keyword"}
        (set? obj) {"@value" (mapv unparse obj), "@type" "xt:set"}
        (instance? Date obj) {"@value" (str (.toInstant ^Date obj),) "@type" "xt:timestamp"}
        (instance? LocalDate obj) {"@value" (str obj), "@type" "xt:date"}
        (instance? Duration obj) {"@value" (str obj), "@type" "xt:duration"}
        (instance? LocalDateTime obj) {"@value" (str obj), "@type" "xt:timestamp"}
        (instance? ZonedDateTime obj) {"@value" (str obj), "@type" "xt:timestamptz"}
        :else (throw (UnsupportedOperationException. (format "obj: %s" (pr-str obj)))))))

  Expr$Exists
  (unparse [e]
    (let [q (unparse (.query e))
          args (.args e)]
      (cond-> {"exists" q}
        args (assoc "args" (mapv unparse args)))))

  Expr$Subquery
  (unparse [e]
    (let [q (unparse (.query e))
          args (.args e)]
      (cond-> {"q" q}
        args (assoc "args" (mapv unparse args))))))

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

(defn- parse-from [this]
  (if-not (map? this)
    (throw (err/illegal-arg :xtql/malformed-from {:from this}))

    (let [{:strs [from forValidTime forSystemTime bind]} this]
      (cond
        (not (string? from))
        (throw (err/illegal-arg :xtql/malformed-table {:table from, :from this}))

        (nil? bind)
        (throw (err/illegal-arg :xtql/missing-bind {:from this}))

        :else
        (cond-> (Query/from from)
          forValidTime (.forValidTime (parse-temporal-filter forValidTime :forValidTime this))
          forSystemTime (.forSystemTime (parse-temporal-filter forSystemTime :forSystemTime this))
          bind (.binding (parse-out-specs bind this)))))))

(defmethod parse-query 'from [from] (parse-from from))
(defmethod parse-unify-clause 'from [from] (parse-from from))

(defmethod parse-unify-clause 'join [{:strs [join args bind] :as query}]
  (if-not (map? join)
    (throw (err/illegal-arg :xtql/malformed-join {:join query}))

    (cond-> (Query/join (parse-query join) (some-> args (parse-arg-specs query)))
      bind (.binding (parse-out-specs bind join)))))

(defmethod parse-unify-clause 'leftJoin [{left-join "leftJoin", :strs [args bind], :as query}]
  (if-not (map? left-join)
    (throw (err/illegal-arg :xtql/malformed-join {:join query}))

    (cond-> (Query/leftJoin (parse-query left-join) (some-> args (parse-arg-specs query)))
      bind (.binding (parse-out-specs bind left-join)))))

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
          for-sys-time (.forSystemTime from)
          bindings (.bindings from)]
      (cond-> {"from" table}
        for-valid-time (assoc "forValidTime" (unparse for-valid-time))
        for-sys-time (assoc "forSystemTime" (unparse for-sys-time))
        bindings (assoc "bind" (mapv unparse bindings)))))

  Query$Join
  (unparse [join]
    (let [args (.args join)
          bindings (.bindings join)]
      (cond-> {"join" (unparse (.query join))}
        args (assoc "args" (mapv unparse args))
        bindings (assoc "bind" (mapv unparse bindings)))))

  Query$LeftJoin
  (unparse [left-join]
    (let [args (.args left-join)
          bindings (.bindings left-join)]
      (cond-> {"leftJoin" (unparse (.query left-join))}
        args (assoc "args" (mapv unparse args))
        bindings (assoc "bind" (mapv unparse bindings))))))

(defmethod parse-query '-> [query]
  (if (empty? query)
    (throw (err/illegal-arg :xtql/malformed-pipeline {:pipeline query}))

    (let [[head & tails] query]
      (Query/pipeline (parse-query head) (mapv parse-query-tail tails)))))

(defmethod parse-query 'unify [{:strs [unify] :as query}]
  (if-not (vector? unify)
    (throw (err/illegal-arg :xtql/malformed-unify {:unify query}))

    (Query/unify (mapv parse-unify-clause unify))))

(defn- parse-where [{:strs [where]}]
  (if-not (vector? where)
    (throw (err/illegal-arg :xtql/malformed-where {:where where}))

    (Query/where (mapv parse-expr where))))

(defmethod parse-query-tail 'where [where] (parse-where where))
(defmethod parse-unify-clause 'where [where] (parse-where where))

(defmethod parse-query-tail 'with [{:strs [with] :as query}]
  (if-not (vector? with)
    (throw (err/illegal-arg :xtql/malformed-with {:with with}))

    (Query/withCols (parse-col-specs with query))))

(defmethod parse-unify-clause 'with [{:strs [with] :as query}]
  (if-not (vector? with)
    (throw (err/illegal-arg :xtql/malformed-with {:with with}))

    (Query/with (parse-var-specs with query))))

(defn check-unnest [unnest]
  (when-not (and (vector? unnest)
                 (= 1 (count unnest))
                 (map? (first unnest))
                 (= 1 (count (first unnest))))
    (throw (err/illegal-arg :xtql/unnest {:unnest unnest ::err/message "Unnest takes only a single binding"}))))

(defmethod parse-query-tail 'unnest [{:strs [unnest] :as this}]
  (check-unnest unnest)
  (Query/unnestCol (first (parse-col-specs unnest this))))

(defmethod parse-unify-clause 'unnest [{:strs [unnest] :as this}]
  (check-unnest unnest)
  (Query/unnestVar (first (parse-var-specs unnest this))))

(defmethod parse-query-tail 'without [{:strs [without] :as query}]
  (if-not (and (vector? without) (every? string? without))
    (throw (err/illegal-arg :xtql/malformed-without {:without query}))

    (Query/without without)))

(defmethod parse-query-tail 'return [{:strs [return] :as query}]
  (if-not (vector? return)
    (throw (err/illegal-arg :xtql/malformed-return {:return query}))

    (Query/ret (parse-col-specs return query))))

(defmethod parse-query-tail 'aggregate [{:strs [aggregate] :as query}]
  (if-not (vector? aggregate)
    (throw (err/illegal-arg :xtql/malformed-aggregate {:aggregate query}))

    (Query/aggregate (parse-col-specs aggregate query))))

(defmethod parse-query-tail 'limit [{:strs [limit] :as query}]
  (when-not (int? limit)
    (throw (err/illegal-arg :xtql/limit {:limit query :message "Limit must be an integer!"})))
  (Query/limit limit))

(defmethod parse-query 'unionAll [{union-all "unionAll", :as query}]
  (if-not (vector? union-all)
    (throw (err/illegal-arg :xtql/malformed-union-all {:union-all query}))

    (Query/unionAll (mapv parse-query union-all))))

(defn parse-table [this]
  (if-not (map? this)
    (throw (err/illegal-arg :xtql/malformed-table {:table this}))

    (let [{:strs [table bind]} this
          ^List parsed-bind (parse-out-specs bind this)]
      (when-not (or (string? table) (vector? table))
        (throw (err/illegal-arg :xtql/table {:table this})))
      (if (string? table)
        (Query/table (Expr/param table) parsed-bind)
        (Query/table ^List (mapv #(update-vals % parse-expr) table) parsed-bind)))))

(defmethod parse-query 'table [this] (parse-table this))
(defmethod parse-unify-clause 'table [this] (parse-table this))

(extend-protocol Unparse
  Query$Pipeline (unparse [q] (into [(unparse (.query q))] (mapv unparse (.tails q))))
  Query$Where (unparse [q] {"where" (mapv unparse (.preds q))})
  Query$With (unparse [q] {"with" (mapv unparse (.vars q))})
  Query$WithCols (unparse [q] {"with" (mapv unparse (.cols q))})
  Query$Without (unparse [q] {"without" (.cols q)})
  Query$Return (unparse [q] {"return" (mapv unparse (.cols q))})
  Query$Limit (unparse [this] {"limit" (.length this)})
  Query$Aggregate (unparse [q] {"aggregate" (mapv unparse (.cols q))})
  Query$Unify (unparse [q] {"unify" (mapv unparse (.clauses q))})
  Query$UnionAll (unparse [q] {"unionAll" (mapv unparse (.queries q))})
  Query$DocsTable (unparse [q] {"table" (mapv #(update-vals % unparse) (.documents q))
                                "bind" (mapv unparse (.bindings q))})
  Query$ParamTable (unparse [q] {"table" (.v (.param q))
                                 "bind" (mapv unparse (.bindings q))})
  Query$UnnestVar (unparse [this] {"unnest" [(unparse (.var this))]})
  Query$UnnestCol (unparse [this] {"unnest" [(unparse (.col this))]}))

(def order-spec-opt-keys (set (map name xtql.edn/order-spec-opt-keys)))

(defn- parse-order-spec [order-spec this]
  (if (map? order-spec)
    (let [{:strs [val dir nulls]} order-spec
          _ (xtql.edn/check-opt-keys order-spec-opt-keys order-spec)
          __ (when-not (contains? order-spec "val")
               (throw (err/illegal-arg :xtql/order-by-val-missing
                                       {:order-spec order-spec, :query this})))
          dir (case dir
                nil nil
                "asc" Query$OrderDirection/ASC
                "desc" Query$OrderDirection/DESC

                (throw (err/illegal-arg :xtql/malformed-order-by-direction
                                        {:direction dir, :order-spec order-spec, :query this})))
          nulls (case nulls
                  nil nil
                  "first" Query$OrderNulls/FIRST
                  "last" Query$OrderNulls/LAST

                  (throw (err/illegal-arg :xtql/malformed-order-by-nulls
                                          {:nulls nulls, :order-spec order-spec, :query this})))]
      (Query/orderSpec (parse-expr val) dir nulls))
    ;:TODO short form can only reasonably be a var, as exprs use maps, would be ambiguous with long form
    (Query/orderSpec (parse-expr order-spec) nil nil)))

(defmethod parse-query-tail 'orderBy [{order-by "orderBy", :as query}]
  (if-not (vector? order-by)
    (throw (err/illegal-arg :xtql/malformed-order-by {:order-by query}))

    (Query/orderBy (mapv #(parse-order-spec % query) order-by))))

(extend-protocol Unparse
  Query$OrderSpec
  (unparse [spec]
    (let [expr (unparse (.expr spec))
          dir (.direction spec)
          nulls (.nulls spec)]
      (if (and (not dir) (not nulls))
        expr
        (cond-> {"val" expr}
          dir (assoc "dir" (if (= Query$OrderDirection/ASC dir) "asc" "desc"))
          nulls (assoc "nulls" (if (= Query$OrderNulls/FIRST nulls) "first" "last"))))))

  Query$OrderBy
  (unparse [q]
    {"orderBy" (mapv unparse (.orderSpecs q))})

  Query$Limit (unparse [q] {"limit" (.length q)})
  Query$Offset (unparse [q] {"offset" (.length q)}))
