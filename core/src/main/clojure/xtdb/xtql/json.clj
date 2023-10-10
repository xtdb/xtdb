(ns xtdb.xtql.json
  (:require [xtdb.error :as err])
  (:import [java.time Duration LocalDate LocalDateTime ZonedDateTime]
           (xtdb.query Expr Expr$Bool Expr$Call Expr$Double Expr$LogicVar Expr$Long Expr$Obj
                       QueryStep QueryStep$BindingSpec QueryStep$From QueryStep$Pipeline QueryStep$Unify QueryStep$Where TemporalFilter TemporalFilter$AllTime TemporalFilter$At TemporalFilter$In)))

(defmulti parse-query
  (fn [query]
    (when-not (and (map? query) (= 1 (count query)))
      (throw (err/illegal-arg :xtql/malformed-query {:query query})))

    (symbol (key (first query)))))

(defprotocol Unparse
  (unparse [this]))

(declare parse-expr)

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

                    (= 1 (count expr)) (let [[f args] (first expr)]
                                         (when-not (vector? args)
                                           (bad-expr expr))
                                         (Expr/call f (mapv parse-expr args)))

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
        :else (throw (UnsupportedOperationException. (format "obj: %s" (pr-str obj))))))))

(defn- parse-temporal-filter [v k step]
  (let [ctx {:v v, :filter k, :step step}]
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

(defn- with-parsed-temporal-filters ^xtdb.query.QueryStep$From [^QueryStep$From from, temporal-filters, step]
  (when-not (map? temporal-filters)
    (throw (err/illegal-arg :xtql/malformed-table-opts {:filters temporal-filters, :from step})))

  (let [{for-valid-time "forValidTime", for-system-time "forSystemTime"} temporal-filters]
    (cond-> from
      for-valid-time (.forValidTime (parse-temporal-filter for-valid-time :forValidTime step))
      for-system-time (.forSystemTime (parse-temporal-filter for-system-time :forSystemTime step)))))

(defn- parse-source ^xtdb.query.QueryStep$From [source step]
  (cond
    (string? source) (QueryStep/from source)

    (and (map? source) (= 1 (count source)))
    (let [[k v] (first source)]
      (case k
        "table" (cond
                  (string? v) (QueryStep/from v)

                  (and (vector? v) (= 2 (count v)))
                  (let [[table temporal-filters] v]
                    (when-not (string? table)
                      (throw (err/illegal-arg :xtql/malformed-table {:table table, :from step})))

                    (-> (QueryStep/from table)
                        (with-parsed-temporal-filters temporal-filters step)))

                  :else (throw (err/illegal-arg :xtql/malformed-from {:from step})))

        "rule" (throw (UnsupportedOperationException.))
        "query" (throw (UnsupportedOperationException.))

        (throw (err/illegal-arg :xtql/malformed-from {:from step}))))

    :else (throw (err/illegal-arg :xtql/malformed-from {:from step}))))

(defn- parse-binding-specs [binding-specs _step]
  (->> binding-specs
       (into [] (mapcat (fn [binding-spec]
                          (cond
                            (string? binding-spec) [(QueryStep/bindSpec binding-spec (Expr/lVar binding-spec))]
                            (map? binding-spec) (for [[attr expr] binding-spec]
                                                  (do
                                                    (when-not (string? attr)
                                                      ;; TODO error
                                                      )
                                                    (QueryStep/bindSpec attr (parse-expr expr))))))))))
(defmethod parse-query 'from [step]
  (let [v (val (first step))]
    (when-not (and (vector? v) (not-empty v))
      (throw (err/illegal-arg :xtql/malformed-from {:from step})))

    (let [[source & binding-specs] v]
      (-> (parse-source source step)
          (.binding (parse-binding-specs binding-specs step))))))

(extend-protocol Unparse
  QueryStep$From
  (unparse [from]
    (let [table (.table from)
          for-valid-time (.forValidTime from)
          for-sys-time (.forSystemTime from)]
      {"from" (into [(if (or for-valid-time for-sys-time)
                       {"table" [table (cond-> {}
                                         for-valid-time (assoc "forValidTime" (unparse for-valid-time))
                                         for-sys-time (assoc "forSystemTime" (unparse for-sys-time)))]}
                       table)]

                    (for [^QueryStep$BindingSpec binding-spec (.bindSpecs from)
                          :let [attr (.attr binding-spec)
                                expr (.expr binding-spec)]]
                      (if (and (instance? Expr$LogicVar expr)
                               (= (.lv ^Expr$LogicVar expr) attr))
                        attr
                        {attr (unparse expr)})))})))

(defmethod parse-query :default [q]
  (throw (err/illegal-arg :xtql/unknown-query-op {:op (key (first q))})))

