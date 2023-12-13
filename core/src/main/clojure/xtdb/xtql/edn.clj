(ns xtdb.xtql.edn
  (:require [clojure.set :as set]
            [clojure.string :as str]
            [xtdb.error :as err])
  (:import (clojure.lang MapEntry)
           (java.util List)
           (xtdb.query ArgSpec ColSpec DmlOps DmlOps$AssertExists DmlOps$AssertNotExists DmlOps$Delete DmlOps$Erase DmlOps$Insert DmlOps$Update
                       Expr Expr$Bool Expr$Call Expr$Double Expr$Exists Expr$Param Expr$Get
                       Expr$LogicVar Expr$Long Expr$Obj Expr$Subquery Expr$Pull Expr$PullMany
                       OutSpec Query Query$Aggregate Query$From Query$LeftJoin Query$Join Query$Limit
                       Query$OrderBy Query$OrderDirection Query$OrderSpec Query$Pipeline Query$Offset
                       Query$Return Query$Unify Query$UnionAll Query$Where Query$With Query$WithCols Query$Without
                       Query$DocsRelation Query$ParamRelation Query$OrderDirection Query$OrderNulls
                       Query$UnnestCol Query$UnnestVar
                       TemporalFilter TemporalFilter$AllTime TemporalFilter$At TemporalFilter$In VarSpec)))

;; TODO inline once the type we support is fixed
(defn- query-type? [q] (seq? q))

(defmulti parse-query
  (fn [query]
    (when-not (query-type? query)
      (throw (err/illegal-arg :xtql/malformed-query {:query query})))

    (let [[op] query]
      (when-not (symbol? op)
        (throw (err/illegal-arg :xtql/malformed-query {:query query})))

      op)))

(defmethod parse-query :default [[op]]
  (throw (err/illegal-arg :xtql/unknown-query-op {:op op})))

(defmulti parse-query-tail
  (fn [query-tail]
    (when-not (query-type? query-tail)
      (throw (err/illegal-arg :xtql/malformed-query-tail {:query-tail query-tail})))

    (let [[op] query-tail]
      (when-not (symbol? op)
        (throw (err/illegal-arg :xtql/malformed-query-tail {:query-tail query-tail})))

      op)))

(defmethod parse-query-tail :default [[op]]
  (throw (err/illegal-arg :xtql/unknown-query-tail {:op op})))

(defmulti parse-unify-clause
  (fn [clause]
    (when-not (query-type? clause)
      (throw (err/illegal-arg :xtql/malformed-unify-clause {:clause clause})))

    (let [[op] clause]
      (when-not (symbol? op)
        (throw (err/illegal-arg :xtql/malformed-unify-clause {:clause clause})))

      op)))

(defmethod parse-unify-clause :default [[op]]
  (throw (err/illegal-arg :xtql/unknown-unify-clause {:op op})))

(defmulti parse-dml
  (fn [dml]
    (when-not (query-type? dml)
      (throw (err/illegal-arg :xtql/malformed-dml {:dml dml})))

    (let [[op] dml]
      (when-not (symbol? op)
        (throw (err/illegal-arg :xtql/malformed-dml {:dml dml})))

      op)))

(defmethod parse-dml :default [[op]]
  (throw (err/illegal-arg :xtql/unknown-dml {:op op})))

(declare parse-arg-specs)

(defn parse-expr [expr]
  (cond
    (true? expr) Expr/TRUE
    (false? expr) Expr/FALSE
    (int? expr) (Expr/val (long expr))
    (double? expr) (Expr/val (double expr))
    (symbol? expr) (let [str-expr (str expr)]
                     (if (str/starts-with? str-expr "$")
                       (Expr/param str-expr)
                       (Expr/lVar str-expr)))
    (keyword? expr) (Expr/val expr)
    (vector? expr) (Expr/val (mapv parse-expr expr))
    (set? expr) (Expr/val (into #{} (map parse-expr) expr))
    (map? expr) (Expr/val (into {} (map (juxt key (comp parse-expr val))) expr))

    (seq? expr) (do
                  (when (empty? expr)
                    (throw (err/illegal-arg :xtql/malformed-call {:call expr})))

                  (let [[f & args] expr]
                    (when-not (symbol? f)
                      (throw (err/illegal-arg :xtql/malformed-call {:call expr})))

                    (case f
                      .
                      (do
                        (when-not (and (= (count args) 2)
                                       (first args)
                                       (symbol? (second args)))
                          (throw (err/illegal-arg :xtql/malformed-get {:expr expr})))

                        (Expr/get (parse-expr (first args)) (str (second args))))

                      (exists? q pull pull*)
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
                            q (Expr/q parsed-query parsed-args)
                            pull (Expr/pull parsed-query parsed-args)
                            pull* (Expr/pullMany parsed-query parsed-args))))

                      (Expr/call (str f) (mapv parse-expr args)))))

    :else (Expr/val expr)))

(defprotocol Unparse
  (unparse [this]))

(extend-protocol Unparse
  Expr$LogicVar (unparse [e] (symbol (.lv e)))
  Expr$Param (unparse [e] (symbol (.v e)))
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
        (map? obj) (->> (map #(MapEntry/create (key %) (unparse (val %))) obj)
                        (into {}))
        :else obj)))

  Expr$Get
  (unparse [e]
    (list '. (unparse (.expr e)) (symbol (.field e))))

  Expr$Exists
  (unparse [e]
    (list* 'exists? (unparse (.query e))
           (when-let [args (.args e)]
             [{:args (mapv unparse args)}])))


  Expr$Subquery
  (unparse [e]
    (list* 'q (unparse (.query e))
           (when-let [args (.args e)]
             [{:args (mapv unparse args)}])))

  Expr$Pull
  (unparse [e]
    (list* 'q (unparse (.query e))
           (when-let [args (.args e)]
             [{:args (mapv unparse args)}])))

  Expr$PullMany
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
  ^List [specs _query]
  (letfn [(parse-out-spec [[attr expr]]
            (when-not (keyword? attr)
              (throw (err/illegal-arg :xtql/malformed-bind-spec
                                      {:attr attr :expr expr ::err/message "Attribute in bind spec must be keyword"})))

            (OutSpec/of (str (symbol attr)) (parse-expr expr)))]

    (if (vector? specs)
      (->> specs
           (into [] (mapcat (fn [spec]
                              (cond
                                (symbol? spec) (let [attr (str spec)]
                                                 [(OutSpec/of attr (Expr/lVar attr))])
                                (map? spec) (map parse-out-spec spec))))))

      (throw (UnsupportedOperationException.)))))

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
  (letfn [(parse-var-spec [[attr expr]]
            (when-not (symbol? attr)
              (throw (err/illegal-arg
                      :xtql/malformed-var-spec {:attr attr :expr expr
                       ::err/message "Attribute in var spec must be symbol"})))


            (VarSpec/of (str attr) (parse-expr expr)))]

    (cond
      (map? specs) (mapv parse-var-spec specs)
      (sequential? specs) (->> specs
                               (into [] (mapcat (fn [spec]
                                                  (if (map? spec) (map parse-var-spec spec)
                                                      (throw (err/illegal-arg
                                                              :xtql/malformed-var-spec
                                                              {:spec spec
                                                               ::err/message "Var specs must be pairs of bindings"})))))))
      :else (throw (UnsupportedOperationException.)))))

(defn- parse-col-specs
  "[{:to-col (from-expr)} :col ...]"
  [specs _query]
  (letfn [(parse-col-spec [[attr expr]]
            (when-not (keyword? attr)
              (throw (err/illegal-arg
                      :xtql/malformed-col-spec
                      {:attr attr :expr expr ::err/message "Attribute in col spec must be keyword"})))

            (ColSpec/of (str (symbol attr)) (parse-expr expr)))]

    (cond
      (map? specs) (mapv parse-col-spec specs)
      (sequential? specs) (->> specs
                               (into [] (mapcat (fn [spec]
                                                  (cond
                                                    (symbol? spec) (let [attr (str spec)]
                                                                     [(ColSpec/of attr (Expr/lVar attr))])
                                                    (map? spec) (map parse-col-spec spec)

                                                    :else
                                                    (throw (err/illegal-arg
                                                            :xtql/malformed-col-spec
                                                            {:spec spec ::err/message "Short form of col spec must be a symbol"})))))))
      :else (throw (UnsupportedOperationException.)))))

(defn check-opt-keys [valid-keys opts]
  (when-let [invalid-opt-keys (not-empty (set/difference (set (keys opts)) valid-keys))]
    (throw (err/illegal-arg :invalid-opt-keys
                            {:opts opts, :valid-keys valid-keys
                             :invalid-keys invalid-opt-keys
                             ::err/message "Invalid keys provided to option map"}))))

(def from-opt-keys #{:bind :for-valid-time :for-system-time})

(defn find-star-projection [star-ident bindings]
  ;;TODO worth checking here or in parse-out-specs for * in col/attr position? {* var}??
  ;;Arguably not checking for this could allow * to be a valid col name?
  (reduce
   (fn [acc binding]
     (if (= binding star-ident)
       (assoc acc :project-all-cols true)
       (update acc :bind conj binding)))
   {:project-all-cols false
    :bind []}
   bindings))

(defn parse-from [[_ table opts :as this]]
  (if-not (keyword? table)
    (throw (err/illegal-arg :xtql/malformed-table {:table table, :from this}))

    (let [q (Query/from (str (symbol table)))]
      (cond
        (or (nil? opts) (map? opts))

        (do
          (check-opt-keys from-opt-keys opts)

          (let [{:keys [for-valid-time for-system-time bind]} opts]
            (cond
              (nil? bind)
              (throw (err/illegal-arg :xtql/missing-bind {:opts opts, :from this}))

              (not (vector? bind))
              (throw (err/illegal-arg :xtql/malformed-bind {:opts opts, :from this}))

              :else
              (let [{:keys [bind project-all-cols]} (find-star-projection '* bind)]
                (-> q
                    (cond-> for-valid-time (.forValidTime (parse-temporal-filter for-valid-time :for-valid-time this))
                            for-system-time (.forSystemTime (parse-temporal-filter for-system-time :for-system-time this))
                            project-all-cols (.projectAllCols true))
                    (.binding (parse-out-specs bind this)))))))

        (vector? opts)
        (let [{:keys [bind project-all-cols]} (find-star-projection '* opts)]
          (-> q
              (cond-> project-all-cols (.projectAllCols true))
              (.binding (parse-out-specs bind this))))

        :else (throw (err/illegal-arg :xtql/malformed-table-opts {:opts opts, :from this}))))))

(defmethod parse-query 'from [this] (parse-from this))
(defmethod parse-unify-clause 'from [this] (parse-from this))

(def join-clause-opt-keys #{:args :bind})

(defmethod parse-unify-clause 'join [[_ query opts :as join]]
  (cond
    (nil? opts) (throw (err/illegal-arg :missing-join-opts {:opts opts, :join join}))

    (map? opts) (do
                  (check-opt-keys join-clause-opt-keys opts)
                  (let [{:keys [args bind]} opts]
                    (-> (Query/join (parse-query query) (parse-arg-specs args join))
                        (.binding (some-> bind (parse-out-specs join))))))

    :else (-> (Query/join (parse-query query) nil)
              (.binding (parse-out-specs opts join)))))

(defmethod parse-unify-clause 'left-join [[_ query opts :as left-join]]
  (cond
    (nil? opts) (throw (err/illegal-arg :missing-join-opts {:opts opts, :left-join left-join}))

    (map? opts) (do
                  (check-opt-keys join-clause-opt-keys opts)
                  (let [{:keys [args bind]} opts]
                    (-> (Query/leftJoin (parse-query query) (parse-arg-specs args left-join))
                        (.binding (some-> bind (parse-out-specs left-join))))))

    :else (-> (Query/leftJoin (parse-query query) nil)
              (.binding (parse-out-specs opts left-join)))))

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
  ColSpec (unparse [spec] (unparse-binding-spec (.attr spec) (.expr spec) symbol keyword))

  Query$From
  (unparse [from]
    (let [for-valid-time (.forValidTime from)
          for-sys-time (.forSystemTime from)
          bind (mapv unparse (.bindings from))
          bind (if (.projectAllCols from) (vec (cons '* bind)) bind)]
      (list 'from (keyword (.table from))
            (if (or for-valid-time for-sys-time)
              (cond-> {:bind bind}
                for-valid-time (assoc :for-valid-time (unparse for-valid-time))
                for-sys-time (assoc :for-system-time (unparse for-sys-time)))
              bind))))

  Query$Join
  (unparse [join]
    (let [args (.args join)
          bind (mapv unparse (.bindings join))]
      (list 'join (unparse (.query join))
            (if args
              (cond-> {:bind bind}
                (seq args) (assoc :args (mapv unparse args)))
              bind))))

  Query$LeftJoin
  (unparse [left-join]
    (let [args (.args left-join)
          bind (mapv unparse (.bindings left-join))]
      (list 'left-join (unparse (.query left-join))
            (if args
              (cond-> {:bind bind}
                (seq args) (assoc :args (mapv unparse args)))
              bind)))))

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
  Query$Offset (unparse [this] (list 'offset (.length this)))
  Query$DocsRelation (unparse [this]
                  (list 'rel
                        (mapv #(into {} (map (fn [[k v]] (MapEntry/create (keyword k) (unparse v)))) %) (.documents this))
                        (mapv unparse (.bindings this))))
  Query$ParamRelation (unparse [this]
                   (list 'rel (symbol (.v (.param this))) (mapv unparse (.bindings this))))
  Query$UnnestCol (unparse [this] (list 'unnest (unparse (.col this))))
  Query$UnnestVar (unparse [this] (list 'unnest (unparse (.var this)))))

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

;; TODO Align errors with json ones where appropriate.

(defmethod parse-query-tail 'with [[_ & cols :as this]]
  ;;TODO with uses col-specs but doesn't support short form, this needs handling
  (Query/withCols (parse-col-specs cols this)))

(defmethod parse-unify-clause 'with [[_ & vars :as this]]
  (Query/with (parse-var-specs vars this)))

(defmethod parse-query-tail 'without [[_ & cols :as this]]
  (when-not (every? keyword? cols)
    (throw (err/illegal-arg :xtql/malformed-without {:without this
                                                     ::err/message "Columns must be keywords in without"})))
  (Query/without (map (comp str symbol) cols)))

(defmethod parse-query-tail 'return [[_ & cols :as this]]
  (Query/returning (parse-col-specs cols this)))

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

(defn- keyword-map? [m]
  (every? keyword? (keys m)))

(defn parse-rel [[_ param-or-docs bind :as this]]
  (when-not (= 3 (count this))
    (throw (err/illegal-arg :xtql/rel {:rel this :message "`rel` takes exactly 3 arguments"})))
  (when-not (or (symbol? param-or-docs)
                (and (vector? param-or-docs) (every? keyword-map? param-or-docs)))
    (throw (err/illegal-arg :xtql/rel {:rel this :message "`rel` takes a param or an explicit relation"})))
  (let [parsed-bind (parse-out-specs bind this)]
    (if (symbol? param-or-docs)
      (let [parsed-expr (parse-expr param-or-docs)]
        (if (instance? Expr$Param parsed-expr)
          (Query/relation ^Expr$Param parsed-expr parsed-bind)
          (throw (err/illegal-arg :xtql/rel {::err/message "Illegal second argument to `rel`"
                                             :arg param-or-docs}))))
      (Query/relation ^List (mapv #(into {} (map (fn [[k v]] (MapEntry/create (subs (str k) 1) (parse-expr v)))) %) param-or-docs) parsed-bind))))

(defmethod parse-query 'rel [this] (parse-rel this))
(defmethod parse-unify-clause 'rel [this] (parse-rel this))

(defn check-unnest [binding unnest]
  (when-not (and (= 2 (count unnest))
                 (map? binding)
                 (= 1 (count binding)))
    (throw (err/illegal-arg :xtql/unnest {:unnest unnest ::err/message "Unnest takes only a single binding"}))))

(defmethod parse-query-tail 'unnest [[_ binding :as this]]
  (check-unnest binding this)
  (Query/unnestCol (first (parse-col-specs binding this))))

(defmethod parse-unify-clause 'unnest [[_ binding :as this]]
  (check-unnest binding this)
  (Query/unnestVar (first (parse-var-specs binding this))))

(def order-spec-opt-keys #{:val :dir :nulls})

(defn- parse-order-spec [order-spec this]
  (if (map? order-spec)
    (let [{:keys [val dir nulls]} order-spec
          _ (check-opt-keys order-spec-opt-keys order-spec)
          __ (when-not (contains? order-spec :val)
               (throw (err/illegal-arg :xtql/order-by-val-missing
                                       {:order-spec order-spec, :query this})))
          dir (case dir
                nil nil
                :asc Query$OrderDirection/ASC
                :desc Query$OrderDirection/DESC

                (throw (err/illegal-arg :xtql/malformed-order-by-direction
                                        {:direction dir, :order-spec order-spec, :query this})))
          nulls (case nulls
                  nil nil
                  :first Query$OrderNulls/FIRST
                  :last Query$OrderNulls/LAST

                  (throw (err/illegal-arg :xtql/malformed-order-by-nulls
                                          {:nulls nulls, :order-spec order-spec, :query this})))]
      (Query/orderSpec (parse-expr val) dir nulls))
    (Query/orderSpec (parse-expr order-spec) nil nil)))

(defmethod parse-query-tail 'order-by [[_ & order-specs :as this]]
  (Query/orderBy (mapv #(parse-order-spec % this) order-specs)))

(extend-protocol Unparse
  Query$OrderSpec
  (unparse [spec]
    (let [expr (unparse (.expr spec))
          dir (.direction spec)
          nulls (.nulls spec)]
      (if (and (not dir) (not nulls))
        expr
        (cond-> {:val expr}
          dir (assoc :dir (if (= Query$OrderDirection/ASC dir) :asc :desc))
          nulls (assoc :nulls (if (= Query$OrderNulls/FIRST nulls) :first :last))))))

  Query$OrderBy
  (unparse [query]
    (list* 'order-by (mapv unparse (.orderSpecs query)))))

(defmethod parse-dml 'insert [[_ table query :as this]]
  (if-not (keyword? table)
    (throw (err/illegal-arg :xtql/malformed-table {:table table, :insert this}))

    (DmlOps/insert (str (symbol table)) (parse-query query))))

(defmethod parse-dml 'update [[_ table opts & unify-clauses :as this]]
  (cond
    (not (keyword? table))
    (throw (err/illegal-arg :xtql/malformed-table {:table table, :update this}))

    (not (map? opts))
    (throw (err/illegal-arg :xtql/malformed-opts {:opts opts, :update this}))

    :else (let [{set-specs :set, :keys [for-valid-time bind]} opts]
            (when-not (map? set-specs)
              (throw (err/illegal-arg :xtql/malformed-set {:set set-specs, :update this})))

            (when-not (or (nil? bind) (vector? bind))
              (throw (err/illegal-arg :xtql/malformed-bind {:bind bind, :update this})))

            (cond-> (DmlOps/update (str (symbol table)) (parse-col-specs set-specs this))
              for-valid-time (.forValidTime (parse-temporal-filter for-valid-time :for-valid-time this))
              bind (.binding (parse-out-specs bind this))
              (seq unify-clauses) (.unify (mapv parse-unify-clause unify-clauses))))))

(defmethod parse-dml 'delete [[_ table opts & unify-clauses :as this]]
  (if-not (keyword? table)
    (throw (err/illegal-arg :xtql/malformed-table {:table table, :delete this}))

    (let [delete (DmlOps/delete (str (symbol table)))
          ^DmlOps$Delete delete (cond
                                  (nil? opts) delete
                                  (vector? opts) (-> delete (.binding (parse-out-specs opts this)))
                                  (map? opts) (let [{:keys [for-valid-time bind]} opts]
                                                (cond-> delete
                                                  for-valid-time (.forValidTime (parse-temporal-filter for-valid-time :for-valid-time this))
                                                  bind (.binding (parse-out-specs bind this))))

                                  :else (throw (err/illegal-arg :xtql/malformed-opts {:opts opts, :delete this})))]

      (cond-> delete
        (seq unify-clauses) (.unify (mapv parse-unify-clause unify-clauses))))))

(defmethod parse-dml 'erase [[_ table opts & unify-clauses :as this]]
  (if-not (keyword? table)
    (throw (err/illegal-arg :xtql/malformed-table {:table table, :erase this}))

    (let [erase (DmlOps/erase (str (symbol table)))
          ^DmlOps$Erase erase (cond
                                (nil? opts) erase
                                (vector? opts) (-> erase (.binding (parse-out-specs opts this)))
                                (map? opts) (let [{:keys [bind]} opts]
                                              (cond-> erase
                                                bind (.binding (parse-out-specs bind this))))

                                :else (throw (err/illegal-arg :xtql/malformed-opts {:opts opts, :erase this})))]

      (cond-> erase
        (seq unify-clauses) (.unify (mapv parse-unify-clause unify-clauses))))))

(defmethod parse-dml 'assert-exists [[_ query]]
  (DmlOps/assertExists (parse-query query)))

(defmethod parse-dml 'assert-not-exists [[_ query]]
  (DmlOps/assertNotExists (parse-query query)))

(extend-protocol Unparse
  DmlOps$Insert
  (unparse [query]
    (list 'insert (keyword (.table query)) (unparse (.query query))))

  DmlOps$Update
  (unparse [query]
    (let [for-valid-time (.forValidTime query)
          bind (.bindSpecs query)]
      (list* 'update (keyword (.table query))
             (cond-> {:set (mapv unparse (.setSpecs query))}
               for-valid-time (assoc :for-valid-time (unparse for-valid-time))
               (seq bind) (assoc :bind (mapv unparse bind)))
             (mapv unparse (.unifyClauses query)))))

  DmlOps$Delete
  (unparse [query]
    (let [for-valid-time (.forValidTime query)
          bind (mapv unparse (.bindSpecs query))]
      (list* 'delete (keyword (.table query))
             (if for-valid-time
               (cond-> {:bind bind}
                 for-valid-time (assoc :for-valid-time (unparse for-valid-time)))
               bind)
             (mapv unparse (.unifyClauses query)))))

  DmlOps$Erase
  (unparse [query]
    (list* 'erase (keyword (.table query))
           (mapv unparse (.bindSpecs query))
           (mapv unparse (.unifyClauses query))))

  DmlOps$AssertExists
  (unparse [query]
    (list 'assert-exists (unparse (.query query))))

  DmlOps$AssertNotExists
  (unparse [query]
    (list 'assert-not-exists (unparse (.query query)))))
