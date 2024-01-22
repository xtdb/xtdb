(ns xtdb.xtql.edn
  (:require [clojure.set :as set]
            [clojure.string :as str]
            [xtdb.error :as err])
  (:import (clojure.lang MapEntry)
           (java.util List)
           (xtdb.api.query Binding
                           Expr Expr$Null Expr$Bool Expr$Call Expr$Double Expr$Exists Expr$Param Expr$Get
                           Expr$LogicVar Expr$Long Expr$Obj Expr$Subquery Expr$Pull Expr$PullMany Expr$SetExpr
                           Expr$ListExpr Expr$MapExpr
                           XtqlQuery XtqlQuery$Aggregate XtqlQuery$From XtqlQuery$LeftJoin XtqlQuery$Join XtqlQuery$Limit
                           XtqlQuery$OrderBy XtqlQuery$OrderDirection XtqlQuery$OrderSpec XtqlQuery$Pipeline XtqlQuery$Offset
                           XtqlQuery$Return XtqlQuery$Unify XtqlQuery$UnionAll XtqlQuery$Where XtqlQuery$With XtqlQuery$WithCols XtqlQuery$Without
                           XtqlQuery$DocsRelation XtqlQuery$ParamRelation XtqlQuery$OrderDirection XtqlQuery$OrderNulls
                           XtqlQuery$UnnestCol XtqlQuery$UnnestVar
                           TemporalFilter TemporalFilter$AllTime TemporalFilter$At TemporalFilter$In)))

;; TODO inline once the type we support is fixed
(defn- query-type? [q] (seq? q))

(defmulti ^xtdb.api.query.XtqlQuery parse-query
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

(declare parse-arg-specs)

(defn parse-expr ^xtdb.api.query.Expr [expr]
  (cond
    (nil? expr) Expr/NULL
    (true? expr) Expr/TRUE
    (false? expr) Expr/FALSE
    (int? expr) (Expr/val (long expr))
    (double? expr) (Expr/val (double expr))
    (symbol? expr) (let [str-expr (str expr)]
                     (if (str/starts-with? str-expr "$")
                       (Expr/param str-expr)
                       (Expr/lVar str-expr)))
    (keyword? expr) (Expr/val expr)
    (vector? expr) (Expr/list ^List (mapv parse-expr expr))
    (set? expr) (Expr/set ^List (mapv parse-expr expr))
    (map? expr) (Expr/map (into {} (map (juxt (comp #(subs % 1) str key) (comp parse-expr val))) expr))

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

                      (Expr/call (str f) ^List (mapv parse-expr args)))))

    :else (Expr/val expr)))

(defprotocol Unparse
  (unparse [this]))

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
(def unparse-arg-spec (partial unparse-binding symbol keyword))
(def unparse-var-spec (partial unparse-binding nil symbol))

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
    (list* 'exists? (unparse (.query e))
           (when-let [args (.args e)]
             [{:args (mapv unparse-arg-spec args)}])))


  Expr$Subquery
  (unparse [e]
    (list* 'q (unparse (.query e))
           (when-let [args (.args e)]
             [{:args (mapv unparse-arg-spec args)}])))

  Expr$Pull
  (unparse [e]
    (list* 'pull (unparse (.query e))
           (when-let [args (.args e)]
             [{:args (mapv unparse-arg-spec args)}])))

  Expr$PullMany
  (unparse [e]
    (list* 'pull* (unparse (.query e))
           (when-let [args (.args e)]
             [{:args (mapv unparse-arg-spec args)}]))))

(defn parse-temporal-filter [v k query]
  (let [ctx {:v v, :filter k, :query query}]
    (if (= :all-time v)
      TemporalFilter/ALL_TIME

      (do
        (when-not (and (seq? v) (not-empty v))
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
  TemporalFilter$At (unparse [at] (list 'at (unparse (.getAt at))))
  TemporalFilter$In (unparse [in] (list 'in (some-> (.getFrom in) unparse) (some-> (.getTo in) unparse))))

;;NOTE out-specs and arg-specs are currently indentical structurally, but one is an input binding,
;;the other an output binding.
;;I could see remerging these into a single binding-spec,
;;that being said, its possible we don't want arbitrary exprs in the from of arg specs, only plain vars
;;
;;TODO binding-spec-errs
(defn parse-out-specs
  "[{:from to-var} from-col-to-var {:col (pred)}]"
  ^List [specs _query]
  (letfn [(parse-out-spec [[attr expr]]
            (when-not (keyword? attr)
              (throw (err/illegal-arg :xtql/malformed-bind-spec
                                      {:attr attr :expr expr ::err/message "Attribute in bind spec must be keyword"})))

            (Binding. (str (symbol attr)) (parse-expr expr)))]

    (if (vector? specs)
      (->> specs
           (into [] (mapcat (fn [spec]
                              (cond
                                (symbol? spec) [(Binding. (str spec))]
                                (map? spec) (map parse-out-spec spec))))))

      (throw (UnsupportedOperationException.)))))

(defn- parse-arg-specs
  "[{:to-var (from-expr)} to-var-from-var]"
  [specs _query]
  (->> specs
       (into [] (mapcat (fn [spec]
                          (cond
                            (symbol? spec) [(Binding. (str spec))]
                            (map? spec) (for [[attr expr] spec]
                                          (do
                                            #_{:clj-kondo/ignore [:missing-body-in-when]}
                                            (when-not (keyword? attr)
                                              ;; TODO error
                                              )
                                            (Binding. (str (symbol attr)) (parse-expr expr))))))))))
(defn- parse-var-specs
  "[{to-var (from-expr)}]"
  [specs _query]
  (letfn [(parse-var-spec [[attr expr]]
            (when-not (symbol? attr)
              (throw (err/illegal-arg
                      :xtql/malformed-var-spec {:attr attr :expr expr
                       ::err/message "Attribute in var spec must be symbol"})))


            (Binding. (str attr) (parse-expr expr)))]

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

(defn parse-col-specs
  "[{:to-col (from-expr)} :col ...]"
  [specs _query]
  (letfn [(parse-col-spec [[attr expr]]
            (when-not (keyword? attr)
              (throw (err/illegal-arg
                      :xtql/malformed-col-spec
                      {:attr attr :expr expr ::err/message "Attribute in col spec must be keyword"})))

            (Binding. (str (symbol attr)) (parse-expr expr)))]

    (cond
      (map? specs) (mapv parse-col-spec specs)
      (sequential? specs) (->> specs
                               (into [] (mapcat (fn [spec]
                                                  (cond
                                                    (symbol? spec) [(Binding. (str spec))]
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

    (let [q (XtqlQuery/from (str (symbol table)))]
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
                (-> (cond-> (doto q (.setBindings (parse-out-specs bind this)))
                      for-valid-time (doto (.forValidTime (parse-temporal-filter for-valid-time :for-valid-time this)))
                      for-system-time (doto (.forSystemTime (parse-temporal-filter for-system-time :for-system-time this)))
                      project-all-cols (doto (.projectAllCols true)))
                    (.build))))))

        (vector? opts)
        (let [{:keys [bind project-all-cols]} (find-star-projection '* opts)]
          (-> (cond-> (doto q (.setBindings (parse-out-specs bind this)))
                project-all-cols (doto (.projectAllCols)))
              (.build)))

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
                    (-> (XtqlQuery/join (parse-query query) (parse-arg-specs args join))
                        (.binding (some-> bind (parse-out-specs join))))))

    :else (-> (XtqlQuery/join (parse-query query) nil)
              (.binding (parse-out-specs opts join)))))

(defmethod parse-unify-clause 'left-join [[_ query opts :as left-join]]
  (cond
    (nil? opts) (throw (err/illegal-arg :missing-join-opts {:opts opts, :left-join left-join}))

    (map? opts) (do
                  (check-opt-keys join-clause-opt-keys opts)
                  (let [{:keys [args bind]} opts]
                    (-> (XtqlQuery/leftJoin (parse-query query) (parse-arg-specs args left-join))
                        (.binding (some-> bind (parse-out-specs left-join))))))

    :else (-> (XtqlQuery/leftJoin (parse-query query) nil)
              (.binding (parse-out-specs opts left-join)))))

(extend-protocol Unparse
  XtqlQuery$From
  (unparse [from]
    (let [for-valid-time (.forValidTime from)
          for-sys-time (.forSystemTime from)
          bind (mapv unparse-out-spec (.bindings from))
          bind (if (.projectAllCols from) (vec (cons '* bind)) bind)]
      (list 'from (keyword (.table from))
            (if (or for-valid-time for-sys-time)
              (cond-> {:bind bind}
                for-valid-time (assoc :for-valid-time (unparse for-valid-time))
                for-sys-time (assoc :for-system-time (unparse for-sys-time)))
              bind))))

  XtqlQuery$Join
  (unparse [join]
    (let [args (.args join)
          bind (mapv unparse-col-spec (.bindings join))]
      (list 'join (unparse (.query join))
            (if args
              (cond-> {:bind bind}
                (seq args) (assoc :args (mapv unparse-arg-spec args)))
              bind))))

  XtqlQuery$LeftJoin
  (unparse [left-join]
    (let [args (.args left-join)
          bind (mapv unparse-col-spec (.bindings left-join))]
      (list 'left-join (unparse (.query left-join))
            (if args
              (cond-> {:bind bind}
                (seq args) (assoc :args (mapv unparse-arg-spec args)))
              bind)))))

(extend-protocol Unparse
  XtqlQuery$Pipeline (unparse [query] (list* '-> (unparse (.query query)) (mapv unparse (.tails query))))
  XtqlQuery$Where (unparse [query] (list* 'where (mapv unparse (.preds query))))
  XtqlQuery$With (unparse [query] (list* 'with (mapv unparse-var-spec (.vars query))))
  XtqlQuery$WithCols (unparse [query] (list* 'with (mapv unparse-col-spec (.cols query))))
  XtqlQuery$Without (unparse [query] (list* 'without (map keyword (.cols query))))
  XtqlQuery$Return (unparse [query] (list* 'return (mapv unparse-col-spec (.cols query))))
  XtqlQuery$Aggregate (unparse [query] (list* 'aggregate (mapv unparse-col-spec (.cols query))))
  XtqlQuery$Unify (unparse [query] (list* 'unify (mapv unparse (.clauses query))))
  XtqlQuery$UnionAll (unparse [query] (list* 'union-all (mapv unparse (.queries query))))
  XtqlQuery$Limit (unparse [this] (list 'limit (.length this)))
  XtqlQuery$Offset (unparse [this] (list 'offset (.length this)))

  XtqlQuery$DocsRelation
  (unparse [this]
    (list 'rel
          (mapv #(into {} (map (fn [[k v]] (MapEntry/create (keyword k) (unparse v)))) %) (.documents this))
          (mapv unparse-out-spec (.bindings this))))

  XtqlQuery$ParamRelation
  (unparse [this]
    (list 'rel (symbol (.v (.param this))) (mapv unparse-out-spec (.bindings this))))

  XtqlQuery$UnnestCol (unparse [this] (list 'unnest (unparse-col-spec (.col this))))
  XtqlQuery$UnnestVar (unparse [this] (list 'unnest (unparse-var-spec (.var this)))))

(defmethod parse-query 'unify [[_ & clauses :as this]]
  (when (> 1 (count clauses))
    (throw (err/illegal-arg :xtql/malformed-unify {:unify this
                                                   :message "Unify most contain at least one sub clause"})))
  (XtqlQuery/unify ^List (mapv parse-unify-clause clauses)))

(defmethod parse-query 'union-all [[_ & queries :as this]]
  (when (> 1 (count queries))
    (throw (err/illegal-arg :xtql/malformed-union {:union this
                                                   :message "Union must contain a least one sub query"})))
  (XtqlQuery/unionAll ^List (mapv parse-query queries)))

(defn parse-where [[_ & preds :as this]]
  (when (> 1 (count preds))
    (throw (err/illegal-arg :xtql/malformed-where {:where this
                                                   :message "Where most contain at least one predicate"})))
  (XtqlQuery/where ^List (mapv parse-expr preds)))

(defmethod parse-query-tail 'where [this] (parse-where this))
(defmethod parse-unify-clause 'where [this] (parse-where this))

(defmethod parse-query '-> [[_ head & tails :as this]]
  (when-not head
    (throw (err/illegal-arg :xtql/malformed-pipeline {:pipeline this
                                                      :message "Pipeline most contain at least one operator"})))
  (XtqlQuery/pipeline (parse-query head) ^List (mapv parse-query-tail tails)))

;; TODO Align errors with json ones where appropriate.

(defmethod parse-query-tail 'with [[_ & cols :as this]]
  ;;TODO with uses col-specs but doesn't support short form, this needs handling
  (XtqlQuery/withCols (parse-col-specs cols this)))

(defmethod parse-unify-clause 'with [[_ & vars :as this]]
  (XtqlQuery/with (parse-var-specs vars this)))

(defmethod parse-query-tail 'without [[_ & cols :as this]]
  (when-not (every? keyword? cols)
    (throw (err/illegal-arg :xtql/malformed-without {:without this
                                                     ::err/message "Columns must be keywords in without"})))
  (XtqlQuery/without ^List (map (comp str symbol) cols)))

(defmethod parse-query-tail 'return [[_ & cols :as this]]
  (XtqlQuery/returning (parse-col-specs cols this)))

(defmethod parse-query-tail 'aggregate [[_ & cols :as this]]
  (XtqlQuery/aggregate (parse-col-specs cols this)))

(defmethod parse-query-tail 'limit [[_ length :as this]]
  (when-not (= 2 (count this))
    (throw (err/illegal-arg :xtql/limit {:limit this :message "Limit can only take a single value"})))
  (XtqlQuery/limit length))

(defmethod parse-query-tail 'offset [[_ length :as this]]
  (when-not (= 2 (count this))
    (throw (err/illegal-arg :xtql/offset {:offset this :message "Offset can only take a single value"})))
  (XtqlQuery/offset length))

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
          (XtqlQuery/relation ^Expr$Param parsed-expr parsed-bind)
          (throw (err/illegal-arg :xtql/rel {::err/message "Illegal second argument to `rel`"
                                             :arg param-or-docs}))))
      (XtqlQuery/relation ^List (mapv #(into {} (map (fn [[k v]] (MapEntry/create (subs (str k) 1) (parse-expr v)))) %) param-or-docs) parsed-bind))))

(defmethod parse-query 'rel [this] (parse-rel this))
(defmethod parse-unify-clause 'rel [this] (parse-rel this))

(defn check-unnest [binding unnest]
  (when-not (and (= 2 (count unnest))
                 (map? binding)
                 (= 1 (count binding)))
    (throw (err/illegal-arg :xtql/unnest {:unnest unnest ::err/message "Unnest takes only a single binding"}))))

(defmethod parse-query-tail 'unnest [[_ binding :as this]]
  (check-unnest binding this)
  (XtqlQuery/unnestCol (first (parse-col-specs binding this))))

(defmethod parse-unify-clause 'unnest [[_ binding :as this]]
  (check-unnest binding this)
  (XtqlQuery/unnestVar (first (parse-var-specs binding this))))

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
                :asc XtqlQuery$OrderDirection/ASC
                :desc XtqlQuery$OrderDirection/DESC

                (throw (err/illegal-arg :xtql/malformed-order-by-direction
                                        {:direction dir, :order-spec order-spec, :query this})))
          nulls (case nulls
                  nil nil
                  :first XtqlQuery$OrderNulls/FIRST
                  :last XtqlQuery$OrderNulls/LAST

                  (throw (err/illegal-arg :xtql/malformed-order-by-nulls
                                          {:nulls nulls, :order-spec order-spec, :query this})))]
      (XtqlQuery/orderSpec (parse-expr val) dir nulls))
    (XtqlQuery/orderSpec (parse-expr order-spec) nil nil)))

(defmethod parse-query-tail 'order-by [[_ & order-specs :as this]]
  (XtqlQuery/orderBy ^List (mapv #(parse-order-spec % this) order-specs)))

(extend-protocol Unparse
  XtqlQuery$OrderSpec
  (unparse [spec]
    (let [expr (unparse (.expr spec))
          dir (.direction spec)
          nulls (.nulls spec)]
      (if (and (not dir) (not nulls))
        expr
        (cond-> {:val expr}
          dir (assoc :dir (if (= XtqlQuery$OrderDirection/ASC dir) :asc :desc))
          nulls (assoc :nulls (if (= XtqlQuery$OrderNulls/FIRST nulls) :first :last))))))

  XtqlQuery$OrderBy
  (unparse [query]
    (list* 'order-by (mapv unparse (.orderSpecs query)))))
