(ns ^:no-doc crux.sparql
  "SPARQL to Datalog compiler. Only handles a subset of SPARQL.

  https://www.w3.org/TR/2013/REC-sparql11-query-20130321/

  See http://docs.rdf4j.org/"
  (:require [clojure.walk :as w]
            [clojure.set :as set]
            [crux.rdf :as rdf])
  (:import [org.eclipse.rdf4j.query BindingSet QueryLanguage]
           org.eclipse.rdf4j.query.parser.QueryParserUtil
           org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor
           [org.eclipse.rdf4j.query.algebra
            And ArbitraryLengthPath BindingSetAssignment Compare Difference Distinct Extension ExtensionElem Exists
            Filter FunctionCall Join LeftJoin ListMemberOperator MathExpr Not Or Order OrderElem Projection QueryModelNode
            Regex SameTerm Slice StatementPattern TupleExpr Union ValueConstant Var ZeroLengthPath]))

(defn- str->sparql-var [s]
  (symbol (str "?" s)))

(defn- arbitrary-length-path-rule-head [s p o min-length]
  (when (coll? p)
    (throw (UnsupportedOperationException. "Only single predicate arbitrary length paths supported.")))
  (let [path-type (case (long min-length)
                    1 "PLUS"
                    0 "STAR")]
    (list (symbol (namespace p)
                  (str (name p) "-" path-type)) s o)))

(defn blank-or-anonymous-var? [x]
  (re-find  #"^\??_" x))

(extend-protocol rdf/RDFToClojure
  And
  (rdf->clj [this]
    (vec (concat (rdf/rdf->clj (.getLeftArg this))
                 (rdf/rdf->clj (.getRightArg this)))))

  ArbitraryLengthPath
  (rdf->clj [this]
    (when-not (instance? StatementPattern (.getPathExpression this))
      (throw (UnsupportedOperationException.
              "Arbitrary length paths only supported for statement patterns.")))
    (let [[[s p o]] (rdf/rdf->clj (.getPathExpression this))]
      [(arbitrary-length-path-rule-head s p o (.getMinLength this))]))

  BindingSetAssignment
  (rdf->clj [this])

  Compare
  (rdf->clj [this]
    (let [left (rdf/rdf->clj (.getLeftArg this))
          right (rdf/rdf->clj (.getRightArg this))]
      (when (some list? [left right])
        (throw (UnsupportedOperationException. "Nested expressions are not supported.")))
      [[(list (symbol (.getSymbol (.getOperator this)))
              left
              right)]]))

  Difference
  (rdf->clj [this]
    (throw (UnsupportedOperationException. "MINUS not supported, use NOT EXISTS.")))

  ;; NOTE: Distinct is the default due to the set semantics of
  ;; Datalog.
  Distinct
  (rdf->clj [this]
    (rdf/rdf->clj (.getArg this)))

  Extension
  (rdf->clj [this]
    (vec (apply concat
                (rdf/rdf->clj (.getArg this))
                (for [arg (.getElements this)]
                  (rdf/rdf->clj arg)))))

  ExtensionElem
  (rdf->clj [this]
    (when-not (instance? Var (.getExpr this))
      [[(rdf/rdf->clj (.getExpr this))
        (str->sparql-var (.getName this))]]))

  Exists
  (rdf->clj [this]
    (rdf/rdf->clj (.getSubQuery this)))

  Filter
  (rdf->clj [this]
    (let [smap (when (instance? SameTerm (.getCondition this))
                 (let [same-term ^SameTerm (.getCondition this)]
                   {(rdf/rdf->clj (.getLeftArg same-term))
                    (rdf/rdf->clj (.getRightArg same-term))}))]
      (vec (concat (w/postwalk-replace smap (rdf/rdf->clj (.getArg this)))
                   (rdf/rdf->clj (.getCondition this))))))

  FunctionCall
  (rdf->clj [this]
    (let [args (for [arg (.getArgs this)]
                 (rdf/rdf->clj arg))]
      (when (some list? args)
        (throw (UnsupportedOperationException. "Nested expressions are not supported.")))
      (->> (cons (symbol (.getURI this)) args)
           (apply list))))

  Join
  (rdf->clj [this]
    (vec (concat (rdf/rdf->clj (.getLeftArg this))
                 (rdf/rdf->clj (.getRightArg this)))))

  MathExpr
  (rdf->clj [this]
    (let [left (rdf/rdf->clj (.getLeftArg this))
          right (rdf/rdf->clj (.getRightArg this))]
      (when (some list? [left right])
        (throw (UnsupportedOperationException. "Nested expressions are not supported.")))
      (list (symbol (.getSymbol (.getOperator this)))
            left right)))

  LeftJoin
  (rdf->clj [this]
    (let [or-join-vars (->> (.getBindingNames this)
                            (remove blank-or-anonymous-var?)
                            (set))
          or-join-vars (set/intersection or-join-vars (set (.getBindingNames (.getRightArg this))))
          or-join-vars (mapv str->sparql-var or-join-vars)
          common-vars (set (mapv str->sparql-var (.getBindingNames (.getLeftArg this))))
          condition (some-> (.getCondition this) (rdf/rdf->clj))
          optional (rdf/rdf->clj (.getRightArg this))
          optional-attr (second (first optional))
          optional (cond-> optional
                     condition (concat condition))
          optional (if (> (count optional) 1)
                     [(cons 'and optional)]
                     optional)
          build-optional-clause (fn [var]
                                  (if (contains? common-vars var)
                                    (list 'not [var optional-attr])
                                    [(list 'identity ::optional) var]))]
      (conj (rdf/rdf->clj (.getLeftArg this))
            (cons 'or-join (cons or-join-vars
                                 (concat
                                  optional
                                  (if (> (count or-join-vars) 1)
                                    [(apply list 'and (map build-optional-clause or-join-vars))]
                                    [(build-optional-clause (first or-join-vars))])))))))

  ListMemberOperator
  (rdf->clj [this]
    [[(list '==
            (rdf/rdf->clj (first (.getArguments this)))
            (set (map rdf/rdf->clj (rest (.getArguments this)))))]])

  ;; TODO: Should be possible to make this work in other cases as
  ;; well.
  Not
  (rdf->clj [this]
    (when-not (and (instance? Exists (.getArg this))
                   (instance? Filter (.getParentNode this)))
      (throw (UnsupportedOperationException. "NOT only supported in FILTER NOT EXISTS.")))
    (let [not-vars (->> (.getAssuredBindingNames (.getSubQuery ^Exists (.getArg this)))
                        (remove blank-or-anonymous-var?)
                        (set))
          parent-vars (set (.getAssuredBindingNames ^Filter (.getParentNode this)))
          is-not? (set/subset? not-vars parent-vars)
          not-join-vars (set/intersection not-vars parent-vars)
          not (rdf/rdf->clj (.getArg this))]
      [(if is-not?
         (cons 'not not)
         (cons 'not-join (cons (mapv str->sparql-var not-join-vars) not)))]))

  ;; TODO: To simplistic. Logical or is really short cutting, but is
  ;; useful to support between predicates in a filter.
  Or
  (rdf->clj [this]
    (let [or-left (rdf/rdf->clj (.getLeftArg this))
          or-left (if (> (count or-left) 1)
                    [(cons 'and or-left)]
                    or-left)
          or-right (rdf/rdf->clj (.getRightArg this))
          or-right (if (> (count or-right) 1)
                     [(cons 'and or-right)]
                     or-right)]
      [(cons 'or (concat or-left or-right))]))

  Order
  (rdf->clj [this]
    (rdf/rdf->clj (.getArg this)))

  OrderElem
  (rdf->clj [this]
    [(rdf/rdf->clj (.getExpr this))
     (if (.isAscending this)
       :asc
       :desc)])


  Projection
  (rdf->clj [this]
    (rdf/rdf->clj (.getArg this)))

  Regex
  (rdf->clj [this]
    [[(list 're-find
            (let [flags (some-> (.getFlagsArg this) (rdf/rdf->clj))]
              (re-pattern (str (when (seq flags)
                                 (str "(?" flags ")"))
                               (rdf/rdf->clj (.getPatternArg this)))))
            (rdf/rdf->clj (.getArg this)))]])

  SameTerm
  (rdf->clj [this])

  Slice
  (rdf->clj [this]
    (rdf/rdf->clj (.getArg this)))

  StatementPattern
  (rdf->clj [this]
    (let [s (.getSubjectVar this)
          p (.getPredicateVar this)
          o (.getObjectVar this)]
      (when-not (.hasValue p)
        (throw (UnsupportedOperationException. (str "Does not support variables in predicate position: " (rdf/rdf->clj p)))))
      [(mapv rdf/rdf->clj [s p o])]))

  Union
  (rdf->clj [this]
    (let [is-or? (= (set (.getAssuredBindingNames this))
                    (->> (.getBindingNames this)
                         (remove blank-or-anonymous-var?)
                         (set)))
          or-join-vars (->> (.getAssuredBindingNames this)
                            (remove blank-or-anonymous-var?)
                            (mapv str->sparql-var))
          or-left (rdf/rdf->clj (.getLeftArg this))
          or-left (if (> (count or-left) 1)
                    [(cons 'and or-left)]
                    or-left)
          or-right (rdf/rdf->clj (.getRightArg this))
          or-right (if (> (count or-right) 1)
                     [(cons 'and or-right)]
                     or-right)]
      [(if is-or?
         (cons 'or (concat or-left or-right))
         (cons 'or-join (cons or-join-vars (concat or-left or-right))))]))

  Var
  (rdf->clj [this]
    (if (.hasValue this)
      (rdf/rdf->clj (.getValue this))
      (str->sparql-var (.getName this))))

  ValueConstant
  (rdf->clj [this]
    (rdf/rdf->clj (.getValue this)))

  ZeroLengthPath
  (rdf->clj [this]
    [[(rdf/rdf->clj (.getSubjectVar this)) :xt/id]
     ['(identity ::zero-matches) (rdf/rdf->clj (.getObjectVar this))]]))

(defn- collect-args [^TupleExpr tuple-expr]
  (let [args (atom nil)]
    (->> (proxy [AbstractQueryModelVisitor] []
           (meetNode [node]
             (when (instance? BindingSetAssignment node)
               (when @args
                 (throw (IllegalStateException. "Only one VALUES block supported.")))
               (let [bsa ^BindingSetAssignment node]
                 (->> (for [^BindingSet bs (.getBindingSets bsa)]
                        (->> (for [bn (.getBindingNames bs)]
                               [(str->sparql-var bn) (if (.hasBinding bs bn)
                                                       (rdf/rdf->clj (.getValue bs bn))
                                                       ::undefined)])
                             (into {})))
                      (vec)
                      (reset! args))))
             (.visitChildren ^QueryModelNode node this)))
         (.visit tuple-expr))
    @args))

(defn- collect-arbritrary-path-rules [^TupleExpr tuple-expr]
  (let [rule-name->rules (atom nil)]
    (->> (proxy [AbstractQueryModelVisitor] []
           (meetNode [node]
             (when (instance? ArbitraryLengthPath node)
               (let [[[_ p _]] (rdf/rdf->clj (.getPathExpression ^ArbitraryLengthPath node))
                     s '?s
                     o '?o
                     min-length (.getMinLength ^ArbitraryLengthPath node)
                     [rule-name] (arbitrary-length-path-rule-head s p o min-length)]
                 (swap! rule-name->rules assoc rule-name
                        (cond-> [[(arbitrary-length-path-rule-head s p o min-length)
                                  [s p o]]
                                 [(arbitrary-length-path-rule-head s p o min-length)
                                  [s p '?t]
                                  (arbitrary-length-path-rule-head '?t p o min-length)]]
                          (zero? min-length) (conj [(arbitrary-length-path-rule-head s p o min-length)
                                                    [s :xt/id]
                                                    ['(identity ::zero-matches) o]])))))
             (.visitChildren ^QueryModelNode node this)))
         (.visit tuple-expr))
    (some->> (for [[_ rules]  @rule-name->rules
                   rule rules]
               rule)
             (not-empty)
             (vec))))

(defn- collect-slice [^TupleExpr tuple-expr]
  (let [slice (atom nil)]
    (->> (proxy [AbstractQueryModelVisitor] []
           (meetNode [node]
             (when (instance? Slice node)
               (reset! slice (merge (when (.hasOffset ^Slice node)
                                      {:offset (.getOffset ^Slice node)})
                                    (when (.hasLimit ^Slice node)
                                      {:limit (.getLimit ^Slice node)}))))
             (.visitChildren ^QueryModelNode node this)))
         (.visit tuple-expr))
    @slice))

(defn- collect-order-by [^TupleExpr tuple-expr]
  (let [order-by (atom nil)]
    (->> (proxy [AbstractQueryModelVisitor] []
           (meetNode [node]
             (when (instance? Order node)
               (reset! order-by (mapv rdf/rdf->clj (.getElements ^Order node))))
             (.visitChildren ^QueryModelNode node this)))
         (.visit tuple-expr))
    @order-by))

(defn sparql->datalog
  ([sparql]
   (sparql->datalog sparql nil))
  ([sparql base-uri]
   (let [tuple-expr (.getTupleExpr (QueryParserUtil/parseQuery QueryLanguage/SPARQL sparql base-uri))
         args (collect-args tuple-expr)
         rules (collect-arbritrary-path-rules tuple-expr)
         slice (collect-slice tuple-expr)
         order-by (collect-order-by tuple-expr)]
     (cond-> {:find (mapv str->sparql-var (.getBindingNames tuple-expr))
              :where (rdf/use-default-language (rdf/rdf->clj tuple-expr) rdf/*default-language*)}
       args (assoc :args args)
       slice (merge slice)
       order-by (assoc :order-by order-by)
       rules (assoc :rules rules)))))
