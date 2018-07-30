(ns crux.rdf
  "Experiments around loading RDF triples into Crux.

  https://www.w3.org/TR/rdf11-primer/
  https://www.w3.org/TR/n-triples/
  https://www.w3.org/TR/json-ld/

  http://docs.rdf4j.org/"
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.walk :as w]
            [clojure.set :as set]
            [clojure.tools.logging :as log]
            [crux.db :as db])
  (:import [java.io StringReader]
           [java.net URLDecoder]
           [javax.xml.datatype DatatypeConstants]
           [org.eclipse.rdf4j.rio Rio RDFFormat]
           [org.eclipse.rdf4j.model BNode IRI Statement Literal Resource]
           [org.eclipse.rdf4j.model.datatypes XMLDatatypeUtil]
           [org.eclipse.rdf4j.model.vocabulary RDF XMLSchema]
           [org.eclipse.rdf4j.query QueryLanguage]
           [org.eclipse.rdf4j.query.parser QueryParserUtil]
           [org.eclipse.rdf4j.query.algebra
            Compare Difference Extension ExtensionElem Exists Filter FunctionCall Join LeftJoin
            MathExpr Not Projection Regex StatementPattern Union ValueConstant Var]))

;;; Main part, uses RDF4J classes to parse N-Triples.

;; NOTE: this shifts the parts of the RDF namespace after the first
;; slash into the Keyword name.
(defn iri->kw [^IRI iri]
  (keyword (URLDecoder/decode (str iri))))

(defn bnode->kw [^BNode bnode]
  (keyword "_" (.getID bnode)))

(def ^:dynamic *default-language* :en)

(defn lang->str [lang language]
  (let [language (if (contains? lang language)
                   language
                   (first (keys lang)))]
    (str (get lang language))))

(defrecord Lang []
  CharSequence
  (charAt [this index]
    (.charAt (str this) index))

  (length [this]
    (.length (str this)))

  (subSequence [this start end]
    (.subSequence (str this) start end))

  (toString [this]
    (lang->str this *default-language*)))

(defn literal->clj [^Literal literal]
  (let [dt (.getDatatype literal)
        language (when (.isPresent (.getLanguage literal))
                   (.get (.getLanguage literal)))]
    (cond
      (XMLDatatypeUtil/isCalendarDatatype dt)
      (.getTime (.toGregorianCalendar
                 (if (= DatatypeConstants/FIELD_UNDEFINED
                        (.getTimezone (.calendarValue literal)))
                   (doto (.calendarValue literal)
                     (.setTimezone 0))
                   (.calendarValue literal))))

      (XMLDatatypeUtil/isFloatingPointDatatype dt)
      (.doubleValue literal)

      (XMLDatatypeUtil/isIntegerDatatype dt)
      (.longValue literal)

      (XMLDatatypeUtil/isNumericDatatype dt)
      (.decimalValue literal)

      (= XMLSchema/BOOLEAN dt)
      (.booleanValue literal)

      (re-find #"^\d+$" (.getLabel literal))
      (.longValue literal)

      (re-find #"^\d+\.\d+$" (.getLabel literal))
      (.doubleValue literal)

      (= RDF/LANGSTRING dt)
      (map->Lang
       {(keyword language)
        (.stringValue literal)})

      :else
      (.stringValue literal))))

(defprotocol RDFToClojure
  (rdf->clj [_]))

(extend-protocol RDFToClojure
  Literal
  (rdf->clj [this]
    (literal->clj this))

  IRI
  (rdf->clj [this]
    (iri->kw this))

  BNode
  (rdf->clj [this]
    (bnode->kw this)))

(defn statement->clj [^Statement statement]
  [(rdf->clj (.getSubject statement))
   (rdf->clj (.getPredicate statement))
   (rdf->clj (.getObject statement))])

(defn use-default-language [rdf-map language]
  (w/postwalk (fn [x]
                (if (instance? Lang x)
                  (lang->str x language)
                  x)) rdf-map))

(defn entity-statements->map [entity-statements]
  (when-let [^Statement statement (first entity-statements)]
    (->> entity-statements
         (reduce
          (fn [m ^Statement statement]
            (try
              (let [p (rdf->clj (.getPredicate statement))
                    o (rdf->clj (.getObject statement))]
                (assoc! m p (let [x (get m p)]
                              (cond
                                (nil? x)
                                o

                                (set? x)
                                (conj x o)

                                :else #{x o}))))
              (catch IllegalArgumentException e
                (log/debug e "Could not turn RDF statement into Clojure:" statement)
                m)))
          (transient {:crux.db/id (rdf->clj (.getSubject statement))}))
         (persistent!))))

(defn statements->maps [statements]
  (->> statements
       (partition-by (fn [^Statement s]
                       (.getSubject s)))
       (map entity-statements->map)))

(defn maps-by-id [rdf-maps]
  (->> (for [m rdf-maps]
         {(:crux.db/id m) m})
       (into {})))

(def ^"[Lorg.eclipse.rdf4j.model.Resource;"
  empty-resource-array (make-array Resource 0))

(def missing-lang-string (format "^^<%s>" RDF/LANGSTRING))

(defn patch-missing-lang-string [s]
  (str/replace s missing-lang-string ""))

(defn parse-ntriples-str [s]
  (Rio/parse (StringReader. s)
             ""
             RDFFormat/NTRIPLES
             empty-resource-array))

(defn ntriples-seq [in]
  (->> (line-seq (io/reader in))
       (map patch-missing-lang-string)
       (partition-by #(re-find #"<.+?>" %))
       (partition-all 100)
       (map (fn [entity-lines]
              (let [lines (apply concat entity-lines)]
                (try
                  (parse-ntriples-str (str/join "\n" lines))
                  (catch Exception e
                    (->> (for [lines entity-lines]
                           (try
                             (log/debug e "Could not parse block of entities, parsing one by one.")
                             (parse-ntriples-str (str/join "\n" lines))
                             (catch Exception e
                               (log/debug e "Could not parse entity:" (str/join "\n" lines)))))
                         (apply concat)))))))
       (apply concat)))

(def ^:dynamic *ntriples-log-size* 100000)

(defn submit-ntriples [tx-log in tx-size]
  (->> (ntriples-seq in)
       (statements->maps)
       (map #(use-default-language % *default-language*))
       (partition-all tx-size)
       (reduce (fn [^long n entities]
                 (when (zero? (long (mod n *ntriples-log-size*)))
                   (log/debug "submitted" n))
                 (let [tx-ops (vec (for [entity entities]
                                     [:crux.tx/put (:crux.db/id entity) entity]))]
                   (db/submit-tx tx-log tx-ops))
                 (+ n (count entities)))
               0)))

(def ^:dynamic *default-prefixes* {:rdf "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                                   :rdfs "http://www.w3.org/2000/01/rdf-schema#"
                                   :xsd "http://www.w3.org/2001/XMLSchema#"
                                   :owl "http://www.w3.org/2002/07/owl#"})

(defn with-prefix
  ([x] (with-prefix {} x))
  ([prefixes x]
   (let [prefixes (merge *default-prefixes* prefixes)]
     (w/postwalk
      #(if-let [ns (and (keyword? %)
                        (get prefixes (keyword (namespace %))))]
         (keyword (str ns (name %)))
         %)
      x))))

;; SPARQL Spike
;; https://www.w3.org/TR/2013/REC-sparql11-query-20130321/

;; TODO: This is a spike transforming a small subset of SPARQL into
;; CRUX's Datalog dialect. Experimental.
(defn- str->sparql-var [s]
  (symbol (str "?" s)))

;; TODO: the returns are not consistent, needs revisiting.
(extend-protocol RDFToClojure
  Compare
  (rdf->clj [this]
    [[(list (symbol (.getSymbol (.getOperator this)))
            (rdf->clj (.getLeftArg this))
            (rdf->clj (.getRightArg this)))]])


  Difference
  (rdf->clj [this]
    (throw (UnsupportedOperationException. "MINUS not supported, use NOT EXISTS.")))

  Extension
  (rdf->clj [this]
    (vec (apply concat
                (rdf->clj (.getArg this))
                (for [arg (.getElements this)]
                  (rdf->clj arg)))))

  ExtensionElem
  (rdf->clj [this]
    (when-not (instance? Var (.getExpr this))
      [[(rdf->clj (.getExpr this))
        (str->sparql-var (.getName this))]]))

  Exists
  (rdf->clj [this]
    (rdf->clj (.getSubQuery this)))

  Filter
  (rdf->clj [this]
    (vec (concat (rdf->clj (.getArg this))
                 (rdf->clj (.getCondition this)))))

  FunctionCall
  (rdf->clj [this]
    (let [args (for [arg (.getArgs this)]
                 (rdf->clj arg))]
      (when (some list? args)
        (throw (UnsupportedOperationException. "Nested expressions are not supported.")))
      (->> (cons (symbol (.getURI this)) args)
           (apply list))))

  Join
  (rdf->clj [this]
    (vec (concat (rdf->clj (.getLeftArg this))
                 (rdf->clj (.getRightArg this)))))

  MathExpr
  (rdf->clj [this]
    (let [left (rdf->clj (.getLeftArg this))
          right (rdf->clj (.getRightArg this))]
      (when (some list? [left right])
        (throw (UnsupportedOperationException. "Nested expressions are not supported.")))
      (list (symbol (.getSymbol (.getOperator this)))
            left right)))

  LeftJoin
  (rdf->clj [this]
    (let [or-join-vars (->> (.getBindingNames this)
                            (remove #(re-find #"\??_" %))
                            (set))
          or-join-vars (set/intersection or-join-vars (set (.getBindingNames (.getRightArg this))))
          or-join-vars (mapv str->sparql-var or-join-vars)
          common-vars (set (mapv str->sparql-var (.getBindingNames (.getLeftArg this))))
          condition (some-> (.getCondition this) (rdf->clj))
          optional (rdf->clj (.getRightArg this))
          optional (cond-> optional
                     condition (concat condition))
          optional (if (> (count optional) 1)
                     [(cons 'and optional)]
                     optional)
          build-optional-clause (fn [var]
                                  (if (contains? common-vars var)
                                    [(list 'identity var)]
                                    [(list 'identity ::optional) var]))]
      (conj (rdf->clj (.getLeftArg this))
            (cons 'or-join (cons or-join-vars
                                 (concat
                                  optional
                                  (if (> (count or-join-vars) 1)
                                    [(apply list 'and (map build-optional-clause or-join-vars))]
                                    [(build-optional-clause (first or-join-vars))])))))))

  Not
  (rdf->clj [this]
    (when-not (and (instance? Exists (.getArg this))
                   (instance? Filter (.getParentNode this)))
      (throw (UnsupportedOperationException. "NOT only supported in FILTER NOT EXISTS.")))
    (let [not-vars (->> (.getAssuredBindingNames (.getSubQuery ^Exists (.getArg this)))
                        (remove #(re-find #"\??_" %))
                        (set))
          parent-vars (set (.getAssuredBindingNames ^Filter (.getParentNode this)))
          is-not? (set/subset? not-vars parent-vars)
          not-join-vars (set/intersection not-vars parent-vars)
          not (rdf->clj (.getArg this))]
      [(if is-not?
         (cons 'not not)
         (cons 'not-join (cons (mapv str->sparql-var not-join-vars) not)))]))

  Projection
  (rdf->clj [this]
    (rdf->clj (.getArg this)))

  Regex
  (rdf->clj [this]
    [[(list 're-find
            (let [flags (some-> (.getFlagsArg this) (rdf->clj))]
              (re-pattern (str (when (seq flags)
                                 (str "(?" flags ")"))
                               (rdf->clj (.getPatternArg this)))))
            (rdf->clj (.getArg this)))]])

  StatementPattern
  (rdf->clj [this]
    (let [s (.getSubjectVar this)
          p (.getPredicateVar this)
          o (.getObjectVar this)]
      (when-not (.hasValue p)
        (throw (UnsupportedOperationException. (str "Does not support variables in predicate position: " (rdf->clj p)))))
      [(mapv rdf->clj [s p o])]))

  Union
  (rdf->clj [this]
    (let [is-or? (= (set (.getAssuredBindingNames this))
                    (set (remove #(re-find #"\??_" %) (.getBindingNames this))))
          or-join-vars (mapv str->sparql-var (.getAssuredBindingNames this))
          or-left (rdf->clj (.getLeftArg this))
          or-left (if (> (count or-left) 1)
                    [(cons 'and or-left)]
                    or-left)
          or-right (rdf->clj (.getRightArg this))
          or-right (if (> (count or-right) 1)
                     [(cons 'and or-right)]
                     or-right)]
      [(if is-or?
         (cons 'or (concat or-left or-right))
         (cons 'or-join (cons or-join-vars (concat or-left or-right))))]))

  Var
  (rdf->clj [this]
    (if (.hasValue this)
      (rdf->clj (.getValue this))
      (if (.isAnonymous this)
        '_
        (str->sparql-var (.getName this)))))

  ValueConstant
  (rdf->clj [this]
    (rdf->clj (.getValue this))))

(defn sparql->datalog
  ([sparql]
   (sparql->datalog sparql nil))
  ([sparql base-uri]
   (let [tuple-expr (.getTupleExpr (QueryParserUtil/parseQuery QueryLanguage/SPARQL sparql base-uri))]
     {:find (mapv str->sparql-var (.getBindingNames tuple-expr))
      :where (use-default-language (rdf->clj tuple-expr) *default-language*)})))
