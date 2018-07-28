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
            MathExpr Not Projection ProjectionElem ProjectionElemList
            Regex QueryModelVisitor StatementPattern Union ValueConstant Var]))

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

;; Could be fronted by this protocol in the REST API:
;; https://www.w3.org/TR/2013/REC-sparql11-protocol-20130321/

(def ^:private ^:dynamic *find* nil)
(def ^:private ^:dynamic *where* nil)

(defn- str->sparql-var [s]
  (symbol (str "?" s)))

;; TODO: There's some confusion about the responsibility between
;; QueryModelVisitor and RDFToClojure. Might be possible to remove the
;; visitor and only use the protocol.
(def ^:private sparql->datalog-visitor
  (reify QueryModelVisitor
    (^void meet [_ ^Compare c]
     (swap! *where* conj (rdf->clj c)))

    (^void meet [_ ^Difference c]
     (throw (UnsupportedOperationException. "MINUS not supported, use NOT EXISTS.")))

    (^void meet [this ^Extension e]
     (.visitChildren e this))

    (^void meet [this ^ExtensionElem ee]
     (when-not (instance? Var (.getExpr ee))
       (swap! *where* conj (rdf->clj ee))))

    (^void meet [this ^Exists e]
     (.visitChildren e this))

    (^void meet [this ^Filter f]
     (.visitChildren f this))

    (^void meet [this ^FunctionCall f]
     (swap! *where* conj (rdf->clj f)))

    (^void meet [this ^LeftJoin lj]
     (throw (UnsupportedOperationException. "OPTIONAL not supported.")))

    (^void meet [this ^Not n]
     (swap! *where* conj (rdf->clj n)))

    (^void meet [this ^ProjectionElemList pel]
     (.visitChildren pel this))

    (^void meet [_ ^ProjectionElem pe]
     (swap! *find* conj (rdf->clj pe)))

    (^void meet [this ^Join j]
     (.visit (.getLeftArg j) this)
     (.visit (.getRightArg j) this))

    (^void meet [_ ^Regex r]
     (swap! *where* conj (rdf->clj r)))

    (^void meet [_ ^StatementPattern sp]
     (swap! *where* conj (rdf->clj sp)))

    (^void meet [_ ^Union u]
     (swap! *where* conj (rdf->clj u)))))

;; TODO: the returns are not consistent, Join returns a vector of
;; vector for example.
(extend-protocol RDFToClojure
  Compare
  (rdf->clj [this]
    [(list (symbol (.getSymbol (.getOperator this)))
           (rdf->clj (.getLeftArg this))
           (rdf->clj (.getRightArg this)))])

  ExtensionElem
  (rdf->clj [this]
    (conj (rdf->clj (.getExpr this))
          (str->sparql-var (.getName this))))

  Exists
  (rdf->clj [this]
    (rdf->clj (.getSubQuery this)))

  FunctionCall
  (rdf->clj [this]
    [(apply
      list
      (cons (symbol (.getURI this))
            (for [arg (.getArgs this)]
              (rdf->clj arg))))])

  Join
  (rdf->clj [this]
    [(rdf->clj (.getLeftArg this))
     (rdf->clj (.getRightArg this))])

  MathExpr
  (rdf->clj [this]
    (when (or (instance? MathExpr (.getLeftArg this))
              (instance? MathExpr (.getRightArg this)))
      (throw (UnsupportedOperationException. "Nested mathematical expressions are not supported.")))
    [(list (symbol (.getSymbol (.getOperator this)))
           (rdf->clj (.getLeftArg this))
           (rdf->clj (.getRightArg this)))])

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
      (if is-not?
        (list 'not not)
        (list 'not-join (mapv str->sparql-var not-join-vars) not))))

  ProjectionElem
  (rdf->clj [this]
    (str->sparql-var (.getSourceName this)))

  Regex
  (rdf->clj [this]
    [(list 're-find
           (let [flags (some-> (.getFlagsArg this) (rdf->clj))]
             (re-pattern (str (when (seq flags)
                                (str "(?" flags ")"))
                              (rdf->clj (.getPatternArg this)))))
           (rdf->clj (.getArg this)))])

  StatementPattern
  (rdf->clj [this]
    (let [s (.getSubjectVar this)
          p (.getPredicateVar this)
          o (.getObjectVar this)]
      (when-not (.hasValue p)
        (throw (UnsupportedOperationException. (str "Does not support variables in predicate position: " (rdf->clj p)))))
      (mapv rdf->clj [s p o])))

  Union
  (rdf->clj [this]
    (let [is-or? (= (set (.getAssuredBindingNames this))
                    (set (remove #(re-find #"\??_" %) (.getBindingNames this))))
          or-join-vars (mapv str->sparql-var (.getAssuredBindingNames this))
          or-left (rdf->clj (.getLeftArg this))
          or-left (if (every? vector? or-left)
                    (cons 'and or-left)
                    or-left)
          or-right (rdf->clj (.getRightArg this))
          or-right (if (every? vector? or-right)
                     (cons 'and or-right)
                     or-right)]
      (if is-or?
        (list 'or or-left or-right)
        (list 'or-join or-join-vars or-left or-right))))

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

(defn sparql->datalog [sparql]
  (let [tuple-expr (.getTupleExpr (QueryParserUtil/parseQuery QueryLanguage/SPARQL sparql nil))]
    (binding [*where* (atom [])
              *find* (atom [])]
      (.visitChildren tuple-expr sparql->datalog-visitor)
      {:find @*find*
       :where (use-default-language @*where* *default-language*)})))
