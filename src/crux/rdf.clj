(ns crux.rdf
  "Experiments around loading RDF triples into Crux.

  https://www.w3.org/TR/rdf11-primer/
  https://www.w3.org/TR/n-triples/
  https://www.w3.org/TR/json-ld/

  http://docs.rdf4j.org/"
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.walk :as w]
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
            Compare Filter Join Projection ProjectionElem ProjectionElemList
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

;; TODO: This is a spike transforming a small subset of SPARQL into
;; CRUX's Datalog dialect. Experimental.

(def ^:private ^:dynamic *find* nil)
(def ^:private ^:dynamic *where* nil)

(defn- str->sparql-var [s]
  (symbol (str "?" s)))

(defn- sparql->clj [v]
  (cond
    (instance? Var v)
    (if (.hasValue ^Var v)
      (rdf->clj (.getValue ^Var v))
      (if (.isAnonymous ^Var v)
        '_
        (str->sparql-var (.getName ^Var v))))
    (instance? ValueConstant v)
    (rdf->clj (.getValue ^ValueConstant v))))

(defn parse-sparql [sparql]
  (let [tuple-expr (.getTupleExpr (QueryParserUtil/parseQuery QueryLanguage/SPARQL sparql nil))]
    (binding [*where* (atom [])
              *find* (atom [])]
      (.visitChildren
       tuple-expr
       (reify QueryModelVisitor
         (^void meet [this ^Compare c]
          (swap! *where* conj
                 [(list (symbol (.getSymbol (.getOperator c)))
                        (sparql->clj (.getLeftArg c))
                        (sparql->clj (.getRightArg c)))]))

         (^void meet [this ^Filter f]
          (.visitChildren f this))

         (^void meet [this ^ProjectionElemList pel]
          (.visitChildren pel this))

         (^void meet [_ ^ProjectionElem pe]
          (swap! *find* conj (str->sparql-var (.getSourceName pe))))

         (^void meet [this ^Join j]
          (.visit (.getLeftArg j) this)
          (.visit (.getRightArg j) this))

         (^void meet [this ^Regex r]
          (swap! *where* conj
                 [(list 're-find
                        (let [flags (sparql->clj (.getFlagsArg r))]
                          (re-pattern (str (when (seq flags)
                                             (str "(?" flags ")"))
                                           (sparql->clj (.getPatternArg r)))))
                        (sparql->clj (.getArg r)))]))

         (^void meet [_ ^StatementPattern sp]
          (let [s (.getSubjectVar sp)
                p (.getPredicateVar sp)
                o (.getObjectVar sp)]
            (swap! *where* conj (mapv sparql->clj [s p o]))))

         (^void meet [this ^Union u]
          (let [or (atom [])]
            (binding [*where* or]
              (.visit (.getLeftArg u) this)
              (.visit (.getRightArg u) this))
            (swap! *where* conj (apply list (cons 'or @or)))))))
      {:find @*find*
       :where @*where*})))
