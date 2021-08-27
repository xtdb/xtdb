(ns ^:no-doc xtdb.rdf
  "Helpers for loading RDF triples into Crux.

  https://www.w3.org/TR/rdf11-primer/
  https://www.w3.org/TR/n-triples/

  See http://docs.rdf4j.org/"
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.walk :as w]
            [clojure.tools.logging :as log]
            [xtdb.codec :as c]
            [xtdb.api :as xt])
  (:import java.io.StringReader
           javax.xml.datatype.DatatypeConstants
           [org.eclipse.rdf4j.rio RDFHandler]
           [org.eclipse.rdf4j.rio.ntriples NTriplesParserFactory NTriplesUtil]
           [org.eclipse.rdf4j.model BNode IRI Statement Literal Resource Value]
           org.eclipse.rdf4j.model.datatypes.XMLDatatypeUtil
           [org.eclipse.rdf4j.model.vocabulary RDF XMLSchema]
           org.eclipse.rdf4j.model.util.Literals
           org.eclipse.rdf4j.model.impl.SimpleValueFactory))

(def crux-unqualified-iri-prefix "http://juxt.pro/crux/unqualified/")

;; NOTE: this shifts the parts of the RDF namespace after the first
;; slash into the Keyword name.
(defn iri->kw [^IRI iri]
  (let [iri (NTriplesUtil/unescapeString (str iri))]
    (if (str/starts-with? iri crux-unqualified-iri-prefix)
      (keyword (str/replace (str iri) crux-unqualified-iri-prefix ""))
      (keyword iri))))

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

      (= RDF/LANGSTRING dt)
      (map->Lang
       {(keyword language)
        (.stringValue literal)})

      (re-find #"^\d+$" (.getLabel literal))
      (.longValue literal)

      (re-find #"^\d+\.\d+$" (.getLabel literal))
      (.doubleValue literal)

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
    (bnode->kw this))

  Statement
  (rdf->clj [this]
    [(rdf->clj (.getSubject this))
     (rdf->clj (.getPredicate this))
     (rdf->clj (.getObject this))]))

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
                (log/warn "Could not turn RDF statement into Clojure:" statement)
                m)))
          (transient {:xt/id (rdf->clj (.getSubject statement))}))
         (persistent!))))

(defn statements->maps [statements]
  (->> statements
       (partition-by (fn [^Statement s]
                       (.getSubject s)))
       (map entity-statements->map)))

(def ^"[Lorg.eclipse.rdf4j.model.Resource;"
  empty-resource-array (make-array Resource 0))

(def missing-lang-string (format "^^<%s>" RDF/LANGSTRING))

(defn patch-missing-lang-string [s]
  (str/replace s missing-lang-string ""))

;; TODO: Investigate:
;; getParserConfig().addNonFatalError(NTriplesParserSettings.FAIL_ON_NTRIPLES_INVALID_LINES);
;; getParserConfig().addNonFatalError(BasicParserSettings.VERIFY_URI_SYNTAX);
(defn parse-ntriples-str [s]
  (let [parser (.getParser (NTriplesParserFactory.))
        statements (atom [])]
    (.setRDFHandler parser (reify RDFHandler
                             (startRDF [_])
                             (endRDF [_])
                             (handleComment [_ _])
                             (handleNamespace [_ ns _])
                             (handleStatement [_ s]
                               (swap! statements conj s))))
    (.parse parser (StringReader. s) "")
    @statements))

(defn ntriples-seq [in]
  (->> (line-seq (io/reader in))
       (map patch-missing-lang-string)
       (partition-by (fn [^String line]
                       (if-let [idx (or (str/index-of line \space)
                                        (str/index-of line \tab))]
                         (subs line 0 (inc idx))
                         line)))
       (partition-all 100)
       (pmap (fn [entity-lines]
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

(defn submit-ntriples [node in tx-size]
  (->> (ntriples-seq in)
       (statements->maps)
       (map #(use-default-language % *default-language*))
       (partition-all tx-size)
       (reduce (fn [{:keys [^long entity-count last-tx]} entities]
                 (when (zero? (long (mod entity-count *ntriples-log-size*)))
                   (log/debug "submitted" entity-count))
                 (let [tx-ops (vec (for [entity entities]
                                     [::xt/put entity]))]
                   {:entity-count (+ entity-count (count entities))
                    :last-tx (xt/submit-tx node tx-ops)}))

               {:entity-count 0})))

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

(defn clj->rdf ^org.eclipse.rdf4j.model.Value [x]
  (let [factory (SimpleValueFactory/getInstance)]
    (if (c/valid-id? x)
      (if (and (keyword? x) (= "_" (namespace x)))
        (.createBNode factory (name x))
        (.createIRI factory (if (keyword? x)
                              (cond->> (subs (str x) 1)
                                (nil? (namespace x)) (str crux-unqualified-iri-prefix))
                              (str x))))
      (Literals/createLiteral factory x))))

(defn parse-ntriple-line ^org.eclipse.rdf4j.model.Statement [^String line]
  (let [factory (SimpleValueFactory/getInstance)
        len (.length line)
        s-end (min (or (str/index-of line \space) len)
                   (or (str/index-of line \tab) len))
        s (NTriplesUtil/parseResource (subs line 0 s-end) factory)
        p-start (loop [i (int (inc s-end))]
                  (if (Character/isWhitespace (.charAt line i))
                    (recur (inc i))
                    i))
        p-end (min (or (str/index-of line \space p-start) len)
                   (or (str/index-of line \tab p-start) len))
        p (NTriplesUtil/parseURI (subs line p-start p-end) factory)
        o-start (inc p-end)
        o-end (or (str/last-index-of line \.) len)
        o (NTriplesUtil/parseValue (str/trimr (subs line o-start o-end)) factory)]
    (.createStatement factory ^Resource s ^IRI p ^Value o)))

;; Usefor for testing:

(defn ntriples [resource]
  (with-open [in (io/input-stream (io/resource resource))]
    (doall
     (->> (ntriples-seq in)
          (statements->maps)))))

(defn with-ntriples [res f]
  (with-open [in (io/input-stream res)]
    (f (statements->maps (ntriples-seq in)))))

(defn ->tx-op [entity]
  [::xt/put entity])

(defn ->tx-ops [ntriples]
  (vec (for [entity ntriples]
         [::xt/put entity])))

(defn ->default-language [c]
  (mapv #(use-default-language % :en) c))

(defn ->maps-by-id [rdf-maps]
  (->> (for [m rdf-maps]
         {(:xt/id m) m})
       (into {})))
