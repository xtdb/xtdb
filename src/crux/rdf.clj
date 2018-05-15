(ns crux.rdf
  "Experiments around loading RDF triples into Crux.

  https://www.w3.org/TR/rdf11-primer/
  https://www.w3.org/TR/n-triples/
  https://www.w3.org/TR/json-ld/

  http://docs.rdf4j.org/"
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.walk :as w]
            [clojure.tools.logging :as log])
  (:import [java.io StringReader]
           [java.net URLDecoder]
           [javax.xml.datatype DatatypeConstants]
           [org.eclipse.rdf4j.rio Rio RDFFormat]
           [org.eclipse.rdf4j.model BNode IRI Statement Literal Resource]
           [org.eclipse.rdf4j.model.datatypes XMLDatatypeUtil]
           [org.eclipse.rdf4j.model.vocabulary RDF XMLSchema]))

;;; Main part, uses RDF4J classes to parse N-Triples.

;; NOTE: this shifts the parts of the RDF namespace after the first
;; slash into the Keyword name.
(defn iri->kw [^IRI iri]
  (let [[_ kw-namespace kw-name] (re-find #"(.+?)/(.+)?" (.getNamespace iri))]
    (keyword kw-namespace
             (str kw-name (URLDecoder/decode (.getLocalName iri))))))

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

      (XMLDatatypeUtil/isDecimalDatatype dt)
      (.decimalValue literal)

      (XMLDatatypeUtil/isFloatingPointDatatype dt)
      (.doubleValue literal)

      (XMLDatatypeUtil/isIntegerDatatype dt)
      (.longValue literal)

      (= XMLSchema/BOOLEAN dt)
      (.booleanValue literal)

      (re-find #"^\d+$" (.getLabel literal))
      (.longValue literal)

      (re-find #"^\d+\.\d+$" (.getLabel literal))
      (.doubleValue literal)

      (= RDF/LANGSTRING dt)
      (map->Lang
       {(keyword language)
        (.getLabel literal)})

      :else
      (.getLabel literal))))

(defn value->clj [value]
  (cond
    (instance? BNode value)
    (bnode->kw value)

    (instance? IRI value)
    (iri->kw value)

    :else
    (literal->clj value)))

(defn statement->clj [^Statement statement]
  [(value->clj (.getSubject statement))
   (iri->kw (.getPredicate statement))
   (value->clj (.getObject statement))])

(defn use-default-language [rdf-map language]
  (w/postwalk (fn [x]
                (if (instance? Lang x)
                  (lang->str x language)
                  x)) rdf-map))

(defn entity-statements->map [entity-statements]
  (when-let [^Statement statement (first entity-statements)]
    (reduce
     (fn [m statement]
       (try
         (let [[_ p o] (statement->clj statement)]
           (update m p (fn [x]
                         (cond
                           (nil? x)
                           o

                           (coll? x)
                           (conj x o)

                           :else #{x o}))))
         (catch IllegalArgumentException e
           (log/debug "Could not turn RDF statement into Clojure:" statement (str e))
           m)))
     {:crux.rdf/iri (value->clj (.getSubject statement))}
     entity-statements)))

(defn statements->maps [statements]
  (->> statements
       (partition-by (fn [^Statement s]
                       (.getSubject s)))
       (map entity-statements->map)))

(defn maps-by-iri [rdf-maps]
  (->> (for [m rdf-maps]
         {(:crux.rdf/iri m) m})
       (into {})))

;; JSON-LD, does not depend on RDF4J, initial parsing via normal JSON
;; parser.

(def jsonld-keyword->clj
  {(keyword "@id")
   :crux.rdf/iri
   (keyword "@type")
   (keyword (str RDF/TYPE))})

(defn jsonld->maps [json-ld]
  (for [json-ld json-ld]
    (reduce-kv (fn [m k v]
                 (let [k (get jsonld-keyword->clj k k)]
                   (assoc m
                          k
                          (if (coll? v)
                            (let [v (for [v v]
                                      (get v (keyword "@value")
                                           (keyword (get v (keyword "@id") v))))]
                              (cond-> (set v)
                                (= 1 (count v)) first))
                            (cond-> v
                              (= :crux.rdf/iri k) keyword)))))
               {} json-ld)))

(def ^"[Lorg.eclipse.rdf4j.model.Resource;"
  empty-resource-array (make-array Resource 0))

(defn patch-missing-lang-string [s]
  (str/replace s
               (format "^^<%s>" RDF/LANGSTRING)
               ""))

(defn ntriples-seq [in]
  (for [lines (->> (line-seq (io/reader in))
                   (map patch-missing-lang-string)
                   (partition-all 1024))
        statement (Rio/parse (StringReader. (str/join "\n" lines))
                             ""
                             RDFFormat/NTRIPLES
                             empty-resource-array)]
    statement))

;;; Regexp-based N-Triples parser.

(def ntriplet-pattern
  #"^(?<subject>.+?)\s*(?<predicate><.+?>)\s*(?<object>.+?)(\^\^(?<datatype><.+?>))?\s+\..*$")

(defn parse-ntriplet [line]
  (let [m (re-matcher ntriplet-pattern line)]
    (when (.find m)
      {:subject (.group m "subject")
       :predicate (.group m "predicate")
       :object (.group m "object")
       :datatype (.group m "datatype")})))

(comment
  (with-open [in (io/input-stream
                  (io/resource "crux/example-data-artists.nt"))]
    (statements->maps (ntriples-seq in)))

  ;; Download from http://wiki.dbpedia.org/services-resources/ontology
  ;; mappingbased_properties_en.nt is the main data.
  ;; instance_types_en.nt contains type definitions only.
  ;; specific_mappingbased_properties_en.nt contains extra literals.
  ;; dbpedia_2014.owl is the OWL schema, not dealt with.
  (with-open [in (io/input-stream
                  (io/file "../dbpedia/mappingbased_properties_en.nt"))]
            (time (reduce (fn [^long x ^long y]
                            (prn x)
                            (+ x y)) (map count (partition-all 10240 (statements->maps (ntriples-seq in))))))))
