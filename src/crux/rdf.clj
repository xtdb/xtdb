(ns crux.rdf
  (:require [clojure.java.io :as io]
            [clojure.string :as str])
  (:import [java.io StringReader]
           [java.net URLDecoder]
           [org.eclipse.rdf4j.rio Rio RDFFormat]
           [org.eclipse.rdf4j.model BNode IRI Statement Literal Resource]
           [org.eclipse.rdf4j.model.datatypes XMLDatatypeUtil]
           [org.eclipse.rdf4j.model.vocabulary XMLSchema]))

;; NOTE: this shifts the parts of the RDF namespace after the first
;; slash into the Keyword name.
(defn iri->kw [^IRI iri]
  (let [[_ kw-namespace kw-name] (re-find #"(.+?)/(.+)?" (.getNamespace iri))]
    (keyword kw-namespace
             (str kw-name (URLDecoder/decode (.getLocalName iri))))))

(defn bnode->kw [^BNode bnode]
  (keyword "_" (.getID bnode)))

(defn literal->clj [^Literal literal]
  (let [dt (.getDatatype literal)]
    (cond
      (XMLDatatypeUtil/isCalendarDatatype dt)
      (.getTime (.toGregorianCalendar (.calendarValue literal)))

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

(defn statements->maps [statements]
  (for [[subject statements] (group-by (fn [^Statement s]
                                         (.getSubject s))
                                       statements)]
    (reduce
     (fn [m statement]
       (let [[_ p o] (statement->clj statement)]
         (update m p (fn [x]
                       (cond
                         (nil? x)
                         o

                         (coll? x)
                         (conj x o)

                         :else #{x o})))))
     {:crux.kv/id (value->clj subject)}
     statements)))

(def jsonld-keyword->clj
  {(keyword "@id")
   :crux.kv/id
   (keyword "@type")
   (keyword "http:" "/www.w3.org/1999/02/22-rdf-syntax-ns#type")})

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
                              (= :crux.kv/id k) keyword)))))
               {} json-ld)))

(def ^"[Lorg.eclipse.rdf4j.model.Resource;"
  empty-resource-array (make-array Resource 0))

(defn ntriples-seq [in]
  (for [lines (partition-all 1024 (line-seq (io/reader in)))
        statement (Rio/parse (StringReader. (str/join "\n" lines))
                             ""
                             RDFFormat/NTRIPLES
                             empty-resource-array)]
    statement))

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
  (with-open [in (io/input-stream
                  (io/file "target/specific_mappingbased_properties_en.nt"))]
    (statements->maps (ntriples-seq in))))
