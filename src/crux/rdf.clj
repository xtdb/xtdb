(ns crux.rdf
  (:require [clojure.java.io :as io]
            [clojure.string :as str])
  (:import [java.io InputStream IOException StringReader]
           [java.net URLDecoder]
           [java.util.concurrent LinkedBlockingQueue]
           [org.eclipse.rdf4j.rio Rio RDFFormat RDFHandler]
           [org.eclipse.rdf4j.model BNode IRI Statement Literal Resource]
           [org.eclipse.rdf4j.model.datatypes XMLDatatypeUtil]
           [org.eclipse.rdf4j.model.vocabulary XMLSchema]))

(defn iri->kw [^IRI iri]
  (keyword (clojure.string/replace
            (.getNamespace iri)
            #"/$" "")
           (URLDecoder/decode (.getLocalName iri))))

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
  (->> (for [[subject statements] (group-by (fn [^Statement s]
                                              (.getSubject s))
                                            statements)
             statement statements
             :let [[s o p] (statement->clj statement)]]
         {s {:crux.kv/id s
             o p}})
       (apply merge-with merge)))

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

;; Download from http://wiki.dbpedia.org/services-resources/ontology
(comment
  (with-open [in (io/input-stream
                  (io/file "target/specific_mappingbased_properties_en.nt"))]
    (statements->maps (ntriples-seq in))))
