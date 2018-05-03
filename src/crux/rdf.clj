(ns crux.rdf
  (:require [clojure.java.io :as io])
  (:import [java.io InputStream]
           [java.net URLDecoder]
           [org.eclipse.rdf4j.rio Rio RDFFormat]
           [org.eclipse.rdf4j.model IRI Statement Literal Resource]
           [org.eclipse.rdf4j.model.datatypes XMLDatatypeUtil]
           [org.eclipse.rdf4j.model.vocabulary XMLSchema]))

;; TODO: All this needs to use event based API for real input sets.
(defn parse-ntriples [^InputStream in]
  (Rio/parse in "" RDFFormat/NTRIPLES
             ^"[Lorg.eclipse.rdf4j.model.Resource;" (make-array Resource 0)))

(defn iri->kw [^IRI iri]
  (keyword (clojure.string/replace
            (.getNamespace iri)
            #"/$" "")
           (URLDecoder/decode (.getLocalName iri))))

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

(defn object->clj [object]
  (if (instance? IRI object)
    (iri->kw object)
    (literal->clj object)))

(defn model->maps [model]
  (->> (for [[subject statements] (group-by (fn [^Statement s]
                                              (.getSubject s))
                                            model)
             ^Statement statement (seq statements)
             :let [iri (iri->kw subject)]]
         {iri
          {:crux.kv/id iri
           (iri->kw (.getPredicate statement))
           (object->clj (.getObject statement))}})
       (apply merge-with merge)))

;; Download from http://wiki.dbpedia.org/services-resources/ontology
(comment
  (with-open [in (io/input-stream
                  (io/file "target/specific_mappingbased_properties_en.nt"))]
    (model->maps (parse-ntriples in))))
