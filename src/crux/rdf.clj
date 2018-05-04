(ns crux.rdf
  (:require [clojure.java.io :as io])
  (:import [java.io InputStream IOException]
           [java.net URLDecoder]
           [java.util.concurrent LinkedBlockingQueue]
           [org.eclipse.rdf4j.rio Rio RDFFormat RDFHandler]
           [org.eclipse.rdf4j.model IRI Statement Literal Resource]
           [org.eclipse.rdf4j.model.datatypes XMLDatatypeUtil]
           [org.eclipse.rdf4j.model.vocabulary XMLSchema]))

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

(defn statement->clj [^Statement statement]
  [(iri->kw (.getSubject statement))
   (iri->kw (.getPredicate statement))
   (object->clj (.getObject statement))])

(defn model->maps [model]
  (->> (for [[subject statements] (group-by (fn [^Statement s]
                                              (.getSubject s))
                                            model)
             statement statements
             :let [[s o p] (statement->clj statement)]]
         {s {:crux.kv/id s
             o p}})
       (apply merge-with merge)))

(defn parse-ntriples [^InputStream in]
  (Rio/parse in "" RDFFormat/NTRIPLES
             ^"[Lorg.eclipse.rdf4j.model.Resource;" (make-array Resource 0)))

(defn parse-ntriples-callback [^InputStream in f]
  (-> (Rio/createParser RDFFormat/NTRIPLES)
      (.setRDFHandler (reify RDFHandler
                        (endRDF [_])
                        (startRDF [_])
                        (handleComment [_ _])
                        (handleNamespace [_ _ _])
                        (handleStatement [_ statement]
                          (f statement))))
      (.parse in "")))

(defn stream-closed? [^Throwable t]
  (= "Stream closed" (.getMessage ^Throwable t)))

(defn ntriples-seq [in]
  (let [queue (LinkedBlockingQueue. 32)]
    (future
      (try
        (parse-ntriples-callback in #(.put queue %))
        (catch Throwable t
          (.put queue t))
        (finally
          (.put queue false))))
    ((fn step []
       (when-let [element (.take queue)]
         (if (instance? Throwable element)
           (when-not (stream-closed? element)
             (throw element))
           (cons element (lazy-seq (step)))))))))

;; Download from http://wiki.dbpedia.org/services-resources/ontology
(comment
  (with-open [in (io/input-stream
                  (io/file "target/specific_mappingbased_properties_en.nt"))]
    (model->maps (parse-ntriples in))))
