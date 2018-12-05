(ns crux.sparql.protocol
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [crux.api :as api]
            [crux.rdf :as rdf]
            [crux.sparql :as sparql]
            [ring.util.request :as req])
  (:import [org.eclipse.rdf4j.model BNode IRI Literal]
           crux.api.Crux$CruxSystem))


;; TODO: This is a bit ad-hoc.
(defn- edn->sparql-type+value+dt [x]
  (let [rdf-value (rdf/clj->rdf x)]
    (cond
      (instance? Literal rdf-value)
      ["literal" (.getLabel ^Literal rdf-value) (str (.getDatatype ^Literal rdf-value))]

      (instance? BNode rdf-value)
      ["bnode" (.getID ^BNode rdf-value)]

      (instance? IRI rdf-value)
      ["iri" (str rdf-value)])))

(defn- unbound-sparql-value? [x]
  (and (keyword? x) (= "crux.sparql" (namespace x))))

(defn- sparql-xml-response [vars results]
  (str "<?xml version=\"1.0\"?>\n"
       "<sparql xmlns=\"http://www.w3.org/2005/sparql-results#\">"
       "<head>"
       (str/join (for [var vars]
                   (format "<variable name=\"%s\"/>" var)))
       "</head>"
       "<results>"
       (str/join (for [result results]
                   (str "<result>"
                        (str/join (for [[var value] (zipmap vars result)
                                        :when (not (unbound-sparql-value? value))
                                        :let [[type value dt] (edn->sparql-type+value+dt value)]]
                                    (if dt
                                      (format "<binding name=\"%s\"/><literal datatype=\"%s\">%s</literal></binding>"
                                              var dt value)
                                      (format "<binding name=\"%s\"/><%s>%s</%s></binding>"
                                              var type value type))))
                        "</result>")))
       "</results>"
       "</sparql>"))

(defn- sparql-json-response [vars results]
  (str "{\"head\": {\"vars\": [" (str/join ", " (map (comp pr-str str) vars)) "]}, "
       "\"results\": { \"bindings\": ["
       (->> (for [result results]
              (str "{"
                   (->> (for [[var value] (zipmap vars result)
                              :when (not (unbound-sparql-value? value))
                              :let [[type value dt] (edn->sparql-type+value+dt value)]]
                          (if dt
                            (format "\"%s\": {\"type\": \"literal\", \"datatype\": \"%s\",  \"value:\": \"%s\"}"
                                    var
                                    dt
                                    value)
                            (format "\"%s\": {\"type\": \"%s\", \"value:\": \"%s\"}"
                                    var
                                    type
                                    value)))
                        (str/join ", "))
                   "}"))
            (str/join ", " ))
       "]}}"))

;; https://www.w3.org/TR/2013/REC-sparql11-protocol-20130321/
(defn sparql-query [^Crux$CruxSystem local-node request]
  (if-let [query (case (:request-method request)
                   :get
                   (get-in request [:query-params "query"])

                   :post
                   (or (get-in request [:form-params "query"])
                       (when (= "application/sparql-query"
                                (get-in request [:headers "content-type"]))
                         (slurp (:body request) (or (req/character-encoding request) "UTF-8")))))]
    (let [accept (get-in request [:headers "accept"])
          accept (if (= "*/*" accept)
                   "application/sparql-results+xml"
                   accept)
          {:keys [find] :as query-map} (sparql/sparql->datalog query)
          results (.q (.db local-node) query-map)]
      (log/debug :sparql query)
      (log/debug :sparql->datalog query-map)
      (cond (= "application/sparql-results+xml" accept)
            {:status 200
             :headers {"Content-Type" accept}
             :body (sparql-xml-response find results)}

            (= "application/sparql-results+json" accept)
            {:status 200
             :headers {"Content-Type" accept}
             :body (sparql-json-response find results)}

            :else
            {:status 406
             :headers {"Content-Type" "text/plain"}}))
    {:status 400
     :headers {"Content-Type" "text/plain"}}))
