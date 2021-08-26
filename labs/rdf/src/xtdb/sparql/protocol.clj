(ns ^:no-doc xtdb.sparql.protocol
  "See https://www.w3.org/TR/2013/REC-sparql11-protocol-20130321/"
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [xtdb.rdf :as rdf]
            [xtdb.sparql :as sparql]
            [juxt.clojars-mirrors.ring-core.v1v9v2.ring.util.request :as req]
            [juxt.clojars-mirrors.ring-core.v1v9v2.ring.util.time :as rt]
            [crux.io :as cio]
            [crux.api :as xt])
  (:import [org.eclipse.rdf4j.model BNode IRI Literal]))

;; TODO: This is a bit ad-hoc.
(defn- edn->sparql-type+value+dt [x]
  (let [rdf-value (rdf/clj->rdf x)]
    (cond
      (instance? Literal rdf-value)
      ["literal" (.getLabel ^Literal rdf-value) (str (.getDatatype ^Literal rdf-value))]

      (instance? BNode rdf-value)
      ["bnode" (.getID ^BNode rdf-value)]

      (instance? IRI rdf-value)
      ["uri" (str rdf-value)])))

(defn- unbound-sparql-value? [x]
  (and (keyword? x)
       (= "xtdb.sparql" (namespace x))))

(defn- strip-qmark [x]
  (str/replace x #"^\?" ""))

(defn- sparql-xml-response [vars results]
  (str "<?xml version=\"1.0\"?>\n"
       "<sparql xmlns=\"http://www.w3.org/2005/sparql-results#\">"
       "<head>"
       (str/join (for [var vars]
                   (format "<variable name=\"%s\"/>" (strip-qmark var))))
       "</head>"
       "<results>"
       (str/join (for [result results]
                   (str "<result>"
                        (str/join (for [[var value] (zipmap vars result)
                                        :when (not (unbound-sparql-value? value))
                                        :let [[type value dt] (edn->sparql-type+value+dt value)]]
                                    (if dt
                                      (format "<binding name=\"%s\"><literal datatype=\"%s\">%s</literal></binding>"
                                              (strip-qmark var) dt value)
                                      (format "<binding name=\"%s\"><%s>%s</%s></binding>"
                                              (strip-qmark var) type value type))))
                        "</result>")))
       "</results>"
       "</sparql>"))

(defn- sparql-json-response [vars results]
  (str "{\"head\": {\"vars\": [" (str/join ", " (map (comp cio/pr-edn-str strip-qmark str) vars)) "]}, "
       "\"results\": { \"bindings\": ["
       (->> (for [result results]
              (str "{"
                   (->> (for [[var value] (zipmap vars result)
                              :when (not (unbound-sparql-value? value))
                              :let [[type value dt] (edn->sparql-type+value+dt value)]]
                          (if dt
                            (format "\"%s\": {\"type\": \"literal\", \"datatype\": \"%s\",  \"value:\": \"%s\"}"
                                    (strip-qmark var)
                                    dt
                                    value)
                            (format "\"%s\": {\"type\": \"%s\", \"value\": \"%s\"}"
                                    (strip-qmark var)
                                    type
                                    value)))
                        (str/join ", "))
                   "}"))
            (str/join ", "))
       "]}}"))

(defn sparql-query [crux-node request]
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
          db (xt/db crux-node)
          results (xt/q db query-map (object-array 0))]
      (log/debug :sparql query)
      (log/debug :sparql->datalog query-map)
      (cond
        (str/index-of accept "application/sparql-results+xml")
        {:status 200
         :headers {"Content-Type" "application/sparql-results+xml"
                   "Last-Modified" (rt/format-date (xt/transaction-time db))}
         :body (sparql-xml-response find results)}

        (str/index-of accept "application/sparql-results+json")
        {:status 200
         :headers {"Content-Type" "application/sparql-results+json"
                   "Last-Modified" (rt/format-date (xt/transaction-time db))}
         :body (sparql-json-response find results)}

        :else
        {:status 406
         :headers {"Content-Type" "text/plain"}}))
    {:status 400
     :headers {"Content-Type" "text/plain"}}))
