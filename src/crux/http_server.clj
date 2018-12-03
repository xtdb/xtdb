(ns crux.http-server
  "HTTP API for Crux.

  Requires ring/ring-core, ring/ring-jetty-adapter,
  org.apache.kafka/kafka-clients and
  org.eclipse.rdf4j/rdf4j-queryparser-sparql on the classpath"
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [crux.api :as api]
            [crux.rdf :as rdf]
            [crux.sparql :as sparql]
            [ring.adapter.jetty :as j]
            [ring.middleware.params :as p]
            [ring.util.request :as req]
            [ring.util.io :as rio])
  (:import java.io.Closeable
           java.time.Duration
           java.util.UUID
           org.apache.kafka.clients.consumer.KafkaConsumer
           [org.eclipse.rdf4j.model BNode IRI Literal]))

;; ---------------------------------------------------
;; Utils

(defn uuid-str? [s]
  (re-matches #"[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$" s))

(defn sha1-20-str? [s]
  (re-matches #"[a-f0-9]{20}" s))

(defn read-unknown [s]
  (cond
    (uuid-str? s)
    (UUID/fromString s)

    (sha1-20-str? s)
    s

    (try (edn/read-string s)
         (catch Exception e false))
    (edn/read-string s)

    :default
    (keyword s)))

(defn param
  ([request]
   (param request nil))
  ([request param-name]
   (case (:request-method request)
     :get (-> request
              :query-params
              (get param-name)
              read-unknown)
     :post (-> request
               req/body-string
               edn/read-string))))

(defn check-path [request valid-paths valid-methods]
  (let [path (req/path-info request)
        method (:request-method request)]
    (and (some #{path} valid-paths)
         (some #{method} valid-methods))))

(defn response
  ([status headers body]
   {:status status
    :headers headers
    :body body}))

(defn success-response [m]
  (response 200
            {"Content-Type" "application/edn"}
            (pr-str m)))

(defn exception-response [status ^Exception e]
  (response status
            {"Content-Type" "text/plain"}
            (str
             (.getMessage e) "\n"
             (ex-data e))))

(defn wrap-exception-handling [handler]
  (fn [request]
    (try
      (try
        (handler request)
        (catch Exception e
          (if (str/starts-with? (.getMessage e) "Invalid input")
            (exception-response 400 e) ;; Valid edn, invalid content
            (exception-response 500 e)))) ;; Valid content; something internal failed, or content validity is not properly checked
      (catch Exception e
        (exception-response 400 e))))) ;;Invalid edn

;; ---------------------------------------------------
;; Services

(defn status [local-node]
  (let [status-map (api/status local-node)]
    (if (:crux.zk/zk-active? status-map)
      (success-response status-map)
      (response 500
                {"Content-Type" "application/edn"}
                (pr-str status-map)))))

(defn document [local-node request]
  (let [content-hash (param request "hash")]
    (success-response
     (api/document local-node content-hash))))

;; param must be compatible with index/id->bytes (e.g. keyworded UUID)
(defn history [local-node request]
  (let [entity (param request "entity")]
    (success-response
     (api/history local-node entity))))

(defn- db-for-request [local-node {:keys [business-time transact-time]}]
  (cond
    (and business-time transact-time)
    (api/db local-node business-time transact-time)

    business-time
    (api/db local-node business-time)

    :else
    (api/db local-node)))

(defn query [local-node request]
  (let [query-map (param request "q")]
    (success-response
     (api/q (db-for-request local-node query-map)
            (dissoc query-map :business-time :transact-time)))))

(defn query-stream [local-node request]
  (let [query-map (param request "q")
        db (db-for-request local-node query-map)
        snapshot (api/new-snapshot db)
        result (api/q db snapshot (dissoc query-map :business-time :transact-time))]
    (try
      (response 200
                {"Content-Type" "application/edn"}
                (rio/piped-input-stream
                 (fn [out]
                   (with-open [out (io/writer out)
                               snapshot snapshot]
                     (.write out "(")
                     (doseq [tuple result]
                       (.write out (pr-str tuple)))
                     (.write out ")")))))
      (catch Throwable t
        (.close snapshot)
        (throw t)))))

(defn entity [local-node request]
  (let [body (param request "entity")]
    (success-response
     (api/entity (db-for-request local-node body)
                 (:eid body)))))

(defn entity-tx [local-node request]
  (let [body (param request "entity-tx")]
    (success-response
     (api/entity-tx (db-for-request local-node body)
                    (:eid body)))))

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
(defn sparql-query [local-node request]
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
          results (api/q (api/db local-node) query-map)]
      (log/debug :sparql query)
      (log/debug :sparql->datalog query-map)
      (cond (= "application/sparql-results+xml" accept)
            (response
             200
             {"Content-Type" accept}
             (sparql-xml-response find results))

            (= "application/sparql-results+json" accept)
            (response
             200
             {"Content-Type" accept}
             (sparql-json-response find results))

            :else
            (response 406 {"Content-Type" "text/plain"} nil)))
    (response 400 {"Content-Type" "text/plain"} nil)))

(defn transact [local-node request]
  (let [tx-ops (param request)]
    (success-response
     (api/submit-tx local-node tx-ops))))

;; ---------------------------------------------------
;; Jetty server

(defn handler [local-node request]
  (cond
    (check-path request ["/"] [:get])
    (status local-node)

    (check-path request ["/document"] [:get :post])
    (document local-node request)

    (check-path request ["/history"] [:get :post])
    (history local-node request)

    (check-path request ["/query"] [:get :post])
    (query local-node request)

    (check-path request ["/query-stream"] [:get :post])
    (query-stream local-node request)

    (check-path request ["/entity"] [:get :post])
    (entity local-node request)

    (check-path request ["/entity-tx"] [:get :post])
    (entity-tx local-node request)

    (check-path request ["/tx-log"] [:post])
    (transact local-node request)

    (check-path request ["/sparql/"] [:get :post])
    (sparql-query local-node request)

    :default
    {:status 400
     :headers {"Content-Type" "text/plain"}
     :body "Unsupported method on this address."}))

(defn ^Closeable start-server
  ([kv tx-log {:keys [server-port]
               :as options}]
   (let [local-node (api/->LocalNode (promise)
                                     options
                                     kv
                                     tx-log)
         server (j/run-jetty (-> (partial handler local-node)
                                 (p/wrap-params)
                                 (wrap-exception-handling))
                             {:port server-port
                              :join? false})]
     (log/info "HTTP server started on port: " server-port)
     (reify Closeable (close [_]
                        (.stop server))))))
