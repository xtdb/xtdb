(ns crux.http-server
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as st]
            [clojure.tools.logging :as log]
            [crux.db :as db]
            [crux.doc :as doc]
            [crux.index :as idx]
            [crux.io :as cio]
            [crux.kafka :as k]
            [crux.rdf :as rdf]
            [crux.sparql :as sparql]
            [crux.tx :as tx]
            [crux.query :as q]
            [crux.kv-store :as ks]
            [ring.adapter.jetty :as j]
            [ring.middleware.params :as p]
            [ring.util.request :as req]
            [ring.util.io :as rio])
  (:import java.io.Closeable
           java.time.Duration
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
    (java.util.UUID/fromString s)

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
          (if (st/starts-with? (.getMessage e) "Invalid input")
            (exception-response 400 e) ;; Valid edn, invalid content
            (exception-response 500 e)))) ;; Valid content; something internal failed, or content validity is not properly checked
      (catch Exception e
        (exception-response 400 e))))) ;;Invalid edn

(defn zk-active? [bootstrap-servers]
  (try
    (with-open [^KafkaConsumer consumer
                (k/create-consumer
                 {"bootstrap.servers" bootstrap-servers
                  "default.api.timeout.ms" (int 1000)})]
      (boolean (.listTopics consumer)))
    (catch Exception e
      (log/debug e "Could not list Kafka topics:")
      false)))

;; ---------------------------------------------------
;; Services

(defn status-map [kv bootstrap-servers]
  {:crux.zk/zk-active? (zk-active? bootstrap-servers)
   :crux.kv-store/kv-backend (ks/kv-name kv)
   :crux.kv-store/estimate-num-keys (ks/count-keys kv)
   :crux.kv-store/size (some-> (ks/db-dir kv) (cio/folder-size))
   :crux.tx-log/tx-time (doc/read-meta kv :crux.tx-log/tx-time)})

(defn status [kv bootstrap-servers]
  (let [status-map (status-map kv bootstrap-servers)]
    (if (:crux.zk/zk-active? status-map)
      (success-response status-map)
      (response 500
                {"Content-Type" "application/edn"}
                (pr-str status-map)))))

(defn document [kv request]
  (let [object-store (doc/->DocObjectStore kv)
        content-hash (idx/new-id (param request "hash"))]
    (with-open [snapshot (ks/new-snapshot kv)]
      (success-response
       (get (db/get-objects object-store snapshot [content-hash]) content-hash)))))

;; param must be compatible with index/id->bytes (e.g. keyworded UUID)
(defn history [kv request]
  (let [entity (param request "entity")]
    (with-open [snapshot (ks/new-snapshot kv)]
      (success-response
       (doc/entity-history snapshot entity)))))

(defn- db-for-request [kv {:keys [business-time transact-time]}]
  (cond
    (and business-time transact-time)
    (q/db kv business-time transact-time)

    business-time
    (q/db kv business-time)

    :else
    (q/db kv)))

(defn query [kv request]
  (let [query-map (param request "q")]
    (success-response
     (q/q (db-for-request kv query-map)
          (dissoc query-map :business-time :transact-time)))))

(defn query-stream [kv request]
  (let [query-map (param request "q")]
    (let [snapshot (ks/new-snapshot kv)
          result (q/q (db-for-request kv query-map) snapshot
                      (dissoc query-map :business-time :transact-time))]
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
          (throw t))))))

(defn entity [kv request]
  (let [body (param request "entity")]
    (success-response
     (q/entity (db-for-request kv body)
               (:eid body)))))

(defn entity-tx [kv request]
  (let [body (param request "entity-tx")]
    (success-response
     (q/entity-tx (db-for-request kv body)
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
       (st/join (for [var vars]
                  (format "<variable name=\"%s\"/>" var)))
       "</head>"
       "<results>"
       (st/join (for [result results]
                  (str "<result>"
                       (st/join (for [[var value] (zipmap vars result)
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
  (str "{\"head\": {\"vars\": [" (st/join ", " (map (comp pr-str str) vars)) "]}, "
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
                        (st/join ", "))
                   "}"))
            (st/join ", " ))
       "]}}"))

;; https://www.w3.org/TR/2013/REC-sparql11-protocol-20130321/
(defn sparql-query [kv request]
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
          results (q/q (q/db kv) query-map)]
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

(defn transact [tx-log request]
  (let [tx-ops (param request)]
    (success-response
     @(db/submit-tx tx-log tx-ops))))

;; ---------------------------------------------------
;; Jetty server

(defn handler [kv tx-log bootstrap-servers request]
  (cond
    (check-path request ["/"] [:get])
    (status kv bootstrap-servers)

    (check-path request ["/document"] [:get :post])
    (document kv request)

    (check-path request ["/history"] [:get :post])
    (history kv request)

    (check-path request ["/query"] [:get :post])
    (query kv request)

    (check-path request ["/query-stream"] [:get :post])
    (query-stream kv request)

    (check-path request ["/entity"] [:get :post])
    (entity kv request)

    (check-path request ["/entity-tx"] [:get :post])
    (entity-tx kv request)

    (check-path request ["/tx-log"] [:post])
    (transact tx-log request)

    (check-path request ["/sparql/"] [:get :post])
    (sparql-query kv request)

    :default
    {:status 400
     :headers {"Content-Type" "text/plain"}
     :body "Unsupported method on this address."}))

(defn ^Closeable create-server
  ([kv tx-log bootstrap-servers port]
   (let [server (j/run-jetty (-> (partial handler kv tx-log bootstrap-servers)
                                 (p/wrap-params)
                                 (wrap-exception-handling))
                             {:port port
                              :join? false})]
     (log/info (str "HTTP server started on port: " port))
     (reify Closeable (close [_] (.stop server))))))
