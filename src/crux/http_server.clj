(ns crux.http-server
  (:require [clojure.edn :as edn]
            [clojure.string :as st]
            [clojure.tools.logging :as log]
            [crux.db :as db]
            [crux.doc :as doc]
            [crux.index :as idx]
            [crux.io :as cio]
            [crux.kafka :as k]
            [crux.rdf :as rdf]
            [crux.tx :as tx]
            [crux.query :as q]
            [crux.kv-store :as kvs]
            [ring.adapter.jetty :as j]
            [ring.middleware.params :as p]
            [ring.util.request :as req])
  (:import [java.io Closeable]
           [org.apache.kafka.clients.consumer
            KafkaConsumer]))

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
              (find param-name)
              last
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

(defmacro let-valid [bindings expr]
  `(try
     (let ~bindings
       (try
         ~expr
         (catch Exception e#
           (if (st/starts-with? (.getMessage e#) "Invalid input")
             (exception-response 400 e#) ;; Valid edn, invalid content
             (exception-response 500 e#))))) ;; Valid content; something internal failed, or content validity is not properly checked
     (catch Exception e#
       (exception-response 400 e#)))) ;;Invalid edn

(defn zk-active? [bootstrap-servers]
  (try
    (with-open [^KafkaConsumer consumer
                (k/create-consumer
                 {"bootstrap.servers" bootstrap-servers})]
      (boolean (.listTopics consumer)))
    (catch Exception e false)))

;; ---------------------------------------------------
;; Services

(defn status [kvs db-dir bootstrap-servers]
  (let [zk-status (zk-active? bootstrap-servers)
        status-map {:crux.zk/zk-active? zk-status
                    :crux.kv-store/kv-backend (.getName (class kvs))
                    :crux.kv-store/estimate-num-keys (kvs/count-keys kvs)
                    :crux.kv-store/size (cio/folder-human-size db-dir)
                    :crux.tx-log/tx-time (doc/read-meta kvs :crux.tx-log/tx-time)}]
    (if zk-status
      (success-response status-map)
      (response 500
                {"Content-Type" "application/edn"}
                (pr-str status-map)))))

(defn document [kv request]
  (let-valid [object-store (doc/->DocObjectStore kv)
              content-hash (param request "hash")]
    (with-open [snapshot (kvs/new-snapshot kv)]
      (success-response
       (db/get-objects object-store snapshot [content-hash]))))) ;;TODO doesn't work

;; param must be compatible with index/id->bytes (e.g. keyworded UUID)
(defn history [kvs request]
  (let-valid [snapshot (kvs/new-snapshot kvs)
              entity (param request "entity")]
    (success-response
     (doc/entity-history snapshot entity))))

(defn query [kvs request]
  (let-valid [query-map (param request "q")]
    (success-response
     (q/q (q/db kvs) query-map))))

;; TODO: This is a bit ad-hoc.
(defn- edn->sparql-type+value+dt [x]
  (let [sparql-value (if (keyword? x)
                       (subs (str x) 1)
                       (str x))
        type (try
               (idx/id->bytes x)
               (if (rdf/blank-or-anonymous-var? sparql-value)
                 "bnode"
                 "uri")
               (catch Exception _
                 "literal"))]
    (if (= "literal" type)
      (let [literal (rdf/clj->rdf-literal x)]
        [type (.getLabel literal) (str (.getDatatype literal))])
      [type sparql-value])))

(defn- unbound-sparql-value? [x]
  (and (keyword? x) (= "crux.rdf" (namespace x))))

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
(defn sparql-query [kvs request]
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
          {:keys [find] :as query-map} (rdf/sparql->datalog query)
          results (q/q (q/db kvs) query-map)]
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
  (let-valid [tx-op (param request)]
    (success-response
     @(db/submit-tx tx-log [tx-op]))))

;; ---------------------------------------------------
;; Jetty server

(defn handler [kv tx-log db-dir bootstrap-servers request]
  (cond
    (check-path request ["/"] [:get])
    (status kv db-dir bootstrap-servers)

    (check-path request ["/d" "/document"] [:get :post])
    (document kv request)

    (check-path request ["/h" "/history"] [:get :post])
    (history kv request)

    (check-path request ["/q" "/query"] [:get :post])
    (query kv request)

    (check-path request ["/tx-log"] [:post])
    (transact tx-log request)

    (check-path request ["/sparql/"] [:get :post])
    (sparql-query kv request)

    :default
    {:status 400
     :headers {"Content-Type" "text/plain"}
     :body "Unsupported method on this address."}))

(defn ^Closeable create-server
  ([kv tx-log db-dir bootstrap-servers port]
   (let [server (j/run-jetty (p/wrap-params (partial handler kv tx-log db-dir bootstrap-servers))
                             {:port port
                              :join? false})]
     (log/info (str "HTTP server started on port: " port))
     (reify Closeable (close [_] (.stop server))))))
