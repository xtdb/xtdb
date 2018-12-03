(ns crux.http-server
  "HTTP API for Crux.

  Requires ring/ring-core, ring/ring-jetty-adapter,
  org.apache.kafka/kafka-clients and
  org.eclipse.rdf4j/rdf4j-queryparser-sparql on the classpath"
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            [crux.api :as api]
            [crux.io]
            [crux.kafka]
            [crux.rdf :as rdf]
            [crux.sparql.protocol :as sparql-protocol]
            [ring.adapter.jetty :as j]
            [ring.middleware.params :as p]
            [ring.util.request :as req]
            [ring.util.io :as rio])
  (:import java.io.Closeable
           java.time.Duration
           java.util.UUID
           org.eclipse.jetty.server.Server))

;; ---------------------------------------------------
;; Utils

(defn- uuid-str? [s]
  (re-matches #"[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$" s))

(defn- sha1-20-str? [s]
  (re-matches #"[a-f0-9]{20}" s))

(defn- read-unknown [s]
  (cond
    (uuid-str? s)
    (UUID/fromString s)

    (sha1-20-str? s)
    s

    (try
      (edn/read-string s)
      (catch Exception e false))
    (edn/read-string s)

    :default
    (keyword s)))

(defn- param
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

(defn- check-path [request valid-paths valid-methods]
  (let [path (req/path-info request)
        method (:request-method request)]
    (and (some #{path} valid-paths)
         (some #{method} valid-methods))))

(defn- response
  ([status headers body]
   {:status status
    :headers headers
    :body body}))

(defn- success-response [m]
  (response 200
            {"Content-Type" "application/edn"}
            (pr-str m)))

(defn- exception-response [status ^Exception e]
  (response status
            {"Content-Type" "text/plain"}
            (str
             (.getMessage e) "\n"
             (ex-data e))))

(defn- wrap-exception-handling [handler]
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

(defn- status [local-node]
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
(defn- history [local-node request]
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

(defn- query [local-node request]
  (let [query-map (param request "q")]
    (success-response
     (api/q (db-for-request local-node query-map)
            (dissoc query-map :business-time :transact-time)))))

(defn- query-stream [local-node request]
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

(defn- entity [local-node request]
  (let [body (param request "entity")]
    (success-response
     (api/entity (db-for-request local-node body)
                 (:eid body)))))

(defn- entity-tx [local-node request]
  (let [body (param request "entity-tx")]
    (success-response
     (api/entity-tx (db-for-request local-node body)
                    (:eid body)))))

(defn- transact [local-node request]
  (let [tx-ops (param request)]
    (success-response
     (api/submit-tx local-node tx-ops))))

;; ---------------------------------------------------
;; Jetty server

(defn- handler [local-node request]
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
    (sparql-protocol/sparql-query local-node request)

    :default
    {:status 400
     :headers {"Content-Type" "text/plain"}
     :body "Unsupported method on this address."}))

(s/def ::server-port :crux.io/port)

(s/def ::options (s/keys :req-un [:crux.kafka/bootstrap-servers
                                  ::server-port]))

(defrecord HTTPServer [^Server server options]
  Closeable
  (close [_]
    (.stop server)))

(defn ^Closeable start-http-server
  "Starts a HTTP server listening to the specified server-port, serving
  the Crux HTTP API. Takes a either a crux.api.LocalNode or its
  dependencies explicitly as arguments."
  ([{:keys [kv-store tx-log] :as local-node} {:keys [server-port]
                                              :or {server-port 3000}
                                              :as options}]
   (when (s/invalid? (s/conform ::options options))
     (throw (IllegalArgumentException.
             (str "Invalid options: " (s/explain-str ::options options)))))
   (let [server (j/run-jetty (-> (partial handler local-node)
                                 (p/wrap-params)
                                 (wrap-exception-handling))
                             {:port server-port
                              :join? false})]
     (log/info "HTTP server started on port: " server-port)
     (->HTTPServer server options)))
  ([kv-store tx-log {:keys [bootstrap-servers
                            server-port]
                     :as options}]
   (start-http-server (api/->LocalNode (promise)
                                       kv-store
                                       tx-log
                                       options)
                      options)))
