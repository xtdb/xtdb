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
            [crux.codec :as c]
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
           org.eclipse.jetty.server.Server
           [crux.api ICruxDatasource ICruxSystem]))

;; ---------------------------------------------------
;; Utils

(defn- read-unknown [s]
  (cond
    (c/valid-id? s)
    (c/new-id s)

    (try
      (edn/read-string s)
      (catch Exception e))
    (edn/read-string s)

    :else
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
          (if (str/starts-with? (.getMessage e) "Spec assertion failed")
            (exception-response 400 e) ;; Valid edn, invalid content
            (exception-response 500 e)))) ;; Valid content; something internal failed, or content validity is not properly checked
      (catch Exception e
        (exception-response 400 e))))) ;;Invalid edn

;; ---------------------------------------------------
;; Services

(defn- status [^ICruxSystem local-node]
  (let [status-map (.status local-node)]
    (if (:crux.zk/zk-active? status-map)
      (success-response status-map)
      (response 500
                {"Content-Type" "application/edn"}
                (pr-str status-map)))))

(defn document [^ICruxSystem local-node request]
  (let [content-hash (param request "hash")]
    (success-response
     (.document local-node content-hash))))

;; param must be compatible with index/id->bytes (e.g. keyworded UUID)
(defn- history [^ICruxSystem local-node request]
  (let [entity (param request "entity")]
    (success-response
     (.history local-node entity))))

(defn- db-for-request ^ICruxDatasource [^ICruxSystem local-node {:keys [business-time transact-time]}]
  (cond
    (and business-time transact-time)
    (.db local-node business-time transact-time)

    business-time
    (.db local-node business-time)

    :else
    (.db local-node)))

(defn- streamed-edn-response [^Closeable ctx edn]
  (try
    (->> (rio/piped-input-stream
          (fn [out]
            (with-open [ctx ctx
                        out (io/writer out)]
              (.write out "(")
              (doseq [x edn]
                (.write out (pr-str x)))
              (.write out ")"))))
         (response 200 {"Content-Type" "application/edn"}))
    (catch Throwable t
      (.close ctx)
      (throw t))))

(defn- query [^ICruxSystem local-node request]
  (let [query-map (param request "q")]
    (success-response
     (.q (db-for-request local-node query-map)
         (dissoc query-map :business-time :transact-time)))))

(defn- query-stream [^ICruxSystem local-node request]
  (let [query-map (param request "q")
        db (db-for-request local-node query-map)
        snapshot (.newSnapshot db)
        result (.q db snapshot (dissoc query-map :business-time :transact-time))]
    (streamed-edn-response snapshot result)))

(defn- entity [^ICruxSystem local-node request]
  (let [body (param request "entity")]
    (success-response
     (.entity (db-for-request local-node body)
              (:eid body)))))

(defn- entity-tx [^ICruxSystem local-node request]
  (let [body (param request "entity-tx")]
    (success-response
     (.entityTx (db-for-request local-node body)
                (:eid body)))))

(defn- transact [^ICruxSystem local-node request]
  (let [tx-ops (param request)]
    (success-response
     (.submitTx local-node tx-ops))))

(defn- tx-log [^ICruxSystem local-node request]
  (let [ctx (.newTxLogContext local-node)
        result (.txLog local-node ctx)]
    (streamed-edn-response ctx result)))

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

    (check-path request ["/tx-log"] [:get])
    (tx-log local-node request)

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

(defn start-http-server
  "Starts a HTTP server listening to the specified server-port, serving
  the Crux HTTP API. Takes a either a crux.api.LocalNode or its
  dependencies explicitly as arguments."
  (^crux.http_server.HTTPServer
   [{:keys [kv-store tx-log] :as local-node} {:keys [server-port]
                                              :or {server-port 3000}
                                              :as options}]
   (s/assert ::options options)
   (let [server (j/run-jetty (-> (partial handler local-node)
                                 (p/wrap-params)
                                 (wrap-exception-handling))
                             {:port server-port
                              :join? false})]
     (log/info "HTTP server started on port: " server-port)
     (->HTTPServer server options)))
  (^crux.http_server.HTTPServer
   [kv-store tx-log {:keys [bootstrap-servers
                            server-port]
                     :as options}]
   (start-http-server (api/->LocalNode (promise)
                                       kv-store
                                       tx-log
                                       options)
                      options)))
