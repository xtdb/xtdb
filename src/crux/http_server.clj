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

(defn- body->edn [request]
  (-> request
      req/body-string
      edn/read-string))

(defn- check-path [[path-pattern valid-methods] request]
  (let [path (req/path-info request)
        method (:request-method request)]
    (and (re-find path-pattern path)
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
             (pr-str (ex-data e)))))

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
  (let [[_ content-hash] (re-find #"^/document/(.+)$" (req/path-info request))]
    (success-response
     (.document local-node (c/new-id content-hash)))))

(defn- history [^ICruxSystem local-node request]
  (let [[_ eid] (re-find #"^/history/(.+)$" (req/path-info request))]
    (success-response
     (.history local-node (c/new-id eid)))))

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

(s/def ::business-time inst?)
(s/def ::transact-time inst?)

(s/def ::query-map (s/keys :req-un [:crux.query/query]
                           :opt-un [::business-time
                                    ::transact-time]))

;; TODO: Potentially require both business and transaction time sent
;; by the client?
(defn- query [^ICruxSystem local-node request]
  (let [query-map (s/assert ::query-map (body->edn request))]
    (success-response
     (.q (db-for-request local-node query-map)
         (:query query-map)))))

(defn- query-stream [^ICruxSystem local-node request]
  (let [query-map (s/assert ::query-map (body->edn request))
        db (db-for-request local-node query-map)
        snapshot (.newSnapshot db)
        result (.q db snapshot (:query query-map))]
    (streamed-edn-response snapshot result)))

(s/def ::eid c/valid-id?)
(s/def ::entity-map (s/keys :req-un [::eid]
                            :opt-un [::business-time
                                     ::transact-time]))

;; TODO: Could support as-of now via path and GET.
(defn- entity [^ICruxSystem local-node request]
  (let [body (s/assert ::entity-map (body->edn request))]
    (success-response
     (.entity (db-for-request local-node body)
              (:eid body)))))

(defn- entity-tx [^ICruxSystem local-node request]
  (let [body (s/assert ::entity-map (body->edn request))]
    (success-response
     (.entityTx (db-for-request local-node body)
                (:eid body)))))

(defn- transact [^ICruxSystem local-node request]
  (let [tx-ops (body->edn request)]
    (success-response
     (.submitTx local-node tx-ops))))

;; TODO: Could add from date parameter.
(defn- tx-log [^ICruxSystem local-node request]
  (let [ctx (.newTxLogContext local-node)
        result (.txLog local-node ctx)]
    (streamed-edn-response ctx result)))

(defn- sync-handler [^ICruxSystem local-node request]
  (let [timeout (Long/parseLong (get-in request [:query-params "timeout"] "0"))]
    (success-response
     (.sync local-node timeout))))

;; ---------------------------------------------------
;; Jetty server

(defn- handler [local-node request]
  (condp check-path request
    [#"^/$" [:get]]
    (status local-node)

    [#"^/document/.+$" [:get :post]]
    (document local-node request)

    [#"^/history/.+$" [:get :post]]
    (history local-node request)

    [#"^/query$" [:post]]
    (query local-node request)

    [#"^/query-stream$" [:post]]
    (query-stream local-node request)

    [#"^/entity$" [:post]]
    (entity local-node request)

    [#"^/entity-tx$" [:post]]
    (entity-tx local-node request)

    [#"^/tx-log$" [:post]]
    (transact local-node request)

    [#"^/tx-log$" [:get]]
    (tx-log local-node request)

    [#"^/sync$" [:get]]
    (sync-handler local-node request)

    [#"^/sparql/?$" [:get :post]]
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
  dependencies explicitly as arguments (internal use)."
  (^java.io.Closeable
   [local-node {:keys [server-port]
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
   [kv-store tx-log indexer consumer-config {:keys [server-port]
                                             :as options}]
   (start-http-server (api/map->LocalNode {:kv-store kv-store
                                           :tx-log tx-log
                                           :indexer indexer
                                           :consumer-config consumer-config
                                           :options options})
                      options)))
