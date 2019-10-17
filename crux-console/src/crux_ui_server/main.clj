(ns crux-ui-server.main
  "Routing and bootstrapping"
  (:require [aleph.http :as http]
            [bidi.bidi :as bidi]
            [crux.http-server]
            [crux.api]
            [crux-ui-server.pages :as pages]
            [clojure.java.io :as io]
            [clojure.string :as s])
  (:gen-class)
  (:import (java.io Closeable)))

(defonce closables (atom nil))

(def routes
  [""
   [["" ::home]
    ["/" ::home]
    ["/query-perf" ::query-perf]
    ["/service-worker-for-console.js" ::service-worker-for-console]
    ["/console"
     {"" ::console
      "/output" ::console
      ["/output/" :rd/out-tab] ::console
      ["/" :rd/tab] ::console}]
    ["/static/"
     {true ::static}]
    [true ::not-found]]])


(defmulti handler
  (fn [{:keys [uri] :as req}]
    (some-> (bidi/match-route routes uri) :handler)))

(defmethod handler ::home [req]
  {:status 301
   :headers {"location" "/console"}})

(defmethod handler ::console [req]
  {:status 200
   :headers {"content-type" "text/html"}
   :body (pages/gen-console-page req)})

(defmethod handler ::service-worker-for-console [req]
  {:status 200
   :headers {"content-type" "text/javascript"}
   :body (pages/gen-service-worker req)})

(defn uri->mime-type [uri]
  (cond
    (re-find #".css$" uri) "text/css"
    (re-find #".js$" uri) "text/javascript"
    (re-find #".json$" uri) "application/json"
    (re-find #".png$" uri) "image/png"
    (re-find #".jpe?g$" uri) "image/jpeg"
    (re-find #".svg$" uri) "image/svg"
    :else "text/plain"))

(defmethod handler ::static [{:keys [uri] :as req}]
  (let [relative-uri (s/replace uri #"^/" "")
        resource (io/input-stream (io/resource relative-uri))
        mime-type (uri->mime-type uri)]
    {:status 200
     :headers {"content-type" mime-type}
     :body resource}))

(defmethod handler ::not-found [req]
  {:status 200
   :headers {"content-type" "text/plain"}
   :body "Not found"})

(defmethod handler :default [req]
  {:status 200
  ;:headers {"content-type" "text/plain"}
   :headers {"location" "/console"}
   :body "Not implemented"})

(defn stop-servers []
  (when-let [closables' @closables]
    (println "stopping console server")
    (doseq [^Closeable closable closables']
      (.close closable))
    (reset! closables nil)))

(def node-opts
  {:crux.node/kv-store "crux.kv.rocksdb.RocksKv"
   :crux.standalone/event-log-dir "data/eventlog-1"
   :crux.kv/db-dir "data/db-dir-1"})

(def http-opts
  {:server-port 8080
   :cors-access-control
   [:access-control-allow-origin [#".*"]
    :access-control-allow-headers ["X-Requested-With"
                                   "Content-Type"
                                   "Cache-Control"
                                   "Origin"
                                   "Accept"
                                   "Authorization"
                                   "X-Custom-Header"]
    :access-control-allow-methods [:get :options :head :post]]})

(defn -main []
  (let [runtime (Runtime/getRuntime)]
    (.addShutdownHook runtime (Thread. #'stop-servers)))
  (println "starting console server")
  (let [node (crux.api/start-node node-opts)
        crux-http-server (crux.http-server/start-http-server node http-opts)
        console-http-server (http/start-server handler {:port 5000})]
    (reset! closables [node crux-http-server console-http-server])))
