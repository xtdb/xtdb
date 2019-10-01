(ns juxt.crux-ui.server.main
  (:require [aleph.http :as http]
            [bidi.bidi :as bidi]
            [crux.http-server]
            [crux.api]
            [juxt.crux-ui.server.pages :as pages]
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
        resource (io/file (io/resource relative-uri))
        mime-type (uri->mime-type uri)]
    (println uri)
    {:status 200
     :headers {"content-type" mime-type}
     :body resource}))

(handler {:uri "/static/styles/monokai.css"})

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
    (doseq [^Closeable closable closables']
      (.close closable))
    (reset! closables nil)))

(def node-opts
  {:kv-backend "crux.kv.rocksdb.RocksKv"
   :event-log-dir "data/eventlog-1"
   :db-dir "data/db-dir-1"})

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
  (println "starting console server")
  (stop-servers)
  (let [node (crux.api/start-standalone-node node-opts)
        crux-http-server (crux.http-server/start-http-server node http-opts)
        console-http-server (http/start-server handler {:port 8300})]
    (reset! closables [node crux-http-server console-http-server])))
