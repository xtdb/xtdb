(ns crux-ui-server.main
  "Routing and bootstrapping"
  (:require [aleph.http :as http]
            [bidi.bidi :as bidi]
            [crux-ui-server.crux-auto-start :as crux-auto-start]
            [crux-ui-server.config :as config]
            [clojure.tools.logging :as log]
            [crux-ui-server.pages :as pages]
            [clojure.java.io :as io]
            [clojure.string :as s]
            [clojure.pprint :as pprint])
  (:gen-class)
  (:import (java.io Closeable)))

(defonce closables (atom {}))

(defonce config (atom nil))
(defonce routes (atom nil))

(defn calc-routes [routes-prefix]
  [routes-prefix
   [["" ::home]
    ["/" ::home]
    ["/app"
     {"" ::console
      "/output" ::console
      ["/output/" :rd/out-tab] ::console
      ["/" :rd/tab] ::console}]
    ["/query-perf" ::query-perf]
    ["/service-worker-for-console.js" ::service-worker-for-console]
    ["/static/"
     {true ::static}]
    [true ::not-found]]])


(comment
  (bidi/match-route @routes "/console")
  (bidi/match-route @routes "/console/app")
  (reset! routes (calc-routes "/console")))


(defmulti handler
  (fn [{:keys [uri] :as req}]
    (println ::uri uri)
    (some-> (bidi/match-route @routes uri) :handler)))

(defmethod handler ::home [req]
  {:status 301
   :headers {"location" (bidi/path-for @routes ::console)}})

(defmethod handler ::console [req]
  {:status 200
   :headers {"content-type" "text/html"}
   :body (pages/gen-console-page req @config)})

(defmethod handler ::service-worker-for-console [req]
  {:status 200
   :headers {"content-type" "text/javascript"}
   :body (pages/gen-service-worker req @config)})

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
  (let [relative-uri (s/replace uri (re-pattern (str "^" @pages/routes-prefix)) "")
        relative-uri (s/replace relative-uri #"^/" "")
        resource (io/resource relative-uri)]
    (if resource
      (let [stream (io/input-stream resource)
            mime-type (uri->mime-type uri)]
        {:status 200
         :headers {"content-type" mime-type}
         :body stream})
      (log/warn "No resouce for" uri))))

(defmethod handler ::not-found [req]
  {:status 200
   :headers {"content-type" "text/plain"}
   :body "Not found"})

(defmethod handler :default [req]
  {:status 200
   :headers {"content-type" "text/plain"}
   :body "Default handler : Not implemented"})

(defn stop-servers []
  (when-let [closables' (not-empty @closables)]
    (println "stopping console server")
    (doseq [^Closeable closable (vals closables')]
      (.close closable))
    (reset! closables {})))


(defn- start-servers [{:keys [console/frontend-port console/embed-crux] :as conf}]
  (swap! closables assoc :frontend (http/start-server handler {:port frontend-port}))
  (if embed-crux
    (swap! closables merge (crux-auto-start/try-start-servers conf))))

(defn -main
  "Accepted args
   --frontend-port         5000
   --conf-file             crux-console-conf.edn
   --embed-crux            false
   --crux-http-server-port 8080"
  [& {:as args}]
  (.addShutdownHook (Runtime/getRuntime) (Thread. #'stop-servers))
  (let [conf (config/calc-conf args)]
    (reset! routes (calc-routes (:console/routes-prefix conf)))
    (reset! config conf)
    (reset! pages/routes-prefix (:console/routes-prefix conf))
    (println
      (str "starting console server w conf: \n"
           (with-out-str (pprint/pprint conf))))
    (start-servers conf)))
