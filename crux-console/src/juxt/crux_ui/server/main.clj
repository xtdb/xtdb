(ns juxt.crux-ui.server.main
  (:require [aleph.http :as http]
            [bidi.bidi :as bidi]
            [juxt.crux-ui.server.pages :as pages]
            [clojure.java.io :as io]
            [clojure.string :as s])
  (:gen-class))

(defonce srv (atom nil))

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


(bidi/match-route routes "/static/333.js")


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


(defn stop-server []
  (when-let [closeable @srv]
    (.close closeable)
    (reset! srv nil)))

(defn -main []
  (println "starting console server")
  (stop-server)
  (reset! srv (http/start-server handler {:port 8300})))
