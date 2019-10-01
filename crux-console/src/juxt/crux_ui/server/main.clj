(ns juxt.crux-ui.server.main
  (:require [aleph.http :as http]
            [bidi.bidi :as bidi])
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
    ["/static/" [::static :edge.yada.ig/resources]]
    [true ::not-found]]])


(bidi/match-route routes "/console")


(defmulti handler
  (fn [{:keys [uri] :as req}]
    (println :matching-route)
    (some-> (bidi/match-route routes uri) :handler)))

(defmethod handler ::home [req]
  {:status 301
   :headers {"location" "/console"}})

(defmethod handler ::console [req]
  {:status 200
   :headers {"content-type" "text/plain"}
   :body "hello!"})

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
