(ns crux-ui-server.main
  "Routing and bootstrapping"
  (:require [aleph.http :as http]
            [bidi.bidi :as bidi]
            [crux-ui-server.crux-auto-start :as crux-auto-start]
            [clojure.tools.logging :as log]
            [crux-ui-server.pages :as pages]
            [clojure.java.io :as io]
            [clojure.string :as s])
; (:gen-class)
  (:import (java.io Closeable)))

(defonce closables (atom {}))

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
  ;:headers {"content-type" "text/plain"}
   :headers {"location" "/console"}
   :body "Not implemented"})

(defn stop-servers []
  (when-let [closables' (not-empty @closables)]
    (println "stopping console server")
    (doseq [^Closeable closable (vals closables')]
      (.close closable))
    (reset! closables {})))


(defn- fix-key [[k v]]
  [(cond-> k
     (and (string? k) (.startsWith k "--"))
     (subs 2))
   v])

(defn- parse-args [args-map]
  (let [w-norm-keys (into {} (map fix-key args-map))
        w-kw-keys (into {} (map (fn [[k v]] [(keyword k) v]) w-norm-keys))
        w-parsed (into {} (map (fn [[k v]] [k (cond-> v (string? v) read-string)]) w-kw-keys))]
    w-parsed))

(assert
  (= {:frontend-port 5000, :embed-crux false, :crux-http-server-port 8080}
     (parse-args
       {"--frontend-port"         "5000"
        "--embed-crux"            "false"
        "--crux-http-server-port" "8080"})))

(defn- calc-conf [args]
  (merge {:frontend-port 5000
          :embed-crux false
          :crux-http-server-port 8080}
         (parse-args args)))

(defn- start-servers [{:keys [frontend-port embed-crux] :as conf}]
  (swap! closables assoc :frontend (http/start-server handler {:port frontend-port}))
  (if embed-crux
    (swap! closables merge (crux-auto-start/try-start-servers conf))))

(defn -main
  "Accepted args
   --frontend-port 5000
   --embed-crux false
   --crux-http-server-port 8080"
  [& {:as args}]
  (.addShutdownHook (Runtime/getRuntime) (Thread. #'stop-servers))
  (let [conf (calc-conf args)]
    (println (str "starting console server w conf: \n" (pr-str conf)))
    (start-servers conf)))
