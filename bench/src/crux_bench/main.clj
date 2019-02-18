(ns crux-bench.main
  (:require [crux.api :as api]
            [clojure.tools.logging :as log]
            [hiccup2.core :refer [html]]
            [hiccup.util]
            [yada.yada :refer [handler listener]]
            [yada.resource :refer [resource]]
            [yada.resources.classpath-resource]

            [crux-bench.watdiv :as watdiv])
  (:import [java.io Closeable]))

(defn index-handler
  [ctx system]
  (str
    "<!DOCTYPE html>"
    (html
      [:html {:lang "en"}
       [:head
        [:title "Crux BenchMarker"]
        [:meta {:charset "utf-8"}]
        [:meta {:http-equiv "Content-Language" :content "en"}]
        [:meta {:name "google" :content "notranslate"}]
        [:link {:rel "stylesheet" :type "text/css" :href "/static/styles/normalize.css"}]
        [:link {:rel "stylesheet" :type "text/css" :href "/static/styles/main.css"}]]
       [:body
        [:header
         [:h2 [:a {:href "/"} "Bench Mark runner"]]
         [:pre (pr-str (-> system :benchmark-runner :status deref))]
         [:div.buttons
          [:input {:value "Run!" :name "run" :type "button"}]
          [:input {:value "Stop!" :name "run" :type "button"}]]]

        [:hr]
        [:div.status-content
         [:h3 "Status"]
         [:pre
          "current status"]]]])))

(defn application-resource
  [system]
  ["/"
   [[""
     (resource
      {:methods
       {:get {:produces "text/html"
              :response #(index-handler % system)}}})]
    ["static"
     (yada.resources.classpath-resource/new-classpath-resource
       "static")]]])

(def index-dir "data/db-dir")
(def log-dir "data/eventlog")

(def crux-options
  {:kv-backend "crux.kv.rocksdb.RocksKv"
   :bootstrap-servers "kafka-cluster-kafka-brokers.crux.svc.cluster.local:9092"
   :event-log-dir log-dir
   :db-dir index-dir
   :server-port 8080})

(defrecord BenchMarkRunner [status crux-system]
  Closeable
  (close [_]
    (println "closing benchmark")))

(defn ^BenchMarkRunner bench-mark-runner [crux-system]
  (map->BenchMarkRunner
    {:crux-system crux-system
     :status (atom {:running? false})}))

(defn run-system
  [{:keys [server-port] :as options} with-system-fn]
  (with-open [crux-system (case (System/getenv "CRUX_MODE")
                            "LOCAL_NODE" (api/start-local-node options)
                            (api/start-standalone-system options))
              benchmark-runner (bench-mark-runner crux-system)

              http-server
              (let [l (listener
                        (application-resource
                          {:crux crux-system
                           :benchmark-runner benchmark-runner})
                        {:port server-port})]
                (log/info "started webserver on port:" server-port)
                (reify Closeable
                  (close [_]
                    ((:close l)))))]
    (with-system-fn crux-system)))

(defn -main []
  )

(comment
  (def s (future
           (run-system
            crux-options
            (fn [_]
              (def crux)
              (Thread/sleep Long/MAX_VALUE)))))
  (future-cancel s))
