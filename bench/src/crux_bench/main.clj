(ns crux-bench.main
  (:gen-class)
  (:require [clojure.java.shell :refer [sh]]
            [clojure.pprint :as pp]
            [crux.io :as crux-io]
            [clojure.string :as str]
            [amazonica.aws.s3 :as s3]
            [clojure.tools.logging :as log]
            [crux-bench.watdiv :as watdiv]
            [crux.api :as api]
            [hiccup2.core :refer [html]]
            [yada.yada :refer [listener]]
            [yada.resource :refer [resource]]
            yada.resources.classpath-resource)
  (:import [crux.api IndexVersionOutOfSyncException]
           java.io.Closeable))

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
         [:pre
          (with-out-str
            (pp/pprint (.status (:crux system))))]
         [:pre
          (with-out-str
            (pp/pprint (-> system :benchmark-runner :status deref)))]

         [:div.buttons
          [:form {:action "/start-bench" :method "POST"}
           [:input {:type "input" :name "test-count"}]
           [:input {:value "Run!" :type "submit"}]]

          [:form {:action "/stop-bench" :method "POST"}
           [:input {:value "Stop!" :name "run" :type "submit"}]]]]

        [:hr]
        [:div.status-content
         [:h3 "Status"]
         [:pre
          (when-let [f (-> system :benchmark-runner :status deref
                           :watdiv-runner :out-file)]
            (:out (sh "tail" "-40" (.getPath ^java.io.File f))))]]

        [:div.previus-benchmarks
         (for [obj (:object-summaries
                    (s3/list-objects-v2
                      :bucket-name (System/getenv "CRUX_BENCHMARK_BUCKET")))]
           [:div
            [:a {:href (s3/get-url (System/getenv "CRUX_BENCHMARK_BUCKET") (:key obj))}
             (:key obj)]])]]])))

(defn application-resource
  [{:keys [benchmark-runner] :as system}]
  ["/"
   [[""
     (resource
      {:methods
       {:get {:produces "text/html"
              :response #(index-handler % system)}}})]

    ["start-bench"
     (resource
       {:methods
        {:post {:consumes "application/x-www-form-urlencoded"
                :produces "text/html"
                :parameters {:form {:test-count String}}
                :response
                (fn [ctx]
                  (log/info "starting benchmark tests")
                  (swap!
                    (:status benchmark-runner)
                    merge
                    {:running? true
                     :watdiv-runner
                     (watdiv/run-watdiv-test
                       system
                       (let [t (some-> ctx :parameters :form :test-count)]
                         (if (str/blank? t)
                           100
                           (Integer/parseInt t))))})
                  (assoc (:response ctx)
                         :status 302
                         :headers {"location" "/"}))}}})]

    ["stop-bench"
     (resource
       {:methods
        {:post {:consumes "application/x-www-form-urlencoded"
                :produces "text/html"
                :response
                (fn [ctx]
                  (log/info "stopping benchmark tests")
                  (when-let [watdiv-runner (:watdiv-runner @(:status benchmark-runner))]
                    (.close ^Closeable watdiv-runner))
                  (reset! (:status benchmark-runner) {:running? false})
                  (assoc (:response ctx)
                         :status 302
                         :headers {"location" "/"}))}}})]

    ["static"
     (yada.resources.classpath-resource/new-classpath-resource
       "static")]]])

(def index-dir "data/db-dir")
(def log-dir "data/eventlog")

(def crux-options
  {:kv-backend "crux.kv.rocksdb.RocksKv"
   :bootstrap-servers "kafka-cluster2-kafka-bootstrap.crux:9092"
   :event-log-dir log-dir

   :tx-topic "crux-bench-transaction-log"
   :doc-topic "crux-bench-docs"
   :db-dir index-dir
   :server-port 8080})

(defrecord BenchMarkRunner [status crux-system]
  Closeable
  (close [_]
    (when-let [watdiv-runner (:watdiv-runner @status)]
      (.close ^Closeable watdiv-runner))))

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
  (log/info "bench runner starting")
  (try
    (run-system crux-options (fn [_] (.join (Thread/currentThread))))
    (catch IndexVersionOutOfSyncException e
      (crux-io/delete-dir index-dir)
      (-main)))
  (log/info "bench runner exiting"))

(comment
  (def s (future
           (run-system
            crux-options
            (fn [_]
              (def crux)
              (Thread/sleep Long/MAX_VALUE)))))
  (future-cancel s)

  )
