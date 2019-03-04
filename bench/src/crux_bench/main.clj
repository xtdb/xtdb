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
           [org.rocksdb Options RocksDB
            CompactionStyle CompressionType LRUCache
            HashSkipListMemTableConfig BlockBasedTableConfig]
           [org.rocksdb.util SizeUnit]
           java.io.Closeable))

;; getApproximateMemTableStats

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
            (pp/pprint
              (into
                {}
                (for [p ["rocksdb.estimate-table-readers-mem"
                         "rocksdb.size-all-mem-tables"
                         "rocksdb.cur-size-all-mem-tables"
                         "rocksdb.estimate-num-keys"]]
                  (let [^RocksDB db (-> system :crux :kv-store :kv :db)]
                    [p (-> db (.getProperty (.getDefaultColumnFamily db) p))])))))]
         [:pre
          (with-out-str
            (pp/pprint (.status ^crux.api.ICruxSystem (:crux system))))]

         [:pre
          (with-out-str
            (pp/pprint {:max-memory (.maxMemory (Runtime/getRuntime))
                        :total-memory (.totalMemory (Runtime/getRuntime))
                        :free-memory (.freeMemory (Runtime/getRuntime))}))]

         [:pre
          (slurp
            (java.io.FileReader.
              (format "/proc/%s/status" (.pid (java.lang.ProcessHandle/current)))))]

         [:pre
          (with-out-str
            (pp/pprint (-> system :benchmark-runner :status deref)))]

         [:div.buttons
          [:form {:action "/start-bench" :method "POST"}
           [:div
            [:label "Test Count:"]
            [:input {:type "input" :name "test-count"}]]
           [:div
            [:label "Backend"]
            [:select {:name "backend"}
             (for [backend watdiv/supported-backends]
               [:option {:value backend} backend])]]
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
                :parameters {:form {:test-count String
                                    :backend String}}
                :response
                (fn [ctx]
                  (let [num-tests (let [t (some-> ctx :parameters :form :test-count)]
                                    (if (str/blank? t)
                                      100
                                      (Integer/parseInt t)))
                        backend (some-> ctx :parameters :form :backend keyword)]
                    (log/info "starting benchmark tests")
                    (swap!
                      (:status benchmark-runner)
                      merge
                      {:running? true
                       :watdiv-runner
                       (watdiv/start-and-run backend system num-tests)})
                    (assoc (:response ctx)
                           :status 302
                           :headers {"location" "/"})))}}})]

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

(def ^:private default-block-cache-size (* 10 SizeUnit/MB))
(def ^:private default-block-size (* 16 SizeUnit/KB))

(def crux-options
  {:kv-backend "crux.kv.rocksdb.RocksKv"
   :bootstrap-servers "kafka-cluster2-kafka-bootstrap.crux:9092"
   :event-log-dir log-dir
   :db-dir index-dir
   :tx-topic "crux-bench-transaction-log"
   :doc-topic "crux-bench-docs"
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
    (with-system-fn
      {:crux crux-system
       :benchmark-runner benchmark-runner})))

(defn -main []
  (log/info "bench runner starting")
  (try
    (run-system
      crux-options
      (fn [system]
        (while true
          (Thread/sleep 3000)
          (log/info
            (with-out-str
              (pp/pprint {:max-memory (.maxMemory (Runtime/getRuntime))
                          :total-memory (.totalMemory (Runtime/getRuntime))
                          :free-memory (.freeMemory (Runtime/getRuntime))})))
          (log/info
            (with-out-str
              (pp/pprint (some-> system :benchmark-runner :status deref)))))))
    (catch IndexVersionOutOfSyncException e
      (crux-io/delete-dir index-dir)
      (-main)))
  (log/info "bench runner exiting"))

(comment
  (def s (future
           (try
             (run-system
               crux-options
               (fn [c]
                 (def crux c)
                 (Thread/sleep Long/MAX_VALUE)))
             (catch Exception e
               (println e)
               (throw e)))))
  (future-cancel s))
