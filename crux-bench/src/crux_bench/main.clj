(ns crux-bench.main
  (:gen-class)
  (:require [amazonica.aws.s3 :as s3]
            [buddy.hashers :as hashers]
            [clojure.java.shell :refer [sh]]
            [clojure.pprint :as pp]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [crux-bench.watdiv :as watdiv]
            [crux.api :as api]
            [crux.io :as crux-io]
            [hiccup2.core :refer [html]]
            [yada.resource :refer [resource]]
            yada.resources.classpath-resource
            [yada.yada :refer [listener]])
  (:import crux.api.IndexVersionOutOfSyncException
           java.io.Closeable
           org.rocksdb.RocksDB
           org.rocksdb.util.SizeUnit))

;; getApproximateMemTableStats

(defn- body-wrapper
  [content]
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
        [:div.content content]]])))

(defn- previus-run-reports
  []
  [:div.previus-benchmarks
   [:h2 "Previous run reports"]
   (for [obj (:object-summaries
              (s3/list-objects-v2
                :bucket-name (System/getenv "CRUX_BENCHMARK_BUCKET")))]
     [:div
      [:a {:href (s3/get-url (System/getenv "CRUX_BENCHMARK_BUCKET") (:key obj))}
       (:key obj)]])])

(defn index-handler
  [ctx node]
  (body-wrapper
    [:div
     [:h1 "Benchmark runner"]
     (previus-run-reports)]))

(defn admin-handler
  [ctx node]
  (body-wrapper
    [:div
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
               (let [^RocksDB db (-> node :crux :kv-store :kv :db)]
                 [p (-> db (.getProperty (.getDefaultColumnFamily db) p))])))))]
      [:pre
       (with-out-str
         (pp/pprint (.status ^crux.api.ICruxAPI (:crux node))))]

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
         (pp/pprint (-> node :benchmark-runner :status deref)))]

      [:div.buttons
       [:form {:action "/start-bench" :method "POST"}
        [:div
         [:label "Test Count: (default 100)"]
         [:input {:type "input" :name "test-count"}]]
        [:div
         [:label "Thread Count: (default 1)"]
         [:input {:type "input" :name "thread-count"}]]
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
       (when-let [f (-> node :benchmark-runner :status deref
                        :watdiv-runner :out-file)]
         (:out (sh "tail" "-40" (.getPath ^java.io.File f))))]]

     (previus-run-reports)]))

(def secret-password
  "bcrypt+sha512$ad3066f667bdcfa2a9e0fbc79710bfdb$12$aabc70396ad92c1147f556105c8acda0c17a6f231b85c658")
(defn application-resource
  [{:keys [benchmark-runner] :as node}]
  ["/"
   [[""
     (resource
       {:methods
        {:get {:produces "text/html"
               :response #(index-handler % node)}}})]

    ["admin"
     (resource
       {:access-control
        {:authentication-schemes
         [{:scheme "Basic"
           :verify (fn [[user password]]
                     :bcrypt+blake2b-512
                     (when (and (= user "admin")
                                (hashers/check password secret-password))
                       {:roles #{:admin-user}}))}]

         :authorization
         {:methods {:get :admin-user}}}

         :methods
         {:get {:produces "text/html"
                :response #(admin-handler % node)}}})]

    ["start-bench"
     (resource
       {:methods
        {:post {:consumes "application/x-www-form-urlencoded"
                :produces "text/html"
                :parameters {:form {:test-count String
                                    :thread-count String
                                    :backend String}}
                :response
                (fn [ctx]
                  (let [num-tests (let [t (some-> ctx :parameters :form :test-count)]
                                    (if (str/blank? t)
                                      100
                                      (Integer/parseInt t)))
                        num-threads (let [t (some-> ctx :parameters :form :thread-count)]
                                      (if (str/blank? t)
                                        1
                                        (Integer/parseInt t)))
                        backend (some-> ctx :parameters :form :backend keyword)]
                    (log/info "starting benchmark tests")
                    (swap!
                      (:status benchmark-runner)
                      merge
                      {:running? true
                       :watdiv-runner
                       (watdiv/start-and-run backend node num-tests num-threads)})
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
  {:crux.node/topology :crux.kafka/topology
   :crux.node/kv-store "crux.kv.rocksdb/kv"
   :crux.kafka/bootstrap-servers "kafka-cluster2-kafka-bootstrap.crux:9092"
   :crux.standalone/event-log-dir log-dir
   :crux.kv/db-dir index-dir
   :crux.kafka/tx-topic "crux-bench-transaction-log"
   :crux.kafka/doc-topic "crux-bench-docs"
   :server-port 8080})

(defrecord BenchMarkRunner [status crux-node]
  Closeable
  (close [_]
    (when-let [watdiv-runner (:watdiv-runner @status)]
      (.close ^Closeable watdiv-runner))))

(defn ^BenchMarkRunner bench-mark-runner [crux-node]
  (map->BenchMarkRunner
    {:crux-node crux-node
     :status (atom {:running? false})}))

(defn run-node
  [{:keys [server-port] :as options} with-node-fn]
  (with-open [crux-node (api/start-node options)

              benchmark-runner (bench-mark-runner crux-node)

              http-server
              (let [l (listener
                        (application-resource
                          {:crux crux-node
                           :benchmark-runner benchmark-runner})
                        {:port server-port})]
                (log/info "started webserver on port:" server-port)
                (reify Closeable
                  (close [_]
                    ((:close l)))))]
    (with-node-fn
      {:crux crux-node
       :benchmark-runner benchmark-runner})))

(defn -main []
  (log/info "bench runner starting")
  (try
    (run-node
      crux-options
      (fn [node]
        (while true
          (Thread/sleep 3000)
          (log/info
            (with-out-str
              (pp/pprint {:max-memory (.maxMemory (Runtime/getRuntime))
                          :total-memory (.totalMemory (Runtime/getRuntime))
                          :free-memory (.freeMemory (Runtime/getRuntime))})))
          (log/info
            (with-out-str
              (pp/pprint (some-> node :benchmark-runner :status deref)))))))
    (catch IndexVersionOutOfSyncException e
      (crux-io/delete-dir index-dir)
      (-main)))
  (log/info "bench runner exiting"))

(comment
  (def s (future
           (try
             (run-node
               crux-options
               (fn [c]
                 (def crux c)
                 (Thread/sleep Long/MAX_VALUE)))
             (catch Exception e
               (println e)
               (throw e)))))
  (future-cancel s))
