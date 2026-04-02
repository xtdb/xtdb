(ns xtdb.healthz
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [integrant.core :as ig]
            [reitit.http :as http]
            [reitit.http.interceptors.exception :as ri.exception]
            [reitit.interceptor.sieppari :as r.sieppari]
            [reitit.ring :as r.ring]
            [ring.adapter.jetty9 :as j]
            [xtdb.api :as xt]
            [xtdb.node :as xtn]
            [xtdb.util :as util])
  (:import (io.micrometer.prometheusmetrics PrometheusMeterRegistry)
           [java.lang AutoCloseable]
           [java.net InetAddress]
           org.eclipse.jetty.server.Server
           (xtdb.api Xtdb$Config)
           (xtdb.api.log SourceMessage$FlushBlock)
           (xtdb.api.metrics HealthzConfig)
           xtdb.api.Xtdb$Config
           (xtdb.database Database Database$Catalog)
           xtdb.NodeBase
           xtdb.storage.BufferPoolKt))

(defn get-ingestion-error [^Database db]
  (.getIngestionError db))

(defn- ->block-lag [^Database db]
  (max 0 (- (BufferPoolKt/getLatestAvailableBlockIndex (.getBufferPool db))
            (or (.getCurrentBlockIndex (.getBlockCatalog db)) -1))))

(defn- critical? [^Database db]
  (or (= "xtdb" (.getName db))
      (.getCritical (.getConfig db))))

(defn- all-databases
  "Returns all currently attached databases from the catalog."
  [^Database$Catalog db-cat]
  (into [] (map (fn [db-name] (.databaseOrNull db-cat db-name)))
        (.getDatabaseNames db-cat)))

(def index-html-str
  "<!DOCTYPE html>
<html>
<head><title>XTDB Healthz API</title></head>
<body>
  <h1>XTDB Healthz API</h1>
  <p>See <a href=\"https://docs.xtdb.com/ops/config/monitoring.html\">Monitoring</a> and <a href=\"https://docs.xtdb.com/ops/maintenance.html\">Maintenance</a> documentation for details on using this API endpoint.</p>
</body>
</html>")

(def router
  (http/router [["/" {:name :index
                      :get (fn [_]
                             {:status 200
                              :headers {"Content-Type" "text/html; charset=utf-8"}
                              :body index-html-str})}]

                ["/metrics" {:name :metrics
                             :get (fn [{:keys [^PrometheusMeterRegistry meter-registry]}]
                                    {:status 200,
                                     :headers {"Content-Type" "text/plain; version=0.0.4"}
                                     :body (.scrape meter-registry)})}]

                ["/healthz/started" {:name :started
                                     :get (fn [{:keys [^Database$Catalog db-cat initial-target-message-ids]}]
                                            (let [catching-up (for [[db-name target-msg-ids] initial-target-message-ids
                                                                    :let [db (.databaseOrNull db-cat db-name)]
                                                                    :when db
                                                                    :let [lpm-id (.getLatestProcessedMsgId db)]
                                                                    :when (some #(< lpm-id (long %)) target-msg-ids)]
                                                                {:db db-name, :current lpm-id, :targets target-msg-ids})]
                                              (if (seq catching-up)
                                                {:status 503,
                                                 :headers {"X-XTDB-Databases-Catching-Up" (str (count catching-up))}
                                                 :body (str "Catching up: "
                                                            (->> catching-up
                                                                 (map (fn [{:keys [db current targets]}]
                                                                        (format "%s (at: %d, targets: %s)" db current targets)))
                                                                 (str/join ", ")))}

                                                {:status 200,
                                                 :body "Started."})))}]

                ["/healthz/alive" {:name :alive
                                   :get (fn [{:keys [^Database$Catalog db-cat]}]
                                          (let [dbs (all-databases db-cat)
                                                problems (for [^Database db dbs
                                                               :let [db-name (.getName db)
                                                                     critical (critical? db)
                                                                     ingestion-error (get-ingestion-error db)
                                                                     block-lag (->block-lag db)
                                                                     block-lag-healthy? (<= block-lag 5)]
                                                               :when (or ingestion-error (not block-lag-healthy?))]
                                                           {:db db-name
                                                            :critical critical
                                                            :ingestion-error ingestion-error
                                                            :block-lag block-lag
                                                            :block-lag-healthy? block-lag-healthy?})
                                                critical-problems (filter :critical problems)]

                                            (doseq [{:keys [db ingestion-error block-lag block-lag-healthy?]} problems]
                                              (if ingestion-error
                                                (log/error (format "Ingestion error detected for database '%s': %s" db ingestion-error))
                                                (when-not block-lag-healthy?
                                                  (log/warn (format "Block lag for database '%s' is %d blocks, exceeds healthy threshold" db block-lag)))))

                                            (cond-> {:headers {"X-XTDB-Databases-Checked" (str (count dbs))
                                                               "X-XTDB-Databases-Unhealthy" (str (count problems))}}

                                              (seq critical-problems)
                                              (assoc :status 503
                                                     :body (str "Unhealthy: "
                                                                (->> critical-problems
                                                                     (map (fn [{:keys [db ingestion-error block-lag]}]
                                                                            (if ingestion-error
                                                                              (format "%s (ingestion error: %s)" db ingestion-error)
                                                                              (format "%s (block lag: %d)" db block-lag))))
                                                                     (str/join ", "))))

                                              (empty? critical-problems)
                                              (assoc :status 200, :body "Alive."))))}]

                ["/healthz/ready" {:name :ready
                                   :get (fn [_] {:status 200, :body "Ready."})}]

                ["/system/finish-block" {:name :finish-block
                                         :post (fn [{:keys [^Database$Catalog db-cat query-string]}]
                                                 (let [db-name (some->> query-string
                                                                        (re-find #"(?:^|&)db=([^&]+)")
                                                                        second)
                                                       dbs (if db-name
                                                             (if-let [db (.databaseOrNull db-cat db-name)]
                                                               [db]
                                                               (throw (ex-info (str "Unknown database: " db-name) {})))
                                                             (all-databases db-cat))]
                                                   (try
                                                     (doseq [^Database db dbs]
                                                       (let [flush-msg (SourceMessage$FlushBlock. (or (.getCurrentBlockIndex (.getBlockCatalog db)) -1))]
                                                         (.appendMessageBlocking (.getSourceLog db) flush-msg)))
                                                     {:status 200,
                                                      :body (format "Block flush message sent to %d database(s)." (count dbs))}
                                                     (catch Exception e
                                                       {:status 500, :body (str "Error sending flush block message: " (.getMessage e))}))))}]]

               {:data {:interceptors [[ri.exception/exception-interceptor
                                       (merge ri.exception/default-handlers
                                              {::ri.exception/wrap (fn [_handler e _req]
                                                                     (log/debug e (format "response error (%s): '%s'" (class e) (ex-message e)))
                                                                     {:status 500 :body (str "Exception when calling endpoint - " e)})})]]}}))

(defn- with-opts [opts]
  {:enter (fn [ctx]
            (update ctx :request merge opts))})

(defn handler [opts]
  (http/ring-handler router
                     (r.ring/create-default-handler)
                     {:executor r.sieppari/executor
                      :interceptors [[with-opts opts]]}))

(defmethod xtn/apply-config! :xtdb/healthz [^Xtdb$Config config _ healthz-config]
  (.healthz config
            (let [host (:host healthz-config ::absent)
                  port (:port healthz-config ::absent)]
              (cond-> (HealthzConfig.)
                (not= ::absent host) (.host (when (and host (not= host "*"))
                                              (InetAddress/getByName host)))
                (not= ::absent port) (.port port)))))

(defmethod ig/expand-key :xtdb/healthz [k ^HealthzConfig config]
  {k {:host (.getHost config)
      :port (.getPort config)
      :base (ig/ref :xtdb/base)
      :db-cat (ig/ref :xtdb/db-catalog)
      :node (ig/ref :xtdb/node)}})

(defmethod ig/init-key :xtdb/healthz [_ {:keys [node, ^InetAddress host, ^long port, ^NodeBase base, ^Database$Catalog db-cat]}]
  (let [initial-target-message-ids (into {}
                                         (for [db-name (.getDatabaseNames db-cat)
                                               :let [^Database db (.databaseOrNull db-cat db-name)]
                                               :when db]
                                           [db-name [(.getLatestSubmittedMsgId (.getSourceLog db))]]))

        ^Server server (-> (handler {:meter-registry (.getMeterRegistry base)
                                     :db-cat db-cat
                                     :initial-target-message-ids initial-target-message-ids
                                     :node node})
                           (j/run-jetty {:host (some-> host (.getHostAddress)), :port port, :async? true, :join? false}))]

    (log/info "Healthz server started at" (str (.getURI server)))

    (reify AutoCloseable
      (close [_]
        (.stop server)
        (log/info "Healthz server stopped.")))))

(defmethod ig/halt-key! :xtdb/healthz [_ srv]
  (util/close srv))
