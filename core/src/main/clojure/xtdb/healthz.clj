(ns xtdb.healthz
  (:require [clojure.tools.logging :as log]
            [integrant.core :as ig]
            [reitit.http :as http]
            [reitit.http.interceptors.exception :as ri.exception]
            [reitit.interceptor.sieppari :as r.sieppari]
            [reitit.ring :as r.ring]
            [ring.adapter.jetty9 :as j]
            [xtdb.api :as xt]
            [xtdb.node :as xtn]
            [xtdb.util :as util])
  (:import io.micrometer.core.instrument.composite.CompositeMeterRegistry
           (io.micrometer.prometheusmetrics PrometheusConfig PrometheusMeterRegistry)
           [java.lang AutoCloseable]
           org.eclipse.jetty.server.Server
           xtdb.BufferPoolKt
           (xtdb.api Xtdb$Config) 
           (xtdb.api.metrics HealthzConfig)
           xtdb.api.Xtdb$Config
           (xtdb.indexer LiveIndex LogProcessor)))

(defn get-ingestion-error [^LogProcessor log-processor]
  (.getIngestionError log-processor))

(defn- ->block-lag [{:keys [buffer-pool ^LiveIndex live-index]}]
  ;; we could add a gauge for this too
  (max 0 (- (BufferPoolKt/getLatestAvailableBlockIndex buffer-pool)
            (.getLatestBlockIndex live-index))))

(def router
  (http/router [["/metrics" {:name :metrics
                             :get (fn [{:keys [^PrometheusMeterRegistry meter-registry]}]
                                    {:status 200,
                                     :headers {"Content-Type" "text/plain; version=0.0.4"}
                                     :body (.scrape meter-registry)})}]

                ["/healthz/started" {:name :started
                                     :get (fn [{:keys [^long initial-target-message-id, ^LogProcessor log-processor]}]
                                            (let [lpm-id (.getLatestProcessedMsgId log-processor)]
                                              (into {:headers {"X-XTDB-Target-Message-Id" (str initial-target-message-id)
                                                               "X-XTDB-Current-Message-Id" (str lpm-id)}}
                                                    (if (< lpm-id initial-target-message-id)
                                                      {:status 503,
                                                       :body (format "Catching up - at: %d, target: %d" lpm-id initial-target-message-id)}

                                                      {:status 200,
                                                       :body "Started."}))))}]

                ["/healthz/alive" {:name :alive
                                   :get (fn [{:keys [log-processor] :as ctx}]
                                          (or (when-let [ingestion-error (get-ingestion-error log-processor)]
                                                {:status 503, :body (str "Ingestion error - " ingestion-error)})

                                              (let [block-lag (->block-lag ctx)
                                                    block-lag-healthy? (<= block-lag 5)]
                                                (-> (if block-lag-healthy?
                                                      {:status 200, :body "Alive."}
                                                      {:status 503, :body "Unhealthy - see headers for more info."})
                                                    (assoc :headers {"X-XTDB-Block-Lag" (str block-lag)
                                                                     "X-XTDB-Block-Lag-Healthy" (str block-lag-healthy?)})))))}]

                ["/healthz/ready" {:name :ready
                                   :get (fn [_] {:status 200, :body "Ready."})}]]

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

(defmethod xtn/apply-config! :xtdb/healthz [^Xtdb$Config config _ {:keys [^long port]}]
  (.healthz config (HealthzConfig. port)))

(defmethod ig/prep-key :xtdb/healthz [_ ^HealthzConfig config]
  {:port (.getPort config) 
   :meter-registry (ig/ref :xtdb.metrics/registry)
   :log-processor (ig/ref :xtdb.log/processor)
   :buffer-pool (ig/ref :xtdb/buffer-pool)
   :live-index (ig/ref :xtdb.indexer/live-index)
   :node (ig/ref :xtdb/node)})

(defmethod ig/init-key :xtdb/healthz [_ {:keys [node, ^long port, meter-registry, ^LogProcessor log-processor, buffer-pool live-index]}]
  (let [^Server server (-> (handler {:metrics-registry meter-registry
                                     :log-processor log-processor
                                     :buffer-pool buffer-pool
                                     :live-index live-index
                                     :initial-target-message-id (.getLatestSubmittedMsgId log-processor)
                                     :node node})
                           (j/run-jetty {:port port, :async? true, :join? false}))]

    (log/info "Healthz server started on port:" port)

    (reify AutoCloseable
      (close [_]
        (.stop server)
        (log/info "Healthz server stopped.")))))

(defmethod ig/halt-key! :xtdb/healthz [_ srv]
  (util/close srv))
