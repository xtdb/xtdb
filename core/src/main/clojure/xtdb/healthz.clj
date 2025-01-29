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
            [xtdb.protocols :as xtp]
            [xtdb.util :as util])
  (:import io.micrometer.core.instrument.composite.CompositeMeterRegistry
           (io.micrometer.prometheusmetrics PrometheusConfig PrometheusMeterRegistry)
           [java.lang AutoCloseable]
           org.eclipse.jetty.server.Server
           (xtdb.api Xtdb$Config)
           (xtdb.api.metrics HealthzConfig)
           xtdb.api.Xtdb$Config
           xtdb.indexer.LogProcessor))

(defn get-ingestion-error [^LogProcessor log-processor]
  (.getIngestionError log-processor))

(def router
  (http/router [["/metrics" {:name :metrics
                             :get (fn [{:keys [^PrometheusMeterRegistry prometheus-registry]}]
                                    {:status 200,
                                     :headers {"Content-Type" "text/plain; version=0.0.4"}
                                     :body (.scrape prometheus-registry)})}]

                ["/healthz/started" {:name :started
                                     :get (fn [{:keys [node, ^long initial-target-tx-id]}]
                                            (let [^long lc-tx-id (:tx-id (xtp/latest-completed-tx node) -1)]
                                              (if (< lc-tx-id initial-target-tx-id)
                                                {:status 503,
                                                 :body (format "Catching up - at: %d, target: %d" lc-tx-id initial-target-tx-id)}
                                            
                                                {:status 200, :body "Started."})))}]

                ["/healthz/alive" {:name :alive
                                   :get (fn [{:keys [log-processor]}]
                                          (if-let [ingestion-error (get-ingestion-error log-processor)]
                                            {:status 503, :body (str "Ingestion error - " ingestion-error)}
                                            {:status 200, :body "Alive."}))}]

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
   :metrics-registry (ig/ref :xtdb.metrics/registry)
   :log-processor (ig/ref :xtdb.log/processor)
   :node (ig/ref :xtdb/node)})

(defmethod ig/init-key :xtdb/healthz [_ {:keys [node, ^long port, ^CompositeMeterRegistry metrics-registry, log-processor]}]
  (let [prometheus-registry (PrometheusMeterRegistry. PrometheusConfig/DEFAULT)
        ^Server server (-> (handler {:prometheus-registry prometheus-registry
                                     :log-processor log-processor
                                     :initial-target-tx-id (xtp/latest-submitted-tx-id node)
                                     :node node})
                           (j/run-jetty {:port port, :async? true, :join? false}))]
    (.add metrics-registry prometheus-registry) 

    (log/info "Healthz server started on port:" port)

    (reify AutoCloseable
      (close [_]
        (.stop server)
        (log/info "Healthz server stopped.")))))

(defmethod ig/halt-key! :xtdb/healthz [_ srv]
  (util/close srv))
