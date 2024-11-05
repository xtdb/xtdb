(ns xtdb.healthz
  (:require [clojure.tools.logging :as log]
            [juxt.clojars-mirrors.integrant.core :as ig]
            [reitit.http :as http]
            [reitit.http.coercion :as rh.coercion]
            [reitit.http.interceptors.exception :as ri.exception]
            [reitit.http.interceptors.muuntaja :as ri.muuntaja]
            [reitit.interceptor.sieppari :as r.sieppari]
            [reitit.ring :as r.ring]
            [ring.adapter.jetty9 :as j]
            [xtdb.api :as xt]
            [xtdb.node :as xtn]
            [xtdb.util :as util])
  (:import io.micrometer.core.instrument.composite.CompositeMeterRegistry
           (io.micrometer.prometheus PrometheusMeterRegistry)
           [java.lang AutoCloseable]
           org.eclipse.jetty.server.Server
           (xtdb.api Xtdb$Config)
           (xtdb.api.metrics HealthzConfig)
           xtdb.api.Xtdb$Config
           xtdb.indexer.IIndexer))

(def router
  (http/router [["/metrics" {:name :metrics
                             :get (fn [{:keys [^PrometheusMeterRegistry prometheus-registry]}]
                                    {:status 200, :body (.scrape prometheus-registry)})}]]
               {:data {:interceptors [[ri.exception/exception-interceptor
                                       (merge ri.exception/default-handlers
                                              {::ri.exception/wrap (fn [handler e req]
                                                                     (log/debug e (format "response error (%s): '%s'" (class e) (ex-message e)))
                                                                     (handler e req))})]

                                      [ri.muuntaja/format-request-interceptor]
                                      [rh.coercion/coerce-request-interceptor]]}}))

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
   :metrics-registry (ig/ref :xtdb.metrics/registry)})

(defmethod ig/init-key :xtdb/healthz [_ {:keys [^long port, ^CompositeMeterRegistry metrics-registry]}]
  (let [prometheus-registry (PrometheusMeterRegistry. io.micrometer.prometheus.PrometheusConfig/DEFAULT)
        ^Server server (j/run-jetty (handler {:prometheus-registry prometheus-registry})
                                    {:port port, :async? true, :join? false})]

    (.add metrics-registry prometheus-registry)

    (log/info "Healthz server started on port:" port)

    (reify AutoCloseable
      (close [_]
        (.stop server)
        (log/info "Healthz server stopped.")))))

(defmethod ig/halt-key! :xtdb/healthz [_ srv]
  (util/close srv))
