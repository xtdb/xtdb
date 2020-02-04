(ns crux.metrics.dropwizard.prometheus
  (:require [crux.metrics.dropwizard :as dropwizard])
  (:import [org.dhatim.dropwizard.prometheus PrometheusReporter]
           [io.prometheus.client.exporter PushGateway]
           [java.util.concurrent TimeUnit]
           [java.io Closeable]))

(defn reporter ^PrometheusReporter

  [^MetricRegistry reg {::keys [prefix metric-filter pushgateway]}]
  (let [pushgateway (PushGateway. (or pushgateway "localhost:9091"))]
    {:pushgateway pushgateway
     :reporter (.build (cond-> (PrometheusReporter/forRegistry reg)
                         prefix (.prefixedWith prefix)
                         metric-filter (.filter metric-filter))
                       pushgateway)}))
(defn start

  ([^PrometheusReporter reporter seconds]
   (.start reporter seconds (TimeUnit/SECONDS))
   (reify Closeable
     (close [this]
       (.stop reporter))))

  ([^PrometheusReporter reporter length ^TimeUnit unit]
   (.start reporter length unit)
   (reify Closeable
     (close [this]
       (.stop reporter)))))
