(ns crux.metrics.dropwizard.prometheus
  (:require [crux.metrics.dropwizard :as dropwizard]
            [iapetos.core :as prometheus]
            [iapetos.collector.jvm :as jvm]
            [iapetos.standalone :as server])
  (:import [org.dhatim.dropwizard.prometheus PrometheusReporter Pushgateway]
           [java.util.concurrent TimeUnit]
           [java.time Duration]
           [java.io Closeable]
           [com.codahale.metrics MetricRegistry]
           [io.prometheus.client.dropwizard DropwizardExports]))

(defn start-reporter [registry {::keys [prefix metric-filter push-gateway report-frequency]}]
  (-> (PrometheusReporter/forRegistry registry)
      (cond-> prefix (.prefixedWith prefix)
              metric-filter (.filter metric-filter))
      (.build (Pushgateway. push-gateway))
      (doto (.start (.toMillis ^Duration report-frequency) (TimeUnit/MILLISECONDS)))))

(defn start-http-exporter [^MetricRegistry registry {::keys [port jvm-metrics?]}]
  (-> (prometheus/collector-registry)
      (prometheus/register (DropwizardExports. registry))
      (cond-> jvm-metrics? (jvm/initialize))
      (server/metrics-server {:port port})))
