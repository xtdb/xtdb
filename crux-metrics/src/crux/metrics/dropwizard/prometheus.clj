(ns crux.metrics.dropwizard.prometheus
  (:require [crux.metrics.dropwizard :as dropwizard]
            [iapetos.core :as prometheus]
            [iapetos.collector.jvm :as jvm]
            [iapetos.standalone :as server])
  (:import [org.dhatim.dropwizard.prometheus PrometheusReporter]
           [io.prometheus.client.exporter PushGateway]
           [java.util.concurrent TimeUnit]
           [java.time Duration]
           [java.io Closeable]
           [com.codahale.metrics MetricRegistry]
           [io.prometheus.client.dropwizard DropwizardExports]))

(defn reporter ^PrometheusReporter

  [^MetricRegistry reg {::keys [prefix metric-filter pushgateway]}]
  (let [pushgateway (PushGateway. pushgateway)]
    {:pushgateway pushgateway
     :reporter (.build (cond-> (PrometheusReporter/forRegistry reg)
                         prefix (.prefixedWith prefix)
                         metric-filter (.filter metric-filter))
                       pushgateway)}))
(defn start-reporter

  [registry {::keys [duration] :as args}]
  (.start (reporter registry args) (.toMillis (Duration/parse duration)) (TimeUnit/MILLISECONDS))
  (reify Closeable
    (close [this]
      (.stop reporter))))

(defn prometheus-registry ^iapetos.registry.IapetosRegistry

  [^MetricRegistry registry {::keys [jvm-metrics?]}]
  (cond-> (-> (prometheus/collector-registry)
              (prometheus/register (DropwizardExports. registry)))
    jvm-metrics? (jvm/initialize)))

(defn server

  [^iapetos.registry.IapetosRegistry prometheus-registry {::keys [server-port]}]
  (server/metrics-server prometheus-registry {:port (or server-port 8080)}))

(defn start-exporter
  
  [registry args]
  (-> registry
      (prometheus-registry args)
      (server args)))
