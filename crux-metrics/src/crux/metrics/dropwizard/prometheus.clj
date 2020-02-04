(ns crux.metrics.dropwizard.prometheus
  (:import [org.dhatim.dropwizard.prometheus PrometheusReporter]
           [io.prometheus.client.exporter PushGateway]
           [java.util.concurrent TimeUnit]
           [java.time Duration]
           [java.io Closeable]
           [com.codahale.metrics MetricRegistry]))

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
