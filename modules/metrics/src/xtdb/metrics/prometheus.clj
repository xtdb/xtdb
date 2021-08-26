(ns xtdb.metrics.prometheus
  (:require [xtdb.metrics :as metrics]
            [crux.system :as sys]
            [iapetos.collector.jvm :as jvm]
            [iapetos.core :as prometheus]
            [iapetos.standalone :as server])
  (:import com.codahale.metrics.MetricRegistry
           io.prometheus.client.dropwizard.DropwizardExports
           java.time.Duration
           java.util.concurrent.TimeUnit
           [org.dhatim.dropwizard.prometheus PrometheusReporter Pushgateway]))

(defn ->reporter {::sys/deps {:registry ::metrics/registry
                              :metrics ::metrics/metrics}
                  ::sys/args {:report-frequency {:doc "Frequency of reporting metrics"
                                                 :default (Duration/ofSeconds 1)
                                                 :spec ::sys/duration}
                              :prefix {:doc "Prefix all metrics with this string"
                                       :required? false
                                       :spec ::sys/string}
                              :push-gateway {:doc "Address of the prometheus server"
                                             :required? true
                                             :spec ::sys/string}}}
  [{:keys [registry prefix metric-filter push-gateway report-frequency]}]
  (-> (PrometheusReporter/forRegistry registry)
      (cond-> prefix (.prefixedWith prefix)
              metric-filter (.filter metric-filter))
      (.build (Pushgateway. push-gateway))
      (doto (.start (.toMillis ^Duration report-frequency) (TimeUnit/MILLISECONDS)))))

(defn ->http-exporter {::sys/deps {:registry ::metrics/registry
                                   :metrics ::metrics/metrics}
                       ::sys/args {:port {:doc "Port for prometheus exporter server"
                                          :default 8080
                                          :spec ::sys/int}
                                   :jvm-metrics? {:doc "Dictates if JVM metrics are exported"
                                                  :default false
                                                  :spec ::sys/boolean}}}
  [{:keys [^MetricRegistry registry port jvm-metrics?]}]
  (-> (prometheus/collector-registry)
      (prometheus/register (DropwizardExports. registry))
      (cond-> jvm-metrics? (jvm/initialize))
      (server/metrics-server {:port port})))
