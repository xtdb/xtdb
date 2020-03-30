(ns crux.metrics.dropwizard.prometheus
  (:require [crux.metrics :as metrics]
            [crux.metrics.dropwizard :as dropwizard]
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

(def reporter
  (merge metrics/registry
         {::reporter {:start-fn (fn [{:crux.metrics/keys [registry]}
                                     args]
                                  (start-reporter registry args))
                      :deps #{:crux.metrics/registry :crux.metrics/all-metrics-loaded}
                      :args {::report-frequency {:doc "Frequency of reporting metrics"
                                                 :default (Duration/ofSeconds 1)
                                                 :crux.config/type :crux.config/duration}
                             ::prefix {:doc "Prefix all metrics with this string"
                                       :required? false
                                       :crux.config/type :crux.config/string}
                             ::push-gateway {:doc "Address of the prometheus server"
                                             :required? true
                                             :crux.config/type :crux.config/string}}}}))

(def http-exporter
  (merge metrics/registry
         {::http-exporter {:start-fn (fn [{:crux.metrics/keys [registry]} args]
                                       (start-http-exporter registry args))
                           :deps #{:crux.metrics/registry :crux.metrics/all-metrics-loaded}
                           :args {::port {:doc "Port for prometheus exporter server"
                                          :default 8080
                                          :crux.config/type :crux.config/int}
                                  ::jvm-metrics? {:doc "Dictates if jvm metrics are exported"
                                                  :default false
                                                  :crux.config/type :crux.config/boolean}}}}))
