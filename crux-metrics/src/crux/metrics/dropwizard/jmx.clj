(ns crux.metrics.dropwizard.jmx
  (:require [crux.metrics :as metrics])
  (:import (java.io Closeable)
           (java.util.concurrent TimeUnit)
           (java.time Duration)
           (com.codahale.metrics MetricRegistry)
           (com.codahale.metrics.jmx JmxReporter)))

(defn start-reporter ^com.codahale.metrics.jmx.JmxReporter
  [^MetricRegistry reg {::keys [domain rate-unit duration-unit]}]

  (-> (JmxReporter/forRegistry reg)
      (cond-> domain (.inDomain domain)
              rate-unit (.convertRatesTo rate-unit)
              duration-unit (.convertDurationsTo duration-unit))
      .build
      (doto (.start))))

(def reporter
  (merge metrics/registry
         {::reporter {:start-fn (fn [{:crux.metrics/keys [registry]} args]
                                  (start-reporter registry args))
                      :deps #{:crux.metrics/registry :crux.metrics/all-metrics-loaded}
                      :args {::domain {:doc "Add custom domain"
                                       :required? false
                                       :crux.config/type :crux.config/string}
                             ::rate-unit {:doc "Set rate unit"
                                          :required? false
                                          :default TimeUnit/SECONDS
                                          :crux.config/type :crux.config/time-unit}
                             ::duration-unit {:doc "Set duration unit"
                                              :required? false
                                              :default TimeUnit/MILLISECONDS
                                              :crux.config/type :crux.config/time-unit}}}}))
