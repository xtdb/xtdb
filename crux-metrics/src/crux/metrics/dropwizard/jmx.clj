(ns crux.metrics.dropwizard.jmx
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
