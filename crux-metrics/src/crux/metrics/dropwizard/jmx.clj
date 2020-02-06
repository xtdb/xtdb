(ns crux.metrics.dropwizard.jmx
  (:import [java.io Closeable]
           [java.util.concurrent TimeUnit]
           [com.codahale.metrics MetricRegistry]
           [com.codahale.metrics.jmx JmxReporter]))

(defn reporter ^JmxReporter
  [^MetricRegistry reg {::keys [domain ^TimeUnit rate-unit ^TimeUnit duration-unit]}]
  (-> (JmxReporter/forRegistry reg)
      (cond-> domain (.inDomain domain)
              rate-unit (.convertRatesTo rate-unit)
              duration-unit (.convertDurationsTo duration-unit))
      .build))

(defn start-reporter
  "Report all metrics via JMX"
  [registry args]
  (let [reporter (reporter registry args)]
    (reify Closeable
      (close [this]
        (.stop reporter)))))
