(ns crux.metrics.dropwizard.jmx
  (:import [java.io Closeable]
           [com.codahale.metrics MetricRegistry]
           [com.codahale.metrics.jmx JmxReporter]))

(defn reporter ^JmxReporter
  [^MetricRegistry reg {::keys [domain rate-unit duration-unit]}]
  (.build
    (cond-> (JmxReporter/forRegistry reg)
      domain (.inDomain domain)
      rate-unit (.convertRatesTo rate-unit)
      duration-unit (.convertDurationsTo duration-unit))))

(defn stop
  "Stop reporting metrics via JMX"
  [^JmxReporter r]
  (.stop ^JmxReporter r))

(defn start
  "Report all metrics via JMX"
  [^JmxReporter r]
  (let [reporter (.start ^JmxReporter r)]
    (reify Closeable
      (close [this]
        (stop reporter)))))

(defn start-reporter
  [registry args]
  (start (reporter registry args)))
