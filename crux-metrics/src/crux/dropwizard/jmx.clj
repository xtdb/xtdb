(ns crux.dropwizard.jmx
  (:import java.util.concurrent.TimeUnit
           [com.codahale.metrics MetricRegistry]
           [com.codahale.metrics.jmx JmxReporter]))

(defn ^com.codahale.metrics.jmx.JmxReporter reporter
  [^MetricRegistry reg opts]
  (let [b (JmxReporter/forRegistry reg)]
    (when-let [^String d (:domain opts)]
      (.inDomain b d))
    (when-let [^TimeUnit ru (:rate-unit opts)]
      (.convertRatesTo b ru))
    (when-let [^TimeUnit du (:duration-unit opts)]
      (.convertDurationsTo b du))
    (.build b)))

(defn start
  "Report all metrics via JMX"
  [^JmxReporter r]
  (.start ^JmxReporter r))

(defn stop
  "Stop reporting metrics via JMX"
  [^JmxReporter r]
  (.stop ^JmxReporter r))
