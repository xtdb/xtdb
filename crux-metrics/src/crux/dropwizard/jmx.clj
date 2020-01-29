(ns crux.dropwizard.jmx
  "JMX reporting"
  (:import java.util.concurrent.TimeUnit
           [com.codahale.metrics JmxReporter MetricRegistry MetricFilter]))

(defn ^com.codahale.metrics.JmxReporter reporter
  [^MetricRegistry reg opts]
  (let [b (JmxReporter/forRegistry reg)]
    (when-let [^String d (:domain opts)]
      (.inDomain b d))
    (when-let [^TimeUnit ru (:rate-unit opts)]
      (.convertRatesTo b ru))
    (when-let [^TimeUnit du (:duration-unit opts)]
      (.convertDurationsTo b du))
    (when-let [^MetricFilter f (:filter opts)]
      (.filter b f))
    (.build b)))

(defn start
  "Report all metrics via JMX"
  [^JmxReporter r]
  (.start ^JmxReporter r))

(defn stop
  "Stop reporting metrics via JMX"
  [^JmxReporter r]
  (.stop ^JmxReporter r))
