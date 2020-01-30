(ns crux.metrics.dropwizard.console
  "Console reporting"
  (:import java.util.concurrent.TimeUnit
           [com.codahale.metrics MetricRegistry ScheduledReporter ConsoleReporter Clock MetricFilter]
           java.io.PrintStream
           java.util.Locale))

(defn reporter ^ConsoleReporter
  [^MetricRegistry reg opts]
  (let [b (ConsoleReporter/forRegistry reg)]
    (when-let [^PrintStream s (:stream opts)]
      (.outputTo b s))
    (when-let [^Locale l (:locale opts)]
      (.formattedFor b l))
    (when-let [^Clock c (:clock opts)]
      (.withClock b c))
    (when-let [^TimeUnit ru (:rate-unit opts)]
      (.convertRatesTo b ru))
    (when-let [^TimeUnit du (:duration-unit opts)]
      (.convertDurationsTo b du))
    (when-let [^MetricFilter f (:filter opts)]
      (.filter b f))
    (.build b)))

(defn start
  "Report all metrics to standard out periodically"
  [^ScheduledReporter r ^long seconds]
  (.start r seconds))

(defn stop
  "Stops reporting all metrics to standard out."
  [^ScheduledReporter r]
  (.stop r))
