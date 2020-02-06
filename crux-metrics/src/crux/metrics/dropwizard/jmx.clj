(ns crux.metrics.dropwizard.jmx
  (:import [java.io Closeable]
           [java.util.concurrent TimeUnit]
           [com.codahale.metrics MetricRegistry]
           [com.codahale.metrics.jmx JmxReporter]))
(TimeUnit/valueOf (.toUpperCase "seconds"))

(defn reporter ^JmxReporter
  [^MetricRegistry reg {::keys [domain rate-unit duration-unit]}]
  (-> (JmxReporter/forRegistry reg)
      (cond-> domain (.inDomain domain)
              rate-unit (.convertRatesTo (TimeUnit/valueOf (.toUpperCase rate-unit)))
              duration-unit (.convertDurationsTo (TimeUnit/valueOf (.toUpperCase duration-unit))))
      .build))

(defn start-reporter
  "Report all metrics via JMX"
  [registry args]
  (let [reporter (reporter registry args)]
    (reify Closeable
      (close [this]
        (.stop reporter)))))
