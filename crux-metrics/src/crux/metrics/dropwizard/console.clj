(ns crux.metrics.dropwizard.console
  "Console reporting"
  (:import (com.codahale.metrics MetricRegistry ConsoleReporter)
           (java.util.concurrent TimeUnit)
           (java.io Closeable)))

(defn start-reporter ^ConsoleReporter
  [^MetricRegistry reg {::keys [report-rate stream locale clock rate-unit duration-unit metric-filter]}]
  (let [reporter
        (.build
          (cond-> (ConsoleReporter/forRegistry reg)
            stream (.outputTo stream)
            locale (.formattedFor locale)
            clock (.withClock clock)
            rate-unit (.convertRatesTo (TimeUnit/of (.toUpperCase rate-unit)))
            duration-unit (.convertDurationsTo (TimeUnit/of (.toUpperCase duration-unit)))
            metric-filter (.filter metric-filter)))]
    (.start
      reporter
      (or report-rate 1))
    (reify Closeable
      (close [this]
        (.stop reporter)))))
