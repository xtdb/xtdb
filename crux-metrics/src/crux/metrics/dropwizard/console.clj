(ns crux.metrics.dropwizard.console
  "Console reporting"
  (:import (com.codahale.metrics MetricRegistry
                                 ConsoleReporter
                                 ScheduledReporter)
           (java.util.concurrent TimeUnit)
           (java.io Closeable)))

(defn start-reporter ^Closeable
  [^MetricRegistry reg {::keys [report-rate stream locale clock rate-unit duration-unit metric-filter]}]
  (doto (-> (ConsoleReporter/forRegistry reg) 
            (cond->
              stream (.outputTo stream)
              locale (.formattedFor locale)
              clock (.withClock clock)
              rate-unit (.convertRatesTo (TimeUnit/of (.toUpperCase rate-unit)))
              duration-unit (.convertDurationsTo (TimeUnit/of (.toUpperCase duration-unit)))
              metric-filter (.filter metric-filter))
            (.build))
    (.start (or report-rate 1))))
