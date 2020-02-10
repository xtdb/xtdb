(ns crux.metrics.dropwizard.console
  "Console reporting"
  (:require [clojure.string :as string])
  (:import (com.codahale.metrics MetricRegistry
                                 ConsoleReporter
                                 ScheduledReporter)
           (java.util.concurrent TimeUnit)
           (java.io Closeable)))

(defn start-reporter ^Closeable
  [^MetricRegistry reg {::keys [report-rate stream ^java.util.Locale locale clock rate-unit duration-unit metric-filter]}]
  (doto (-> (ConsoleReporter/forRegistry reg)
            (doto
              stream (.outputTo stream)
              locale (.formattedFor locale)
              clock (.withClock clock)
              rate-unit (.convertRatesTo (TimeUnit/of (string/upper-case rate-unit)))
              duration-unit (.convertDurationsTo (TimeUnit/of (string/upper-case duration-unit)))
              metric-filter (.filter metric-filter))
            (.build))
    (.start (or report-rate 1) TimeUnit/SECONDS)))
