(ns crux.metrics.dropwizard.console
  "Console reporting"
  (:require [crux.metrics.dropwizard :as dropwizard])
  (:import [com.codahale.metrics MetricRegistry ConsoleReporter]
           [java.util.concurrent TimeUnit]))

(defn start-reporter ^ConsoleReporter
  [^MetricRegistry reg {::keys [report-rate stream locale clock rate-unit duration-unit metric-filter]}]
  (dropwizard/start-reporter
    (.build
      (cond-> (ConsoleReporter/forRegistry reg)
        stream (.outputTo stream)
        locale (.formattedFor locale)
        clock (.withClock clock)
        rate-unit (.convertRatesTo (TimeUnit/of (.toUpperCase rate-unit)))
        duration-unit (.convertDurationsTo (TimeUnit/of (.toUpperCase duration-unit)))
        metric-filter (.filter metric-filter)))
    (or report-rate 1)))
