(ns crux.metrics.dropwizard.console
  "Console reporting"
  (:require [crux.metrics.dropwizard :as dropwizard])
  (:import [com.codahale.metrics MetricRegistry ConsoleReporter]))

(defn reporter ^ConsoleReporter
  [^MetricRegistry reg {::keys [stream locale clock rate-unit duration-unit metric-filter]}]
  (.build
    (cond-> (ConsoleReporter/forRegistry reg)
      stream (.outputTo stream)
      locale (.formattedFor locale)
      clock (.withClock clock)
      rate-unit (.convertRatesTo rate-unit)
      duration-unit (.convertDurationsTo duration-unit)
      metric-filter (.filter metric-filter))))

(defn start-reporter
  [registry {::keys [report-rate] :as args}]
  (dropwizard/start (reporter registry args) (or report-rate 1)))
