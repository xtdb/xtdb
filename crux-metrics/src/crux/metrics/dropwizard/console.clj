(ns crux.metrics.dropwizard.console
  (:require [clojure.string :as string])
  (:import (com.codahale.metrics MetricRegistry ConsoleReporter ScheduledReporter)
           (java.util Locale)
           (java.util.concurrent TimeUnit)
           (java.time Duration)
           (java.io Closeable)))

(defn start-reporter ^com.codahale.metrics.ConsoleReporter
  [^MetricRegistry reg {::keys [stream metric-filter locale clock report-frequency rate-unit duration-unit]}]

  (-> (ConsoleReporter/forRegistry reg)
      (cond-> stream (.outputTo stream)
              locale (.formattedFor ^Locale locale)
              clock (.withClock clock)
              rate-unit (.convertRatesTo rate-unit)
              duration-unit (.convertDurationsTo duration-unit)
              metric-filter (.filter metric-filter))
      (.build)
      (doto (.start (.toMillis ^Duration report-frequency) TimeUnit/MILLISECONDS))))
