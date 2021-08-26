(ns xtdb.metrics.console
  (:require [xtdb.metrics :as metrics]
            [clojure.string :as string]
            [crux.system :as sys])
  (:import (com.codahale.metrics MetricRegistry ConsoleReporter ScheduledReporter)
           (java.util Locale)
           (java.util.concurrent TimeUnit)
           (java.time Duration)
           (java.io Closeable)))

(defn ->reporter {::sys/deps {:registry ::metrics/registry
                              :metrics ::metrics/metrics}
                  ::sys/args {:report-frequency {:doc "Frequency of reporting metrics"
                                                 :default (Duration/ofSeconds 1)
                                                 :spec ::sys/duration}
                              :rate-unit {:doc "Set rate unit"
                                          :required? false
                                          :default TimeUnit/SECONDS
                                          :spec ::sys/time-unit}
                              :duration-unit {:doc "Set duration unit"
                                              :required? false
                                              :default TimeUnit/MILLISECONDS
                                              :spec ::sys/time-unit}}}
  ^com.codahale.metrics.ConsoleReporter
  [{:keys [^MetricRegistry registry stream metric-filter locale clock report-frequency rate-unit duration-unit]}]

  (-> (ConsoleReporter/forRegistry registry)
      (cond-> stream (.outputTo stream)
              locale (.formattedFor ^Locale locale)
              clock (.withClock clock)
              rate-unit (.convertRatesTo rate-unit)
              duration-unit (.convertDurationsTo duration-unit)
              metric-filter (.filter metric-filter))
      (.build)
      (doto (.start (.toMillis ^Duration report-frequency) TimeUnit/MILLISECONDS))))
