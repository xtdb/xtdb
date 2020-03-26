(ns crux.metrics.dropwizard.console
  (:require [crux.metrics :as metrics]
            [clojure.string :as string])
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


(def reporter
  (merge metrics/registry
         {::reporter {:start-fn (fn [{:crux.metrics/keys [registry] :as opts} args]
                                  (start-reporter registry args))
                      :deps #{:crux.metrics/registry :crux.metrics/all-metrics-loaded}
                      :args {::report-frequency {:doc "Frequency of reporting metrics"
                                                 :default (Duration/ofSeconds 1)
                                                 :crux.config/type :crux.config/duration}
                             ::rate-unit {:doc "Set rate unit"
                                          :required? false
                                          :default TimeUnit/SECONDS
                                          :crux.config/type :crux.config/time-unit}
                             ::duration-unit {:doc "Set duration unit"
                                              :required? false
                                              :default TimeUnit/MILLISECONDS
                                              :crux.config/type :crux.config/time-unit}}}}))
