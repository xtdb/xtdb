(ns crux.metrics.dropwizard.csv
  (:require [crux.metrics :as metrics]
            [clojure.java.io :as io])
  (:import (com.codahale.metrics CsvReporter MetricRegistry)
           (java.io Closeable)
           (java.util.concurrent TimeUnit)
           (java.time Duration)))

(defn start-reporter ^com.codahale.metrics.CsvReporter
  [^MetricRegistry reg {::keys [report-frequency locale rate-unit duration-unit metric-filter file-name]}]

  (-> (CsvReporter/forRegistry reg)
      (cond-> locale (.formatFor locale)
              rate-unit (.convertRatesTo rate-unit)
              duration-unit (.convertDurationsTo duration-unit)
              metric-filter (.filter metric-filter))
      (.build (io/file file-name))
      (doto (.start (.toMillis ^Duration report-frequency) TimeUnit/MILLISECONDS))))

(def reporter
  (merge metrics/registry
         {::reporter {:start-fn (fn [{:crux.metrics/keys [registry]} args]
                                  (start-reporter registry args))
                      :deps #{:crux.metrics/registry :crux.metrics/all-metrics-loaded}
                      :args {::file-name {:doc "Output file name"
                                          :required? true
                                          :crux.config/type :crux.config/string}
                             ::report-frequency {:doc "Frequency of reporting metrics"
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
