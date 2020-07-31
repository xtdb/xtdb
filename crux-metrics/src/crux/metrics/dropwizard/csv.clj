(ns crux.metrics.dropwizard.csv
  (:require [clojure.java.io :as io]
            [crux.metrics :as metrics]
            [crux.system :as sys])
  (:import [com.codahale.metrics CsvReporter MetricRegistry]
           java.time.Duration
           java.util.concurrent.TimeUnit))

(defn ->reporter {::sys/deps {:registry ::metrics/registry
                              :metrics ::metrics/metrics}
                  ::sys/args {:file-name {:doc "Output file name"
                                          :required? true
                                          :spec ::sys/string}
                              :report-frequency {:doc "Frequency of reporting metrics"
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
  ^com.codahale.metrics.CsvReporter
  [{:keys [^MetricRegistry registry report-frequency locale rate-unit duration-unit metric-filter file-name]}]

  (-> (CsvReporter/forRegistry registry)
      (cond-> locale (.formatFor locale)
              rate-unit (.convertRatesTo rate-unit)
              duration-unit (.convertDurationsTo duration-unit)
              metric-filter (.filter metric-filter))
      (.build (io/file file-name))
      (doto (.start (.toMillis ^Duration report-frequency) TimeUnit/MILLISECONDS))))
