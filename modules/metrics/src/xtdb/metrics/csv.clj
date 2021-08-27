(ns xtdb.metrics.csv
  (:require [clojure.java.io :as io]
            [xtdb.metrics :as metrics]
            [xtdb.system :as sys])
  (:import [com.codahale.metrics CsvReporter MetricRegistry]
           java.nio.file.Path
           java.time.Duration
           java.util.concurrent.TimeUnit))

(defn ->reporter {::sys/deps {:registry ::metrics/registry
                              :metrics ::metrics/metrics}
                  ::sys/args {:output-file {:doc "Output file name"
                                            :required? true
                                            :spec ::sys/path}
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
  [{:keys [^MetricRegistry registry report-frequency rate-unit duration-unit ^Path output-file]}]
  (let [output-file (doto (.toFile output-file)
                      (io/make-parents))]
    (-> (CsvReporter/forRegistry registry)
        (cond-> rate-unit (.convertRatesTo rate-unit)
                duration-unit (.convertDurationsTo duration-unit))
        (.build output-file)
        (doto (.start (.toMillis ^Duration report-frequency) TimeUnit/MILLISECONDS)))))
