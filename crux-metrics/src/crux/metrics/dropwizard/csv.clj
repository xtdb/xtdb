(ns crux.metrics.dropwizard.csv
  (:require [clojure.java.io :as io]
            [crux.metrics.dropwizard :as dropwizard])
  (:import [com.codahale.metrics CsvReporter MetricRegistry]))

(defn start-reporter ^CsvReporter
  [^MetricRegistry reg {::keys [report-rate locale rate-unit duration-unit metric-filter dir]}]
  (dropwizard/start-reporter
    (.build (cond-> (CsvReporter/forRegistry reg)
              locale (.formatFor locale)
              rate-unit (.convertRatesTo rate-unit)
              duration-unit (.convertDurationsTo duration-unit)
              metric-filter (.filter metric-filter))
            (io/file dir))
    (or report-rate 1)))
