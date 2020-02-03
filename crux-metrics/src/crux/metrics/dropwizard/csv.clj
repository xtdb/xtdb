(ns crux.metrics.dropwizard.csv
  (:require [clojure.java.io :as io]
            [crux.metrics.dropwizard :as dropwizard])
  (:import [com.codahale.metrics CsvReporter MetricRegistry ScheduledReporter]
           [java.io Closeable]))

(defn reporter ^CsvReporter
  [^MetricRegistry reg {::keys [locale rate-unit duration-unit metric-filter dir]}]
  (.build (cond-> (CsvReporter/forRegistry reg)
            locale (.formatFor locale)
            rate-unit (.convertRatesTo rate-unit)
            duration-unit (.convertDurationsTo duration-unit)
            metric-filter (.filter metric-filter))
          (io/file dir)))

(defn start-reporter
  [registry {::keys [report-rate] :as args}]
  (dropwizard/start (reporter registry args) (or report-rate 1)))
