(ns crux.metrics.dropwizard.csv
  (:require [clojure.java.io :as io])
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
