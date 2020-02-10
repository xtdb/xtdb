(ns crux.metrics.dropwizard.csv
  (:require [clojure.java.io :as io])
  (:import (com.codahale.metrics CsvReporter MetricRegistry)
           (java.io Closeable)))

(defn start-reporter ^CsvReporter
  [^MetricRegistry reg {::keys [report-rate locale rate-unit duration-unit metric-filter dir]}]
  (let [reporter (.build (cond-> (CsvReporter/forRegistry reg)
                           locale (.formatFor locale)
                           rate-unit (.convertRatesTo rate-unit)
                           duration-unit (.convertDurationsTo duration-unit)
                           metric-filter (.filter metric-filter))
                         (io/file dir))]
    (.start reporter
            (or report-rate 1))
    (reify Closeable
      (close [this]
        (.stop reporter)))))
