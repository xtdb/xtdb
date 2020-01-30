(ns crux.metrics.dropwizard.csv
  (:require [clojure.java.io :as io])
  (:import java.util.concurrent.TimeUnit
           [com.codahale.metrics CsvReporter MetricRegistry MetricFilter ScheduledReporter]
           java.util.Locale))

(defn- validate-create-output-dir
  [^java.io.File d]
     (when-not (.exists d)
       (.mkdirs d))
     (when-not (.canWrite d)
       (throw (java.io.IOException. (str "Don't have write permissions to " d))))
     (when-not (.isDirectory d)
       (throw (java.io.IOException. (str d " is not a directory.")))))

(defn reporter ^CsvReporter
  [^MetricRegistry reg dir opts]
  (let [b (CsvReporter/forRegistry reg)
        d (io/file dir)]
    (validate-create-output-dir d)
    (when-let [^Locale l (:locale opts)]
      (.formatFor b l))
    (when-let [^TimeUnit ru (:rate-unit opts)]
      (.convertRatesTo b ru))
    (when-let [^TimeUnit du (:duration-unit opts)]
      (.convertDurationsTo b du))
    (when-let [^MetricFilter f (:filter opts)]
      (.filter b f))
    (.build b d)))

(defn start
  "Report all metrics to csv"
  [^ScheduledReporter r ^long seconds]
  (.start r seconds))

(defn stop
  "Stops reporting."
  [^ScheduledReporter r]
  (.stop r))
