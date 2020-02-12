(ns crux.metrics.dropwizard.cloudwatch
  (:require [clojure.string :as string])
  (:import [io.github.azagniotov.metrics.reporter.cloudwatch CloudWatchReporter]
           [java.io Closeable]
           [software.amazon.awssdk.services.cloudwatch CloudWatchAsyncClient CloudWatchAsyncClientBuilder]
           [software.amazon.awssdk.regions Region]
           [java.util.concurrent TimeUnit]
           [java.time Duration]
           [com.codahale.metrics MetricRegistry MetricFilter]))

(defn start-reporter
  [^MetricRegistry reg {::keys [region dry-run? jvm-metrics? dimensions
                                report-frequency include-metrics
                                exclude-metrics high-resolution?]}]

  (let [cw-client (-> (CloudWatchAsyncClient/builder)
                      (cond-> region ^CloudWatchAsyncClientBuilder (.region (Region/of region)))
                      (.build))]

    (-> (CloudWatchReporter/forRegistry reg cw-client "crux.metrics.dropwizard.cloudwatch")
        (cond-> jvm-metrics? .withJvmMetrics
                dry-run? .withDryRun
                high-resolution? .withHighResolution
                exclude-metrics (.filter (reify MetricFilter
                                           (matches [_ metric-name _]
                                             (every?
                                               #(not (string/includes? metric-name %))
                                               exclude-metrics))))
                include-metrics (.filter (reify MetricFilter
                                           (matches [_ metric-name _]
                                             (some?
                                               #(string/includes? metric-name %)
                                               exclude-metrics))))
                dimensions (.withGlobalDimensions (->> dimensions
                                                       (map (fn [[k v]] (format "%s=%s" k v)))
                                                       (into-array String))))
        (.withMeterUnitSentToCW)
        (.build)
        (doto (.start (.toMillis ^Duration report-frequency) TimeUnit/MILLISECONDS)))))
