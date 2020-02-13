(ns crux.metrics.dropwizard.cloudwatch
  (:require [clojure.string :as string])
  (:import [io.github.azagniotov.metrics.reporter.cloudwatch CloudWatchReporter]
           [java.io Closeable]
           [software.amazon.awssdk.services.cloudwatch CloudWatchAsyncClient CloudWatchAsyncClientBuilder]
           [software.amazon.awssdk.regions Region]
           [java.util.concurrent TimeUnit]
           [java.time Duration]
           [com.codahale.metrics MetricRegistry MetricFilter]))

(defn- include-metric? [metric-name ignore-rules]
  (reduce (fn [include? ignore-rule]
            (let [[_ prefix body] (re-matches #"(!)?(.+)" ignore-rule)]
              (if (or (= "*" body)
                      (= metric-name body)
                      (string/starts-with? metric-name (str body ".")))
                (= prefix "!")
                include?)))
          true
          ignore-rules))

(defn start-reporter
  [^MetricRegistry reg {::keys [region dry-run? jvm-metrics? dimensions
                                dry-run-report-frequency high-resolution? ignore-rules]
                        :as opts}]

  (let [cw-client (-> (CloudWatchAsyncClient/builder)
                      (cond-> region ^CloudWatchAsyncClientBuilder (.region (Region/of region)))
                      (.build))]

    (-> (CloudWatchReporter/forRegistry reg cw-client "crux.metrics.dropwizard.cloudwatch")
        (cond-> jvm-metrics? .withJvmMetrics
                dry-run? .withDryRun
                high-resolution? .withHighResolution

                (seq ignore-rules) (.filter (reify MetricFilter
                                              (matches [_ metric-name _]
                                                (include-metric? metric-name ignore-rules))))

                dimensions (.withGlobalDimensions (->> dimensions
                                                       (map (fn [[k v]] (format "%s=%s" k v)))
                                                       (into-array String))))
        (.withMeterUnitSentToCW)
        (.build)
        (doto (.start (.toMillis ^Duration dry-run-report-frequency) TimeUnit/MILLISECONDS)))))
