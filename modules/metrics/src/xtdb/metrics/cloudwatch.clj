(ns xtdb.metrics.cloudwatch
  (:require [clojure.string :as string]
            [xtdb.metrics :as metrics]
            [xtdb.system :as sys])
  (:import [com.codahale.metrics MetricFilter MetricRegistry]
           [io.github.azagniotov.metrics.reporter.cloudwatch CloudWatchReporter CloudWatchReporter$Percentile]
           java.time.Duration
           java.util.concurrent.TimeUnit
           software.amazon.awssdk.regions.Region
           [software.amazon.awssdk.services.cloudwatch CloudWatchAsyncClient CloudWatchAsyncClientBuilder]))

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

(defn ->reporter {::sys/deps {:registry ::metrics/registry
                              :metrics ::metrics/metrics}
                  ::sys/args {:region {:doc "Region for uploading metrics. Tries to get it using api. If this fails, you will need to specify region."
                                       :required? false
                                       :spec ::sys/string}
                              :dry-run-report-frequency {:doc "Frequency of reporting metrics on a dry run"
                                                         :default (Duration/ofSeconds 1)
                                                         :spec ::sys/duration}
                              :dry-run? {:doc "When true, the reporter prints to console instead of uploading to cw"
                                         :required? false
                                         :spec ::sys/boolean}
                              :jvm-metrics? {:doc "When true, include jvm metrics for upload"
                                             :required? false
                                             :spec ::sys/boolean}
                              :dimensions {:doc "Add global dimensions to metrics"
                                           :required? false
                                           :spec ::sys/string-map}
                              :high-resolution? {:doc "Increase the push rate from 1 minute to 1 second"
                                                 :default false
                                                 :spec ::sys/boolean}
                              :ignore-rules {:doc "An ordered list of ignore rules for metrics, using gitignore syntax. e.g.
[\"xtdb.tx\" \"!xtdb.tx.ingest-rate\"] -> exclude xtdb.tx.*, but keep xtdb.tx.ingest-rate"
                                             :required? false
                                             :spec ::sys/string-list}}}
  [{:keys [^MetricRegistry registry
           region dry-run? jvm-metrics? dimensions
           dry-run-report-frequency high-resolution? ignore-rules]}]
  (let [cw-client (-> (CloudWatchAsyncClient/builder)
                      (cond-> region ^CloudWatchAsyncClientBuilder (.region (Region/of region)))
                      (.build))]

    (-> (CloudWatchReporter/forRegistry registry cw-client "xtdb.metrics.cloudwatch")
        (cond-> jvm-metrics? .withJvmMetrics
                dry-run? .withDryRun
                high-resolution? .withHighResolution

                (seq ignore-rules) (.filter (reify MetricFilter
                                              (matches [_ metric-name _]
                                                (include-metric? metric-name ignore-rules))))

                dimensions (.withGlobalDimensions (->> dimensions
                                                       (map (fn [[k v]] (format "%s=%s" k v)))
                                                       (into-array String))))
        (.withPercentiles (into-array CloudWatchReporter$Percentile []))
        (.build)
        (doto (.start (.toMillis ^Duration dry-run-report-frequency) TimeUnit/MILLISECONDS)))))
