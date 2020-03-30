(ns crux.metrics.dropwizard.cloudwatch
  (:require [crux.metrics :as metrics]
            [clojure.string :as string])
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
        (.build)
        (doto (.start (.toMillis ^Duration dry-run-report-frequency) TimeUnit/MILLISECONDS)))))

(def reporter
  (merge metrics/registry
         {::reporter {:start-fn (fn [{:crux.metrics/keys [registry]} args]
                                  (start-reporter registry args))
                      :deps #{:crux.metrics/registry :crux.metrics/all-metrics-loaded}
                      :args {::region {:doc "Region for uploading metrics. Tries to get it using api. If this fails, you will need to specify region."
                                       :required? false
                                       :crux.config/type :crux.config/string}
                             ::dry-run-report-frequency {:doc "Frequency of reporting metrics on a dry run"
                                                         :default (Duration/ofSeconds 1)
                                                         :crux.config/type :crux.config/duration}
                             ::dry-run? {:doc "When true, the reporter prints to console instead of uploading to cw"
                                         :required? false
                                         :crux.config/type :crux.config/boolean}
                             ::jvm-metrics? {:doc "When true, include jvm metrics for upload"
                                             :required? false
                                             :crux.config/type :crux.config/boolean}
                             ::dimensions {:doc "Add global dimensions to metrics"
                                           :required? false
                                           :crux.config/type :crux.config/string-map}
                             ::high-resolution? {:doc "Increase the push rate from 1 minute to 1 second"
                                                 :default false
                                                 :crux.config/type :crux.config/boolean}
                             ::ignore-rules {:doc "An ordered list of ignore rules for metrics, using gitignore syntax. e.g.
[\"crux.tx\" \"!crux.tx.ingest-rate\"] -> exclude crux.tx.*, but keep crux.tx.ingest-rate"
                                             :required? false
                                             :crux.config/type :crux.config/string-list}}}}))
