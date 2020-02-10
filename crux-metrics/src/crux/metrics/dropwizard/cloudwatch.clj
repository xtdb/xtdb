(ns crux.metrics.dropwizard.cloudwatch
  (:import [io.github.azagniotov.metrics.reporter.cloudwatch CloudWatchReporter]
           [java.io Closeable]
           [software.amazon.awssdk.services.cloudwatch CloudWatchAsyncClient CloudWatchAsyncClientBuilder]
           [software.amazon.awssdk.regions Region]
           [java.util.concurrent TimeUnit]
           [java.time Duration]
           [com.codahale.metrics MetricRegistry]))

(defn start-reporter
  [^MetricRegistry reg {::keys [region dry-run? jvm-metrics? dimensions ^Duration duration]}]

  (let [cwac (-> (CloudWatchAsyncClient/builder)
                 (cond-> region ^CloudWatchAsyncClientBuilder (.region (Region/of region)))
                 (.build))
        reporter (-> (CloudWatchReporter/forRegistry reg cwac "crux.metrics.dropwizard.cloudwatch")
                     (cond-> jvm-metrics? .withJvmMetrics
                             dry-run? .withDryRun
                             dimensions (.withGlobalDimensions (->> dimensions
                                                                    (map (fn [[k v]] (format "%s=%s" k v)))
                                                                    (into-array String))))
                     .build
                     (doto (.start (.toMillis duration) TimeUnit/MILLISECONDS)))]
    (reify Closeable
      (close [this]
        (.stop reporter)))))
