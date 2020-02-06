(ns crux.metrics.dropwizard.cloudwatch
  (:require crux.metrics.dropwizard)
  (:import [io.github.azagniotov.metrics.reporter.cloudwatch CloudWatchReporter]
           [java.io Closeable]
           [software.amazon.awssdk.services.cloudwatch CloudWatchAsyncClient CloudWatchAsyncClientBuilder]
           [software.amazon.awssdk.regions.providers DefaultAwsRegionProviderChain]
           [software.amazon.awssdk.regions Region]
           [software.amazon.awssdk.auth.credentials DefaultCredentialsProvider]
           [java.util.concurrent TimeUnit]
           [java.time Duration]
           [com.codahale.metrics MetricRegistry]))

(defn report ^io.github.azagniotov.metrics.reporter.cloudwatch.CloudWatchReporter
  [^MetricRegistry reg {::keys [region dry-run? jvm-metrics? dimensions]}]

  (let [cwac (-> (CloudWatchAsyncClient/builder)
                 (cond-> region ^CloudWatchAsyncClientBuilder (.region (Region/of region)))
                 (.build))]
    (-> (CloudWatchReporter/forRegistry reg cwac "crux.metrics.dropwizard.cloudwatch")
        (cond-> jvm-metrics? .withJvmMetrics
                dry-run? .withDryRun
                dimensions (.withGlobalDimensions (->> dimensions
                                                       (map (fn [[k v]] (format "%s=%s" k v)))
                                                       (into-array String))))
        .build)))

(defn start
  [^CloudWatchReporter reporter ^Duration duration]

  (.start reporter (.toMillis duration) TimeUnit/MILLISECONDS)
  (reify Closeable
    (close [this]
      (.stop reporter))))
