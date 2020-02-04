(ns crux.metrics.dropwizard.cloudwatch
  (:require crux.metrics.dropwizard)
  (:import [io.github.azagniotov.metrics.reporter.cloudwatch CloudWatchReporter]
           [java.io Closeable]
           [software.amazon.awssdk.services.cloudwatch CloudWatchAsyncClient]
           [software.amazon.awssdk.regions.providers DefaultAwsRegionProviderChain]
           [software.amazon.awssdk.regions Region]
           [software.amazon.awssdk.auth.credentials DefaultCredentialsProvider]
           [java.util.concurrent TimeUnit]
           [java.time Duration]
           [com.codahale.metrics MetricRegistry]))

(defn report ^CloudWatchReporter

  [^MetricRegistry reg {::keys [region dry-run? jvm-metrics? dimensions]}]
  (let [cwac (-> (CloudWatchAsyncClient/builder)
                 (.region (if region
                            (Region/of region)
                            (.getRegion (new DefaultAwsRegionProviderChain))))
                 (.credentialsProvider (DefaultCredentialsProvider/create))
                 .build)]
    (-> (cond-> (CloudWatchReporter/forRegistry reg cwac "crux.metrics.dropwizard.cloudwatch")
          jvm-metrics? .withJvmMetrics
          dry-run? .withDryRun
          dimensions (.withGlobalDimensions (into-array String dimensions)) )
        .build)))

(defn start

  [^CloudWatchReporter reporter ^Duration duration]
  (.start reporter (.toMillis duration) TimeUnit/MILLISECONDS)
  (reify Closeable
    (close [this]
      (.stop reporter))))
