(ns crux.dropwizard.cloudwatch
  (:require crux.dropwizard)
  (:import [io.github.azagniotov.metrics.reporter.cloudwatch CloudWatchReporter]
           [java.io Closeable]
           [software.amazon.awssdk.services.cloudwatch CloudWatchAsyncClient]
           [software.amazon.awssdk.regions.providers DefaultAwsRegionProviderChain]
           [software.amazon.awssdk.auth.credentials DefaultCredentialsProvider]
           [java.util.concurrent TimeUnit]
           [com.codahale.metrics MetricRegistry MetricFilter]))

(defn ^CloudWatchReporter report
  [^MetricRegistry reg {::keys [region dry-run? jvm-metrics?] :as args}]
  (let [cwac (-> (CloudWatchAsyncClient/builder)
                 (.region (.getRegion (new DefaultAwsRegionProviderChain)))
                 (.credentialsProvider (DefaultCredentialsProvider/create))
                 .build)]
    ;; Might need seperate cwacs for regions
    (-> (cond-> (CloudWatchReporter/forRegistry reg cwac "crux.dropwizard.cloudwatch")
          jvm-metrics? .withJvmMetrics
          dry-run? .withDryRun)
        (.withGlobalDimensions (into-array String ["crux.node=metrics"]))
        .build)))

(defn start

  ([^CloudWatchReporter reporter ^long seconds]
   (.start reporter seconds (TimeUnit/SECONDS))
   (reify Closeable
     (close [this]
       (.stop reporter))))

  ([^CloudWatchReporter reporter ^long length ^TimeUnit unit]
   (.start reporter length unit)
   (reify Closeable
     (close [this]
       (.stop reporter)))))
