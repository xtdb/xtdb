(ns crux.dropwizard.cloudwatch
  (:require crux.dropwizard)
  (:import [io.github.azagniotov.metrics.reporter.cloudwatch CloudWatchReporter]
           [java.io Closeable]
           [software.amazon.awssdk.services.cloudwatch CloudWatchAsyncClient]
           [software.amazon.awssdk.regions Region]
           [java.util.concurrent TimeUnit]
           [com.codahale.metrics MetricRegistry MetricFilter]))

;; Does not throw an exception with an invalid region, beware
;; Could add some sort of identifier for global dimensions
(defn ^CloudWatchReporter report
  [^MetricRegistry reg {::keys [regions dry-run? jvm-metrics?] :as args}]
  (let [cwac (-> (CloudWatchAsyncClient/builder)
                 ((fn [c] (reduce #(.region %1 (Region/of %2)) c regions)))
                 (.build))]
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
