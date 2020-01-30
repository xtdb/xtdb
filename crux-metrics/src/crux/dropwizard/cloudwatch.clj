(ns crux.dropwizard.cloudwatch
  (:require crux.dropwizard)
  (:import [io.github.azagniotov.metrics.reporter.cloudwatch CloudWatchReporter]
           [software.amazon.awssdk.services.cloudwatch CloudWatchAsyncClient]
           [software.amazon.awssdk.regions Region]
           [java.util.concurrent TimeUnit]
           [com.codahale.metrics MetricRegistry]))

;; Does not throw an exception with an invalid region, beware
(defn ^CloudWatchReporter report
  [^MetricRegistry reg {::keys [regions dry-run? jvm-metrics?]}]
  (let [cwac (-> (CloudWatchAsyncClient/builder)
                 ((fn [c] (reduce #(.region %1 (Region/of %2)) c regions)))
                 (.build))]
    ;; Might need seperate cwacs for regions
    (let [rep-builder (CloudWatchReporter/forRegistry reg cwac "crux.dropwizard.cloudwatch")]
      (cond-> rep-builder
        jvm-metrics? (.withJvmMetrics)
        dry-run? (.withDryRun)
        :build (.build)))))

(defn start
  ([^CloudWatchReporter reporter ^long seconds]
   (.start reporter seconds))
  ([^CloudWatchReporter reporter ^long length ^TimeUnit unit]
   (.start reporter length unit)))

(defn stop
  [^CloudWatchReporter reporter]
  (.stop reporter))

