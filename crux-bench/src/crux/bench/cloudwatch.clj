(ns crux.bench.cloudwatch
  (:import java.util.List
           software.amazon.awssdk.regions.Region
           software.amazon.awssdk.services.cloudwatch.CloudWatchClient
           [software.amazon.awssdk.services.cloudwatch.model Dimension MetricDatum PutMetricDataRequest StandardUnit]))

(def ^CloudWatchClient ^:private cloudwatch-client
  (delay
    (.. (CloudWatchClient/builder)
        (region (Region/EU_WEST_2))
        (build))))

(defn- ->cw-dimension ^Dimension [k v]
  (.. (Dimension/builder)
      (name (name k))
      (value (name v))
      (build)))

(def ^:private cw-namespace
  ;; uncomment this to test CW metrics locally, but don't commit it!
  (or #_"crux.bench.dev"
      (when (Boolean/parseBoolean (System/getenv "CRUX_BENCH_CW_METRICS"))
        "crux.bench")))

(defn put-cw-metrics! [results]
  (when cw-namespace
    (when-let [^List
               metric-data (seq (for [{:keys [time-taken-ms bench-type] :as result} results
                                      :when (and (:crux-node-type result)
                                                 (:success? result))
                                      :when (contains? #{:ingest :queries :queries-warm} bench-type)]
                                  (let [^List
                                        dimensions (for [k #{:crux-node-type :bench-ns :bench-type}]
                                                     (->cw-dimension k (get result k)))]
                                    (.. (MetricDatum/builder)
                                        (metricName "time-taken")
                                        (dimensions dimensions)
                                        (unit StandardUnit/MILLISECONDS)
                                        (value (double time-taken-ms))
                                        (build)))))]
      (let [^PutMetricDataRequest
            req (.. (PutMetricDataRequest/builder)
                    (namespace cw-namespace)
                    (metricData metric-data)
                    (build))]
        (.putMetricData ^CloudWatchClient @cloudwatch-client req)))))
