(ns xtdb.metrics
  (:require [xtdb.metrics.dropwizard :as dropwizard]
            [xtdb.metrics.index-store :as index-store-metrics]
            [xtdb.metrics.query :as query-metrics]
            [xtdb.status :as status]
            [xtdb.system :as sys])
  (:import [com.codahale.metrics Gauge Meter MetricRegistry Snapshot Timer]))

(defn ->registry {::sys/args {:registry {:doc       "Your own com.codahale.metrics.MetricRegistry"
                                         :default   (dropwizard/new-registry)}}}
  [{:keys [registry]}]
  registry)

(defn ->metrics {::sys/deps {:registry ::registry
                             :xtdb/node :xtdb/node
                             :xtdb/index-store :xtdb/index-store
                             :xtdb/bus :xtdb/bus}
                 ::sys/args {:with-index-store-metrics? {:doc "Include metrics on the index-store"
                                                         :default true
                                                         :spec ::sys/boolean}
                             :with-query-metrics? {:doc "Include metrics on queries"
                                                   :default true
                                                   :spec ::sys/boolean}}}
  [{:keys [registry with-index-store-metrics? with-query-metrics?] :as opts}]
  (let [deps (select-keys opts #{:xtdb/node :xtdb/index-store :xtdb/bus})]
    {:registry (cond-> registry
                 with-index-store-metrics? (doto (index-store-metrics/assign-listeners deps))
                 with-query-metrics? (doto (query-metrics/assign-listeners deps)))}))

(defn- ns->ms [time-ns]
  (/ time-ns 1e6))

(defrecord StatusReporter [^MetricRegistry registry]
  status/Status
  (status-map [this]
    {:xtdb.metrics
     (into (sorted-map)
           (concat
            (map
             (fn [[^String name ^Gauge gauge]]
               {name (.getValue gauge)})
             (.getGauges registry))
            (map
             (fn [[^String name ^Meter meter]]
               {name {"rate-1-min" (.getOneMinuteRate meter)
                      "rate-5-min" (.getFiveMinuteRate meter)
                      "rate-15-min" (.getFifteenMinuteRate meter)}})
             (.getMeters registry))
            (map
             (fn [[^String name ^Timer timer]]
               (let [^Snapshot snapshot (.getSnapshot timer)]
                 {name {"rate-1-min" (.getOneMinuteRate timer)
                        "rate-5-min" (.getFiveMinuteRate timer)
                        "rate-15-min" (.getFifteenMinuteRate timer)
                        "minimum-ms" (ns->ms (.getMin snapshot))
                        "maximum-ms" (ns->ms (.getMax snapshot))
                        "mean-ms" (ns->ms (.getMean snapshot))
                        "std-dev-ms" (ns->ms (.getStdDev snapshot))
                        "percentile-75-ms" (ns->ms (.get75thPercentile snapshot))
                        "percentile-99-ms" (ns->ms (.get99thPercentile snapshot))
                        "percentile-99.9-ms" (ns->ms (.get999thPercentile snapshot))}}))
             (.getTimers registry))))}))

(defn ->status-reporter {::sys/deps {:registry ::registry
                                     :metrics ::metrics}}
  [{:keys [registry]}]
  (->StatusReporter registry))
