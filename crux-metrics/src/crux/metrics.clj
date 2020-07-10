(ns crux.metrics
  (:require [crux.metrics.indexer :as indexer-metrics]
            [crux.metrics.query :as query-metrics]
            [crux.metrics.dropwizard :as dropwizard]
            [crux.status :as status])
  (:import [java.time Duration]
           [java.util.concurrent TimeUnit]
           [com.codahale.metrics MetricRegistry Gauge Meter Timer Snapshot]))

(def registry-module {:start-fn (fn [deps {::keys [with-indexer-metrics?  with-query-metrics?]}]
                                  (cond-> (dropwizard/new-registry)
                                    with-indexer-metrics? (doto (indexer-metrics/assign-listeners deps))
                                    with-query-metrics? (doto (query-metrics/assign-listeners deps))))
                      :deps #{:crux.node/node :crux.node/indexer :crux.node/bus :crux.node/kv-store}
                      :args {::with-indexer-metrics? {:doc "Include metrics on the indexer"
                                                      :default true
                                                      :crux.config/type :crux.config/boolean}
                             ::with-query-metrics? {:doc "Include metrics on queries"
                                                    :default true
                                                    :crux.config/type :crux.config/boolean}}})

(defrecord StatusReporter [^MetricRegistry registry]
  status/Status
  (status-map [this]
    {:crux.metrics
     (into (sorted-map)
           (concat
            (map
             (fn [[^String name ^Gauge gauge]]
               {name (.getValue gauge)})
             (.getGauges registry))
            (mapcat
             (fn [[^String name ^Meter meter]]
               {name {"1-min-rate" (.getOneMinuteRate meter)
                      "5-min-rate" (.getFiveMinuteRate meter)
                      "15-min-rate" (.getFifteenMinuteRate meter)}})
             (.getMeters registry))
            (mapcat
             (fn [[^String name ^Timer timer]]
               (let [^Snapshot snapshot (.getSnapshot timer)]
                 {name {"1-min-rate" (.getOneMinuteRate timer)
                        "5-min-rate" (.getFiveMinuteRate timer)
                        "15-min-rate" (.getFifteenMinuteRate timer)
                        "minimum" (.getMin snapshot)
                        "maximum" (.getMax snapshot)
                        "mean" (.getMean snapshot)
                        "std-dev" (.getStdDev snapshot)
                        "75th-percentile" (.get75thPercentile snapshot)
                        "99th-percentile" (.get99thPercentile snapshot)
                        "99.9th percentile" (.get999thPercentile snapshot)}}))
             (.getTimers registry))))}))

(def all-metrics-loaded {:start-fn (fn [_ _])
                         :deps #{::registry}})

(def registry
  {::registry registry-module

   ;; virtual component that metrics can hook into with `:before` to ensure they're included in reporters
   ::all-metrics-loaded all-metrics-loaded
   ::status-reporter {:start-fn (fn [{::keys [registry] :as opts} args]
                                  (->StatusReporter registry))
                      :deps #{::registry ::all-metrics-loaded}}})
