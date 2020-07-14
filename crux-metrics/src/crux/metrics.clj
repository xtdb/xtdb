(ns crux.metrics
  (:require [crux.metrics.indexer :as indexer-metrics]
            [crux.metrics.query :as query-metrics]
            [crux.metrics.dropwizard :as dropwizard]
            [crux.status :as status])
  (:import [java.time Duration]
           [java.util.concurrent TimeUnit]
           [com.codahale.metrics MetricRegistry Gauge Meter Timer Snapshot]))

(defn- ns->ms [time-ns]
  (/ time-ns 1e6))

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

(def all-metrics-loaded {:start-fn (fn [_ _])
                         :deps #{::registry}})

(def registry
  {::registry registry-module

   ;; virtual component that metrics can hook into with `:before` to ensure they're included in reporters
   ::all-metrics-loaded all-metrics-loaded
   ::status-reporter {:start-fn (fn [{::keys [registry] :as opts} args]
                                  (->StatusReporter registry))
                      :deps #{::registry ::all-metrics-loaded}}})
