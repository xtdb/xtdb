(ns crux.metrics
  (:require [crux.metrics.indexer :as indexer-metrics]
            [crux.metrics.query :as query-metrics]
            [crux.metrics.dropwizard :as dropwizard]
            [crux.status :as status])
  (:import [java.time Duration]
           [java.util.concurrent TimeUnit]
           [com.codahale.metrics MetricRegistry Gauge Meter Timer]))

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
               {(str name "/1-min-rate") (.getOneMinuteRate meter)
                (str name "/5-min-rate") (.getFiveMinuteRate meter)
                (str name "/15-min-rate") (.getFifteenMinuteRate meter)})
             (.getMeters registry))
            (mapcat
             (fn [[^String name ^Timer timer]]
               {(str name "/1-min-rate") (.getOneMinuteRate timer)
                (str name "/5-min-rate") (.getFiveMinuteRate timer)
                (str name "/15-min-rate") (.getFifteenMinuteRate timer)})
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
