(ns crux.metrics
  (:require [crux.metrics.indexer :as indexer-metrics]
            [crux.metrics.query :as query-metrics]
            [crux.metrics.dropwizard :as dropwizard])
  (:import [java.time Duration]
           [java.util.concurrent TimeUnit]))

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

(def all-metrics-loaded {:start-fn (fn [_ _])
                         :deps #{::registry}})

(def registry
  {::registry registry-module

   ;; virtual component that metrics can hook into with `:before` to ensure they're included in reporters
   ::all-metrics-loaded all-metrics-loaded})
