(ns crux.metrics
  (:require [crux.metrics.indexer :as indexer-metrics]
            [crux.metrics.query :as query-metrics]
            [crux.metrics.dropwizard :as dropwizard]
            [crux.metrics.dropwizard.jmx :as jmx]
            [crux.metrics.dropwizard.console :as console]
            [crux.metrics.dropwizard.csv :as csv]
            [crux.metrics.dropwizard.cloudwatch :as cloudwatch]
            [crux.metrics.dropwizard.prometheus :as prometheus])
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

(def jmx-reporter
  {::jmx-reporter {:start-fn (fn [{::keys [registry]} args]
                               (jmx/start-reporter registry args))
                   :args {::jmx/domain {:doc "Add custom domain"
                                        :required? false
                                        :crux.config/type :crux.config/string}
                          ::jmx/rate-unit {:doc "Set rate unit"
                                           :required? false
                                           :default TimeUnit/SECONDS
                                           :crux.config/type :crux.config/time-unit}
                          ::jmx/duration-unit {:doc "Set duration unit"
                                               :required? false
                                               :default TimeUnit/MILLISECONDS
                                               :crux.config/type :crux.config/time-unit}}
                   :deps #{::registry ::all-metrics-loaded}}})

(def console-reporter
  {::console-reporter {:start-fn (fn [{::keys [registry]} args]
                                   (console/start-reporter registry args))
                       :deps #{::registry ::all-metrics-loaded}
                       :args {::console/report-frequency {:doc "Frequency of reporting metrics"
                                                          :default (Duration/ofSeconds 1)
                                                          :crux.config/type :crux.config/duration}
                              ::console/rate-unit {:doc "Set rate unit"
                                                   :required? false
                                                   :default TimeUnit/SECONDS
                                                   :crux.config/type :crux.config/time-unit}
                              ::console/duration-unit {:doc "Set duration unit"
                                                       :required? false
                                                       :default TimeUnit/MILLISECONDS
                                                       :crux.config/type :crux.config/time-unit}}}})

(def csv-reporter
  {::csv-reporter {:start-fn (fn [{::keys [registry]} args]
                               (csv/start-reporter registry args))
                   :deps #{::registry ::all-metrics-loaded}
                   :args {::csv/file-name {:doc "Output file name"
                                           :required? true
                                           :crux.config/type :crux.config/string}
                          ::csv/report-frequency {:doc "Frequency of reporting metrics"
                                                  :default (Duration/ofSeconds 1)
                                                  :crux.config/type :crux.config/duration}
                          ::csv/rate-unit {:doc "Set rate unit"
                                           :required? false
                                           :default TimeUnit/SECONDS
                                           :crux.config/type :crux.config/time-unit}
                          ::csv/duration-unit {:doc "Set duration unit"
                                               :required? false
                                               :default TimeUnit/MILLISECONDS
                                               :crux.config/type :crux.config/time-unit}}}})

(def cloudwatch-reporter
  {::cloudwatch-reporter {:start-fn (fn [{::keys [registry]} args]
                                      (cloudwatch/start-reporter registry args))
                          :deps #{::registry ::all-metrics-loaded}
                          :args {::cloudwatch/region {:doc "Region for uploading metrics. Tries to get it using api. If this fails, you will need to specify region."
                                                      :required? false
                                                      :crux.config/type :crux.config/string}
                                 ::cloudwatch/dry-run-report-frequency {:doc "Frequency of reporting metrics on a dry run"
                                                                        :default (Duration/ofSeconds 1)
                                                                        :crux.config/type :crux.config/duration}
                                 ::cloudwatch/dry-run? {:doc "When true, the reporter prints to console instead of uploading to cw"
                                                        :required? false
                                                        :crux.config/type :crux.config/boolean}
                                 ::cloudwatch/jvm-metrics? {:doc "When true, include jvm metrics for upload"
                                                            :required? false
                                                            :crux.config/type :crux.config/boolean}
                                 ::cloudwatch/dimensions {:doc "Add global dimensions to metrics"
                                                          :required? false
                                                          :crux.config/type :crux.config/string-map}
                                 ::cloudwatch/high-resolution? {:doc "Increase the push rate from 1 minute to 1 second"
                                                                :default false
                                                                :crux.config/type :crux.config/boolean}
                                 ::cloudwatch/ignore-rules {:doc "An ordered list of ignore rules for metrics, using gitignore syntax. e.g.
[\"crux.tx\" \"!crux.tx.ingest-rate\"] -> exclude crux.tx.*, but keep crux.tx.ingest-rate"
                                                            :required? false
                                                            :crux.config/type :crux.config/string-list}}}})

(def prometheus-reporter
  {::prometheus-reporter {:start-fn (fn [{::keys [registry]}
                                         args]
                                      (prometheus/start-reporter registry args))
                          :deps #{::registry ::all-metrics-loaded}
                          :args {::prometheus/report-frequency {:doc "Frequency of reporting metrics"
                                                                :default (Duration/ofSeconds 1)
                                                                :crux.config/type :crux.config/duration}
                                 ::prometheus/prefix {:doc "Prefix all metrics with this string"
                                                      :required? false
                                                      :crux.config/type :crux.config/string}
                                 ::prometheus/push-gateway {:doc "Address of the prometheus server"
                                                            :required? true
                                                            :crux.config/type :crux.config/string}}}})

(def prometheus-http-exporter
  {::prometheus-http-exporter {:start-fn (fn [{::keys [registry]} args]
                                           (prometheus/start-http-exporter registry args))
                               :deps #{::registry ::all-metrics-loaded}
                               :args {::prometheus/port {:doc "Port for prometheus exporter server"
                                                         :default 8080
                                                         :crux.config/type :crux.config/int}
                                      ::prometheus/jvm-metrics? {:doc "Dictates if jvm metrics are exported"
                                                                 :default false
                                                                 :crux.config/type :crux.config/boolean}}}})

(def with-jmx (merge registry jmx-reporter))
(def with-console (merge registry console-reporter))
(def with-csv (merge registry csv-reporter))
(def with-cloudwatch (merge registry cloudwatch-reporter))
(def with-prometheus-reporter (merge registry prometheus-reporter))
(def with-prometheus-http-exporter (merge registry prometheus-http-exporter))
