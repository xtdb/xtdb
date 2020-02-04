(ns crux.metrics
  (:require [crux.metrics.indexer :as indexer-metrics]
            [crux.metrics.kv-store :as kv-metrics]
            [crux.metrics.query :as query-metrics]
            [crux.metrics.dropwizard :as dropwizard]
            [crux.metrics.dropwizard.jmx :as jmx]
            [crux.metrics.dropwizard.console :as console]
            [crux.metrics.dropwizard.csv :as csv]
            [crux.metrics.dropwizard.cloudwatch :as cloudwatch])
  (:import [java.time Duration]))

(def registry
  {::registry {:start-fn (fn [deps _]
                           ;; When more metrics are added we can pass a
                           ;; registry around
                           (doto (dropwizard/new-registry)
                             (indexer-metrics/assign-listeners deps)
                             (kv-metrics/assign-listeners deps)
                             (query-metrics/assign-listeners deps)))
               :deps #{:crux.node/node :crux.node/indexer :crux.node/bus :crux.node/kv-store}}})

(def jmx-reporter
  {::jmx-reporter {:start-fn (fn [{::keys [registry]} args]
                               (jmx/start-reporter registry args))
                   :args {:crux.metrics.dropwizard.jmx/domain {:doc "Add custom domain"
                                                               :required? false
                                                               :crux.config/type :crux.config/string-vector}
                          :crux.metrics.dropwizard.jmx/rate-unit {:doc "Set rate unit"
                                                                  :required? false
                                                                  :crux.config/type :crux.config/string}
                          :crux.metrics.dropwizard.jmx/duration-unit {:doc "Set duration unit"
                                                                      :required? false
                                                                      :crux.config/type :crux.config/string}}
                   :deps #{::registry}}})

(def console-reporter
  {::console-reporter {:start-fn (fn [{::keys [registry]} args]
                                   (console/start-reporter registry args))
                       :deps #{::registry}}})

(def csv-reporter
  {::csv-reporter {:start-fn (fn [{::keys [registry]} args]
                               (csv/start-reporter registry args))
                   :deps #{::registry}}})

;; TODO a decision for consistency, should args be a flat map in the config, or
;; contained in a arg themself

(def cloudwatch-reporter
  {::cloudwatch-reporter {:start-fn (fn [{::keys [registry]}
                                         {:crux.metrics.dropwizard.cloudwatch/keys
                                          [duration]
                                          :as args}]
                                      (let [cw-rep (cloudwatch/report registry args)]
                                        (cloudwatch/start cw-rep (Duration/parse duration))))
                          :deps #{::registry}
                          :args {:crux.metrics.dropwizard.cloudwatch/region {:doc "Region for uploading metrics. Tries to get it using api. If this fails, you will need to specify region."
                                                                             :required? false
                                                                             :crux.config/type :crux.config/string}
                                 :crux.metrics.dropwizard.cloudwatch/duration {:doc "Duration rate of metrics push in ISO-8601 standard"
                                                                               :default "PT1S"
                                                                               :crux.config/type :crux.config/string}
                                 :crux.metrics.dropwizard.cloudwatch/dry-run? {:doc "When true, the reporter prints to console instead of uploading to cw"
                                                                               :required? false
                                                                               :crux.config/type :crux.config/boolean}
                                 :crux.metrics.dropwizard.cloudwatch/jvm-metrics? {:doc "When true, include jvm metrics for upload"
                                                                                   :required? false
                                                                                   :crux.config/type :crux.config/boolean}
                                 :crux.metrics.dropwizard.cloudwatch/dimensions {:doc "Add global dimensions to metrics"
                                                                                 :required? false
                                                                                 :crux.config/type :crux.config/string-vector}}}})

(def with-jmx (merge registry jmx-reporter))
(def with-console (merge registry console-reporter))
(def with-csv (merge registry csv-reporter))
(def with-cloudwatch (merge registry cloudwatch-reporter))
