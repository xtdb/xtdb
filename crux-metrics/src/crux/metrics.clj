(ns crux.metrics
  (:require [crux.metrics.indexer :as indexer-metrics]
            [crux.metrics.kv-store :as kv-metrics]
            [crux.metrics.query :as query-metrics]
            [crux.metrics.dropwizard :as dropwizard]
            [crux.metrics.dropwizard.jmx :as jmx]
            [crux.metrics.dropwizard.console :as console]
            [crux.metrics.dropwizard.csv :as csv]
            [crux.metrics.dropwizard.cloudwatch :as cloudwatch]))

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
                                          [seconds length unit]
                                          :as args}]
                                      (let [cw-rep (cloudwatch/report registry args)]
                                        (if (and length unit)
                                          (cloudwatch/start cw-rep length unit)
                                          (cloudwatch/start cw-rep (or seconds 1)))))
                          :deps #{::registry}}})

(def with-jmx (merge registry jmx-reporter))
(def with-console (merge registry console-reporter))
(def with-csv (merge registry csv-reporter))
(def with-cloudwatch (merge registry cloudwatch-reporter))
