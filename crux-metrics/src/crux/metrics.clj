(ns crux.metrics
  (:require [crux.metrics.indexer :as indexer-metrics]
            [crux.metrics.kv-store :as kv-metrics]
            [metrics.core :as drpwz-m]
            [metrics.reporters.jmx :as jmx]
            [metrics.reporters.console :as console]
            [metrics.reporters.csv :as csv]))

(def registry
  {::registry {:start-fn (fn [deps _]
                           ;; When more metrics are added we can pass a
                           ;; registry around
                           (doto (drpwz-m/new-registry)
                             (indexer-metrics/assign-listeners deps)
                             (kv-metrics/assign-listeners deps)))
               :deps #{:crux.node/node :crux.node/indexer :crux.node/bus :crux.node/kv-store}}})

(def jmx-reporter
  {::jmx-reporter {:start-fn (fn [{::keys [registry]} {::keys [jmx-reporter-opts]}]
                               (doto (jmx/reporter registry
                                                   (merge {:domain "crux.metrics"}
                                                          jmx-reporter-opts))
                                 jmx/start))
                   :deps #{::registry}}})

(def console-reporter
  {::console-reporter {:start-fn (fn [{::keys [registry]} {::keys [console-reporter-opts console-reporter-rate]}]
                                   (doto (console/reporter registry (merge {} console-reporter-opts))
                                     (console/start (or console-reporter-rate 1))))
                       :deps #{::registry}}})

(def csv-reporter
  {::csv-reporter {:start-fn (fn [{::keys [registry]} {::keys [csv-reporter-opts csv-reporter-file csv-reporter-rate]}]
                               (doto (csv/reporter registry
                                                   (or csv-reporter-file "/tmp/csv_reporter")
                                                   (merge {}
                                                          csv-reporter-opts))
                                 (csv/start (or csv-reporter-rate 1))))
                   :deps #{::registry}}})

(def with-jmx (merge registry jmx-reporter))
(def with-console (merge registry console-reporter))
(def with-csv (merge registry csv-reporter))
