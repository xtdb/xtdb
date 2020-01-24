(ns crux.metrics
  (:require [crux.metrics.ingest :as ingest]
            [metrics.core :as drpwz-m]
            [metrics.reporters.jmx :as jmx]
            [metrics.reporters.console :as console]
            [metrics.reporters.csv :as csv]))

(def registry
  {::registry {:start-fn (fn [{:crux.node/keys [indexer bus]} _]
                           ;; When more metrics are added we can pass a
                           ;; registry around
                           (let [reg (drpwz-m/new-registry)]
                             (ingest/assign-ingest bus indexer reg)
                             reg))
               :deps #{:crux.node/indexer :crux.node/bus}}})

(def jmx-reporter
  {::jmx-reporter {:start-fn (fn [{::keys [registry]} {::keys [jmx-reporter-opts]}]
                               (doto (jmx/reporter registry
                                                   (merge {:domain "crux.metrics"}
                                                          jmx-reporter-opts))
                                 jmx/start))
                   :deps #{::registry}}})

(def console-reporter
  {::console-reporter {:start-fn (fn [{::keys [registry]} {::keys [console-reporter-opts console-reporter-rate]}]
                                   (let [console-rep (console/reporter registry
                                                                       (merge {}
                                                                              console-reporter-opts))]
                                     (console/start console-rep (or console-reporter-rate 1))
                                     console-rep))
                       :deps #{::registry}}})

(def csv-reporter
  {::csv-reporter {:start-fn (fn [{::keys [registry]} {::keys [csv-reporter-opts csv-reporter-file csv-reporter-rate]}]
                               (let [csv-rep (csv/reporter registry
                                                           (or csv-reporter-file "/tmp/csv_reporter")
                                                           (merge {}
                                                                  csv-reporter-opts))]
                                 (csv/start csv-rep (or csv-reporter-rate 1))
                                 csv-rep))
                   :deps #{::registry}}})

(def with-jmx (merge registry jmx-reporter))
(def with-console (merge registry console-reporter))
(def with-csv (merge registry csv-reporter))
