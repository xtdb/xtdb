(ns crux.metrics.dropwizard
  (:require [crux.metrics.bus :as met-bus]
            [metrics.gauges :as gauges]
            [metrics.core :as drpwz-m]
            [metrics.reporters.jmx :as jmx]
            [metrics.reporters.console :as console]
            [metrics.reporters.csv :as csv]
            crux.metrics.gauges
            crux.metrics))

(defn register-metrics [reg !metrics]
  (run! (fn [[fn-sym func]]
          (gauges/gauge-fn reg ["node" "ingest" (str fn-sym)] #(func !metrics)))
        crux.metrics.gauges/ingest-gauges))

(defn create-assign-metrics [reg bus indexer]
  (register-metrics reg (met-bus/assign-ingest bus indexer)))

(def registry
  {::registry {:start-fn (fn [{:keys [crux.metrics/state]} _]
                           (let [reg (drpwz-m/new-registry)]
                             (register-metrics reg state)
                             reg))
               :deps #{:crux.metrics/state}}})

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

(def with-jmx (merge crux.metrics/state registry jmx-reporter))
(def with-console (merge crux.metrics/state registry console-reporter))
(def with-csv (merge crux.metrics/state registry csv-reporter))
