(ns crux.metrics.dropwizard
  (:require [crux.metrics.bus :as met-bus]
            [metrics.gauges :as gauges]
            [metrics.core :as drpwz-m]
            [metrics.reporters.jmx :as jmx]
            [metrics.reporters.console :as console]
            [metrics.reporters.csv :as csv]
            crux.metrics.gauges))

(defn register-metrics [reg !metrics]
  (run! (fn [[fn-sym func]]
          (gauges/gauge-fn reg ["node" "ingest" (str fn-sym)] #(func !metrics)))
        crux.metrics.gauges/ingest-gauges))

(defn create-assign-metrics [reg bus indexer]
  (register-metrics reg (met-bus/assign-ingest bus indexer)))

(def registry
  {::registry {:start-fn (fn [{:crux.node/keys [bus indexer]} _]
                           (let [reg  (drpwz-m/new-registry)]
                             (register-metrics reg
                                               (met-bus/assign-ingest bus indexer))
                             reg))
               :deps #{:crux.node/bus :crux.node/indexer}}})

(def jmx-reporter
  {::jmx-reporter {:start-fn (fn [{::keys [registry]} {::keys [jmx-reporter-opts]}]
                               (let [jmx-rep (jmx/reporter registry
                                                           (merge {:domain "crux.metrics"}
                                                                  jmx-reporter-opts))]
                                 (jmx/start jmx-rep)
                                 jmx-rep))
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
