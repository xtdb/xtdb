(ns crux.metrics.dropwizard
  (:require [crux.metrics.bus :as met-bus]
            [metrics.gauges :as gauges]
            [metrics.core :as drpwz-m]
            [metrics.reporters.jmx :as jmx]
            crux.metrics.gauges))

(defn register-metrics [reg !metrics]
  (map (fn [[fn-sym func]]
         (gauges/gauge-fn reg (str fn-sym) #(func !metrics)))
       crux.metrics.gauges/gauges))

(defn create-assign-metrics [reg bus indexer]
  (register-metrics reg (met-bus/assign-ingest bus indexer)))

(def registry
  {::registry {:start-fn (fn [{:crux.node/keys [bus indexer]} args]
                           (let [reg  (drpwz-m/new-registry)]
                             (register-metrics reg
                                              (met-bus/assign-ingest bus indexer))
                             reg))
               :deps #{:crux.node/bus :crux.node/indexer}}})

(def jmx-reporter
  {::jmx {:start-fn (fn [{::keys [registry]} args]
                      (let [jmx-rep (jmx/reporter registry {})]
                        (jmx/start jmx-rep)
                        jmx-rep))
          :deps #{::registry}}})
