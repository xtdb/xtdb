(ns crux.metrics.dropwizard
  (:require [crux.metrics.bus :as met-bus]
            [metrics.core :as dropwiz]
            [metrics.gauges :as gauges]))

;; TODO better name
(defn cx-met->metrics [reg !metrics]
  (map (fn [[fn-sym func]]
         (gauges/gauge-fn reg (str fn-sym) #(func !metrics)))
       (ns-publics 'crux.metrics.gauges)))

(defn node->metrics [reg node]
  (cx-met->metrics reg (met-bus/assign-ingest node)))
