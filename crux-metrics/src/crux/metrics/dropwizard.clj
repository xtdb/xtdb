(ns crux.metrics.dropwizard
  (:require [metrics.core :as dropwiz]
            [metrics.gauges :as gauges]
            [crux.metrics :as c-mets]))

(defn codahale-register-node [reg node]
  (map (fn [[namesp data]]
         (gauges/gauge-fn reg (name namesp) (:function data)))
       c-mets/metrics-map))

