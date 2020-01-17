(ns crux.metrics.dropwizard
  (:require [metrics.core :as dropwiz]
            [metrics.gauges :as gauges]
            [crux.metrics :as c-mets]))

(defn codahale-register-node [reg node]
  (gauges/gauge-fn))

(defn codahale-register-node [reg node]
  (let [metrics (filter (fn [[k v]] (= "metric" (namespace k))) (api/status node))]
    (apply hash-map (flatten (map (fn [[k v]] [(name k) (gauges/gauge-fn reg (name k) (fn [] (get (api/status node) k)))]) metrics)))))

(def regs (codahale-register-node reg node))

(defn get-metrics []
  (apply hash-map (flatten (map (fn [[k v]] [k (gauges/value v)]) regs))))

(gauges/value (gauges/gauge reg "doc-count"))
