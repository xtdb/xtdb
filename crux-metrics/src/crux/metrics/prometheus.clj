(ns crux.metrics.prometheus
  (:require [prometheus.core :as prometheus]
            [clojure.string :as string]
            [ring.server.standalone :as ring]
            crux.metrics.gauges))

(defn init-gauges []
  (reduce (fn [store [func-sym func]] (prometheus/register-gauge 
                                        store
                                        "crux.metrics"
                                        (string/replace (str func-sym) "-" "_")
                                        (:doc (meta func))
                                        ["ingest"]))
          (prometheus/init-defaults)
          crux.metrics.gauges/ingest-gauges))

(defn handler [!metrics !state]
  (fn [_]
    (run! (fn [[func-sym func]]
            (prometheus/set-gauge
              @!state
              "crux.metrics"
              (string/replace (str func-sym) "-" "_")
              (func !metrics)
              ["ingest"]))
          crux.metrics.gauges/ingest-gauges)
    (prometheus/dump-metrics (:registry @!state))))

(def server {::server {:start-fn (fn [{:keys [crux.metrics/state]} args]
                                   (ring/serve (handler state (atom (init-gauges)))))
                       :deps {:crux.metrics/state}}})
