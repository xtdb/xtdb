(ns crux.metrics.prometheus
  (:require [prometheus.core :as prometheus]
            [clojure.string :as string]
            [ring.server.standalone :as ring]
            crux.metrics.gauges))

(defn init-gauges []
  (reduce (fn [store [func-sym func]] (prometheus/register-gauge
                                        store
                                        "crux_metrics"
                                        (string/replace (name func-sym) "-" "_")
                                        (:doc (meta func))
                                        ["ingest"]))
          (prometheus/init-defaults)
          crux.metrics.gauges/ingest-gauges))

(defn handler [!metrics !store]
  (fn [_]
    (run! (fn [[func-sym func]]
            (tap> (str func-sym " " (func !metrics)))
            (prometheus/set-gauge
              @!store
              "crux_metrics"
              (string/replace (str func-sym) "-" "_")
              (func !metrics)
              ["ingest"]))
          crux.metrics.gauges/ingest-gauges)
    (prometheus/dump-metrics (:registry @!store))))

(def server {::server {:start-fn (fn [{:keys [crux.metrics/state]} args]
                                   (let [!store (atom (init-gauges))]
                                     (ring/serve (prometheus/instrument-handler
                                                   (handler state !store)
                                                   "crux_node"
                                                   (:registry @!store)))))
                       :deps #{:crux.metrics/state}}})

(def with-server (merge crux.metrics/state server))
