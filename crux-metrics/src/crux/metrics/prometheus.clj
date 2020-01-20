(ns crux.metrics.prometheus
  (:require [crux.metrics.gauges :as metrics]
            [prometheus.core :as prometheus]
            [clojure.repl :as rpl]))

(defn init-node-gauges [node]
  (let [mets (metrics/metrics-map)]
    (reduce (fn [s [k v]] (prometheus/register-gauge
                            s
                            "node"
                            (string/replace (name k) "-" "_")
                            "HELP!"
                            ["label_name_gauge"]))
            (prometheus/init-defaults) metrics)))

(def state (atom nil))

(defn prom-handler [state]
  (fn [_]
    (let [metrics (filter (fn [[k v]] (= "metric" (namespace k))) (api/status node))]
      (vec (map (fn [[k v]] (prometheus/set-gauge
                              @state
                              "node"
                              (string/replace (name k) "-" "_")
                              v
                              ["bar"]))
                metrics)))
    (prometheus/dump-metrics (:registry @state))))

(def handler (prom-handler (doto state (reset! (init-node-gauges node)))))

(prometheus/dump-metrics (:registry @state))

(ring/serve #'handler)
