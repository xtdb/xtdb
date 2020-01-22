(ns crux.bench.main
  (:require [crux.bench :as bench]
            [crux.bench.ts-weather :as weather]
            [crux.bench.ts-devices :as devices]))

(defn -main []
  (bench/with-node [node]
    (devices/run-devices-bench node)
    (weather/run-weather-bench node)))
