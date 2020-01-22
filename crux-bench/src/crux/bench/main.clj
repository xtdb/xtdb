(ns crux.bench.main
  (:require [crux.bench :as bench]
            [crux.bench.ts-weather :as weather]
            [crux.bench.ts-devices :as devices]
            [clj-http.client :as client]))

(defn -main []
  (bench/post-to-slack (format "*Starting Benchmark*, Crux Version: %s, Commit Hash: %s\n"
                               bench/crux-version bench/commit-hash))
  (bench/with-node [node]
    (devices/run-devices-bench node)
    (weather/run-weather-bench node)))
