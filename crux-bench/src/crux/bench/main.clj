(ns crux.bench.main
  (:require [crux.bench :as bench]
            [crux.bench.ts-weather :as weather]
            [crux.bench.ts-devices :as devices]
            [crux.bench.watdiv :as watdiv]
            [clj-http.client :as client]
            [clojure.tools.logging :as log]
            [crux.bench.watdiv :as watdiv]))

(defn -main []
  (bench/post-to-slack (format "*Starting Benchmark*, Crux Version: %s, Commit Hash: %s\n"
                               bench/crux-version bench/commit-hash))
  (bench/with-node [node]
    (devices/run-devices-bench node)
    (weather/run-weather-bench node)
    (watdiv/run-watdiv-bench-crux node {:thread-count 1 :test-count 2}))
  (watdiv/run-watdiv-bench-rdf 2))


