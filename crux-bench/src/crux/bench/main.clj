(ns crux.bench.main
  (:require [crux.bench :as bench]
            [crux.bench.ts-weather :as weather]
            [crux.bench.ts-devices :as devices]
            [clj-http.client :as client]
            [clojure.tools.cli :as cli]
            [crux.bench.watdiv-crux :as watdiv-crux]
            [clojure.string :as string]))

(defn -main []
  (bench/post-to-slack (format "*Starting Benchmark*, Crux Version: %s, Commit Hash: %s\n"
                               bench/crux-version bench/commit-hash))
  (bench/with-node [node]
    (let [result-messages
          [(bench/results->slack-message (devices/run-devices-bench node) :ts-devices)
           (bench/results->slack-message (weather/run-weather-bench node) :ts-weather)
           (bench/results->slack-message (watdiv-crux/run-watdiv-bench node {:test-count 100}) :watdiv-crux)]]
      (bench/send-email-via-ses (string/join result-messages "\n"))))
  (shutdown-agents))
