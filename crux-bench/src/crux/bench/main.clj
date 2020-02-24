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

  (bench/with-nodes [node]
    (let [devices-results (devices/run-devices-bench node)
          weather-results (weather/run-weather-bench node)
          watdiv-results (watdiv-crux/run-watdiv-bench node {:test-count 100})
          result-messages [(bench/results->slack-message devices-results :ts-devices)
                           (bench/results->slack-message weather-results :ts-weather)
                           (bench/results->slack-message
                            [(first watdiv-results) (watdiv-crux/->query-result (rest watdiv-results))]
                            :watdiv-crux)]]
      (bench/send-email-via-ses (string/join "\n" result-messages))))

  (shutdown-agents))
