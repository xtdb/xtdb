(ns crux.bench.main
  (:require [clojure.string :as string]
            [crux.bench :as bench]
            [crux.bench.sorted-maps-microbench :as sorted-maps]
            [crux.bench.ts-devices :as devices]
            [crux.bench.ts-weather :as weather]
            [crux.bench.watdiv-crux :as watdiv-crux]))

(defn post-to-slack [results]
  (doto results
    (-> (bench/results->slack-message) (bench/post-to-slack))))

(defn -main []
  (bench/post-to-slack (format "*Starting Benchmark*, Crux Version: %s, Commit Hash: %s\n"
                               bench/crux-version bench/commit-hash))

  (bench/with-embedded-kafka
    (let [bench-results
          (concat (bench/with-nodes [node (select-keys bench/nodes ["embedded-kafka-rocksdb"])]
                    (-> (bench/with-comparison-times
                          (sorted-maps/run-sorted-maps-microbench node))
                        (doto post-to-slack)))

                  (bench/with-nodes [node bench/nodes]
                    (-> (bench/with-comparison-times
                          (devices/run-devices-bench node))
                        (doto post-to-slack)))

                  (bench/with-nodes [node bench/nodes]
                    (-> (bench/with-comparison-times
                          (weather/run-weather-bench node))
                        (doto post-to-slack)))

                  (bench/with-nodes [node bench/nodes]
                    (let [[ingest-results query-results] (->> (watdiv-crux/run-watdiv-bench node {:test-count 100})
                                                              (split-at 2))]
                      (-> (concat (->> ingest-results (map watdiv-crux/render-comparison-durations))
                                  (watdiv-crux/summarise-query-results query-results))
                          (bench/with-comparison-times)
                          (doto post-to-slack)))))]
      (bench/send-email-via-ses
       (bench/results->email bench-results))))

  (shutdown-agents))
