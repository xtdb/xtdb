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

  (let [bench-results (concat (bench/with-nodes [node (select-keys bench/nodes ["embedded-kafka-rocksdb"])]
                                [(-> (sorted-maps/run-sorted-maps-microbench node)
                                     (doto post-to-slack))])

                              (bench/with-nodes [node bench/nodes]
                                [(-> (devices/run-devices-bench node)
                                     (doto post-to-slack))

                                 (-> (weather/run-weather-bench node)
                                     (doto post-to-slack))

                                 (let [raw-watdiv-results (watdiv-crux/run-watdiv-bench node {:test-count 100})]
                                   (-> [(first raw-watdiv-results)
                                        (watdiv-crux/summarise-query-results (rest raw-watdiv-results))]
                                       (doto post-to-slack)))]))]

    (bench/send-email-via-ses
     (str "<h1>Crux bench results</h1><br>"
          (->> (for [[node-type results] bench-results]
                 (str (format "<h2>%s</h2>" node-type)
                      (->> (for [result results]
                             (format "<p>%s</p>" result))
                           (string/join))))
               (string/join)))))

  (shutdown-agents))
