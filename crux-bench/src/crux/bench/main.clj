(ns crux.bench.main
  (:require [clojure.string :as string]
            [crux.bench :as bench]
            [crux.bench.ts-devices :as devices]
            [crux.bench.ts-weather :as weather]
            [crux.bench.watdiv-crux :as watdiv-crux]))

(defn -main []
  (bench/post-to-slack (format "*Starting Benchmark*, Crux Version: %s, Commit Hash: %s\n"
                               bench/crux-version bench/commit-hash))

  (let [bench-results (bench/with-nodes [node bench/nodes]
                        [(doto (devices/run-devices-bench node)
                           (bench/post-slack-results :ts-devices))

                         (doto (weather/run-weather-bench node)
                           (bench/post-slack-results :ts-weather))

                         (let [raw-watdiv-results (watdiv-crux/run-watdiv-bench node {:test-count 100})]
                           (doto [(first raw-watdiv-results)
                                  (watdiv-crux/summarise-query-results (rest raw-watdiv-results))]
                             (bench/post-slack-results :watdiv-crux)))])]

    (bench/send-email-via-ses
     (str "<h1>Crux bench results</h1><br>"
          (->> (for [[node-type results] bench-results]
                 (str (format "<h2>%s</h2>" node-type)
                      (->> (for [result results]
                             (format "<p>%s</p>" result))
                           (string/join))))
               (string/join)))))

  (shutdown-agents))
