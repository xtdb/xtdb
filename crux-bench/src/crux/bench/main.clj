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

  (->> (bench/with-nodes [node]
         (let [devices-results (doto (devices/run-devices-bench node)
                                 (bench/post-slack-results :ts-devices))

               weather-results (doto (weather/run-weather-bench node)
                                 (bench/post-slack-results :ts-weather))

               raw-watdiv-results (watdiv-crux/run-watdiv-bench node {:test-count 100})

               watdiv-results (doto [(first raw-watdiv-results)
                                     (watdiv-crux/summarise-query-results (rest raw-watdiv-results))]
                                (bench/post-slack-results :watdiv-crux))]
           [devices-results
            weather-results
            watdiv-results]))

       (reduce (fn [acc [node-type result]]
                 (str acc
                      (format "<h2>%s</h2>" node-type)
                      (string/join (map (fn [r] (string/join (map #(format "<p>%s</p>" %) r))) result))))
               "<h1>Crux bench results</h1><br>")
       (bench/send-email-via-ses))

  (shutdown-agents))
