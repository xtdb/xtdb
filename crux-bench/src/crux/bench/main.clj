(ns crux.bench.main
  (:require [crux.bench :as bench]
            [crux.bench.ts-weather :as weather]
            [crux.bench.ts-devices :as devices]
            [crux.bench.watdiv :as watdiv]
            [clj-http.client :as client]
            [clojure.tools.logging :as log]
            [clojure.tools.cli :as cli]
            [crux.bench.watdiv :as watdiv]))

(def cli-options
  [["-nc" "--no-crux" "Don't run Crux benchmark"]
   ["-d" "--datomic" "Run Datomic benchmark against watdiv"]
   ["-r" "--rdf4j" "Run RDF4J benchmark against watdiv"]])

(defn -main [& args]
  (let [{:keys [options] :as parsed-args} (cli/parse-opts args cli-options)]
    (when-not (:no-crux options)
      (bench/post-to-slack (format "*Starting Benchmark*, Crux Version: %s, Commit Hash: %s\n"
                                   bench/crux-version bench/commit-hash))
      (bench/with-node [node]
        (devices/run-devices-bench node)
        (weather/run-weather-bench node)
        (watdiv/run-watdiv-bench-crux node {:thread-count 1 :test-count 100})))
    (when (:datomic options)
      (watdiv/run-watdiv-bench-datomic {:thread-count 1 :test-count 100}))
    (when (:rdf4j options)
      (watdiv/run-watdiv-bench-rdf {:thread-count 1 :test-count 100}))
    (shutdown-agents)))
