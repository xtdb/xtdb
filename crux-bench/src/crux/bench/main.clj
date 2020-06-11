(ns crux.bench.main
  (:require [clojure.string :as string]
            [clojure.tools.cli :as cli]
            [crux.bench :as bench]
            [crux.bench.sorted-maps-microbench :as sorted-maps]
            [crux.bench.ts-devices :as devices]
            [crux.bench.ts-weather :as weather]
            [crux.bench.watdiv-crux :as watdiv-crux]
            [crux.bench.tpch-stress-test :as tpch-stress]
            [clojure.set :as set]))

(defn post-to-slack [results]
  (doto results
    (-> (bench/results->slack-message) (bench/post-to-slack))))

(def bench-tests
  {:sorted-maps (fn [nodes]
                  (bench/with-nodes [node (select-keys nodes #{"embedded-kafka-rocksdb"})]
                    (-> (bench/with-comparison-times
                          (sorted-maps/run-sorted-maps-microbench node))
                        (doto post-to-slack))))

   :ts-devices (fn [nodes]
                 (bench/with-nodes [node nodes]
                   (-> (bench/with-comparison-times
                         (devices/run-devices-bench node))
                       (doto post-to-slack))))

   :ts-weather (fn [nodes]
                 (bench/with-nodes [node nodes]
                   (-> (bench/with-comparison-times
                         (weather/run-weather-bench node))
                       (doto post-to-slack))))

   :watdiv (fn [nodes]
             (bench/with-nodes [node nodes]
               (let [[ingest-results query-results] (->> (watdiv-crux/run-watdiv-bench node {:test-count 100})
                                                         (split-at 2))]
                 (-> (concat (->> ingest-results (map watdiv-crux/render-comparison-durations))
                             (watdiv-crux/summarise-query-results query-results))
                     (bench/with-comparison-times)
                     (doto post-to-slack)))))
   :tpch-stress (fn [nodes]
                  (bench/with-nodes [node (select-keys nodes #{"standalone-rocksdb"})]
                    (-> (bench/with-comparison-times
                          (tpch-stress/run-tpch-stress-test node {:query-count 15 :field-count 10}))
                        (doto post-to-slack))))})

(defn parse-args [args]
  (let [{:keys [options summary errors]}
        (cli/parse-opts args
                        [[nil "--nodes node1,node2" "Node types"
                          :id :selected-nodes
                          :default (set (keys bench/nodes))
                          :parse-fn #(set (string/split % #","))]

                         [nil "--tests test1,test2" "Tests to run"
                          :id :selected-tests
                          :default (set (keys bench-tests))
                          :parse-fn #(into #{} (map keyword (set (string/split % #","))))]])]
    (if errors
      (binding [*out* *err*]
        (run! println errors)
        (println summary))

      options)))

(defn run-benches [{:keys [selected-nodes selected-tests]}]
  (let [nodes (select-keys bench/nodes selected-nodes)]
    (bench/with-embedded-kafka
      (->> (for [test-fn (vals (select-keys bench-tests selected-tests))]
             (test-fn nodes))
           (into [] (mapcat identity))))))

(defn -main [& args]
  (bench/post-to-slack (format "*Starting Benchmark*, Crux Version: %s, Commit Hash: %s\n"
                               bench/crux-version bench/commit-hash))

  (let [bench-results (run-benches (-> (or (parse-args args)
                                           (System/exit 1))
                                       (update :selected-nodes disj "h2-rocksdb" "sqlite-rocksdb")))]

    (bench/send-email-via-ses (bench/results->email bench-results)))

  (shutdown-agents))
