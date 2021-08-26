(ns xtdb.bench.main
  (:require [clojure.string :as string]
            [clojure.tools.cli :as cli]
            [xtdb.bench :as bench]
            [xtdb.bench.sorted-maps-microbench :as sorted-maps]
            [xtdb.bench.tpch-stress-test :as tpch-stress]
            [xtdb.bench.tpch :as tpch]
            [xtdb.bench.ts-devices :as devices]
            [xtdb.bench.ts-weather :as weather]
            [xtdb.bench.watdiv-crux :as watdiv-crux]))

(defn post-to-slack [results]
  (doto results
    (-> (bench/results->slack-message) (bench/post-to-slack))))

(def bench-tests
  {:sorted-maps (fn [nodes _]
                  (bench/with-nodes [node nodes]
                    (-> (bench/with-comparison-times
                          (sorted-maps/run-sorted-maps-microbench node))
                        (doto post-to-slack))))

   :ts-devices (fn [nodes _]
                 (bench/with-nodes [node nodes]
                   (-> (bench/with-comparison-times
                         (devices/run-devices-bench node))
                       (doto post-to-slack))))

   :ts-weather (fn [nodes _]
                 (bench/with-nodes [node nodes]
                   (-> (bench/with-comparison-times
                         (weather/run-weather-bench node))
                       (doto post-to-slack))))

   :watdiv (fn [nodes _]
             (bench/with-nodes [node nodes]
               (let [[ingest-results query-results] (->> (watdiv-crux/run-watdiv-bench node {:test-count 100})
                                                         (split-at 2))]
                 (-> (concat (->> ingest-results (map watdiv-crux/render-comparison-durations))
                             (watdiv-crux/summarise-query-results query-results))
                     (bench/with-comparison-times)
                     (doto post-to-slack)))))

   :tpch-stress (fn [nodes {:keys [tpch-query-count tpch-field-count]}]
                  (bench/with-nodes [node nodes]
                    (-> (bench/with-comparison-times
                          (tpch-stress/run-tpch-stress-test node {:query-count tpch-query-count
                                                                  :field-count tpch-field-count}))
                        (doto post-to-slack))))

   :tpch (fn [nodes {:keys [tpch-scale-factor]}]
           (bench/with-nodes [node nodes]
             (-> (bench/with-comparison-times
                   (tpch/run-tpch node {:scale-factor tpch-scale-factor}))
                 (doto post-to-slack))))

   :tpch-ingest (fn [_nodes {:keys [tpch-scale-factor]}]
                  (bench/with-nodes [node (select-keys bench/nodes ["rocksdb-lucene"])]
                    (-> (bench/with-comparison-times
                          (tpch/run-tpch-ingest-only node {:scale-factor tpch-scale-factor}))
                        (doto post-to-slack))))

   :lmdb-tpch (fn [_ _]
                (bench/with-nodes [node (select-keys bench/nodes ["standalone-lmdb" "kafka-lmdb"])]
                  (-> (bench/with-comparison-times
                        (tpch/run-tpch node {:scale-factor 0.01}))
                      (doto post-to-slack))))})

(defn parse-args [args]
  (let [{:keys [options summary errors]}
        (cli/parse-opts args
                        [[nil "--nodes node1,node2" "Node types"
                          :id :selected-nodes
                          :default (set (keys (dissoc bench/nodes "standalone-lmdb" "kafka-lmdb" "rocksdb-lucene")))
                          :parse-fn #(set (string/split % #","))]

                         [nil "--tests test1,test2" "Tests to run"
                          :id :selected-tests
                          :default (set (keys (dissoc bench-tests :tpch-stress)))
                          :parse-fn #(into #{} (map keyword (set (string/split % #","))))]

                         [nil "--tpch-query-count 20" "Number of queries to run on TPCH stress"
                          :id :tpch-query-count
                          :default 35
                          :parse-fn #(Long/parseLong %)]

                         [nil "--tpch-field-count 10" "Number of fields to run queries with on TPCH stress"
                          :id :tpch-field-count
                          :default (count tpch-stress/fields)
                          :parse-fn #(Long/parseLong %)]

                         [nil "--tpch-scale-factor 0.01" "Scale factor for regular TPCH test"
                          :id :tpch-scale-factor
                          :default 0.01
                          :parse-fn #(Double/parseDouble %)]

                         [nil "--repeat 10" "Number of times to repeat the current bench run"
                          :id :repetitions
                          :default 1
                          :parse-fn #(Long/parseLong %)]])]
    (if errors
      (binding [*out* *err*]
        (run! println errors)
        (println summary))

      options)))

(defn run-benches [{:keys [selected-nodes selected-tests repetitions] :as opts}]
  (let [nodes (select-keys bench/nodes selected-nodes)]
    (bench/with-embedded-kafka
      (->> (for [n (range repetitions)
                 test-fn (vals (select-keys bench-tests selected-tests))]
             (test-fn nodes opts))
           (into [] (mapcat identity))))))

(defn -main [& args]
  (bench/post-to-slack (format "*Starting Benchmark*, Commit Hash: %s\n" bench/commit-hash))

  (let [bench-results (run-benches (-> (or (parse-args args)
                                           (System/exit 1))
                                       (update :selected-nodes disj "h2-rocksdb" "sqlite-rocksdb")))]

    (bench/post-to-slack (format "*Finished Benchmark*, Commit Hash: %s\n" bench/commit-hash))
    (bench/send-email-via-ses (bench/results->email bench-results)))

  (shutdown-agents))
