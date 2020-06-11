(ns crux.bench.watdiv-crux
  (:require [crux.bench.watdiv :as watdiv]
            [crux.bench :as bench]
            [clojure.java.io :as io]
            [crux.rdf :as rdf]
            [crux.api :as crux]
            [crux.sparql :as sparql]
            [clojure.data.json :as json])
  (:import (java.time Duration)))

(defn parse-results [resource]
  (with-open [rdr (io/reader resource)]
    (let [rdr-lines (line-seq rdr)]
      {:ingest (-> (first rdr-lines)
                   (read-string)
                   (:time-taken-ms))
       :queries (->> (rest rdr-lines)
                     (map read-string)
                     (filter :result-count)
                     (map (juxt :query-idx identity))
                     (into {}))})))

(def parsed-db-results
  (delay
    {:rdf4j (some-> (bench/load-from-s3 "rdf4j-3.0.0/rdf4j-20200406-170508Z.edn") parse-results)
     :neo4j (some-> (bench/load-from-s3 "neo4j-4.0.0/neo4j-20200406-171121Z.edn") parse-results)
     :datomic (some-> (bench/load-from-s3 "datomic-0.9.5697/datomic-20200406-170841Z.edn") parse-results)}))

(defn ingest-crux
  [node]
  (bench/run-bench :ingest
    (bench/with-additional-index-metrics node
      (let [{:keys [last-tx entity-count]} (with-open [in (io/input-stream watdiv/watdiv-input-file)]
                                             (rdf/submit-ntriples node in 1000))]
        (crux/await-tx node last-tx)
        {:entity-count entity-count
         :neo4j-time-taken-ms (get-in @parsed-db-results [:neo4j :ingest])
         :rdf4j-time-taken-ms (get-in @parsed-db-results [:rdf4j :ingest])
         :datomic-time-taken-ms (get-in @parsed-db-results [:datomic :ingest])}))))

(def db-query-results
  (delay
    {:rdf4j (get-in @parsed-db-results [:rdf4j :queries])
     :neo4j (get-in @parsed-db-results [:neo4j :queries])
     :datomic (get-in @parsed-db-results [:datomic :queries])}))

(defn get-db-results-at-idx [idx]
  (->> (for [[db-name db-results] @db-query-results]
         (let [time-taken-ms (get-in db-results [idx :time-taken-ms])
               result-count (get-in db-results [idx :result-count])]
           [db-name {:db-time-taken-ms time-taken-ms
                     :db-result-count result-count}]))
       (into {})))

(defn render-duration [m from-k to-k]
  (let [duration (some-> (get m from-k) (Duration/ofMillis))]
    (cond-> m
      duration (-> (dissoc m from-k)
                   (assoc to-k (str duration))))))

(defn render-comparison-durations [m]
  (-> m
      (render-duration :neo4j-time-taken-ms :neo4j-time-taken)
      (render-duration :rdf4j-time-taken-ms :rdf4j-time-taken)
      (render-duration :datomic-time-taken-ms :datomic-time-taken)))

(defn summarise-query-results [watdiv-query-results]
  (let [base-map (select-keys (first watdiv-query-results) [:bench-ns :crux-node-type])
        query-summary (merge base-map
                             {:bench-type "queries"
                              :time-taken-ms (->> watdiv-query-results (map :time-taken-ms) (reduce +))})
        summarised-results (concat
                            [query-summary]
                            (mapv
                             (fn [db-name]
                               (let [watdiv-results-with-db (for [query-result watdiv-query-results]
                                                              (merge query-result (get query-result db-name)))
                                     both-completed (->> watdiv-results-with-db (filter (every-pred :db-result-count :result-count)))
                                     crux-correct (->> both-completed (filter #(= (:db-result-count %) (:result-count %))))
                                     correct-idxs (into #{} (map :query-idx) crux-correct)]
                                 (-> (merge base-map
                                            {:bench-type (str "queries-" (name db-name))
                                             :crux-failures (->> both-completed (map :query-idx) (remove correct-idxs) sort vec)
                                             :crux-errors (->> watdiv-results-with-db (filter :db-result-count) (remove :result-count) (map :query-idx) sort vec)
                                             :time-taken-ms (->> crux-correct (map :time-taken-ms) (reduce +))
                                             :db-time-taken-ms (->> crux-correct (map :db-time-taken-ms) (reduce +))})
                                     (render-duration :db-time-taken-ms :db-time-taken))))
                             (keys @db-query-results)))]
    (run! (comp println json/write-str) summarised-results)
    summarised-results))

(defn run-watdiv-bench [node {:keys [test-count] :as opts}]
  (bench/with-bench-ns :watdiv-crux
    (bench/with-crux-dimensions
      (ingest-crux node)
      (bench/compact-node node)

      (watdiv/with-watdiv-queries watdiv/watdiv-stress-100-1-sparql
        (fn [queries]
          (-> queries
              (cond->> test-count (take test-count))
              (->> (bench/with-thread-pool opts
                     (fn [{:keys [idx q]}]
                       (bench/with-dimensions (merge {:query-idx idx} (get-db-results-at-idx idx))
                         (bench/run-bench (format "query-%d" idx)
                                          {:result-count (count (crux/q (crux/db node) (sparql/sparql->datalog q)))})))))))))))

(comment
  (def foo-raw-watdiv-results
    (with-redefs [watdiv/watdiv-input-file (io/file "crux-bench/data/watdiv.10.nt")]
     (bench/with-nodes [node (select-keys bench/nodes ["standalone-rocksdb"])]
       (run-watdiv-bench node {:test-count 10}))))

  (def foo-summarised-watdiv-results
    (let [[ingest-results query-results] (->> foo-raw-watdiv-results
                                              (split-at 2))]
      (-> (concat (->> ingest-results (map render-comparison-durations))
                  (summarise-query-results query-results))
          (bench/with-comparison-times))))

  (bench/results->slack-message foo-summarised-watdiv-results)
  (bench/results->email foo-summarised-watdiv-results))
