(ns crux.bench.watdiv-crux
  (:require [crux.bench.watdiv :as watdiv]
            [crux.bench :as bench]
            [clojure.java.io :as io]
            [crux.rdf :as rdf]
            [crux.api :as crux]
            [crux.sparql :as sparql])
  (:import (java.time Duration)))

(defn ingest-crux
  [node]
  (bench/run-bench :ingest
                   (let [{:keys [last-tx entity-count]} (with-open [in (io/input-stream watdiv/watdiv-input-file)]
                                                          (rdf/submit-ntriples node in 1000))]
                     (crux/await-tx node last-tx)
                     {:entity-count entity-count
                      :node-size-bytes (bench/node-size-in-bytes node)})))

(defn parse-results [resource]
  (with-open [rdr (io/reader resource)]
    (->> (line-seq rdr)
         (map read-string)
         (filter :result-count)
         (map (juxt :query-idx identity))
         (into {}))))

(def db-query-results
  {:rdf4j (some->
            (bench/load-from-s3 "rdf4j-3.0.0/rdf4j-20200214-174740Z.edn") parse-results)
   :neo4j (some-> (bench/load-from-s3 "neo4j-4.0.0/neo4j-20200219-114016Z.edn") parse-results)
   :datomic (some-> (bench/load-from-s3 "datomic-0.9.5697/datomic-20200303-155352Z.edn") parse-results)})

(defn get-db-results-at-idx [idx]
  (->> (for [[db-name db-results] db-query-results]
         (let [time-taken-ms (get-in db-results [idx :time-taken-ms])
               result-count (get-in db-results [idx :result-count])]
           [db-name {:db-time-taken-ms time-taken-ms
                     :db-result-count result-count}]))
       (into {})))

(defn summarise-query-results [watdiv-query-results]
  (merge (select-keys (first watdiv-query-results) [:bench-ns :crux-node-type])
         {:bench-type "queries"
          :time-taken-ms (->> watdiv-query-results (map :time-taken-ms) (reduce +))}
         (->> (for [db-name (keys db-query-results)]
                (let [watdiv-results-with-db (for [query-result watdiv-query-results]
                                               (merge query-result (get query-result db-name)))
                      both-completed (->> watdiv-results-with-db (filter (every-pred :db-result-count :result-count)))
                      crux-correct (->> both-completed (filter #(= (:db-result-count %) (:result-count %))))
                      correct-idxs (into #{} (map :query-idx) crux-correct)]
                  [db-name
                   {:crux-failures (->> both-completed (map :query-idx) (remove correct-idxs) sort vec)
                    :crux-errors (->> watdiv-results-with-db (filter :db-result-count) (remove :result-count) (map :query-idx) sort vec)

                    :crux-time-taken (->> crux-correct (map :time-taken-ms) (reduce +) (Duration/ofMillis) (str))
                    :db-time-taken (->> crux-correct (map :db-time-taken-ms) (reduce +) (Duration/ofMillis) (str))}]))
              (into {}))))

(defn run-watdiv-bench [node {:keys [test-count] :as opts}]
  (bench/with-bench-ns :watdiv-crux
    (bench/with-crux-dimensions
      (ingest-crux node)

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
  (with-redefs [watdiv/watdiv-input-file (io/resource "watdiv.10.nt")]
    (bench/with-nodes [node (select-keys bench/nodes ["standalone-rocksdb"])]
      (run-watdiv-bench node {:test-count 10}))))
