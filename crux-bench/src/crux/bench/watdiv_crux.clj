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
                     {:entity-count entity-count})))

(defn parse-results [resource]
  (with-open [rdr (io/reader resource)]
    (->> (line-seq rdr)
         (map read-string)
         (filter :result-count)
         (map (juxt :query-idx identity))
         (into {}))))

(defn ->query-result [watdiv-query-results]
  (let [both-completed (->> watdiv-query-results (filter (every-pred :rdf4j-result-count :result-count)))
        crux-correct (->> both-completed (filter #(= (:rdf4j-result-count %) (:result-count %))))
        correct-idxs (into #{} (map :query-idx) crux-correct)]

    {:bench-type "queries"
     :crux-failures (->> both-completed (map :query-idx) (remove correct-idxs) vec)
     :crux-errors (->> watdiv-query-results (filter :rdf4j-result-count) (remove :result-count) (mapv :query-idx))

     :time-taken-ms (->> watdiv-query-results (map :time-taken-ms) (reduce +))
     :crux-time-taken (->> crux-correct (map :time-taken-ms) (reduce +) (Duration/ofMillis) (str))
     :rdf4j-time-taken (->> crux-correct (map :rdf4j-time-taken-ms) (reduce +) (Duration/ofMillis) (str))}))

(defn run-watdiv-bench [node {:keys [test-count] :as opts}]
  (let [rdf4j-results (some-> (bench/load-from-s3 "rdf4j-3.0.0/rdf4j-20200214-174740Z.edn") parse-results)
        watdiv-results
        (bench/with-bench-ns :watdiv-crux
          (bench/with-crux-dimensions
            (ingest-crux node)

            (watdiv/with-watdiv-queries watdiv/watdiv-stress-100-1-sparql
              (fn [queries]
                (-> queries
                    (cond->> test-count (take test-count))
                    (->> (bench/with-thread-pool opts
                           (fn [{:keys [idx q]}]
                             (bench/with-dimensions {:rdf4j-time-taken-ms (get-in rdf4j-results [idx :time-taken-ms])
                                                     :rdf4j-result-count (get-in rdf4j-results [idx :result-count])
                                                     :query-idx idx}
                               (bench/run-bench (format "query-%d" idx)
                                 {:result-count (count (crux/q (crux/db node) (sparql/sparql->datalog q)))}))))))))))]

    (-> [(first watdiv-results) (->query-result (rest watdiv-results))]
        (bench/results->slack-message :watdiv-crux)
        (doto (bench/post-to-slack))
        (bench/send-email-via-ses :watdiv-crux))

    watdiv-results))

(comment
  (with-redefs [watdiv/watdiv-input-file (io/resource "watdiv.10.nt")]
    (bench/with-node [node]
      (run-watdiv-bench node {:test-count 10}))))
