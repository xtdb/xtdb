(ns crux.bench.watdiv-crux
  (:require [crux.bench.watdiv :as watdiv]
            [crux.bench :as bench]
            [clojure.java.io :as io]
            [crux.rdf :as rdf]
            [crux.api :as crux]
            [crux.sparql :as sparql]))

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

(defn run-watdiv-bench [node {:keys [test-count] :as opts}]
  (bench/with-bench-ns :watdiv-crux
    (bench/with-crux-dimensions
      (ingest-crux node)

      (let [rdf4j-results (some-> (io/resource "rdf4j-results.edn") parse-results)]
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
                             {:result-count (count (crux/q (crux/db node) (sparql/sparql->datalog q)))}))))))))))))
