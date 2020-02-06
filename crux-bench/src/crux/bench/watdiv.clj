(ns crux.bench.watdiv
  (:require [clojure.java.io :as io]
            [crux.api :as crux]
            [crux.bench :as bench]
            [crux.rdf :as rdf]
            [crux.sparql :as sparql]
            [clojure.tools.logging :as log]))

(def watdiv-tests
  {"watdiv-stress-100/warmup.1.desc" "watdiv-stress-100/warmup.sparql"
   "watdiv-stress-100/test.1.desc" "watdiv-stress-100/test.1.sparql"
   "watdiv-stress-100/test.2.desc" "watdiv-stress-100/test.2.sparql"
   "watdiv-stress-100/test.3.desc" "watdiv-stress-100/test.3.sparql"
   "watdiv-stress-100/test.4.desc" "watdiv-stress-100/test.4.sparql"})

(def query-timeout-ms 30000)

(def watdiv-input-file (io/resource "watdiv.10M.nt"))
(def watdiv-stress-100-1-sparql (io/resource "watdiv-stress-100/test.1.sparql"))

(defn with-watdiv-queries [resource f]
  (with-open [sparql-in (io/reader resource)]
    (f (->> (line-seq sparql-in)
            (map-indexed (fn [idx q]
                           {:idx idx, :q q}))))))

(defn ingest-crux
  [node]
  (bench/run-bench :ingest
    (let [{:keys [last-tx entity-count]} (with-open [in (io/input-stream watdiv-input-file)]
                                           (rdf/submit-ntriples node in 1000))]
      (crux/await-tx node last-tx)
      {:entity-count entity-count})))

(defn run-watdiv-bench-crux [node {:keys [test-count] :as opts}]
  (bench/with-bench-ns :watdiv-crux
    (ingest-crux node)

    (with-watdiv-queries watdiv-stress-100-1-sparql
      (fn [queries]
        (-> queries
            (cond->> test-count (take test-count))
            (->> (bench/with-thread-pool opts
                   (fn [{:keys [idx q]}]
                     (bench/run-bench (format "query-%d" idx)
                                      {:result-count (count (crux/q (crux/db node) (sparql/sparql->datalog q)))
                                       :query-idx idx})))))))))
