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

(defn run-watdiv-bench-crux [node {:keys [test-count] :as opts}]
  (bench/with-bench-ns :watdiv-crux
    (bench/with-crux-dimensions
      (ingest-crux node)

      (watdiv/with-watdiv-queries watdiv/watdiv-stress-100-1-sparql
        (fn [queries]
          (-> queries
              (cond->> test-count (take test-count))
              (->> (bench/with-thread-pool opts
                     (fn [{:keys [idx q]}]
                       (bench/run-bench (format "query-%d" idx)
                         {:result-count (count (crux/q (crux/db node) (sparql/sparql->datalog q)))
                          :query-idx idx}))))))))))
