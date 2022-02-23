(ns xtdb.bench.tpch
  (:require [xtdb.bench :as bench]
            [xtdb.fixtures.tpch :as tpch]
            [xtdb.api :as xt]))

(defn run-tpch-query [node n]
  (tpch/run-query (xt/db node)
                  (-> (get tpch/tpch-queries (dec n))
                      (assoc :timeout 120000))))

(defn run-tpch-queries [node {:keys [scale-factor]}]
  (every? true? (for [n (range 1 23)]
                  (let [actual (run-tpch-query node n)]
                    (if (= 0.01 scale-factor)
                      (tpch/validate-tpch-query actual (tpch/parse-tpch-result n))
                      (boolean actual))))))

(defn run-tpch-ingest-only [node {:keys [scale-factor], :or {scale-factor 0.01}}]
  (bench/with-bench-ns :tpch-ingest-only
    (bench/with-xtdb-dimensions
      (bench/run-bench :ingest
                       (bench/with-additional-index-metrics node
                         (tpch/load-docs! node scale-factor)
                         {:success true})))))

(defn run-tpch [node {:keys [scale-factor] :as opts}]
  (let [{:keys [scale-factor] :as opts} (assoc opts :scale-factor (or scale-factor 0.01))]
    (bench/with-bench-ns :tpch
      (bench/with-xtdb-dimensions
        (bench/run-bench :ingest
          (bench/with-additional-index-metrics node
            (tpch/load-docs! node scale-factor)
            {:success true}))

        ;; TODO we may want to split this up, à la WatDiv, so that we can see if
        ;; specific queries are slower than our comparison databases
        (bench/run-bench :queries
          {:success? (run-tpch-queries node opts)})

        (bench/run-bench :queries-warm
          {:success? (run-tpch-queries node opts)})))))
