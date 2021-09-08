(ns xtdb.bench.tpch
  (:require [xtdb.api :as xt]
            [xtdb.bench :as bench]
            [xtdb.fixtures.tpch :as tpch]))

(defn run-tpch-query [node n]
  (xt/q (xt/db node) (assoc (get tpch/tpch-queries (dec n)) :timeout 120000)))

(defn run-tpch-queries [node {:keys [scale-factor] :as opts}]
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
                         (tpch/load-docs! node scale-factor tpch/tpch-entity->pkey-doc)
                         {:success true})))))

(defn run-tpch [node {:keys [scale-factor] :as opts}]
  (let [{:keys [scale-factor] :as opts} (assoc opts :scale-factor (or scale-factor 0.01))]
    (bench/with-bench-ns :tpch
      (bench/with-xtdb-dimensions
        (bench/run-bench :ingest
          (bench/with-additional-index-metrics node
            (tpch/load-docs! node scale-factor tpch/tpch-entity->pkey-doc)
            {:success true}))

        ;; TODO we may want to split this up, à la WatDiv, so that we can see if
        ;; specific queries are slower than our comparison databases
        (bench/run-bench :queries
          {:success? (run-tpch-queries node opts)})

        (bench/run-bench :queries-warm
          {:success? (run-tpch-queries node opts)})))))
