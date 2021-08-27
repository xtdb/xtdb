(ns xtdb.bench.tpch-stress-test
  (:require [xtdb.bench :as bench]
            [xtdb.api :as xt]
            [clojure.tools.logging :as log]
            [xtdb.fixtures.tpch :as tpch]))

(defn- load-tpch-docs [node]
  (bench/run-bench :ingest
    (bench/with-additional-index-metrics node
      (tpch/load-docs! node)
      {:success? true})))

(def fields '{:l_orderkey l_orderkey
              :l_partkey l_partkey
              :l_suppkey l_suppkey
              :l_linenumber l_linenumber
              :l_quantity l_quantity
              :l_extendedprice l_extendedprice
              :l_discount l_discount
              :l_tax l_tax
              :l_returnflag l_returnflag
              :l_linestatus l_linestatus
              :l_shipdate l_shipdate
              :l_commitdate l_commitdate
              :l_receiptdate l_receiptdate
              :l_shipinstruct l_shipinstruct
              :l_shipmode l_shipmode
              :l_comment l_comment})

(defn run-stress-queries [node {:keys [query-count field-count] :or {query-count 50 field-count (count fields)} :as opts}]
  (let [q {:find '[e],
           :where (->> fields
                       (take field-count)
                       (mapcat (fn [[a v]] [['e a v] [(list 'identity v) (gensym)]]))
                       vec)
           :timeout 1000000}]
    (log/infof "Stressing query: %s" (prn-str q))
    (bench/run-bench :query-stress
      (bench/with-thread-pool
        opts
        (fn [{:keys [idx q]}]
          (log/info (format "Starting query #%s" idx))
          {:count (count (xt/q (xt/db node) q))})
        (->> q
             (repeat query-count)
             (map-indexed (fn [idx q] {:idx idx, :q q}))))
      {:run-success? true
       :num-queries query-count
       :num-fields field-count})))

(defn run-tpch-stress-test [node {:keys [query-count field-count] :as opts}]
  (bench/with-bench-ns :tpch-stress
    (bench/with-crux-dimensions
      (load-tpch-docs node)
      (run-stress-queries node opts))))

(comment
  (let [node (user/crux-node)]
    (bench/with-bench-ns :tpch-stress
      (load-tpch-docs node)
      (run-stress-queries node {}))))
