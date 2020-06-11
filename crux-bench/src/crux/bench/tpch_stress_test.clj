(ns crux.bench.tpch-stress-test
  (:require [crux.bench :as bench]
            [crux.api :as crux]
            [crux.fixtures.tpch :as tpch]))

(defn- load-tpch-docs [node]
  (bench/run-bench :ingest
   (bench/with-additional-index-metrics node
     (tpch/load-docs! node))))

(defn run-tpch-stress-test [node {:keys [query-count] :as opts}]
  (bench/with-bench-ns :tpch-stress
    (bench/with-crux-dimensions
      (load-tpch-docs node)
      (bench/run-bench
       :query-stress
       (dotimes [_ query-count]
         {:count (count (crux/q (crux/db node) '{:find [l_orderkey],
                                                 :where [[e :l_orderkey l_orderkey]
                                                         [e :l_partkey l_partkey]
                                                         [e :l_suppkey l_suppkey]
                                                         [e :l_linenumber l_linenumber]
                                                         [e :l_quantity l_quantity]
                                                         ;; [e :l_extendedprice l_extendedprice]
                                                         ;; [e :l_discount l_discount]
                                                         ;; [e :l_tax l_tax]
                                                         ;; [e :l_returnflag l_returnflag]
                                                         ;; [e :l_linestatus l_linestatus]
                                                         ;; [e :l_shipdate l_shipdate]
                                                         ;; [e :l_commitdate l_commitdate]
                                                         ;; [e :l_receiptdate l_receiptdate]
                                                         ;; [e :l_shipinstruct l_shipinstruct]
                                                         ;; [e :l_shipmode l_shipmode]
                                                         ;; [e :l_comment l_comment]
                                                         ]
                                                 :timeout 100000}))})
       {:run-success? true}))))
