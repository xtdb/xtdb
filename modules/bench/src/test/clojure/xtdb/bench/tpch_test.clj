(ns xtdb.bench.tpch-test
  (:require [clojure.test :as t :refer [deftest]]
            [xtdb.bench :as b]
            [xtdb.bench.tpch]))

(deftest an-sql-run-logs-under-its-own-benchmark-name
  (t/is (= "TPC-H (OLAP)" (:title (b/->benchmark :tpch {:query-language :ra}))))
  (t/is (= "TPC-H (OLAP, SQL)" (:title (b/->benchmark :tpch {:query-language :sql})))))
