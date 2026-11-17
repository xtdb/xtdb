(ns xtdb.bench.tpch-test
  (:require [clojure.test :as t :refer [deftest]]
            [xtdb.bench :as b]
            [xtdb.bench.tpch]))

(deftest an-sql-run-logs-under-its-own-benchmark-name
  (t/is (= "TPC-H (OLAP)" (:title (b/->benchmark :tpch {:query-language :ra}))))
  (t/is (= "TPC-H (OLAP, SQL)" (:title (b/->benchmark :tpch {:query-language :sql})))))

(defn- stage-names [task]
  (when task
    (cons (:stage task) (mapcat stage-names (:tasks task)))))

(deftest hot-iterations-repeat-the-hot-pass-over-the-chosen-queries
  (let [stages (->> (:tasks (b/->benchmark :tpch {:scale-factor 0.01, :no-load? true, :queries #{1 6}, :hot-iterations 3}))
                    (mapcat stage-names)
                    (remove nil?))]
    (t/is (= [:cold-queries :cold-queries-q1-pricing-summary-report :cold-queries-q6-forecasting-revenue-change
              :hot-queries :hot-queries-q1-pricing-summary-report :hot-queries-q6-forecasting-revenue-change
              :hot-2-queries :hot-2-queries-q1-pricing-summary-report :hot-2-queries-q6-forecasting-revenue-change
              :hot-3-queries :hot-3-queries-q1-pricing-summary-report :hot-3-queries-q6-forecasting-revenue-change]
             stages))))
