(ns core2.sql.tpch-sql-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t]
            [core2.sql :as sql]
            core2.sql-test))

(t/deftest test-parse-tpch-queries
  (doseq [q (range 22)
          :let [f (format "q%02d.sql" (inc q))
                plan (sql/compile-query (slurp (io/resource (str "core2/sql/tpch/" f))))]]
    (t/is (vector? plan))))

(defn slurp-tpch-query [query-no]
   (slurp (io/resource (str "core2/sql/tpch/" (format "q%02d.sql" query-no)))))

(t/deftest test-q1-pricing-summary-report
  (t/is
    (=plan-file
      "test-q1-pricing-summary-report"
      (sql/compile-query (slurp-tpch-query 1)))))

(t/deftest test-q2-minimum-cost-supplier
  (t/is
    (=plan-file
      "test-q2-minimum-cost-supplier"
      (sql/compile-query (slurp-tpch-query 2)))))

(t/deftest test-q3-shipping-priority
  (t/is
    (=plan-file
      "test-q3-shipping-priority"
      (sql/compile-query (slurp-tpch-query 3)))))

(t/deftest test-q4-order-priority-checking
  (t/is
    (=plan-file
      "test-q4-order-priority-checking"
      (sql/compile-query (slurp-tpch-query 4)))))

(t/deftest test-q5-local-supplier-volume
  (t/is
    (=plan-file
      "test-q5-local-supplier-volume"
      (sql/compile-query (slurp-tpch-query 5)))))

(t/deftest test-q6-forecasting-revenue-change
  (t/is
    (=plan-file
      "test-q6-forecasting-revenue-change"
      (sql/compile-query (slurp-tpch-query 6)))))

(t/deftest test-q7-volume-shipping
  (t/is
    (=plan-file
      "test-q7-volume-shipping"
      (sql/compile-query (slurp-tpch-query 7)))))

(t/deftest test-q8-national-market-share
  (t/is
    (=plan-file
      "test-q8-national-market-share"
      (sql/compile-query (slurp-tpch-query 8)))))

(t/deftest test-q9-product-type-profit-measure
  (t/is
    (=plan-file
      "test-q9-product-type-profit-measure"
      (sql/compile-query (slurp-tpch-query 9)))))

(t/deftest test-q10-returned-item-reporting
  (t/is
    (=plan-file
      "test-q10-returned-item-reporting"
      (sql/compile-query (slurp-tpch-query 10)))))

(t/deftest test-q11-important-stock-identification
  (t/is
    (=plan-file
      "test-q11-important-stock-identification"
      (sql/compile-query (slurp-tpch-query 11)))))

(t/deftest test-q12-shipping-modes-and-order-priority
  (t/is
    (=plan-file
      "test-q12-shipping-modes-and-order-priority"
      (sql/compile-query (slurp-tpch-query 12)))))

(t/deftest test-q13-customer-distribution
  (t/is
    (=plan-file
      "test-q13-customer-distribution"
      (sql/compile-query (slurp-tpch-query 13)))))

(t/deftest test-q14-promotion-effect
  (t/is
    (=plan-file
      "test-q14-promotion-effect"
      (sql/compile-query (slurp-tpch-query 14)))))

(t/deftest test-q15-top-supplier
  (t/is
    (=plan-file
      "test-q15-top-supplier"
      (sql/compile-query (slurp-tpch-query 15)))))

(t/deftest test-q16-part-supplier-relationship
  (t/is
    (=plan-file
      "test-q16-part-supplier-relationship"
      (sql/compile-query (slurp-tpch-query 16)))))

(t/deftest test-q17-small-quantity-order-revenue
  (t/is
    (=plan-file
      "test-q17-small-quantity-order-revenue"
      (sql/compile-query (slurp-tpch-query 17)))))

(t/deftest test-q18-large-volume-customer
  (t/is
    (=plan-file
      "test-q18-large-volume-customer"
      (sql/compile-query (slurp-tpch-query 18)))))

(t/deftest test-q19-discounted-revenue ;;TODO unable to decorr, select stuck under this top/union exists thing
  (t/is
    (=plan-file
      "test-q19-discounted-revenue"
      (sql/compile-query (slurp-tpch-query 19)))))

(t/deftest test-q20-potential-part-promotion
  (t/is
    (=plan-file
      "test-q20-potential-part-promotion"
      (sql/compile-query (slurp-tpch-query 20)))))

(t/deftest test-q21-suppliers-who-kept-orders-waiting
  (t/is
    (=plan-file
      "test-q21-suppliers-who-kept-orders-waiting"
      (sql/compile-query (slurp-tpch-query 21)))))

(t/deftest test-q22-global-sales-opportunity
  (t/is
    (=plan-file
      "test-q22-global-sales-opportunity"
      (sql/compile-query (slurp-tpch-query 22)))))
