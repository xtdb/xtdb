(ns core2.sql.tpch-sql-test
  (:require [core2.sql.parser :as p]
            [core2.sql.plan :as plan]
            [core2.sql.plan-test :as pt]
            [clojure.java.io :as io]
            [clojure.test :as t]))

(t/deftest test-parse-tpch-queries
  (doseq [q (range 22)
          :let [f (format "q%02d.sql" (inc q))
                sql-ast (p/parse (slurp (io/resource (str "core2/sql/tpch/" f))))
                {:keys [errs plan]} (plan/plan-query sql-ast)]]
    (t/is (empty? errs) (str f))
    (t/is (vector? plan))))

(defn slurp-tpch-query [query-no]
   (slurp (io/resource (str "core2/sql/tpch/" (format "q%02d.sql" query-no)))))

(t/deftest test-q1-pricing-summary-report
  (t/is
    (=plan-file
      "test-q1-pricing-summary-report"
      (pt/plan-sql (slurp-tpch-query 1)))))

(t/deftest test-q2-minimum-cost-supplier
  (t/is
    (=plan-file
      "test-q2-minimum-cost-supplier"
      (pt/plan-sql (slurp-tpch-query 2)))))

(t/deftest test-q3-shipping-priority
  (t/is
    (=plan-file
      "test-q3-shipping-priority"
      (pt/plan-sql (slurp-tpch-query 3)))))

(t/deftest test-q4-order-priority-checking
  (t/is
    (=plan-file
      "test-q4-order-priority-checking"
      (pt/plan-sql (slurp-tpch-query 4)))))

(t/deftest test-q5-local-supplier-volume
  (t/is
    (=plan-file
      "test-q5-local-supplier-volume"
      (pt/plan-sql (slurp-tpch-query 5)))))

(t/deftest test-q6-forecasting-revenue-change
  (t/is
    (=plan-file
      "test-q6-forecasting-revenue-change"
      (pt/plan-sql (slurp-tpch-query 6)))))

(t/deftest test-q7-volume-shipping
  (t/is
    (=plan-file
      "test-q7-volume-shipping"
      (pt/plan-sql (slurp-tpch-query 7)))))

(t/deftest test-q8-national-market-share
  (t/is
    (=plan-file
      "test-q8-national-market-share"
      (pt/plan-sql (slurp-tpch-query 8)))))

(t/deftest test-q9-product-type-profit-measure
  (t/is
    (=plan-file
      "test-q9-product-type-profit-measure"
      (pt/plan-sql (slurp-tpch-query 9)))))

(t/deftest test-q10-returned-item-reporting
  (t/is
    (=plan-file
      "test-q10-returned-item-reporting"
      (pt/plan-sql (slurp-tpch-query 10)))))

(t/deftest test-q11-important-stock-identification
  (t/is
    (=plan-file
      "test-q11-important-stock-identification"
      (pt/plan-sql (slurp-tpch-query 11)))))

(t/deftest test-q12-shipping-modes-and-order-priority
  (t/is
    (=plan-file
      "test-q12-shipping-modes-and-order-priority"
      (pt/plan-sql (slurp-tpch-query 12)))))

(t/deftest test-q13-customer-distribution
  (t/is
    (=plan-file
      "test-q13-customer-distribution"
      (pt/plan-sql (slurp-tpch-query 13)))))

(t/deftest test-q14-promotion-effect
  (t/is
    (=plan-file
      "test-q14-promotion-effect"
      (pt/plan-sql (slurp-tpch-query 14)))))

(t/deftest test-q15-top-supplier
  (t/is
    (=plan-file
      "test-q15-top-supplier"
      (pt/plan-sql (slurp-tpch-query 15)))))

(t/deftest test-q16-part-supplier-relationship
  (t/is
    (=plan-file
      "test-q16-part-supplier-relationship"
      (pt/plan-sql (slurp-tpch-query 16)))))

(t/deftest test-q17-small-quantity-order-revenue
  (t/is
    (=plan-file
      "test-q17-small-quantity-order-revenue"
      (pt/plan-sql (slurp-tpch-query 17)))))

(t/deftest test-q18-large-volume-customer
  (t/is
    (=plan-file
      "test-q18-large-volume-customer"
      (pt/plan-sql (slurp-tpch-query 18)))))

(t/deftest test-q19-discounted-revenue ;;TODO unable to decorr, select stuck under this top/union exists thing
  (t/is
    (=plan-file
      "test-q19-discounted-revenue"
      (pt/plan-sql (slurp-tpch-query 19)))))

(t/deftest test-q20-potential-part-promotion
  (t/is
    (=plan-file
      "test-q20-potential-part-promotion"
      (pt/plan-sql (slurp-tpch-query 20)))))

(t/deftest test-q21-suppliers-who-kept-orders-waiting
  (t/is
    (=plan-file
      "test-q21-suppliers-who-kept-orders-waiting"
      (pt/plan-sql (slurp-tpch-query 21)))))

(t/deftest test-q22-global-sales-opportunity
  (t/is
    (=plan-file
      "test-q22-global-sales-opportunity"
      (pt/plan-sql (slurp-tpch-query 22)))))
