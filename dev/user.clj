(ns user
  (:require [clojure.tools.namespace.repl :as ctn]))

(doseq [ns '[user core2.temporal.simd]]
  (ctn/disable-reload! (create-ns ns)))

(defn reset []
  (ctn/refresh))

(defn dev []
  (require 'dev)
  (in-ns 'dev))

(comment
  (def node (time (core2.test-util/->local-node {:node-dir (core2.util/->path "target/tpch-queries-sf-001")})))
  (with-open [watermark (core2.core/open-watermark node)]
    (with-bindings {#'core2.tpch-queries/*node* node
                    #'core2.tpch-queries/*watermark* watermark}
      (time
       (doseq [q [#'core2.tpch-queries/tpch-q1-pricing-summary-report
                  #'core2.tpch-queries/tpch-q2-minimum-cost-supplier
                  #'core2.tpch-queries/tpch-q3-shipping-priority
                  #'core2.tpch-queries/tpch-q4-order-priority-checking
                  #'core2.tpch-queries/tpch-q5-local-supplier-volume
                  #'core2.tpch-queries/tpch-q6-forecasting-revenue-change
                  #'core2.tpch-queries/tpch-q7-volume-shipping
                  #'core2.tpch-queries/tpch-q8-national-market-share
                  #'core2.tpch-queries/tpch-q9-product-type-profit-measure
                  #'core2.tpch-queries/tpch-q10-returned-item-reporting
                  #'core2.tpch-queries/tpch-q11-important-stock-identification
                  #'core2.tpch-queries/tpch-q12-shipping-modes-and-order-priority
                  #'core2.tpch-queries/tpch-q13-customer-distribution
                  #'core2.tpch-queries/tpch-q14-promotion-effect
                  #'core2.tpch-queries/tpch-q15-top-supplier
                  #'core2.tpch-queries/tpch-q16-part-supplier-relationship
                  #'core2.tpch-queries/tpch-q17-small-quantity-order-revenue
                  #'core2.tpch-queries/tpch-q18-large-volume-customer
                  #'core2.tpch-queries/tpch-q19-discounted-revenue
                  #'core2.tpch-queries/tpch-q20-potential-part-promotion
                  #'core2.tpch-queries/tpch-q21-suppliers-who-kept-orders-waiting
                  #'core2.tpch-queries/tpch-q22-global-sales-opportunity]]
         (prn q)
         (prn (time (count (@q)))))))))
