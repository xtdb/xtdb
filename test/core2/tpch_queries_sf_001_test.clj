(ns core2.tpch-queries-sf-001-test
  (:require [clojure.test :as t]
            [core2.tpch-queries :as tpch-queries])
  (:import org.apache.arrow.vector.util.Text))

(t/use-fixtures :once (tpch-queries/with-tpch-data 0.01))

;; TODO: some of these values are wrong, see (slurp (io/resource "io/airlift/tpch/queries/q1.result"))

(t/deftest ^:integration test-q1-pricing-summary-report
  (t/is (= [{:l_returnflag (Text. "A"),
             :l_linestatus (Text. "F"),
             :sum_qty 380456,
             :sum_base_price 532341074,
             :sum_disc_price 5.058224414861E8,
             :sum_charge 5.26165934000839E8,
             :avg_qty 25.575154611454693,
             :avg_price 35785.22949717666,
             :avg_disc 0.05008133906964238,
             :count_order 14876}
            {:l_returnflag (Text. "N"),
             :l_linestatus (Text. "F"),
             :sum_qty 8971,
             :sum_base_price 12384637,
             :sum_disc_price 1.1798257208E7,
             :sum_charge 1.2282485056933E7,
             :avg_qty 25.778735632183906,
             :avg_price 35588.03735632184,
             :avg_disc 0.047758620689655175,
             :count_order 348}
            {:l_returnflag (Text. "N"),
             :l_linestatus (Text. "O"),
             :sum_qty 742308,
             :sum_base_price 1040762808,
             :sum_disc_price 9.890562040665E8,
             :sum_charge 1.028705326106105E9,
             :avg_qty 25.454632741238598,
             :avg_price 35689.00651532817,
             :avg_disc 0.04992695974213017,
             :count_order 29162}
            {:l_returnflag (Text. "R"),
             :l_linestatus (Text. "F"),
             :sum_qty 381449,
             :sum_base_price 534587384,
             :sum_disc_price 5.079964544067E8,
             :sum_charge 5.28524219358903E8,
             :avg_qty 25.597168165346933,
             :avg_price 35873.53268017716,
             :avg_disc 0.049827539927526504,
             :count_order 14902}]
           (tpch-queries/tpch-q1-pricing-summary-report))))
