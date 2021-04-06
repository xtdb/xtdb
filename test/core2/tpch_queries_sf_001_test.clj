(ns core2.tpch-queries-sf-001-test
  (:require [clojure.test :as t]
            [core2.util :as util]
            [core2.tpch-queries :as tpch-queries])
  (:import org.apache.arrow.vector.util.Text))

(t/use-fixtures :once (tpch-queries/with-tpch-data 0.01 "tpch-queries-sf-001"))

(t/deftest ^:integration test-q1-pricing-summary-report
  (t/is (= [{:l_returnflag (Text. "A")
             :l_linestatus (Text. "F")
             :sum_qty 380456
             :sum_base_price 5.3234821165E8
             :sum_disc_price 5.058224414861E8
             :sum_charge 5.26165934000839E8
             :avg_qty 25.575154611454693
             :avg_price 35785.709306937344
             :avg_disc 0.05008133906964238
             :count_order 14876}
            {:l_returnflag (Text. "N")
             :l_linestatus (Text. "F")
             :sum_qty 8971
             :sum_base_price 1.238480137E7
             :sum_disc_price 1.1798257208E7
             :sum_charge 1.2282485056933E7
             :avg_qty 25.778735632183906
             :avg_price 35588.50968390804
             :avg_disc 0.047758620689655175
             :count_order 348}
            {:l_returnflag (Text. "N")
             :l_linestatus (Text. "O")
             :sum_qty 742802
             :sum_base_price 1.04150284145E9
             :sum_disc_price 9.897375186346E8
             :sum_charge 1.02941853152335E9
             :avg_qty 25.45498783454988
             :avg_price 35691.1292090744
             :avg_disc 0.04993111956409993
             :count_order 29181}
            {:l_returnflag (Text. "R")
             :l_linestatus (Text. "F")
             :sum_qty 381449
             :sum_base_price 5.3459444535E8
             :sum_disc_price 5.079964544067E8
             :sum_charge 5.28524219358903E8
             :avg_qty 25.597168165346933
             :avg_price 35874.00653268018
             :avg_disc 0.049827539927526504
             :count_order 14902}]
           (tpch-queries/tpch-q1-pricing-summary-report))))

(t/deftest ^:integration test-q3-shipping-priority
  (t/is (= [{:l_orderkey (Text. "orderkey_47714")
             :revenue 267010.5894
             :o_orderdate (util/date->local-date-time #inst "1995-03-11")
             :o_shippriority 0}
            {:l_orderkey (Text. "orderkey_22276")
             :revenue 266351.5562
             :o_orderdate (util/date->local-date-time #inst "1995-01-29")
             :o_shippriority 0}
            {:l_orderkey (Text. "orderkey_32965")
             :revenue 263768.3414
             :o_orderdate (util/date->local-date-time #inst "1995-02-25")
             :o_shippriority 0}
            {:l_orderkey (Text. "orderkey_21956")
             :revenue 254541.1285
             :o_orderdate (util/date->local-date-time #inst "1995-02-02")
             :o_shippriority 0}
            {:l_orderkey (Text. "orderkey_1637")
             :revenue 243512.79809999999
             :o_orderdate (util/date->local-date-time #inst "1995-02-08")
             :o_shippriority 0}
            {:l_orderkey (Text. "orderkey_10916")
             :revenue 241320.08140000002
             :o_orderdate (util/date->local-date-time #inst "1995-03-11")
             :o_shippriority 0}
            {:l_orderkey (Text. "orderkey_30497")
             :revenue 208566.69689999998
             :o_orderdate (util/date->local-date-time #inst "1995-02-07")
             :o_shippriority 0}
            {:l_orderkey (Text. "orderkey_450")
             :revenue 205447.42320000002
             :o_orderdate (util/date->local-date-time #inst "1995-03-05")
             :o_shippriority 0}
            {:l_orderkey (Text. "orderkey_47204")
             :revenue 204478.52130000002
             :o_orderdate (util/date->local-date-time #inst "1995-03-13")
             :o_shippriority 0}
            {:l_orderkey (Text. "orderkey_9696")
             :revenue 201502.21879999997
             :o_orderdate (util/date->local-date-time #inst "1995-02-20")
             :o_shippriority 0}]
           (tpch-queries/tpch-q3-shipping-priority))))
