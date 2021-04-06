(ns core2.tpch-queries-sf-0001-test
  (:require [clojure.test :as t]
            [core2.tpch-queries :as tpch-queries]
            [core2.util :as util])
  (:import org.apache.arrow.vector.util.Text))

(t/use-fixtures :once (tpch-queries/with-tpch-data 0.001 "tpch-queries-sf-0001"))

(t/deftest test-q1-pricing-summary-report
  (t/is (= [{:l_returnflag (Text. "A")
             :l_linestatus (Text. "F")
             :sum_qty 37474
             :sum_base_price 3.756962464E7
             :sum_disc_price 3.5676192097E7
             :sum_charge 3.7101416222424E7
             :avg_qty 25.354533152909337
             :avg_price 25419.231826792962
             :avg_disc 0.0508660351826793
             :count_order 1478}
            {:l_returnflag (Text. "N")
             :l_linestatus (Text. "F")
             :sum_qty 1041
             :sum_base_price 1041301.07
             :sum_disc_price 999060.898
             :sum_charge 1036450.8022800001
             :avg_qty 27.394736842105264
             :avg_price 27402.659736842103
             :avg_disc 0.04289473684210526
             :count_order 38}
            {:l_returnflag (Text. "N")
             :l_linestatus (Text. "O")
             :sum_qty 75168,
             :sum_base_price 7.538495537E7
             :sum_disc_price 7.16531663034E7
             :sum_charge 7.4498798133073E7
             :avg_qty 25.558653519211152
             :avg_price 25632.42277116627
             :avg_disc 0.049697381842910573
             :count_order 2941}
            {:l_returnflag (Text. "R")
             :l_linestatus (Text. "F")
             :sum_qty 36511
             :sum_base_price 3.657084124E7
             :sum_disc_price 3.47384728758E7
             :sum_charge 3.6169060112193E7
             :avg_qty 25.059025394646532
             :avg_price 25100.09693891558
             :avg_disc 0.05002745367192862
             :count_order 1457}]
           (tpch-queries/tpch-q1-pricing-summary-report))))

(t/deftest test-q3-shipping-priority
  (t/is (= [{:l_orderkey (Text. "orderkey_1637")
             :revenue 164224.9253
             :o_orderdate (util/date->local-date-time #inst "1995-02-08")
             :o_shippriority 0}
            {:l_orderkey (Text. "orderkey_5191")
             :revenue 49378.309400000006
             :o_orderdate (util/date->local-date-time #inst "1994-12-11")
             :o_shippriority 0}
            {:l_orderkey (Text. "orderkey_742")
             :revenue 43728.048
             :o_orderdate (util/date->local-date-time #inst "1994-12-23")
             :o_shippriority 0}
            {:l_orderkey (Text. "orderkey_3492")
             :revenue 43716.072400000005,
             :o_orderdate (util/date->local-date-time #inst "1994-11-24")
             :o_shippriority 0}
            {:l_orderkey (Text. "orderkey_2883")
             :revenue 36666.9612,
             :o_orderdate (util/date->local-date-time #inst "1995-01-23")
             :o_shippriority 0}
            {:l_orderkey (Text. "orderkey_998")
             :revenue 11785.548600000002,
             :o_orderdate (util/date->local-date-time #inst "1994-11-26")
             :o_shippriority 0}
            {:l_orderkey (Text. "orderkey_3430")
             :revenue 4726.6775,
             :o_orderdate (util/date->local-date-time #inst "1994-12-12")
             :o_shippriority 0}
            {:l_orderkey (Text. "orderkey_4423")
             :revenue 3055.9365,
             :o_orderdate (util/date->local-date-time #inst "1995-02-17")
             :o_shippriority 0}]
           (tpch-queries/tpch-q3-shipping-priority))))

(t/deftest test-q5-local-supplier-volume
  (t/is (= []
           (tpch-queries/tpch-q5-local-supplier-volume))))

(t/deftest test-q6-forecasting-revenue-change
  (t/is (= [{:revenue 77949.9186}]
           (tpch-queries/tpch-q6-forecasting-revenue-change))))
