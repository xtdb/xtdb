(ns core2.tpch-queries-sf-0001-test
  (:require [clojure.test :as t]
            [core2.tpch-queries :as tpch-queries])
  (:import org.apache.arrow.vector.util.Text))

(t/use-fixtures :once (tpch-queries/with-tpch-data 0.001 "tpch-queries-sf-0001"))

(t/deftest test-q1-pricing-summary-report
  (t/is (= [{:l_returnflag (Text. "A")
             :l_linestatus (Text. "F")
             :sum_qty 37474
             :sum_base_price 37568959
             :sum_disc_price 3.5676192097E7
             :sum_charge 3.7101416222424E7
             :avg_qty 25.354533152909337
             :avg_price 25418.78146143437
             :avg_disc 0.0508660351826793
             :count_order 1478}
            {:l_returnflag (Text. "N")
             :l_linestatus (Text. "F")
             :sum_qty 1041
             :sum_base_price 1041285
             :sum_disc_price 999060.898
             :sum_charge 1036450.8022800001
             :avg_qty 27.394736842105264,
             :avg_price 27402.236842105263,
             :avg_disc 0.04289473684210526,
             :count_order 38}
            {:l_returnflag (Text. "N")
             :l_linestatus (Text. "O")
             :sum_qty 75168,
             :sum_base_price 75383627,
             :sum_disc_price 7.16531663034E7,
             :sum_charge 7.4498798133073E7,
             :avg_qty 25.558653519211152,
             :avg_price 25631.971098265894
             :avg_disc 0.049697381842910573,
             :count_order 2941}
            {:l_returnflag (Text. "R")
             :l_linestatus (Text. "F")
             :sum_qty 36511
             :sum_base_price 36570197
             :sum_disc_price 3.47384728758E7
             :sum_charge 3.6169060112193E7
             :avg_qty 25.059025394646532,
             :avg_price 25099.654770075496,
             :avg_disc 0.05002745367192862,
             :count_order 1457}]
           (tpch-queries/tpch-q1-pricing-summary-report))))
