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

(t/deftest test-q10-returned-item-reporting
  (t/is (= [{:c_custkey (Text. "custkey_121")
             :c_name (Text. "Customer#000000121")
             :revenue 282635.1719
             :c_acctbal 6428.32
             :c_phone (Text. "27-411-990-2959")
             :n_name (Text. "PERU")
             :c_address (Text. "tv nCR2YKupGN73mQudO")
             :c_comment (Text. "uriously stealthy ideas. carefully final courts use carefully")}
            {:c_custkey (Text. "custkey_124")
             :c_name (Text. "Customer#000000124")
             :revenue 222182.5188
             :c_acctbal 1842.49
             :c_phone (Text. "28-183-750-7809")
             :n_name (Text. "CHINA")
             :c_address (Text. "aTbyVAW5tCd,v09O")
             :c_comment (Text. "le fluffily even dependencies. quietly s")}
            {:c_custkey (Text. "custkey_106")
             :c_name (Text. "Customer#000000106")
             :revenue 190241.3334
             :c_acctbal 3288.42
             :c_phone (Text. "11-751-989-4627")
             :n_name (Text. "ARGENTINA")
             :c_address (Text. "xGCOEAUjUNG")
             :c_comment (Text. "lose slyly. ironic accounts along the evenly regular theodolites wake about the special, final gifts. ")}
            {:c_custkey (Text. "custkey_16")
             :c_name (Text. "Customer#000000016")
             :revenue 161422.04609999998
             :c_acctbal 4681.03
             :c_phone (Text. "20-781-609-3107")
             :n_name (Text. "IRAN")
             :c_address (Text. "cYiaeMLZSMAOQ2 d0W,")
             :c_comment (Text. "kly silent courts. thinly regular theodolites sleep fluffily after ")}
            {:c_custkey (Text. "custkey_44")
             :c_name (Text. "Customer#000000044")
             :revenue 149364.56519999998
             :c_acctbal 7315.94
             :c_phone (Text. "26-190-260-5375")
             :n_name (Text. "MOZAMBIQUE")
             :c_address (Text. "Oi,dOSPwDu4jo4x,,P85E0dmhZGvNtBwi")
             :c_comment (Text. "r requests around the unusual, bold a")}
            {:c_custkey (Text. "custkey_71")
             :c_name (Text. "Customer#000000071")
             :revenue 129481.0245
             :c_acctbal -611.19
             :c_phone (Text. "17-710-812-5403")
             :n_name (Text. "GERMANY")
             :c_address (Text. "TlGalgdXWBmMV,6agLyWYDyIz9MKzcY8gl,w6t1B")
             :c_comment (Text. "g courts across the regular, final pinto beans are blithely pending ac")}
            {:c_custkey (Text. "custkey_89")
             :c_name (Text. "Customer#000000089")
             :revenue 121663.1243
             :c_acctbal 1530.76
             :c_phone (Text. "24-394-451-5404")
             :n_name (Text. "KENYA")
             :c_address (Text. "dtR, y9JQWUO6FoJExyp8whOU")
             :c_comment (Text. "counts are slyly beyond the slyly final accounts. quickly final ideas wake. r")}
            {:c_custkey (Text. "custkey_112")
             :c_name (Text. "Customer#000000112")
             :revenue 111137.7141
             :c_acctbal 2953.35
             :c_phone (Text. "29-233-262-8382")
             :n_name (Text. "ROMANIA")
             :c_address (Text. "RcfgG3bO7QeCnfjqJT1")
             :c_comment (Text. "rmanently unusual multipliers. blithely ruthless deposits are furiously along the")}
            {:c_custkey (Text. "custkey_62")
             :c_name (Text. "Customer#000000062")
             :revenue 106368.0153
             :c_acctbal 595.61
             :c_phone (Text. "17-361-978-7059")
             :n_name (Text. "GERMANY")
             :c_address (Text. "upJK2Dnw13,")
             :c_comment (Text. "kly special dolphins. pinto beans are slyly. quickly regular accounts are furiously a")}
            {:c_custkey (Text. "custkey_146")
             :c_name (Text. "Customer#000000146")
             :revenue 103265.98879999999
             :c_acctbal 3328.68
             :c_phone (Text. "13-835-723-3223")
             :n_name (Text. "CANADA")
             :c_address (Text. "GdxkdXG9u7iyI1,,y5tq4ZyrcEy")
             :c_comment (Text. "ffily regular dinos are slyly unusual requests. slyly specia")}
            {:c_custkey (Text. "custkey_19")
             :c_name (Text. "Customer#000000019")
             :revenue 99306.0127
             :c_acctbal 8914.71
             :c_phone (Text. "28-396-526-5053")
             :n_name (Text. "CHINA")
             :c_address (Text. "uc,3bHIx84H,wdrmLOjVsiqXCq2tr")
             :c_comment (Text. " nag. furiously careful packages are slyly at the accounts. furiously regular in")}
            {:c_custkey (Text. "custkey_145")
             :c_name (Text. "Customer#000000145")
             :revenue 99256.90179999999
             :c_acctbal 9748.93
             :c_phone (Text. "23-562-444-8454")
             :n_name (Text. "JORDAN")
             :c_address (Text. "kQjHmt2kcec cy3hfMh969u")
             :c_comment (Text. "ests? express, express instructions use. blithely fina")}
            {:c_custkey (Text. "custkey_103")
             :c_name (Text. "Customer#000000103")
             :revenue 97311.77240000002
             :c_acctbal 2757.45
             :c_phone (Text. "19-216-107-2107")
             :n_name (Text. "INDONESIA")
             :c_address (Text. "8KIsQX4LJ7QMsj6DrtFtXu0nUEdV,8a")
             :c_comment (Text. "furiously pending notornis boost slyly around the blithely ironic ideas? final, even instructions cajole fl")}
            {:c_custkey (Text. "custkey_136")
             :c_name (Text. "Customer#000000136")
             :revenue 95855.39799999999
             :c_acctbal -842.39
             :c_phone (Text. "17-501-210-4726")
             :n_name (Text. "GERMANY")
             :c_address (Text. "QoLsJ0v5C1IQbh,DS1")
             :c_comment (Text. "ackages sleep ironic, final courts. even requests above the blithely bold requests g")}
            {:c_custkey (Text. "custkey_53")
             :c_name (Text. "Customer#000000053")
             :revenue 92568.9124
             :c_acctbal 4113.64
             :c_phone (Text. "25-168-852-5363")
             :n_name (Text. "MOROCCO")
             :c_address (Text. "HnaxHzTfFTZs8MuCpJyTbZ47Cm4wFOOgib")
             :c_comment (Text. "ar accounts are. even foxes are blithely. fluffily pending deposits boost")}
            {:c_custkey (Text. "custkey_49")
             :c_name (Text. "Customer#000000049")
             :revenue 90965.7262
             :c_acctbal 4573.94
             :c_phone (Text. "20-908-631-4424")
             :n_name (Text. "IRAN")
             :c_address (Text. "cNgAeX7Fqrdf7HQN9EwjUa4nxT,68L FKAxzl")
             :c_comment (Text. "nusual foxes! fluffily pending packages maintain to the regular ")}
            {:c_custkey (Text. "custkey_37")
             :c_name (Text. "Customer#000000037")
             :revenue 88065.74579999999
             :c_acctbal -917.75
             :c_phone (Text. "18-385-235-7162")
             :n_name (Text. "INDIA")
             :c_address (Text. "7EV4Pwh,3SboctTWt")
             :c_comment (Text. "ilent packages are carefully among the deposits. furiousl")}
            {:c_custkey (Text. "custkey_82")
             :c_name (Text. "Customer#000000082")
             :revenue 86998.9644
             :c_acctbal 9468.34
             :c_phone (Text. "28-159-442-5305")
             :n_name (Text. "CHINA")
             :c_address (Text. "zhG3EZbap4c992Gj3bK,3Ne,Xn")
             :c_comment (Text. "s wake. bravely regular accounts are furiously. regula")}
            {:c_custkey (Text. "custkey_125")
             :c_name (Text. "Customer#000000125")
             :revenue 84808.068
             :c_acctbal -234.12
             :c_phone (Text. "29-261-996-3120")
             :n_name (Text. "ROMANIA")
             :c_address (Text. ",wSZXdVR xxIIfm9s8ITyLl3kgjT6UC07GY0Y")
             :c_comment (Text. "x-ray finally after the packages? regular requests c")}
            {:c_custkey (Text. "custkey_59")
             :c_name (Text. "Customer#000000059")
             :revenue 84655.5711
             :c_acctbal 3458.6
             :c_phone (Text. "11-355-584-3112")
             :n_name (Text. "ARGENTINA")
             :c_address (Text. "zLOCP0wh92OtBihgspOGl4")
             :c_comment (Text. "ously final packages haggle blithely after the express deposits. furiou")}]
           (tpch-queries/tpch-q10-returned-item-reporting))))

(t/deftest test-q12-shipping-modes-and-order-priority
  (t/is (= [{:l_shipmode (Text. "MAIL")
             :high_line_count 5
             :low_line_count 5}
            {:l_shipmode (Text. "SHIP")
             :high_line_count 5
             :low_line_count 10}]
           (tpch-queries/tpch-q12-shipping-modes-and-order-priority))))

(t/deftest test-q14-promotion-effect
  (t/is (= [{:promo_revenue 15.23021261159725}]
           (tpch-queries/tpch-q14-promotion-effect))))
