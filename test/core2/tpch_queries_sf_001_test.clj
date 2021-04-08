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

(t/deftest ^:integration test-q5-local-supplier-volume
  (t/is (= [{:n_name (Text. "VIETNAM")
             :revenue 1000926.6999}
            {:n_name (Text. "CHINA")
             :revenue 740210.757}
            {:n_name (Text. "JAPAN")
             :revenue 660651.2424999999}
            {:n_name (Text. "INDONESIA")
             :revenue 566379.5276}
            {:n_name (Text. "INDIA")
             :revenue 422874.68439999997}]
           (tpch-queries/tpch-q5-local-supplier-volume))))

(t/deftest ^:integration test-q6-forecasting-revenue-change
  (t/is (= [{:revenue 1193053.2253}]
           (tpch-queries/tpch-q6-forecasting-revenue-change))))

(t/deftest ^:integration test-q10-returned-item-reporting
  (t/is (= [{:c_custkey (Text. "custkey_679")
             :c_name (Text. "Customer#000000679")
             :revenue 378211.32519999996
             :c_acctbal 1394.44
             :c_phone (Text. "20-146-696-9508")
             :n_name (Text. "IRAN")
             :c_address (Text. "IJf1FlZL9I9m,rvofcoKy5pRUOjUQV")
             :c_comment (Text. "ely pending frays boost carefully")}
            {:c_custkey (Text. "custkey_1201")
             :c_name (Text. "Customer#000001201")
             :revenue 374331.534
             :c_acctbal 5165.39
             :c_phone (Text. "20-825-400-1187")
             :n_name (Text. "IRAN")
             :c_address (Text. "LfCSVKWozyWOGDW02g9UX,XgH5YU2o5ql1zBrN")
             :c_comment (Text. "lyly pending packages. special requests sleep-- platelets use blithely after the instructions. sometimes even id")}
            {:c_custkey (Text. "custkey_422")
             :c_name (Text. "Customer#000000422")
             :revenue 366451.0126
             :c_acctbal -272.14
             :c_phone (Text. "19-299-247-2444")
             :n_name (Text. "INDONESIA")
             :c_address (Text. "AyNzZBvmIDo42JtjP9xzaK3pnvkh Qc0o08ssnvq")
             :c_comment (Text. "eposits; furiously ironic packages accordi")}
            {:c_custkey (Text. "custkey_334")
             :c_name (Text. "Customer#000000334")
             :revenue 360370.755
             :c_acctbal -405.91
             :c_phone (Text. "14-947-291-5002")
             :n_name (Text. "EGYPT")
             :c_address (Text. "OPN1N7t4aQ23TnCpc")
             :c_comment (Text. "fully busily special ideas. carefully final excuses lose slyly carefully express accounts. even, ironic platelets ar")}
            {:c_custkey (Text. "custkey_805")
             :c_name (Text. "Customer#000000805")
             :revenue 359448.9036
             :c_acctbal 511.69
             :c_phone (Text. "20-732-989-5653")
             :n_name (Text. "IRAN")
             :c_address (Text. "wCKx5zcHvwpSffyc9qfi9dvqcm9LT,cLAG")
             :c_comment (Text. "busy sentiments. pending packages haggle among the express requests-- slyly regular excuses above the slyl")}
            {:c_custkey (Text. "custkey_932")
             :c_name (Text. "Customer#000000932")
             :revenue 341608.2753
             :c_acctbal 6553.37
             :c_phone (Text. "23-300-708-7927")
             :n_name (Text. "JORDAN")
             :c_address (Text. "HN9Ap0NsJG7Mb8O")
             :c_comment (Text. "packages boost slyly along the furiously express foxes. ev")}
            {:c_custkey (Text. "custkey_853")
             :c_name (Text. "Customer#000000853")
             :revenue 341236.6246
             :c_acctbal -444.73
             :c_phone (Text. "12-869-161-3468")
             :n_name (Text. "BRAZIL")
             :c_address (Text. "U0 9PrwAgWK8AE0GHmnCGtH9BTexWWv87k")
             :c_comment (Text. "yly special deposits wake alongside of")}
            {:c_custkey (Text. "custkey_872")
             :c_name (Text. "Customer#000000872")
             :revenue 338328.7808
             :c_acctbal -858.61
             :c_phone (Text. "27-357-139-7164")
             :n_name (Text. "PERU")
             :c_address (Text. "vLP7iNZBK4B,HANFTKabVI3AO Y9O8H")
             :c_comment (Text. " detect. packages wake slyly express foxes. even deposits ru")}
            {:c_custkey (Text. "custkey_737")
             :c_name (Text. "Customer#000000737")
             :revenue 338185.3365
             :c_acctbal 2501.74
             :c_phone (Text. "28-658-938-1102")
             :n_name (Text. "CHINA")
             :c_address (Text. "NdjG1k243iCLSoy1lYqMIrpvuH1Uf75")
             :c_comment (Text. "ding to the final platelets. regular packages against the carefully final ideas hag")}
            {:c_custkey (Text. "custkey_1118")
             :c_name (Text. "Customer#000001118")
             :revenue 319875.728
             :c_acctbal 4130.18
             :c_phone (Text. "21-583-715-8627")
             :n_name (Text. "IRAQ")
             :c_address (Text. "QHg,DNvEVXaYoCdrywazjAJ")
             :c_comment (Text. "y regular requests above the blithely ironic accounts use slyly bold packages: regular pinto beans eat carefully spe")}
            {:c_custkey (Text. "custkey_223")
             :c_name (Text. "Customer#000000223")
             :revenue 319564.27499999997
             :c_acctbal 7476.2
             :c_phone (Text. "30-193-643-1517")
             :n_name (Text. "SAUDI ARABIA")
             :c_address (Text. "ftau6Pk,brboMyEl,,kFm")
             :c_comment (Text. "al, regular requests run furiously blithely silent packages. blithely ironic accounts across the furious")}
            {:c_custkey (Text. "custkey_808")
             :c_name (Text. "Customer#000000808")
             :revenue 314774.6167
             :c_acctbal 5561.93
             :c_phone (Text. "29-531-319-7726")
             :n_name (Text. "ROMANIA")
             :c_address (Text. "S2WkSKCGtnbhcFOp6MWcuB3rzFlFemVNrg ")
             :c_comment (Text. " unusual deposits. furiously even packages against the furiously even ac")}
            {:c_custkey (Text. "custkey_478")
             :c_name (Text. "Customer#000000478")
             :revenue 299651.8026
             :c_acctbal -210.4
             :c_phone (Text. "11-655-291-2694")
             :n_name (Text. "ARGENTINA")
             :c_address (Text. "clyq458DIkXXt4qLyHlbe,n JueoniF")
             :c_comment (Text. "o the foxes. ironic requests sleep. c")}
            {:c_custkey (Text. "custkey_1441")
             :c_name (Text. "Customer#000001441")
             :revenue 294705.3935
             :c_acctbal 9465.15
             :c_phone (Text. "33-681-334-4499")
             :n_name (Text. "UNITED KINGDOM")
             :c_address (Text. "u0YYZb46w,pwKo5H9vz d6B9zK4BOHhG jx")
             :c_comment (Text. "nts haggle quietly quickly final accounts. slyly regular accounts among the sl")}
            {:c_custkey (Text. "custkey_1478")
             :c_name (Text. "Customer#000001478")
             :revenue 294431.9178
             :c_acctbal 9701.54
             :c_phone (Text. "17-420-484-5959")
             :n_name (Text. "GERMANY")
             :c_address (Text. "x7HDvJDDpR3MqZ5vg2CanfQ1hF0j4")
             :c_comment (Text. "ng the furiously bold foxes. even notornis above the unusual ")}
            {:c_custkey (Text. "custkey_211")
             :c_name (Text. "Customer#000000211")
             :revenue 287905.6368
             :c_acctbal 4198.72
             :c_phone (Text. "23-965-335-9471")
             :n_name (Text. "JORDAN")
             :c_address (Text. "URhlVPzz4FqXem")
             :c_comment (Text. "furiously regular foxes boost fluffily special ideas. carefully regular dependencies are. slyly ironic ")}
            {:c_custkey (Text. "custkey_197")
             :c_name (Text. "Customer#000000197")
             :revenue 283190.48069999996
             :c_acctbal 9860.22
             :c_phone (Text. "11-107-312-6585")
             :n_name (Text. "ARGENTINA")
             :c_address (Text. "UeVqssepNuXmtZ38D")
             :c_comment (Text. "ickly final accounts cajole. furiously re")}
            {:c_custkey (Text. "custkey_1030")
             :c_name (Text. "Customer#000001030")
             :revenue 282557.3566
             :c_acctbal 6359.27
             :c_phone (Text. "18-759-877-1870")
             :n_name (Text. "INDIA")
             :c_address (Text. "Xpt1BiB5h9o")
             :c_comment (Text. "ding to the slyly unusual accounts. even requests among the evenly")}
            {:c_custkey (Text. "custkey_1049")
             :c_name (Text. "Customer#000001049")
             :revenue 281134.1117
             :c_acctbal 8747.99
             :c_phone (Text. "19-499-258-2851")
             :n_name (Text. "INDONESIA")
             :c_address (Text. "bZ1OcFhHaIZ5gMiH")
             :c_comment (Text. "uriously according to the furiously silent packages")}
            {:c_custkey (Text. "custkey_1094")
             :c_name (Text. "Customer#000001094")
             :revenue 274877.444
             :c_acctbal 2544.49
             :c_phone (Text. "12-234-721-9871")
             :n_name (Text. "BRAZIL")
             :c_address (Text. "OFz0eedTmPmXk2 3XM9v9Mcp13NVC0PK")
             :c_comment (Text. "tes serve blithely quickly pending foxes. express, quick accounts")}]
           (tpch-queries/tpch-q10-returned-item-reporting))))

(t/deftest ^:integration test-q12-shipping-modes-and-order-priority
  (t/is (= [{:l_shipmode (Text. "MAIL")
             :high_line_count 64
             :low_line_count 86}
            {:l_shipmode (Text. "SHIP")
             :high_line_count 61
             :low_line_count 96}]
           (tpch-queries/tpch-q12-shipping-modes-and-order-priority))))

(t/deftest ^:integration test-q14-promotion-effect
  (t/is (= [{:promo_revenue 15.486545812284072}]
           (tpch-queries/tpch-q14-promotion-effect))))
