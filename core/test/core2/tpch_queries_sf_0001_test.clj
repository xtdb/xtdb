(ns core2.tpch-queries-sf-0001-test
  (:require [clojure.test :as t]
            [core2.tpch :as tpch]
            [core2.tpch-test :as tpch-test]
            [core2.util :as util]))

(t/use-fixtures :once (partial tpch-test/with-tpch-data
                               {:scale-factor 0.001
                                :node-dir (util/->path "target/tpch-queries-sf-0001")}))

(t/deftest test-q1-pricing-summary-report
  (t/is (= [{:l_returnflag "A"
             :l_linestatus "F"
             :sum_qty 37474.0
             :sum_base_price 3.756962463999998E7
             :sum_disc_price 3.5676192096999995E7
             :sum_charge 3.710141622242404E7
             :avg_qty 25.354533152909337
             :avg_price 25419.231826792948
             :avg_disc 0.050866035182679493
             :count_order 1478}
            {:l_returnflag "N"
             :l_linestatus "F"
             :sum_qty 1041.0
             :sum_base_price 1041301.07
             :sum_disc_price 999060.8979999998
             :sum_charge 1036450.80228
             :avg_qty 27.394736842105264
             :avg_price 27402.659736842103
             :avg_disc 0.042894736842105284
             :count_order 38}
            {:l_returnflag "N"
             :l_linestatus "O"
             :sum_qty 75168.0
             :sum_base_price 7.538495536999969E7
             :sum_disc_price 7.165316630340016E7
             :sum_charge 7.449879813307281E7
             :avg_qty 25.558653519211152
             :avg_price 25632.422771166166
             :avg_disc 0.04969738184291069
             :count_order 2941}
            {:l_returnflag "R"
             :l_linestatus "F"
             :sum_qty 36511.0
             :sum_base_price 3.657084124E7
             :sum_disc_price 3.473847287580004E7
             :sum_charge 3.616906011219294E7
             :avg_qty 25.059025394646532
             :avg_price 25100.09693891558
             :avg_disc 0.050027453671928686
             :count_order 1457}]
           (tpch-test/run-query tpch/tpch-q1-pricing-summary-report))))

(t/deftest test-q2-minimum-cost-supplier
  (t/is (= []
           (tpch-test/run-query tpch/tpch-q2-minimum-cost-supplier))))

(t/deftest test-q3-shipping-priority
  (t/is (= [{:l_orderkey "orderkey_1637"
             :revenue 164224.9253
             :o_orderdate (util/->zdt #inst "1995-02-08")
             :o_shippriority 0}
            {:l_orderkey "orderkey_5191"
             :revenue 49378.309400000006
             :o_orderdate (util/->zdt #inst "1994-12-11")
             :o_shippriority 0}
            {:l_orderkey "orderkey_742"
             :revenue 43728.048
             :o_orderdate (util/->zdt #inst "1994-12-23")
             :o_shippriority 0}
            {:l_orderkey "orderkey_3492"
             :revenue 43716.072400000005,
             :o_orderdate (util/->zdt #inst "1994-11-24")
             :o_shippriority 0}
            {:l_orderkey "orderkey_2883"
             :revenue 36666.9612,
             :o_orderdate (util/->zdt #inst "1995-01-23")
             :o_shippriority 0}
            {:l_orderkey "orderkey_998"
             :revenue 11785.548600000002,
             :o_orderdate (util/->zdt #inst "1994-11-26")
             :o_shippriority 0}
            {:l_orderkey "orderkey_3430"
             :revenue 4726.6775,
             :o_orderdate (util/->zdt #inst "1994-12-12")
             :o_shippriority 0}
            {:l_orderkey "orderkey_4423"
             :revenue 3055.9365,
             :o_orderdate (util/->zdt #inst "1995-02-17")
             :o_shippriority 0}]
           (tpch-test/run-query tpch/tpch-q3-shipping-priority))))

(t/deftest test-q4-order-priority-checking
  (t/is (= [{:o_orderpriority "1-URGENT", :order_count 9}
            {:o_orderpriority "2-HIGH", :order_count 7}
            {:o_orderpriority "3-MEDIUM", :order_count 9}
            {:o_orderpriority "4-NOT SPECIFIED", :order_count 8}
            {:o_orderpriority "5-LOW", :order_count 12}]
           (tpch-test/run-query tpch/tpch-q4-order-priority-checking))))

(t/deftest test-q5-local-supplier-volume
  (t/is (= []
           (tpch-test/run-query tpch/tpch-q5-local-supplier-volume))))

(t/deftest test-q6-forecasting-revenue-change
  (t/is (= [{:revenue 77949.9186}]
           (tpch-test/run-query tpch/tpch-q6-forecasting-revenue-change))))

(t/deftest test-q7-volume-shipping
  (t/is (= []
           (tpch-test/run-query tpch/tpch-q7-volume-shipping))))

(t/deftest test-q8-national-market-share
  (t/is (= [{:o_year 1995, :mkt_share 0.0}
            {:o_year 1996, :mkt_share 0.0}]
           (tpch-test/run-query tpch/tpch-q8-national-market-share))))

(t/deftest test-q9-product-type-profit-measure
  ;; on 0.001 there are duplicate ids generated in the partsupp table
  ;; we treat these as 'last document wins', so there'll be some partsupp docs that have already been replaced
  (t/is (= [{:nation "ARGENTINA", :o_year 1998, :sum_profit 17779.069700000007}
            {:nation "ARGENTINA", :o_year 1997, :sum_profit 13943.953800000003}
            {:nation "ARGENTINA", :o_year 1996, :sum_profit 7641.422700000003}
            {:nation "ARGENTINA", :o_year 1995, :sum_profit 20892.752500000002}
            {:nation "ARGENTINA", :o_year 1994, :sum_profit 15088.352599999998}
            {:nation "ARGENTINA", :o_year 1993, :sum_profit 17586.344600000004}
            {:nation "ARGENTINA", :o_year 1992, :sum_profit 28732.461499999994}
            {:nation "ETHIOPIA", :o_year 1998, :sum_profit 28217.159999999996}
            {:nation "ETHIOPIA", :o_year 1996, :sum_profit 33970.65}
            {:nation "ETHIOPIA", :o_year 1995, :sum_profit 37720.35}
            {:nation "ETHIOPIA", :o_year 1994, :sum_profit 37251.01}
            {:nation "ETHIOPIA", :o_year 1993, :sum_profit 23782.61}
            {:nation "IRAN", :o_year 1997, :sum_profit 23590.007999999998}
            {:nation "IRAN", :o_year 1996, :sum_profit 7428.232500000005}
            {:nation "IRAN", :o_year 1995, :sum_profit 21000.996499999994}
            {:nation "IRAN", :o_year 1994, :sum_profit 29408.13}
            {:nation "IRAN", :o_year 1993, :sum_profit 49876.41499999999}
            {:nation "IRAN", :o_year 1992, :sum_profit 52064.24}
            {:nation "IRAQ", :o_year 1998, :sum_profit 11619.960399999996}
            {:nation "IRAQ", :o_year 1997, :sum_profit 47910.246}
            {:nation "IRAQ", :o_year 1996, :sum_profit 18459.567499999997}
            {:nation "IRAQ", :o_year 1995, :sum_profit 32782.37010000001}
            {:nation "IRAQ", :o_year 1994, :sum_profit 9041.2317}
            {:nation "IRAQ", :o_year 1993, :sum_profit 30687.2625}
            {:nation "IRAQ", :o_year 1992, :sum_profit 29098.2557}
            {:nation "KENYA", :o_year 1998, :sum_profit 33148.3345}
            {:nation "KENYA", :o_year 1997, :sum_profit 54355.016500000005}
            {:nation "KENYA", :o_year 1996, :sum_profit 43794.4118}
            {:nation "KENYA", :o_year 1995, :sum_profit 69156.86249999999}
            {:nation "KENYA", :o_year 1994, :sum_profit 88510.84769999998}
            {:nation "KENYA", :o_year 1993, :sum_profit 95483.2956}
            {:nation "KENYA", :o_year 1992, :sum_profit 93473.44700000001}
            {:nation "MOROCCO", :o_year 1998, :sum_profit 80034.8312}
            {:nation "MOROCCO", :o_year 1997, :sum_profit 48218.429000000004}
            {:nation "MOROCCO", :o_year 1996, :sum_profit 111533.7568}
            {:nation "MOROCCO", :o_year 1995, :sum_profit 190286.556}
            {:nation "MOROCCO", :o_year 1994, :sum_profit 120458.78839999999}
            {:nation "MOROCCO", :o_year 1993, :sum_profit 121013.21870000001}
            {:nation "MOROCCO", :o_year 1992, :sum_profit 170329.14880000002}
            {:nation "PERU", :o_year 1998, :sum_profit 58330.754799999995}
            {:nation "PERU", :o_year 1997, :sum_profit 54422.6497}
            {:nation "PERU", :o_year 1996, :sum_profit 22129.370899999994}
            {:nation "PERU", :o_year 1995, :sum_profit 94384.7288}
            {:nation "PERU", :o_year 1994, :sum_profit 55023.063200000004}
            {:nation "PERU", :o_year 1993, :sum_profit 87328.3271}
            {:nation "PERU", :o_year 1992, :sum_profit 59177.11840000001}
            {:nation "UNITED KINGDOM", :o_year 1998, :sum_profit 62507.25600000001}
            {:nation "UNITED KINGDOM", :o_year 1997, :sum_profit 52221.212199999994}
            {:nation "UNITED KINGDOM", :o_year 1996, :sum_profit 114503.24999999999}
            {:nation "UNITED KINGDOM", :o_year 1995, :sum_profit 147460.92960000003}
            {:nation "UNITED KINGDOM", :o_year 1994, :sum_profit 70682.74199999998}
            {:nation "UNITED KINGDOM", :o_year 1993, :sum_profit 192054.806}
            {:nation "UNITED KINGDOM", :o_year 1992, :sum_profit 29955.24040000001}
            {:nation "UNITED STATES", :o_year 1998, :sum_profit 32847.96}
            {:nation "UNITED STATES", :o_year 1997, :sum_profit 30849.5}
            {:nation "UNITED STATES", :o_year 1996, :sum_profit 56125.46000000001}
            {:nation "UNITED STATES", :o_year 1995, :sum_profit 15961.7977}
            {:nation "UNITED STATES", :o_year 1994, :sum_profit 31671.2}
            {:nation "UNITED STATES", :o_year 1993, :sum_profit 55057.469}
            {:nation "UNITED STATES", :o_year 1992, :sum_profit 51970.23}]
           (tpch-test/run-query tpch/tpch-q9-product-type-profit-measure))))

(t/deftest test-q10-returned-item-reporting
  (t/is (= [{:c_custkey "custkey_121"
             :c_name "Customer#000000121"
             :revenue 282635.17189999996
             :c_acctbal 6428.32
             :c_phone "27-411-990-2959"
             :n_name "PERU"
             :c_address "tv nCR2YKupGN73mQudO"
             :c_comment "uriously stealthy ideas. carefully final courts use carefully"}
            {:c_custkey "custkey_124"
             :c_name "Customer#000000124"
             :revenue 222182.5188
             :c_acctbal 1842.49
             :c_phone "28-183-750-7809"
             :n_name "CHINA"
             :c_address "aTbyVAW5tCd,v09O"
             :c_comment "le fluffily even dependencies. quietly s"}
            {:c_custkey "custkey_106"
             :c_name "Customer#000000106"
             :revenue 190241.3334
             :c_acctbal 3288.42
             :c_phone "11-751-989-4627"
             :n_name "ARGENTINA"
             :c_address "xGCOEAUjUNG"
             :c_comment "lose slyly. ironic accounts along the evenly regular theodolites wake about the special, final gifts. "}
            {:c_custkey "custkey_16"
             :c_name "Customer#000000016"
             :revenue 161422.0461
             :c_acctbal 4681.03
             :c_phone "20-781-609-3107"
             :n_name "IRAN"
             :c_address "cYiaeMLZSMAOQ2 d0W,"
             :c_comment "kly silent courts. thinly regular theodolites sleep fluffily after "}
            {:c_custkey "custkey_44"
             :c_name "Customer#000000044"
             :revenue 149364.56519999998
             :c_acctbal 7315.94
             :c_phone "26-190-260-5375"
             :n_name "MOZAMBIQUE"
             :c_address "Oi,dOSPwDu4jo4x,,P85E0dmhZGvNtBwi"
             :c_comment "r requests around the unusual, bold a"}
            {:c_custkey "custkey_71"
             :c_name "Customer#000000071"
             :revenue 129481.02450000001
             :c_acctbal -611.19
             :c_phone "17-710-812-5403"
             :n_name "GERMANY"
             :c_address "TlGalgdXWBmMV,6agLyWYDyIz9MKzcY8gl,w6t1B"
             :c_comment "g courts across the regular, final pinto beans are blithely pending ac"}
            {:c_custkey "custkey_89"
             :c_name "Customer#000000089"
             :revenue 121663.1243
             :c_acctbal 1530.76
             :c_phone "24-394-451-5404"
             :n_name "KENYA"
             :c_address "dtR, y9JQWUO6FoJExyp8whOU"
             :c_comment "counts are slyly beyond the slyly final accounts. quickly final ideas wake. r"}
            {:c_custkey "custkey_112"
             :c_name "Customer#000000112"
             :revenue 111137.71409999998
             :c_acctbal 2953.35
             :c_phone "29-233-262-8382"
             :n_name "ROMANIA"
             :c_address "RcfgG3bO7QeCnfjqJT1"
             :c_comment "rmanently unusual multipliers. blithely ruthless deposits are furiously along the"}
            {:c_custkey "custkey_62"
             :c_name "Customer#000000062"
             :revenue 106368.0153
             :c_acctbal 595.61
             :c_phone "17-361-978-7059"
             :n_name "GERMANY"
             :c_address "upJK2Dnw13,"
             :c_comment "kly special dolphins. pinto beans are slyly. quickly regular accounts are furiously a"}
            {:c_custkey "custkey_146"
             :c_name "Customer#000000146"
             :revenue 103265.98879999999
             :c_acctbal 3328.68
             :c_phone "13-835-723-3223"
             :n_name "CANADA"
             :c_address "GdxkdXG9u7iyI1,,y5tq4ZyrcEy"
             :c_comment "ffily regular dinos are slyly unusual requests. slyly specia"}
            {:c_custkey "custkey_19"
             :c_name "Customer#000000019"
             :revenue 99306.01270000002
             :c_acctbal 8914.71
             :c_phone "28-396-526-5053"
             :n_name "CHINA"
             :c_address "uc,3bHIx84H,wdrmLOjVsiqXCq2tr"
             :c_comment " nag. furiously careful packages are slyly at the accounts. furiously regular in"}
            {:c_custkey "custkey_145"
             :c_name "Customer#000000145"
             :revenue 99256.9018
             :c_acctbal 9748.93
             :c_phone "23-562-444-8454"
             :n_name "JORDAN"
             :c_address "kQjHmt2kcec cy3hfMh969u"
             :c_comment "ests? express, express instructions use. blithely fina"}
            {:c_custkey "custkey_103"
             :c_name "Customer#000000103"
             :revenue 97311.77240000002
             :c_acctbal 2757.45
             :c_phone "19-216-107-2107"
             :n_name "INDONESIA"
             :c_address "8KIsQX4LJ7QMsj6DrtFtXu0nUEdV,8a"
             :c_comment "furiously pending notornis boost slyly around the blithely ironic ideas? final, even instructions cajole fl"}
            {:c_custkey "custkey_136"
             :c_name "Customer#000000136"
             :revenue 95855.39799999999
             :c_acctbal -842.39
             :c_phone "17-501-210-4726"
             :n_name "GERMANY"
             :c_address "QoLsJ0v5C1IQbh,DS1"
             :c_comment "ackages sleep ironic, final courts. even requests above the blithely bold requests g"}
            {:c_custkey "custkey_53"
             :c_name "Customer#000000053"
             :revenue 92568.9124
             :c_acctbal 4113.64
             :c_phone "25-168-852-5363"
             :n_name "MOROCCO"
             :c_address "HnaxHzTfFTZs8MuCpJyTbZ47Cm4wFOOgib"
             :c_comment "ar accounts are. even foxes are blithely. fluffily pending deposits boost"}
            {:c_custkey "custkey_49"
             :c_name "Customer#000000049"
             :revenue 90965.7262
             :c_acctbal 4573.94
             :c_phone "20-908-631-4424"
             :n_name "IRAN"
             :c_address "cNgAeX7Fqrdf7HQN9EwjUa4nxT,68L FKAxzl"
             :c_comment "nusual foxes! fluffily pending packages maintain to the regular "}
            {:c_custkey "custkey_37"
             :c_name "Customer#000000037"
             :revenue 88065.74579999999
             :c_acctbal -917.75
             :c_phone "18-385-235-7162"
             :n_name "INDIA"
             :c_address "7EV4Pwh,3SboctTWt"
             :c_comment "ilent packages are carefully among the deposits. furiousl"}
            {:c_custkey "custkey_82"
             :c_name "Customer#000000082"
             :revenue 86998.9644
             :c_acctbal 9468.34
             :c_phone "28-159-442-5305"
             :n_name "CHINA"
             :c_address "zhG3EZbap4c992Gj3bK,3Ne,Xn"
             :c_comment "s wake. bravely regular accounts are furiously. regula"}
            {:c_custkey "custkey_125"
             :c_name "Customer#000000125"
             :revenue 84808.068
             :c_acctbal -234.12
             :c_phone "29-261-996-3120"
             :n_name "ROMANIA"
             :c_address ",wSZXdVR xxIIfm9s8ITyLl3kgjT6UC07GY0Y"
             :c_comment "x-ray finally after the packages? regular requests c"}
            {:c_custkey "custkey_59"
             :c_name "Customer#000000059"
             :revenue 84655.5711
             :c_acctbal 3458.6
             :c_phone "11-355-584-3112"
             :n_name "ARGENTINA"
             :c_address "zLOCP0wh92OtBihgspOGl4"
             :c_comment "ously final packages haggle blithely after the express deposits. furiou"}]
           (tpch-test/run-query tpch/tpch-q10-returned-item-reporting))))

(t/deftest test-q11-important-stock-identification
  (t/is (= []
           (tpch-test/run-query tpch/tpch-q11-important-stock-identification))))

(t/deftest test-q12-shipping-modes-and-order-priority
  (t/is (= [{:l_shipmode "MAIL"
             :high_line_count 5
             :low_line_count 5}
            {:l_shipmode "SHIP"
             :high_line_count 5
             :low_line_count 10}]
           (tpch-test/run-query tpch/tpch-q12-shipping-modes-and-order-priority))))

(t/deftest test-q13-customer-distribution
  (t/is (= [{:c_count 0 :custdist 50}
            {:c_count 16 :custdist 8}
            {:c_count 17 :custdist 7}
            {:c_count 20 :custdist 6}
            {:c_count 13 :custdist 6}
            {:c_count 12 :custdist 6}
            {:c_count 9 :custdist 6}
            {:c_count 23 :custdist 5}
            {:c_count 14 :custdist 5}
            {:c_count 10 :custdist 5}
            {:c_count 21 :custdist 4}
            {:c_count 18 :custdist 4}
            {:c_count 11 :custdist 4}
            {:c_count 8 :custdist 4}
            {:c_count 7 :custdist 4}
            {:c_count 26 :custdist 3}
            {:c_count 22 :custdist 3}
            {:c_count 6 :custdist 3}
            {:c_count 5 :custdist 3}
            {:c_count 4 :custdist 3}
            {:c_count 29 :custdist 2}
            {:c_count 24 :custdist 2}
            {:c_count 19 :custdist 2}
            {:c_count 15 :custdist 2}
            {:c_count 28 :custdist 1}
            {:c_count 25 :custdist 1}
            {:c_count 3 :custdist 1}]
           (tpch-test/run-query tpch/tpch-q13-customer-distribution))))

(t/deftest test-q14-promotion-effect
  (t/is (= [{:promo_revenue 15.230212611597254}]
           (tpch-test/run-query tpch/tpch-q14-promotion-effect))))

(t/deftest test-q15-top-supplier
  (t/is (= [{:total_revenue 797313.3838
             :s_suppkey "suppkey_10"
             :s_name "Supplier#000000010"
             :s_address "Saygah3gYWMp72i PY"
             :s_phone "34-852-489-8585"}]
           (tpch-test/run-query tpch/tpch-q15-top-supplier))))

(t/deftest test-q16-part-supplier-relationship
  (t/is (= [{:p_brand "Brand#11" :p_type "PROMO ANODIZED TIN" :p_size 45 :supplier_cnt 4}
            {:p_brand "Brand#11" :p_type "SMALL PLATED COPPER" :p_size 45 :supplier_cnt 4}
            {:p_brand "Brand#11" :p_type "STANDARD POLISHED TIN" :p_size 45 :supplier_cnt 4}
            {:p_brand "Brand#13" :p_type "MEDIUM ANODIZED STEEL" :p_size 36 :supplier_cnt 4}
            {:p_brand "Brand#14" :p_type "SMALL ANODIZED NICKEL" :p_size 45 :supplier_cnt 4}
            {:p_brand "Brand#15" :p_type "LARGE ANODIZED BRASS" :p_size 45 :supplier_cnt 4}
            {:p_brand "Brand#21" :p_type "LARGE BURNISHED COPPER" :p_size 19 :supplier_cnt 4}
            {:p_brand "Brand#23" :p_type "ECONOMY BRUSHED COPPER" :p_size 9 :supplier_cnt 4}
            {:p_brand "Brand#25" :p_type "MEDIUM PLATED BRASS" :p_size 45 :supplier_cnt 4}
            {:p_brand "Brand#31" :p_type "ECONOMY PLATED STEEL" :p_size 23 :supplier_cnt 4}
            {:p_brand "Brand#31" :p_type "PROMO POLISHED TIN" :p_size 23 :supplier_cnt 4}
            {:p_brand "Brand#32" :p_type "MEDIUM BURNISHED BRASS" :p_size 49 :supplier_cnt 4}
            {:p_brand "Brand#33" :p_type "LARGE BRUSHED TIN" :p_size 36 :supplier_cnt 4}
            {:p_brand "Brand#33" :p_type "SMALL BURNISHED NICKEL" :p_size 3 :supplier_cnt 4}
            {:p_brand "Brand#34" :p_type "LARGE PLATED BRASS" :p_size 45 :supplier_cnt 4}
            {:p_brand "Brand#34" :p_type "MEDIUM BRUSHED COPPER" :p_size 9 :supplier_cnt 4}
            {:p_brand "Brand#34" :p_type "SMALL PLATED BRASS" :p_size 14 :supplier_cnt 4}
            {:p_brand "Brand#35" :p_type "STANDARD ANODIZED STEEL" :p_size 23 :supplier_cnt 4}
            {:p_brand "Brand#43" :p_type "PROMO POLISHED BRASS" :p_size 19 :supplier_cnt 4}
            {:p_brand "Brand#43" :p_type "SMALL BRUSHED NICKEL" :p_size 9 :supplier_cnt 4}
            {:p_brand "Brand#44" :p_type "SMALL PLATED COPPER" :p_size 19 :supplier_cnt 4}
            {:p_brand "Brand#52" :p_type "MEDIUM BURNISHED TIN" :p_size 45 :supplier_cnt 4}
            {:p_brand "Brand#52" :p_type "SMALL BURNISHED NICKEL" :p_size 14 :supplier_cnt 4}
            {:p_brand "Brand#53" :p_type "MEDIUM BRUSHED COPPER" :p_size 3 :supplier_cnt 4}
            {:p_brand "Brand#55" :p_type "STANDARD ANODIZED BRASS" :p_size 36 :supplier_cnt 4}
            {:p_brand "Brand#55" :p_type "STANDARD BRUSHED COPPER" :p_size 3 :supplier_cnt 4}
            {:p_brand "Brand#13" :p_type "SMALL BRUSHED NICKEL" :p_size 19 :supplier_cnt 2}
            {:p_brand "Brand#25" :p_type "SMALL BURNISHED COPPER" :p_size 3 :supplier_cnt 2}
            {:p_brand "Brand#43" :p_type "MEDIUM ANODIZED BRASS" :p_size 14 :supplier_cnt 2}
            {:p_brand "Brand#53" :p_type "STANDARD PLATED STEEL" :p_size 45 :supplier_cnt 2}
            {:p_brand "Brand#24" :p_type "MEDIUM PLATED STEEL" :p_size 19 :supplier_cnt 1}
            {:p_brand "Brand#51" :p_type "ECONOMY POLISHED STEEL" :p_size 49 :supplier_cnt 1}
            {:p_brand "Brand#53" :p_type "LARGE BURNISHED NICKEL" :p_size 23 :supplier_cnt 1}
            {:p_brand "Brand#54" :p_type "ECONOMY ANODIZED BRASS" :p_size 9 :supplier_cnt 1}]
           (tpch-test/run-query tpch/tpch-q16-part-supplier-relationship))))

(t/deftest test-q17-small-quantity-order-revenue
  (t/is (= []
           (tpch-test/run-query tpch/tpch-q17-small-quantity-order-revenue))))

(t/deftest test-q18-large-volume-customer
  (t/is (= []
           (tpch-test/run-query tpch/tpch-q18-large-volume-customer))))

(t/deftest test-q19-discounted-revenue
  (t/is (= []
           (tpch-test/run-query tpch/tpch-q19-discounted-revenue))))

(t/deftest test-q20-potential-part-promotion
  (t/is (= []
           (tpch-test/run-query tpch/tpch-q20-potential-part-promotion))))

(t/deftest test-q21-suppliers-who-kept-orders-waiting
  (t/is (= []
           (tpch-test/run-query tpch/tpch-q21-suppliers-who-kept-orders-waiting))))

(t/deftest test-q22-global-sales-opportunity
  (t/is (= [{:cntrycode "13"
             :numcust 1
             :totacctbal 5679.84}
            {:cntrycode "17"
             :numcust 1
             :totacctbal 9127.27}
            {:cntrycode "18"
             :numcust 2
             :totacctbal 14647.99}
            {:cntrycode "23"
             :numcust 1
             :totacctbal 9255.67}
            {:cntrycode "29"
             :numcust 2
             :totacctbal 17195.08}
            {:cntrycode "30"
             :numcust 1
             :totacctbal 7638.57}
            {:cntrycode "31"
             :numcust 1
             :totacctbal 9331.13}]
           (tpch-test/run-query tpch/tpch-q22-global-sales-opportunity))))
