(ns core2.sql.tpch-sql-test
  (:require [core2.sql :as sql]
            [core2.sql.plan :as plan]
            [core2.sql.plan-test :as pt]
            [clojure.walk :as w]
            [clojure.java.io :as io]
            [clojure.test :as t])
  (:import [java.time LocalDate Period]))

(t/deftest test-parse-tpch-queries
  (doseq [q (range 22)
          :let [f (format "q%02d.sql" (inc q))
                sql-ast (sql/parse (slurp (io/resource (str "core2/sql/tpch/" f))))
                {:keys [errs plan]} (plan/plan-query sql-ast)]]
    (t/is (empty? errs) (str f))
    (t/is (vector? plan))))

(defn slurp-tpch-query [query-no]
   (slurp (io/resource (str "core2/sql/tpch/" (format "q%02d.sql" query-no)))))

(t/deftest test-q1-pricing-summary-report
  (t/is
   (=
    (w/postwalk-replace
      {'?date (LocalDate/parse "1998-12-01")
       '?period (Period/parse "P90D")}
      '[:rename
        {x1 l_returnflag, x13 sum_base_price, x2 l_linestatus, x12 sum_qty, x14 sum_disc_price, x17 avg_price, x15 sum_charge, x16 avg_qty, x18 avg_disc, x19 count_order}
        [:order-by
         [{x1 :asc} {x2 :asc}]
         [:group-by
          [x1 x2 {x12 (sum x3)} {x13 (sum x4)} {x14 (sum x9)} {x15 (sum x10)} {x16 (avg x3)} {x17 (avg x4)} {x18 (avg x5)} {x19 (count x11)}]
          [:map
           [{x9 (* x4 (- 1 x5))} {x10 (* (* x4 (- 1 x5)) (+ 1 x6))} {x11 1}]
           [:rename
            {l_returnflag x1, l_linestatus x2, l_quantity x3, l_extendedprice x4, l_discount x5, l_tax x6, l_shipdate x7}
            [:scan [l_returnflag l_linestatus l_quantity l_extendedprice l_discount l_tax {l_shipdate (<= l_shipdate (- ?date ?period))}]]]]]]])
    (pt/plan-sql (slurp-tpch-query 1)))))

(t/deftest test-q2-minimum-cost-supplier
  (t/is
    (=
     '[:rename
       {x6 s_acctbal, x7 s_name, x18 n_name, x1 p_partkey, x2 p_mfgr, x8 s_address, x9 s_phone, x10 s_comment}
       [:project
        [x6 x7 x18 x1 x2 x8 x9 x10]
        [:top
         {:limit 100}
         [:order-by
          [{x6 :desc} {x18 :asc} {x7 :asc} {x1 :asc}]
          [:select
           (= x16 x38)
           [:group-by
            [x1 x2 x3 x4 x6 x7 x8 x9 x10 x11 x12 x14 x15 x16 x18 x19 x20 x22 x23 $row_number$ {x38 (min x25)}]
            [:left-outer-join
             [{x1 x26}]
             [:map
              [{$row_number$ (row-number)}]
              [:join
               [{x20 x22}]
               [:join
                [{x12 x19}]
                [:join
                 [{x1 x14} {x11 x15}]
                 [:cross-join
                  [:rename {p_partkey x1, p_mfgr x2, p_size x3, p_type x4}
                   [:scan [p_partkey p_mfgr {p_size (= p_size 15)} {p_type (like p_type "%BRASS")}]]]
                  [:rename {s_acctbal x6, s_name x7, s_address x8, s_phone x9, s_comment x10, s_suppkey x11, s_nationkey x12}
                   [:scan [s_acctbal s_name s_address s_phone s_comment s_suppkey s_nationkey]]]]
                 [:rename {ps_partkey x14, ps_suppkey x15, ps_supplycost x16}
                  [:scan [ps_partkey ps_suppkey ps_supplycost]]]]
                [:rename {n_name x18, n_nationkey x19, n_regionkey x20}
                 [:scan [n_name n_nationkey n_regionkey]]]]
               [:rename {r_regionkey x22, r_name x23}
                [:scan [r_regionkey {r_name (= r_name "EUROPE")}]]]]]
             [:join
              [{x33 x35}]
              [:join
               [{x30 x32}]
               [:join
                [{x27 x29}]
                [:rename {ps_supplycost x25, ps_partkey x26, ps_suppkey x27}
                 [:scan [ps_supplycost ps_partkey ps_suppkey]]]
                [:rename {s_suppkey x29, s_nationkey x30}
                 [:scan [s_suppkey s_nationkey]]]]
               [:rename {n_nationkey x32, n_regionkey x33}
                [:scan [n_nationkey n_regionkey]]]]
              [:rename {r_regionkey x35, r_name x36}
               [:scan [r_regionkey {r_name (= r_name "EUROPE")}]]]]]]]]]]]
     (pt/plan-sql (slurp-tpch-query 2)))))

(t/deftest test-q3-shipping-priority
  (t/is
    (=
     (w/postwalk-replace
       {'?date (LocalDate/parse "1995-03-15")}
     '[:rename
       {x9 l_orderkey, x15 revenue, x4 o_orderdate, x5 o_shippriority}
       [:project
        [x9 x15 x4 x5]
        [:top
         {:limit 10}
         [:order-by
          [{x15 :desc} {x4 :asc}]
          [:group-by
           [x9 x4 x5 {x15 (sum x14)}]
           [:map
            [{x14 (* x10 (- 1 x11))}]
            [:join
             [{x7 x9}]
             [:join
              [{x2 x6}]
              [:rename {c_mktsegment x1, c_custkey x2}
               [:scan [{c_mktsegment (= c_mktsegment "BUILDING")} c_custkey]]]
              [:rename {o_orderdate x4, o_shippriority x5, o_custkey x6, o_orderkey x7}
               [:scan [{o_orderdate (< o_orderdate ?date)} o_shippriority o_custkey o_orderkey]]]]
             [:rename {l_orderkey x9, l_extendedprice x10, l_discount x11, l_shipdate x12}
              [:scan [l_orderkey l_extendedprice l_discount {l_shipdate (> l_shipdate ?date)}]]]]]]]]]])
     (pt/plan-sql (slurp-tpch-query 3)))))

(t/deftest test-q4-order-priority-checking
  (t/is
    (=
     (w/postwalk-replace
       {'?date (LocalDate/parse "1993-07-01")
        '?period (Period/parse "P3M")}
       '[:rename
         {x1 o_orderpriority, x15 order_count}
         [:order-by
          [{x1 :asc}]
          [:group-by
           [x1 {x15 (count x14)}]
           [:map
            [{x14 1}]
            [:semi-join
             [{x3 x5}]
             [:rename {o_orderpriority x1, o_orderdate x2, o_orderkey x3}
              [:scan [o_orderpriority {o_orderdate (and (< o_orderdate (+ ?date ?period)) (>= o_orderdate ?date))} o_orderkey]]]
             [:rename {l_orderkey x5, l_commitdate x6, l_receiptdate x7}
              [:select (< l_commitdate l_receiptdate)
               [:scan [l_orderkey l_commitdate l_receiptdate]]]]]]]]])
     (pt/plan-sql (slurp-tpch-query 4)))))

(t/deftest test-q5-local-supplier-volume
  (t/is
    (=
     (w/postwalk-replace
       {'?date (LocalDate/parse "1994-01-01")
        '?period (Period/parse "P1Y")}
       '[:rename
        {x16 n_name, x24 revenue}
        [:order-by
         [{x24 :desc}]
         [:group-by
          [x16 {x24 (sum x23)}]
          [:map
           [{x23 (* x8 (- 1 x9))}]
           [:join
            [{x18 x20}]
            [:join
             [{x14 x17}]
             [:join
              [{x11 x13} {x2 x14}]
              [:join
               [{x5 x10}]
               [:join
                [{x1 x4}]
                [:rename {c_custkey x1, c_nationkey x2}
                 [:scan [c_custkey c_nationkey]]]
                [:rename {o_custkey x4, o_orderkey x5, o_orderdate x6}
                 [:scan [o_custkey o_orderkey {o_orderdate (and (< o_orderdate (+ ?date ?period)) (>= o_orderdate ?date))}]]]]
               [:rename {l_extendedprice x8, l_discount x9, l_orderkey x10, l_suppkey x11}
                [:scan [l_extendedprice l_discount l_orderkey l_suppkey]]]]
              [:rename {s_suppkey x13, s_nationkey x14}
               [:scan [s_suppkey s_nationkey]]]]
             [:rename {n_name x16, n_nationkey x17, n_regionkey x18}
              [:scan [n_name n_nationkey n_regionkey]]]]
            [:rename {r_regionkey x20, r_name x21}
             [:scan [r_regionkey {r_name (= r_name "ASIA")}]]]]]]]])
     (pt/plan-sql (slurp-tpch-query 5)))))

(t/deftest test-q6-forecasting-revenue-change
  (t/is
    (=
     (w/postwalk-replace
       {'?date (LocalDate/parse "1994-01-01")
        '?period (Period/parse "P1Y")}
       '[:rename
         {x7 revenue}
         [:group-by
          [{x7 (sum x6)}]
          [:map
           [{x6 (* x1 x2)}]
           [:rename
            {l_extendedprice x1, l_discount x2, l_shipdate x3, l_quantity x4}
            [:scan
             [l_extendedprice
              {l_discount (between l_discount (- 0.06 0.01) (+ 0.06 0.01))}
              {l_shipdate (and (< l_shipdate (+ ?date ?period)) (>= l_shipdate ?date))}
              {l_quantity (< l_quantity 24)}]]]]]])
     (pt/plan-sql (slurp-tpch-query 6)))))

(t/deftest test-q7-volume-shipping
  (t/is
    (=
     (w/postwalk-replace
       {'?date (LocalDate/parse "1995-01-01")
        '?date2  (LocalDate/parse "1996-12-31")}
       '[:rename
         {x16 supp_nation, x19 cust_nation, x22 l_year, x24 revenue}
         [:order-by
          [{x16 :asc} {x19 :asc} {x22 :asc}]
          [:group-by
           [x16 x19 x22 {x24 (sum x23)}]
           [:project
            [x16 x19 {x22 (extract "YEAR" x4)} {x23 (* x5 (- 1 x6))}]
            [:join
             [{x14 x20}
              (or (and (= x16 "FRANCE") (= x19 "GERMANY")) (and (= x16 "GERMANY") (= x19 "FRANCE")))]
             [:join
              [{x2 x17}]
              [:join
               [{x11 x13}]
               [:join
                [{x8 x10}]
                [:join
                 [{x1 x7}]
                 [:rename {s_suppkey x1, s_nationkey x2} [:scan [s_suppkey s_nationkey]]]
                 [:rename
                  {l_shipdate x4, l_extendedprice x5, l_discount x6, l_suppkey x7, l_orderkey x8}
                  [:scan [{l_shipdate (between l_shipdate ?date ?date2)} l_extendedprice l_discount l_suppkey l_orderkey]]]]
                [:rename {o_orderkey x10, o_custkey x11} [:scan [o_orderkey o_custkey]]]]
               [:rename {c_custkey x13, c_nationkey x14} [:scan [c_custkey c_nationkey]]]]
              [:rename {n_name x16, n_nationkey x17} [:scan [n_name n_nationkey]]]]
             [:rename {n_name x19, n_nationkey x20} [:scan [n_name n_nationkey]]]]]]]])
     (pt/plan-sql (slurp-tpch-query 7)))))

(t/deftest test-q8-national-market-share
  (t/is
    (=
     (w/postwalk-replace
       {'?date (LocalDate/parse "1995-01-01")
        '?date2  (LocalDate/parse "1996-12-31")}
       '[:rename
         {x29 o_year, x35 mkt_share}
         [:order-by
          [{x29 :asc}]
          [:project
           [x29 {x35 (/ x32 x33)}]
           [:group-by
            [x29 {x32 (sum x31)} {x33 (sum x30)}]
            [:map
             [{x31 (if (= x23 "BRAZIL") x30 0)}]
             [:project
              [{x29 (extract "YEAR" x13)} {x30 (* x7 (- 1 x8))} x23]
              [:join
               [{x21 x26}]
               [:join
                [{x5 x24}]
                [:join
                 [{x18 x20}]
                 [:join
                  [{x15 x17}]
                  [:join
                   [{x11 x14}]
                   [:join
                    [{x1 x9} {x4 x10}]
                    [:cross-join
                     [:rename {p_partkey x1, p_type x2}
                      [:scan [p_partkey {p_type (= p_type "ECONOMY ANODIZED STEEL")}]]]
                     [:rename {s_suppkey x4, s_nationkey x5}
                      [:scan [s_suppkey s_nationkey]]]]
                    [:rename {l_extendedprice x7, l_discount x8, l_partkey x9, l_suppkey x10, l_orderkey x11}
                     [:scan [l_extendedprice l_discount l_partkey l_suppkey l_orderkey]]]]
                   [:rename {o_orderdate x13, o_orderkey x14, o_custkey x15}
                    [:scan [{o_orderdate (between o_orderdate ?date ?date2)} o_orderkey o_custkey]]]]
                  [:rename {c_custkey x17, c_nationkey x18}
                   [:scan [c_custkey c_nationkey]]]]
                 [:rename {n_nationkey x20, n_regionkey x21}
                  [:scan [n_nationkey n_regionkey]]]]
                [:rename {n_name x23, n_nationkey x24}
                 [:scan [n_name n_nationkey]]]]
               [:rename {r_regionkey x26, r_name x27}
              [:scan [r_regionkey {r_name (= r_name "AMERICA")}]]]]]]]]]])
     (pt/plan-sql (slurp-tpch-query 8)))))

(t/deftest test-q9-product-type-profit-measure
  (t/is
    (=
     '[:rename
       {x21 nation, x24 o_year, x26 sum_profit}
       [:order-by
        [{x21 :asc} {x24 :desc}]
        [:group-by
         [x21 x24 {x26 (sum x25)}]
         [:project
          [x21 {x24 (extract "YEAR" x18)} {x25 (- (* x7 (- 1 x8)) (* x14 x9))}]
          [:join
           [{x5 x22}]
           [:join
            [{x12 x19}]
            [:join
             [{x10 x15} {x11 x16}]
             [:join
              [{x4 x10} {x1 x11}]
              [:cross-join ;;TODO crossjoin here, probably could be an equi-join with different join order
               [:rename {p_partkey x1, p_name x2}
                [:scan [p_partkey {p_name (like p_name "%green%")}]]]
               [:rename {s_suppkey x4, s_nationkey x5}
                [:scan [s_suppkey s_nationkey]]]]
              [:rename {l_extendedprice x7, l_discount x8, l_quantity x9, l_suppkey x10, l_partkey x11, l_orderkey x12}
               [:scan [l_extendedprice l_discount l_quantity l_suppkey l_partkey l_orderkey]]]]
             [:rename {ps_supplycost x14, ps_suppkey x15, ps_partkey x16}
              [:scan [ps_supplycost ps_suppkey ps_partkey]]]]
            [:rename {o_orderdate x18, o_orderkey x19}
             [:scan [o_orderdate o_orderkey]]]]
           [:rename {n_name x21, n_nationkey x22}
            [:scan [n_name n_nationkey]]]]]]]]
     (pt/plan-sql (slurp-tpch-query 9)))))

(t/deftest test-q10-returned-item-reporting
  (t/is
    (=
     (w/postwalk-replace
       {'?date (LocalDate/parse "1993-10-01")
        '?period (Period/parse "P3M")}
       '[:rename
         {x1 c_custkey, x2 c_name, x22 revenue, x3 c_acctbal, x18 n_name, x4 c_address, x5 c_phone, x6 c_comment}
         [:project
          [x1 x2 x22 x3 x18 x4 x5 x6]
          [:top
           {:limit 20}
           [:order-by
            [{x22 :desc}]
            [:group-by
             [x1 x2 x3 x18 x4 x5 x6 {x22 (sum x21)}]
             [:map
              [{x21 (* x13 (- 1 x14))}]
              [:join
               [{x7 x19}]
               [:join
                [{x10 x15}]
                [:join
                 [{x1 x9}]
                 [:rename {c_custkey x1, c_name x2, c_acctbal x3, c_address x4, c_phone x5, c_comment x6, c_nationkey x7}
                  [:scan [c_custkey c_name c_acctbal c_address c_phone c_comment c_nationkey]]]
                 [:rename
                  {o_custkey x9, o_orderkey x10, o_orderdate x11}
                  [:scan [o_custkey o_orderkey {o_orderdate (and (< o_orderdate (+ ?date ?period)) (>= o_orderdate ?date))}]]]]
                [:rename {l_extendedprice x13, l_discount x14, l_orderkey x15, l_returnflag x16}
                 [:scan [l_extendedprice l_discount l_orderkey {l_returnflag (= l_returnflag "R")}]]]]
               [:rename {n_name x18, n_nationkey x19} [:scan [n_name n_nationkey]]]]]]]]]])
     (pt/plan-sql (slurp-tpch-query 10)))))

(t/deftest test-q11-important-stock-identification
  (t/is
    (=
     '[:rename
       {x1 ps_partkey, x14 value}
       [:project
        [x1 x14]
        [:order-by
         [{x14 :desc}]
         [:join
          [(> x15 x30)]
          [:group-by
           [x1 {x14 (sum x12)} {x15 (sum x13)}]
           [:map
            [{x12 (* x2 x3)} {x13 (* x2 x3)}]
            [:join
             [{x7 x9}]
             [:join
              [{x4 x6}]
              [:rename {ps_partkey x1, ps_supplycost x2, ps_availqty x3, ps_suppkey x4}
               [:scan [ps_partkey ps_supplycost ps_availqty ps_suppkey]]]
              [:rename {s_suppkey x6, s_nationkey x7}
               [:scan [s_suppkey s_nationkey]]]]
             [:rename {n_nationkey x9, n_name x10}
              [:scan [n_nationkey {n_name (= n_name "GERMANY")}]]]]]]
          [:max-1-row
           [:project
            [{x30 (* x28 1.0E-4)}]
            [:group-by
             [{x28 (sum x27)}]
             [:map
              [{x27 (* x17 x18)}]
              [:join
               [{x22 x24}]
               [:join
                [{x19 x21}]
                [:rename {ps_supplycost x17, ps_availqty x18, ps_suppkey x19}
                 [:scan [ps_supplycost ps_availqty ps_suppkey]]]
                [:rename {s_suppkey x21, s_nationkey x22}
                 [:scan [s_suppkey s_nationkey]]]]
               [:rename {n_nationkey x24, n_name x25}
                [:scan [n_nationkey {n_name (= n_name "GERMANY")}]]]]]]]]]]]]
     (pt/plan-sql (slurp-tpch-query 11)))))

(t/deftest test-q12-shipping-modes-and-order-priority
  (t/is
    (=
     (w/postwalk-replace
       {'?date (LocalDate/parse "1994-01-01")
        '?period (Period/parse "P1Y")}
       '[:rename
         {x4 l_shipmode, x19 high_line_count, x20 low_line_count}
         [:order-by
          [{x4 :asc}]
          [:group-by
           [x4 {x19 (sum x17)} {x20 (sum x18)}]
           [:map
            [{x17 (if (or (= x1 "1-URGENT") (= x1 "2-HIGH"))
                    1
                    0)}
             {x18 (if (and (<> x1 "1-URGENT") (<> x1 "2-HIGH")) ;;TODO != or <>
                    1
                    0)}]
            [:semi-join
             [{x4 x10}]
             [:join
              [{x2 x5}]
              [:rename {o_orderpriority x1, o_orderkey x2}
               [:scan [o_orderpriority o_orderkey]]]
              [:rename
               {l_shipmode x4, l_orderkey x5, l_commitdate x6, l_receiptdate x7, l_shipdate x8}
               [:select
                (and (< l_commitdate l_receiptdate) (< l_shipdate l_commitdate))
                [:scan
                 [l_shipmode
                  l_orderkey
                  l_commitdate
                  {l_receiptdate (and (< l_receiptdate (+ ?date ?period)) (>= l_receiptdate ?date))}
                  l_shipdate]]]]]
             [:table [{x10 "MAIL"} {x10 "SHIP"}]]]]]]])
     (pt/plan-sql (slurp-tpch-query 12)))))

(t/deftest test-q13-customer-distribution
  (t/is
    (=
     '[:rename
      {x7 c_count, x10 custdist}
      [:order-by
       [{x10 :desc} {x7 :desc}]
       [:group-by
        [x7 {x10 (count x9)}]
        [:map
         [{x9 1}]
         [:group-by [x1 {x7 (count x3)}]
          [:left-outer-join [{x1 x4}]
           [:rename {c_custkey x1}
            [:scan [c_custkey]]]
           [:rename {o_orderkey x3, o_custkey x4, o_comment x5}
            [:scan [o_orderkey o_custkey {o_comment (not (like o_comment "%special%requests%"))}]]]]]]]]]
     (pt/plan-sql (slurp-tpch-query 13)))))

(t/deftest test-q14-promotion-effect
  (t/is
    (=
     (w/postwalk-replace
       {'?date (LocalDate/parse "1995-09-01")
        '?period (Period/parse "P1M")}
       '[:rename
         {x14 promo_revenue}
         [:project
          [{x14 (/ (* 100.0 x11) x12)}]
          [:group-by
           [{x11 (sum x9)} {x12 (sum x10)}]
           [:map
            [{x9 (if (like x6 "PROMO%") (* x1 (- 1 x2)) 0)} {x10 (* x1 (- 1 x2))}]
            [:join
             [{x3 x7}]
             [:rename
              {l_extendedprice x1, l_discount x2, l_partkey x3, l_shipdate x4}
              [:scan
               [l_extendedprice l_discount l_partkey {l_shipdate (and (< l_shipdate (+ ?date ?period)) (>= l_shipdate ?date))}]]]
             [:rename {p_type x6, p_partkey x7}
              [:scan [p_type p_partkey]]]]]]]])
     (pt/plan-sql (slurp-tpch-query 14)))))

(t/deftest test-q15-top-supplier
  (t/is
    (=
     (w/postwalk-replace
       {'?date (LocalDate/parse "1996-01-01")
        '?period (Period/parse "P3M")}
       '[:rename
         {x1 s_suppkey, x2 s_name, x3 s_address, x4 s_phone, x12 total_revenue}
         [:project
          [x1 x2 x3 x4 x12]
          [:order-by
           [{x1 :asc}]
           [:join
            [{x12 x22}]
            [:join
             [{x1 x6}]
             [:rename {s_suppkey x1, s_name x2, s_address x3, s_phone x4} [:scan [s_suppkey s_name s_address s_phone]]]
             [:group-by
              [x6 {x12 (sum x11)}]
              [:map
               [{x11 (* x7 (- 1 x8))}]
               [:rename
                {l_suppkey x6, l_extendedprice x7, l_discount x8, l_shipdate x9}
                [:scan
                 [l_suppkey
                  l_extendedprice
                  l_discount
                  {l_shipdate (and (< l_shipdate (+ ?date ?period)) (>= l_shipdate ?date))}]]]]]]
            [:max-1-row
             [:group-by
              [{x22 (max x20)}]
              [:group-by
               [x14 {x20 (sum x19)}]
               [:map
                [{x19 (* x15 (- 1 x16))}]
                [:rename
                 {l_suppkey x14, l_extendedprice x15, l_discount x16, l_shipdate x17}
                 [:scan
                  [l_suppkey
                   l_extendedprice
                   l_discount
                   {l_shipdate (and (< l_shipdate (+ ?date ?period)) (>= l_shipdate ?date))}]]]]]]]]]]])
     (pt/plan-sql (slurp-tpch-query 15)))))

(t/deftest test-q16-part-supplier-relationship
  (t/is
    (=
     '[:rename
       {x4 p_brand, x5 p_type, x6 p_size, x24 supplier_cnt}
       [:order-by
        [{x24 :desc} {x4 :asc} {x5 :asc} {x6 :asc}]
        [:group-by
         [x4 x5 x6 {x24 (count x1)}]
         [:anti-join
          [{x1 x16}]
          [:semi-join
           [{x6 x9}]
           [:join
            [{x2 x7}]
            [:rename {ps_suppkey x1, ps_partkey x2}
             [:scan [ps_suppkey ps_partkey]]]
            [:rename {p_brand x4, p_type x5, p_size x6, p_partkey x7}
             [:scan [{p_brand (<> p_brand "Brand#45")} {p_type (not (like p_type "MEDIUM POLISHED%"))} p_size p_partkey]]]]
           [:table [{x9 49} {x9 14} {x9 23} {x9 45} {x9 19} {x9 3} {x9 36} {x9 9}]]]
          [:rename {s_suppkey x16, s_comment x17}
           [:scan [s_suppkey {s_comment (like s_comment "%Customer%Complaints%")}]]]]]]]
     (pt/plan-sql (slurp-tpch-query 16)))))

(t/deftest test-q17-small-quantity-order-revenue
  (t/is
    (=
     '[:rename
       {x19 avg_yearly}
       [:project
        [{x19 (/ x17 7.0)}]
        [:group-by
         [{x17 (sum x1)}]
         [:select
          (< x3 x14)
          [:map
           [{x14 (* 0.2 x12)}]
           [:group-by
            [x1 x2 x3 x5 x6 x7 $row_number$ {x12 (avg x9)}]
            [:left-outer-join
             [{x5 x10}]
             [:map
              [{$row_number$ (row-number)}]
              [:join
               [{x2 x5}]
               [:rename {l_extendedprice x1, l_partkey x2, l_quantity x3}
                [:scan [l_extendedprice l_partkey l_quantity]]]
               [:rename {p_partkey x5, p_brand x6, p_container x7}
                [:scan [p_partkey {p_brand (= p_brand "Brand#23")} {p_container (= p_container "MED BOX")}]]]]]
             [:rename {l_quantity x9, l_partkey x10}
              [:scan [l_quantity l_partkey]]]]]]]]]]
     (pt/plan-sql (slurp-tpch-query 17)))))

(t/deftest test-q18-large-volume-customer
  (t/is
    (=
     '[:rename
       {x1 c_name, x2 c_custkey, x4 o_orderkey, x5 o_orderdate, x6 o_totalprice, x22 $column_6$}
       [:top
        {:limit 100}
        [:order-by
         [{x6 :desc} {x5 :asc}]
         [:group-by
          [x1 x2 x4 x5 x6 {x22 (sum x9)}]
          [:semi-join
           [{x4 x12}]
           [:join
            [{x4 x10}]
            [:join
             [{x2 x7}]
             [:rename {c_name x1, c_custkey x2}
              [:scan [c_name c_custkey]]]
             [:rename {o_orderkey x4, o_orderdate x5, o_totalprice x6, o_custkey x7}
              [:scan [o_orderkey o_orderdate o_totalprice o_custkey]]]]
            [:rename {l_quantity x9, l_orderkey x10}
             [:scan [l_quantity l_orderkey]]]]
           [:select (> x15 300)
            [:group-by [x12 {x15 (sum x13)}]
             [:rename {l_orderkey x12, l_quantity x13}
              [:scan [l_orderkey l_quantity]]]]]]]]]]
     (pt/plan-sql (slurp-tpch-query 18)))))

(t/deftest test-q19-discounted-revenue ;;TODO unable to decorr, select stuck under this top/union exists thing
  (t/is
    (=
     '[:rename
       {x56 revenue}
       [:group-by
        [{x56 (sum x55)}]
        [:map
         [{x55 (* x1 (- 1 x2))}]
         [:select
          (or
            (or
              (and (and (and (and (and (and (and (= x8 x3) (= x9 "Brand#12")) x15) (>= x4 1)) (<= x4 (+ 1 10))) (between x11 1 5)) x22) (= x6 "DELIVER IN PERSON"))
              (and (and (and (and (and (and (and (= x8 x3) (= x9 "Brand#23")) x29) (>= x4 10)) (<= x4 (+ 10 10))) (between x11 1 10)) x36) (= x6 "DELIVER IN PERSON")))
            (and (and (and (and (and (and (and (= x8 x3) (= x9 "Brand#34")) x43) (>= x4 20)) (<= x4 (+ 20 10))) (between x11 1 15)) x50) (= x6 "DELIVER IN PERSON")))
          [:apply
           :cross-join
           {x5 ?x53}
           #{x50}
           [:apply
            :cross-join
            {x10 ?x46}
            #{x43}
            [:apply
             :cross-join
             {x5 ?x39}
             #{x36}
             [:apply
              :cross-join
              {x10 ?x32}
              #{x29}
              [:apply
               :cross-join
               {x5 ?x25}
               #{x22}
               [:apply
                :cross-join
                {x10 ?x18}
                #{x15}
                [:cross-join
                 [:rename {l_extendedprice x1, l_discount x2, l_partkey x3, l_quantity x4, l_shipmode x5, l_shipinstruct x6}
                  [:scan [l_extendedprice l_discount l_partkey l_quantity l_shipmode l_shipinstruct]]]
                 [:rename {p_partkey x8, p_brand x9, p_container x10, p_size x11}
                  [:scan [p_partkey p_brand p_container p_size]]]]
                [:top {:limit 1}
                 [:union-all
                  [:project [{x15 true}]
                   [:select (= ?x18 x13)
                    [:table [{x13 "SM CASE"} {x13 "SM BOX"} {x13 "SM PACK"} {x13 "SM PKG"}]]]]
                  [:table [{x15 false}]]]]]
               [:top {:limit 1}
                [:union-all
                 [:project [{x22 true}]
                  [:select (= ?x25 x20)
                   [:table [{x20 "AIR"} {x20 "AIR REG"}]]]]
                 [:table [{x22 false}]]]]]
              [:top {:limit 1}
               [:union-all
                [:project [{x29 true}]
                 [:select (= ?x32 x27)
                  [:table [{x27 "MED BAG"} {x27 "MED BOX"} {x27 "MED PKG"} {x27 "MED PACK"}]]]]
                [:table [{x29 false}]]]]]
             [:top {:limit 1}
              [:union-all
               [:project [{x36 true}]
                [:select (= ?x39 x34)
                 [:table [{x34 "AIR"} {x34 "AIR REG"}]]]]
               [:table [{x36 false}]]]]]
            [:top {:limit 1}
             [:union-all
              [:project [{x43 true}]
               [:select (= ?x46 x41)
                [:table [{x41 "LG CASE"} {x41 "LG BOX"} {x41 "LG PACK"} {x41 "LG PKG"}]]]]
              [:table [{x43 false}]]]]]
           [:top {:limit 1}
            [:union-all
             [:project [{x50 true}]
              [:select (= ?x53 x48)
               [:table [{x48 "AIR"} {x48 "AIR REG"}]]]]
             [:table [{x50 false}]]]]]]]]]
     (pt/plan-sql (slurp-tpch-query 19)))))

(t/deftest test-q20-potential-part-promotion
  (t/is
    (=
     (w/postwalk-replace
       {'?date (LocalDate/parse "1994-01-01")
        '?period (Period/parse "P1Y")}
       '[:rename
         {x1 s_name, x2 s_address}
         [:project
          [x1 x2]
          [:order-by
           [{x1 :asc}]
           [:semi-join
            [{x3 x9}]
            [:join
             [{x4 x6}]
             [:rename {s_name x1, s_address x2, s_suppkey x3, s_nationkey x4}
              [:scan [s_name s_address s_suppkey s_nationkey]]]
             [:rename {n_nationkey x6, n_name x7}
              [:scan [n_nationkey {n_name (= n_name "CANADA")}]]]]
            [:select
             (> x11 x28)
             [:map
              [{x28 (* 0.5 x26)}]
              [:group-by
               [x9 x10 x11 x16 $row_number$ {x26 (sum x21)}]
               [:left-outer-join
                [{x10 x22} {x9 x23}]
                [:map
                 [{$row_number$ (row-number)}]
                 [:semi-join [{x10 x13}]
                  [:rename {ps_suppkey x9, ps_partkey x10, ps_availqty x11}
                   [:scan [ps_suppkey ps_partkey ps_availqty]]]
                  [:rename {p_partkey x13, p_name x14} [:scan [p_partkey {p_name (like p_name "forest%")}]]]]]
                [:rename
                 {l_quantity x21, l_partkey x22, l_suppkey x23, l_shipdate x24}
                 [:scan
                  [l_quantity l_partkey l_suppkey {l_shipdate (and (< l_shipdate (+ ?date ?period)) (>= l_shipdate ?date))}]]]]]]]]]]])
     (pt/plan-sql (slurp-tpch-query 20)))))

(t/deftest test-q21-suppliers-who-kept-orders-waiting
  (t/is
    (=
     '[:rename
       {x1 s_name, x37 numwait}
       [:top
        {:limit 100}
        [:order-by
         [{x37 :desc} {x1 :asc}]
         [:group-by
          [x1 {x37 (count x36)}]
          [:map
           [{x36 1}]
           [:anti-join
            [(<> x26 x5) {x6 x25}]
            [:semi-join
             [(<> x17 x5) {x6 x16}]
             [:join
              [{x3 x13}]
              [:join
               [{x6 x10}]
               [:join
                [{x2 x5}]
                [:rename {s_name x1, s_suppkey x2, s_nationkey x3}
                 [:scan [s_name s_suppkey s_nationkey]]]
                [:rename {l_suppkey x5, l_orderkey x6, l_receiptdate x7, l_commitdate x8}
                 [:select (> l_receiptdate l_commitdate)
                  [:scan [l_suppkey l_orderkey l_receiptdate l_commitdate]]]]]
               [:rename {o_orderkey x10, o_orderstatus x11}
                [:scan [o_orderkey {o_orderstatus (= o_orderstatus "F")}]]]]
              [:rename {n_nationkey x13, n_name x14}
               [:scan [n_nationkey {n_name (= n_name "SAUDI ARABIA")}]]]]
             [:rename {l_orderkey x16, l_suppkey x17}
              [:scan [l_orderkey l_suppkey]]]]
            [:rename {l_orderkey x25, l_suppkey x26, l_receiptdate x27, l_commitdate x28}
             [:select (> l_receiptdate l_commitdate)
              [:scan [l_orderkey l_suppkey l_receiptdate l_commitdate]]]]]]]]]]
     (pt/plan-sql (slurp-tpch-query 21)))))

(t/deftest test-q22-global-sales-opportunity
  (t/is
    (=
     '[:rename
       {x31 cntrycode, x33 numcust, x34 totacctbal}
       [:order-by
        [{x31 :asc}]
        [:group-by
         [x31 {x33 (count x32)} {x34 (sum x2)}]
         [:map
          [{x32 1}]
          [:project
           [{x31 (substring x1 1 2)} x2]
           [:select
            (> x2 x15)
            [:anti-join
             [{x3 x24}]
             [:cross-join
              [:semi-join
               [(= (substring x1 1 2) x5)]
               [:rename {c_phone x1, c_acctbal x2, c_custkey x3}
                [:scan [c_phone c_acctbal c_custkey]]]
               [:table [{x5 "13"} {x5 "31"} {x5 "23"} {x5 "29"} {x5 "30"} {x5 "18"} {x5 "17"}]]]
              [:max-1-row
               [:group-by
                [{x22 (avg x12)}]
                [:semi-join
                 [(= (substring x13 1 2) x15)]
                 [:rename {c_acctbal x12, c_phone x13}
                  [:scan [{c_acctbal (> c_acctbal 0.0)} c_phone]]]
                 [:table [{x15 "13"} {x15 "31"} {x15 "23"} {x15 "29"} {x15 "30"} {x15 "18"} {x15 "17"}]]]]]]
             [:rename {o_custkey x24}
              [:scan [o_custkey]]]]]]]]]]
     (pt/plan-sql (slurp-tpch-query 22)))))
