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
       '?delta (Period/parse "P90D")}
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
            [:scan [l_returnflag l_linestatus l_quantity l_extendedprice l_discount l_tax {l_shipdate (<= l_shipdate (- ?date ?delta))}]]]]]]])
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
             {x1 x26}
             [:map
              [{$row_number$ (row-number)}]
              [:join
               {x20 x22}
               [:join
                {x12 x19}
                [:select
                 (like x4 "%BRASS")
                 [:select
                  (= x3 15) ;;TODO selects not pushed down
                  [:select
                   (= x11 x15)
                   [:join
                    {x1 x14}
                    [:cross-join
                     [:rename {p_partkey x1, p_mfgr x2, p_size x3, p_type x4}
                      [:scan [p_partkey p_mfgr p_size p_type]]]
                     [:rename {s_acctbal x6, s_name x7, s_address x8, s_phone x9, s_comment x10, s_suppkey x11, s_nationkey x12}
                      [:scan [s_acctbal s_name s_address s_phone s_comment s_suppkey s_nationkey]]]]
                    [:rename {ps_partkey x14, ps_suppkey x15, ps_supplycost x16}
                     [:scan [ps_partkey ps_suppkey ps_supplycost]]]]]]]
                [:rename {n_name x18, n_nationkey x19, n_regionkey x20}
                 [:scan [n_name n_nationkey n_regionkey]]]]
               [:rename {r_regionkey x22, r_name x23}
                [:scan [r_regionkey {r_name (= r_name "EUROPE")}]]]]]
             [:join
              {x33 x35}
              [:join
               {x30 x32}
               [:join {x27 x29}
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
             {x7 x9}
             [:join
              {x2 x6}
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
             {x3 x5}
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
            {x18 x20}
            [:join
             {x14 x17}
             [:select
              (< x6 (+ ?date ?period))
              [:select
               (>= x6 ?date) ;;TODO selects not pushed down
               [:select
                (= x2 x14)
                [:join
                 {x11 x13}
                 [:join
                  {x5 x10}
                  [:join
                   {x1 x4}
                   [:rename {c_custkey x1, c_nationkey x2}
                    [:scan [c_custkey c_nationkey]]]
                   [:rename {o_custkey x4, o_orderkey x5, o_orderdate x6}
                    [:scan [o_custkey o_orderkey o_orderdate]]]]
                  [:rename {l_extendedprice x8, l_discount x9, l_orderkey x10, l_suppkey x11}
                   [:scan [l_extendedprice l_discount l_orderkey l_suppkey]]]]
                 [:rename {s_suppkey x13, s_nationkey x14}
                  [:scan [s_suppkey s_nationkey]]]]]]]
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
              {l_discount (between l_discount (- 0.06 0.01) (+ 0.06 0.01))} ;;TODO assumes between fn exists
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
            [x16 x19 {x22 (extract "YEAR" x4)} {x23 (* x5 (- 1 x6))}] ;;TODO extract
            [:select
             (or (and (= x16 "FRANCE") (= x19 "GERMANY")) (and (= x16 "GERMANY") (= x19 "FRANCE")))
             [:join
              {x14 x20}
              [:join
               {x2 x17}
               [:join
                {x11 x13}
                [:join
                 {x8 x10}
                 [:join
                  {x1 x7}
                  [:rename {s_suppkey x1, s_nationkey x2} [:scan [s_suppkey s_nationkey]]]
                  [:rename
                   {l_shipdate x4, l_extendedprice x5, l_discount x6, l_suppkey x7, l_orderkey x8}
                   [:scan [{l_shipdate (between l_shipdate ?date ?date2)} l_extendedprice l_discount l_suppkey l_orderkey]]]] ;; TODO between
                 [:rename {o_orderkey x10, o_custkey x11} [:scan [o_orderkey o_custkey]]]]
                [:rename {c_custkey x13, c_nationkey x14} [:scan [c_custkey c_nationkey]]]]
               [:rename {n_name x16, n_nationkey x17} [:scan [n_name n_nationkey]]]]
              [:rename {n_name x19, n_nationkey x20} [:scan [n_name n_nationkey]]]]]]]]])
     (pt/plan-sql (slurp-tpch-query 7)))))

