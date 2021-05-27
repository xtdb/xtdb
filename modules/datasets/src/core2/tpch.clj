(ns core2.tpch
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [core2.core :as c2])
  (:import [io.airlift.tpch TpchColumn TpchColumnType$Base TpchEntity TpchTable]
           [java.time Instant Period]
           java.util.Date))

(def table->pkey
  {"part" [:p_partkey]
   "supplier" [:s_suppkey]
   "partsupp" [:ps_partkey :ps_suppkey]
   "customer" [:c_custkey]
   "lineitem" [:l_orderkey :l_linenumber]
   "orders" [:o_orderkey]
   "nation" [:n_nationkey]
   "region" [:r_regionkey]})

(defn tpch-entity->pkey-doc [^TpchTable t ^TpchEntity b]
  (let [doc (->> (for [^TpchColumn c (.getColumns t)]
                   [(keyword (.getColumnName c))
                    (condp = (.getBase (.getType c))
                      TpchColumnType$Base/IDENTIFIER
                      (str (str/replace (.getColumnName c) #".+_" "") "_" (.getIdentifier c b))
                      TpchColumnType$Base/INTEGER
                      (long (.getInteger c b))
                      TpchColumnType$Base/VARCHAR
                      (.getString c b)
                      TpchColumnType$Base/DOUBLE
                      (.getDouble c b)
                      TpchColumnType$Base/DATE
                      (Date/from (.plus Instant/EPOCH (Period/ofDays (.getDate c b)))))])
                 (into {}))
        table-name (.getTableName t)
        pkey-columns (get table->pkey table-name)
        pkey (mapv doc pkey-columns)]
    (assoc doc
           :_id (str/join "___" pkey)
           :_table table-name)))

(def default-scale-factor 0.05)

;; 0.05 = 7500 customers, 75000 orders, 299814 lineitems, 10000 part, 40000 partsupp, 500 supplier, 25 nation, 5 region
(defn tpch-table->docs
  ([^TpchTable table]
   (tpch-table->docs table default-scale-factor))
  ([^TpchTable table scale-factor]
   (for [doc (.createGenerator table scale-factor 1 1)]
     (tpch-entity->pkey-doc table doc))))

(defn submit-docs!
  ([tx-producer]
   (submit-docs! tx-producer default-scale-factor))
  ([tx-producer scale-factor]
   (log/debug "Transacting TPC-H tables...")
   (->> (TpchTable/getTables)
        (reduce (fn [_last-tx ^TpchTable t]
                  (let [[!last-tx doc-count] (->> (tpch-table->docs t scale-factor)
                                                  (partition-all 1000)
                                                  (reduce (fn [[_!last-tx last-doc-count] batch]
                                                            [(c2/submit-tx tx-producer
                                                                           (vec (for [doc batch]
                                                                                  {:op :put, :doc doc})))
                                                             (+ last-doc-count (count batch))])
                                                          [nil 0]))]
                    (log/debug "Transacted" doc-count (.getTableName t))
                    @!last-tx))
                nil))))

(defn- with-params [q params]
  (vary-meta q assoc ::params params))

(def tpch-q1-pricing-summary-report
  (-> '[:order-by [l_returnflag l_linestatus]
        [:group-by [l_returnflag l_linestatus
                    {sum_qty (sum l_quantity)}
                    {sum_base_price (sum l_extendedprice)}
                    {sum_disc_price (sum disc_price)}
                    {sum_charge (sum charge)}
                    {avg_qty (avg l_quantity)}
                    {avg_price (avg l_extendedprice)}
                    {avg_disc (avg l_discount)}
                    {count_order (count l_returnflag)}]
         [:project [l_returnflag l_linestatus l_shipdate l_quantity l_extendedprice l_discount l_tax
                    {disc_price (* l_extendedprice (- 1 l_discount))}
                    {charge (* (* l_extendedprice (- 1 l_discount))
                               (+ 1 l_tax))}]
          [:scan [l_returnflag l_linestatus
                  {l_shipdate (<= l_shipdate ?ship-date)}
                  l_quantity l_extendedprice l_discount l_tax]]]]]
      (with-params {'?ship-date #inst "1998-09-02"})))

(def tpch-q2-minimum-cost-supplier
  (-> '[:assign [PartSupp [:join {s_suppkey ps_suppkey}
                           [:join {n_nationkey s_nationkey}
                            [:join {n_regionkey r_regionkey}
                             [:scan [n_name n_regionkey n_nationkey]]
                             [:scan [r_regionkey {r_name (= r_name ?region)}]]]
                            [:scan [s_nationkey s_suppkey s_acctbal s_name s_address s_phone s_comment]]]
                           [:scan [ps_suppkey ps_partkey ps_supplycost]]]]
        [:slice {:limit 100}
         [:order-by [s_acctbal :desc, n_name s_name p_partkey]
          [:project [s_acctbal s_name n_name p_partkey p_mfgr s_address s_phone s_comment]
           [:select (= ps_supplycost min_ps_supplycost)
            [:join {ps_partkey ps_partkey}
             [:join {ps_partkey p_partkey}
              PartSupp
              [:scan [p_partkey p_mfgr {p_size (= p_size ?size)} {p_type (like p_type "%BRASS")}]]]
             [:group-by [ps_partkey {min_ps_supplycost (min ps_supplycost)}]
              PartSupp]]]]]]]
      (with-params {'?region "EUROPE"
                    ;; '?type "BRASS"
                    '?size 15})))

(def tpch-q3-shipping-priority
  (-> '[:slice {:limit 10}
        [:order-by [revenue :desc, o_orderdate]
         [:group-by [l_orderkey
                     {revenue (sum disc_price)}
                     o_orderdate
                     o_shippriority]
          [:project [l_orderkey o_orderdate o_shippriority
                     {disc_price (* l_extendedprice (- 1 l_discount))}]
           [:join {o_orderkey l_orderkey}
            [:join {c_custkey o_custkey}
             [:scan [c_custkey {c_mktsegment (= c_mktsegment ?segment)}]]
             [:scan [o_orderkey o_custkey o_shippriority
                     {o_orderdate (< o_orderdate ?date)}]]]
            [:scan [l_orderkey l_extendedprice l_discount
                    {l_shipdate (> l_shipdate ?date)}]]]]]]]
      (with-params {'?segment "BUILDING"
                    '?date #inst "1995-03-15"})))

(def tpch-q4-order-priority-checking
  (-> '[:order-by [o_orderpriority]
        [:group-by [o_orderpriority {order_count (count o_orderkey)}]
         [:semi-join {o_orderkey l_orderkey}
          [:scan [{o_orderdate (and (>= o_orderdate ?start-date)
                                    (< o_orderdate ?end-date))} o_orderpriority o_orderkey]]
          [:select (< l_commitdate l_receiptdate)
           [:scan [l_orderkey l_commitdate l_receiptdate]]]]]]
      (with-params {'?start-date #inst "1993-07-01"
                    ;; in the spec this is one date with `+ INTERVAL 3 MONTHS`
                    '?end-date #inst "1993-10-01"})))

(def tpch-q5-local-supplier-volume
  '[:order-by [revenue :desc]
    [:group-by [n_name {revenue (sum disc_price)}]
     [:project [n_name {disc_price (* l_extendedprice (- 1 l_discount))}]
      [:select (= l_suppkey s_suppkey)
       [:join {o_orderkey l_orderkey}
        [:join {s_nationkey c_nationkey}
         [:join {n_nationkey s_nationkey}
          [:join {r_regionkey n_regionkey}
           [:scan [{r_name (= r_name "ASIA")} r_regionkey]]
           [:scan [n_name n_nationkey n_regionkey]]]
          [:scan [s_suppkey s_nationkey]]]
         [:join {o_custkey c_custkey}
          [:scan [o_orderkey o_custkey
                  {o_orderdate (and (>= o_orderdate #inst "1994-01-01")
                                    (< o_orderdate #inst "1995-01-01"))}]]
          [:scan [c_custkey c_nationkey]]]]
        [:scan [l_orderkey l_extendedprice l_discount l_suppkey]]]]]]])

(def tpch-q6-forecasting-revenue-change
  (-> '[:group-by [{revenue (sum disc_price)}]
        [:project [{disc_price (* l_extendedprice l_discount)}]
         [:scan [{l_shipdate (and (>= l_shipdate ?start-date)
                                  (< l_shipdate ?end-date))}
                 l_extendedprice
                 {l_discount (and (>= l_discount ?min-discount)
                                  (<= l_discount ?max-discount))}
                 {l_quantity (< l_quantity 24.0)}]]]]
      (with-params {'?start-date #inst "1994-01-01"
                    '?end-date #inst "1995-01-01"
                    '?min-discount 0.05
                    '?max-discount 0.07})))

(def tpch-q7-volume-shipping
  (-> '[:order-by [supp_nation cust_nation l_year]
        [:group-by [supp_nation cust_nation l_year {revenue (sum volume)}]
         [:project [supp_nation cust_nation
                    {l_year (extract "YEAR" l_shipdate)}
                    {volume (* l_extendedprice (- 1 l_discount))}]
          [:rename {n1_n_name supp_nation, n2_n_name cust_nation}
           [:select (or (and (= n1_n_name ?nation1)
                             (= n2_n_name ?nation2))
                        (and (= n1_n_name ?nation2)
                             (= n2_n_name ?nation1)))
            [:join {c_nationkey n2_n_nationkey}
             [:join {o_custkey c_custkey}
              [:join {s_nationkey n1_n_nationkey}
               [:join {l_orderkey o_orderkey}
                [:join {s_suppkey l_suppkey}
                 [:scan [s_suppkey s_nationkey]]
                 [:scan [l_orderkey l_extendedprice l_discount l_suppkey
                         {l_shipdate (and (>= l_shipdate #inst "1995-01-01")
                                          (<= l_shipdate #inst "1996-12-31"))}]]]
                [:scan [o_orderkey o_custkey]]]
               [:rename n1
                [:scan [{n_name (or (= n_name ?nation1) (= n_name ?nation2))} n_nationkey]]]]
              [:scan [c_custkey c_nationkey]]]
             [:rename n2
              [:scan [{n_name (or (= n_name ?nation1) (= n_name ?nation2))} n_nationkey]]]]]]]]]
      (with-params {'?nation1 "FRANCE"
                    '?nation2 "GERMANY"})))

(def tpch-q8-national-market-share
  (-> '[:order-by [o_year]
        [:project [o_year {mkt_share (/ brazil_revenue revenue)}]
         [:group-by [o_year {brazil_revenue (sum brazil_volume)} {revenue (sum volume)}]
          [:project [{o_year (extract "YEAR" o_orderdate)}
                     {brazil_volume (if (= nation ?nation)
                                      (* l_extendedprice (- 1 l_discount))
                                      0.0)}
                     {volume (* l_extendedprice (- 1 l_discount))}
                     nation]
           [:rename {n2_n_name nation}
            [:join {s_nationkey n2_n_nationkey}
             [:join {c_nationkey n1_n_nationkey}
              [:join {o_custkey c_custkey}
               [:join {l_orderkey o_orderkey}
                [:join {l_suppkey s_suppkey}
                 [:join {p_partkey l_partkey}
                  [:scan [p_partkey {p_type (= p_type ?type)}]]
                  [:scan [l_orderkey l_extendedprice l_discount l_suppkey l_partkey]]]
                 [:scan [s_suppkey s_nationkey]]]
                [:scan [o_orderkey o_custkey
                        {o_orderdate (and (>= o_orderdate #inst "1995-01-01")
                                          (<= o_orderdate #inst "1996-12-31"))}]]]
               [:scan [c_custkey c_nationkey]]]
              [:join {r_regionkey n1_n_regionkey}
               [:scan [r_regionkey {r_name (= r_name ?region)}]]
               [:rename n1
                [:scan [n_name n_nationkey n_regionkey]]]]]
             [:rename n2
              [:scan [n_name n_nationkey]]]]]]]]]
      (with-params {'?nation "BRAZIL"
                    '?region "AMERICA"
                    '?type "ECONOMY ANODIZED STEEL"})))

(def tpch-q9-product-type-profit-measure
  (-> '[:order-by [nation, o_year :desc]
        [:group-by [nation o_year {sum_profit (sum amount)}]
         [:rename {n_name nation}
          [:project [n_name
                     {o_year (extract "YEAR" o_orderdate)}
                     {amount (- (* l_extendedprice (- 1 l_discount))
                                (* ps_supplycost l_quantity))}]
           [:join {s_nationkey n_nationkey}
            [:join {l_orderkey o_orderkey}
             [:join {l_suppkey s_suppkey}
              [:select (= ps_suppkey l_suppkey)
               [:join {l_partkey ps_partkey}
                [:join {p_partkey l_partkey}
                 [:scan [p_partkey {p_name (like p_name "%green%")}]]
                 [:scan [l_orderkey l_extendedprice l_discount l_suppkey l_partkey l_quantity]]]
                [:scan [ps_partkey ps_suppkey ps_supplycost]]]]
              [:scan [s_suppkey s_nationkey]]]
             [:scan [o_orderkey o_orderdate]]]
            [:scan [n_name n_nationkey]]]]]]]
      #_
      (with-params {'?color "green"})))

(def tpch-q10-returned-item-reporting
  (-> '[:slice {:limit 20}
        [:order-by [revenue :desc]
         [:group-by [c_custkey c_name c_acctbal c_phone n_name c_address c_comment
                     {revenue (sum disc_price)}]
          [:project [c_custkey c_name c_acctbal c_phone n_name c_address c_comment
                     {disc_price (* l_extendedprice (- 1 l_discount))}]
           [:join {c_nationkey n_nationkey}
            [:join {o_orderkey l_orderkey}
             [:join {c_custkey o_custkey}
              [:scan [c_custkey c_name c_acctbal c_address c_phone c_comment c_nationkey]]
              [:scan [o_orderkey o_custkey
                      {o_orderdate (and (>= o_orderdate ?start-date)
                                        (< o_orderdate ?end-date))}]]]
             [:scan [l_orderkey {l_returnflag (= l_returnflag "R")} l_extendedprice l_discount]]]
            [:scan [n_nationkey n_name]]]]]]]
      (with-params {'?start-date #inst "1993-10-01"
                    '?end-date #inst "1994-01-01"})))

(def tpch-q11-important-stock-identification
  (-> '[:assign [PartSupp [:project [ps_partkey {value (* ps_supplycost ps_availqty)}]
                           [:join {s_suppkey ps_suppkey}
                            [:join {n_nationkey s_nationkey}
                             [:scan [n_nationkey {n_name (= n_name ?nation)}]]
                             [:scan [s_nationkey s_suppkey]]]
                            [:scan [ps_partkey ps_suppkey ps_supplycost ps_availqty]]]]]
        [:order-by [value :desc]
         [:project [ps_partkey value]
          [:select (> value total)
           [:cross-join
            [:group-by [ps_partkey {value (sum value)}]
             PartSupp]
            [:project [{total (* total ?fraction)}]
             [:group-by [{total (sum value)}]
              PartSupp]]]]]]]
      (with-params {'?nation "GERMANY"
                    '?fraction 0.0001})))

(def tpch-q12-shipping-modes-and-order-priority
  (-> '[:order-by [l_shipmode]
        [:group-by [l_shipmode
                    {high_line_count (sum high_line)}
                    {low_line_count (sum low_line)}]
         [:project [l_shipmode
                    {high_line (if (or (= o_orderpriority "1-URGENT")
                                       (= o_orderpriority "2-HIGH"))
                                 1
                                 0)}
                    {low_line (if (and (!= o_orderpriority "1-URGENT")
                                       (!= o_orderpriority "2-HIGH"))
                                1
                                0)}]
          [:join {o_orderkey l_orderkey}
           [:scan [o_orderkey o_orderpriority]]
           [:select (and (< l_commitdate l_receiptdate)
                         (< l_shipdate l_commitdate))
            [:scan [l_orderkey l_commitdate l_shipdate
                    {l_shipmode (or (= l_shipmode ?ship-mode1)
                                    (= l_shipmode ?ship-mode2))}
                    {l_receiptdate (and (>= l_receiptdate ?start-date)
                                        (< l_receiptdate ?end-date))}]]]]]]]
      (with-params {'?ship-mode1 "MAIL"
                    '?ship-mode2 "SHIP"
                    '?start-date #inst "1994-01-01"
                    '?end-date #inst "1995-01-01"})))

(def tpch-q13-customer-distribution
  (-> '[:assign [Customers [:scan [c_custkey]]
                 Orders [:scan [{o_comment (not (like o_comment "%special%requests%"))} o_custkey]]]
        [:order-by [custdist :desc, c_count :desc]
         [:group-by [c_count {custdist (count c_custkey)}]
          [:group-by [c_custkey {c_count (count-not-null o_comment)}]
           [:union-all
            [:project [c_custkey o_comment]
             [:join {c_custkey o_custkey} Customers Orders]]
            [:cross-join
             [:anti-join {c_custkey o_custkey} Customers Orders]
             [:table [{o_comment nil}]]]]]]]]
      #_
      (with-params {'?word1 "special"
                    '?word2 "requests"})))

(def tpch-q14-promotion-effect
  (-> '[:project [{promo_revenue (* 100 (/ promo_revenue revenue))}]
        [:group-by [{promo_revenue (sum promo_disc_price)}
                    {revenue (sum disc_price)}]
         [:project [{promo_disc_price (if (like p_type "PROMO%")
                                        (* l_extendedprice (- 1 l_discount))
                                        0.0)}
                    {disc_price (* l_extendedprice (- 1 l_discount))}]
          [:join {p_partkey l_partkey}
           [:scan [p_partkey p_type]]
           [:scan [l_partkey l_extendedprice l_discount
                   {l_shipdate (and (>= l_shipdate ?start-date)
                                    (< l_shipdate ?end-date))}]]]]]]
      (with-params {'?start-date #inst "1995-09-01"
                    '?end-date #inst "1995-10-01"})))

(def tpch-q15-top-supplier
  (-> '[:assign [Revenue [:group-by [supplier_no {total_revenue (sum disc_price)}]
                          [:rename {l_suppkey supplier_no}
                           [:project [l_suppkey {disc_price (* l_extendedprice (- 1 l_discount))}]
                            [:scan [l_suppkey l_extendedprice l_discount
                                    {l_shipdate (and (>= l_shipdate ?start-date)
                                                     (< l_shipdate ?end-date))}]]]]]]
        [:project [s_suppkey s_name s_address s_phone total_revenue]
         [:select
          (= total_revenue max_total_revenue)
          [:cross-join
           [:join {supplier_no s_suppkey}
            Revenue
            [:scan [s_suppkey s_name s_address s_phone]]]
           [:group-by [{max_total_revenue (max total_revenue)}]
            Revenue]]]]]
      (with-params {'?start-date #inst "1996-01-01"
                    '?end-date #inst "1996-04-01"})))

(def tpch-q16-part-supplier-relationship
  (-> '[:order-by [supplier_cnt :desc, p_brand p_type p_size]
        [:group-by [p_brand p_type p_size {supplier_cnt (count ps_suppkey)}]
         [:distinct
          [:project [p_brand p_type p_size ps_suppkey]
           [:join {p_partkey ps_partkey}
            [:semi-join {p_size p_size}
             [:scan [p_partkey {p_brand (!= p_brand ?brand)} {p_type (not (like p_type "MEDIUM POLISHED%"))} p_size]]
             [:table ?sizes]]
            [:anti-join {ps_suppkey s_suppkey}
             [:scan [ps_partkey ps_suppkey]]
             [:scan [s_suppkey {s_comment (like s_comment "%Customer%Complaints%")}]]]]]]]]
      (with-meta {::params {'?brand "Brand#45"
                            ; '?type "MEDIUM POLISHED%"
                            '?sizes [{:p_size 49}
                                     {:p_size 14}
                                     {:p_size 23}
                                     {:p_size 45}
                                     {:p_size 19}
                                     {:p_size 3}
                                     {:p_size 36}
                                     {:p_size 9}]}})))

(def tpch-q17-small-quantity-order-revenue
  (-> '[:project [{avg_yearly (/ sum_extendedprice 7)}]
        [:group-by [{sum_extendedprice (sum l_extendedprice)}]
         [:select (< l_quantity small_avg_qty)
          [:join {p_partkey l_partkey}
           [:scan [p_partkey {p_brand (= p_brand ?brand)} {p_container (= p_container ?container)}]]
           [:project [l_partkey {small_avg_qty (* 0.2 avg_qty)}]
            [:group-by [l_partkey {avg_qty (avg l_quantity)}]
             [:scan [l_partkey l_quantity]]]]]]]]
      (with-params {'?brand "Brand#23"
                    '?container "MED_BOX"})))

(def tpch-q18-large-volume-customer
  (-> '[:slice {:limit 100}
        [:order-by [o_totalprice :desc, o_orderdate]
         [:group-by [c_name c_custkey o_orderkey o_orderdate o_totalprice {sum_qty (sum l_quantity)}]
          [:join {o_orderkey l_orderkey}
           [:join {o_custkey c_custkey}
            [:semi-join {o_orderkey l_orderkey}
             [:scan [o_orderkey o_custkey o_orderdate o_totalprice]]
             [:select (> sum_qty ?qty)
              [:group-by [l_orderkey {sum_qty (sum l_quantity)}]
               [:scan [l_orderkey l_quantity]]]]]
            [:scan [c_name c_custkey]]]
           [:scan [l_orderkey l_quantity]]]]]]
      (with-params {'?qty 300})))

(def tpch-q19-discounted-revenue
  (-> '[:group-by [{revenue (sum disc_price)}]
        [:project [{disc_price (* l_extendedprice (- 1 l_discount))}]
         [:select (or (and (= p_brand ?brand1)
                           (or (= p_container "SM CASE")
                               (= p_container "SM BOX")
                               (= p_container "SM PACK")
                               (= p_container "SM PKG"))
                           (>= l_quantity ?qty1)
                           (<= l_quantity (+ ?qty1 10))
                           (>= p_size 1)
                           (<= p_size 5))
                      (and (= p_brand ?brand2)
                           (or (= p_container "MED CASE")
                               (= p_container "MED BOX")
                               (= p_container "MED PACK")
                               (= p_container "MED PKG"))
                           (>= l_quantity ?qty2)
                           (<= l_quantity (+ ?qty2 10))
                           (>= p_size 1)
                           (<= p_size 10))
                      (and (= p_brand ?brand3)
                           (or (= p_container "LG CASE")
                               (= p_container "LG BOX")
                               (= p_container "LG PACK")
                               (= p_container "LG PKG"))
                           (>= l_quantity ?qty3)
                           (<= l_quantity (+ ?qty3 10))
                           (>= p_size 1)
                           (<= p_size 15)))
          [:join {p_partkey l_partkey}
           [:scan [p_partkey p_brand p_container p_size]]
           [:scan [l_partkey l_extendedprice l_discount l_quantity
                   {l_shipmode (or (= l_shipmode "AIR") (= l_shipmode "AIR REG"))}
                   {l_shipinstruct (= l_shipinstruct "DELIVER IN PERSON")}]]]]]]
      (with-params {'?qty1 1, '?qty2 10, '?qty3 20
                    '?brand1 "Brand#12", '?brand2 "Brand23", '?brand3 "Brand#34"})))

(def tpch-q20-potential-part-promotion
  (-> '[:order-by [s_name]
        [:project [s_name s_address]
         [:semi-join {s_suppkey ps_suppkey}
          [:join {n_nationkey s_nationkey}
           [:scan [{n_name (= n_name ?nation)} n_nationkey]]
           [:scan [s_name s_address s_nationkey s_suppkey]]]
          [:select (and (= l_suppkey ps_suppkey)
                        (> ps_availqty sum_qty))
           [:join {ps_partkey l_partkey}
            [:semi-join {ps_partkey p_partkey}
             [:scan [ps_suppkey ps_partkey ps_availqty]]
             [:scan [p_partkey {p_name (like p_name "forest%")}]]]
            [:project [l_partkey l_suppkey {sum_qty (* 0.5 sum_qty)}]
             [:group-by [l_partkey l_suppkey {sum_qty (sum l_quantity)}]
              [:scan [l_partkey l_suppkey l_quantity
                      {l_shipdate (and (>= l_shipdate ?start-date)
                                       (< l_shipdate ?end-date))}]]]]]]]]]
      (with-params {;'?color "forest"
                    '?start-date #inst "1994-01-01"
                    '?end-date #inst "1995-01-01"
                    '?nation "CANADA"})))

(def tpch-q21-suppliers-who-kept-orders-waiting
  (-> '[:assign [L1 [:select (!= l1_l_suppkey l2_l_suppkey)
                     [:join {l1_l_orderkey l2_l_orderkey}
                      [:join {l1_l_suppkey s_suppkey}
                       [:select (> l1_l_receiptdate l1_l_commitdate)
                        [:join {l1_l_orderkey o_orderkey}
                         [:rename l1
                          [:scan [l_orderkey l_suppkey l_receiptdate l_commitdate]]]
                         [:scan [o_orderkey {o_orderstatus (= o_orderstatus "F")}]]]]
                       [:semi-join {s_nationkey n_nationkey}
                        [:scan [s_nationkey s_suppkey s_name]]
                        [:scan [n_nationkey {n_name (= n_name ?nation)}]]]]
                      [:rename l2
                       [:scan [l_orderkey l_suppkey]]]]]]
        [:slice {:limit 100}
         [:order-by [numwait :desc, s_name]
          [:group-by [s_name {numwait (count l1_l_orderkey)}]
           [:distinct
            [:project [s_name l1_l_orderkey]
             [:anti-join {l1_l_orderkey l3_l_orderkey}
              L1
              [:select (!= l3_l_suppkey l1_l_suppkey)
               [:join {l1_l_orderkey l3_l_orderkey}
                L1
                [:select (> l3_l_receiptdate l3_l_commitdate)
                 [:rename l3
                  [:scan [l_orderkey l_suppkey l_receiptdate l_commitdate]]]]]]]]]]]]]
      (with-params {'?nation "SAUDI ARABIA"})))

(def tpch-q22-global-sales-opportunity
  (-> '[:assign [Customer [:semi-join {cntrycode cntrycode}
                           [:project [c_custkey {cntrycode (substr c_phone 1 2)} c_acctbal]
                            [:scan [c_custkey c_phone c_acctbal]]]
                           [:table ?cntrycodes]]]
        [:order-by [cntrycode]
         [:group-by [cntrycode {numcust (count c_custkey)} {totacctbal (sum c_acctbal)}]
          [:anti-join {c_custkey o_custkey}
           [:select (> c_acctbal avg_acctbal)
            [:cross-join
             Customer
             [:group-by [{avg_acctbal (avg c_acctbal)}]
              [:select (> c_acctbal 0.0)
               Customer]]]]
           [:scan [o_custkey]]]]]]
      (with-meta {::params {'?cntrycodes [{:cntrycode "13"}
                                          {:cntrycode "31"}
                                          {:cntrycode "23"}
                                          {:cntrycode "29"}
                                          {:cntrycode "30"}
                                          {:cntrycode "18"}
                                          {:cntrycode "17"}]}})))

(def queries
  [#'tpch-q1-pricing-summary-report
   #'tpch-q2-minimum-cost-supplier
   #'tpch-q3-shipping-priority
   #'tpch-q4-order-priority-checking
   #'tpch-q5-local-supplier-volume
   #'tpch-q6-forecasting-revenue-change
   #'tpch-q7-volume-shipping
   #'tpch-q8-national-market-share
   #'tpch-q9-product-type-profit-measure
   #'tpch-q10-returned-item-reporting
   #'tpch-q11-important-stock-identification
   #'tpch-q12-shipping-modes-and-order-priority
   #'tpch-q13-customer-distribution
   #'tpch-q14-promotion-effect
   #'tpch-q15-top-supplier
   #'tpch-q16-part-supplier-relationship
   #'tpch-q17-small-quantity-order-revenue
   #'tpch-q18-large-volume-customer
   #'tpch-q19-discounted-revenue
   #'tpch-q20-potential-part-promotion
   #'tpch-q21-suppliers-who-kept-orders-waiting
   #'tpch-q22-global-sales-opportunity])
