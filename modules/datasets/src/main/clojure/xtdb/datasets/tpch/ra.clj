(ns xtdb.datasets.tpch.ra
  (:import [java.time LocalDate]))

(defn- with-args [q args]
  (vary-meta q assoc ::args args))

(def q1-pricing-summary-report
  (-> '[:order-by {:order-specs [[l_returnflag] [l_linestatus]]}
        [:group-by [l_returnflag l_linestatus
                    {sum_qty (sum l_quantity)}
                    {sum_base_price (sum l_extendedprice)}
                    {sum_disc_price (sum disc_price)}
                    {sum_charge (sum charge)}
                    {avg_qty (avg l_quantity)}
                    {avg_price (avg l_extendedprice)}
                    {avg_disc (avg l_discount)}
                    {count_order (row-count)}]
         [:project {:projections [l_returnflag l_linestatus l_shipdate l_quantity l_extendedprice l_discount l_tax
                                   {disc_price (* l_extendedprice (- 1 l_discount))}
                                   {charge (* (* l_extendedprice (- 1 l_discount))
                                              (+ 1 l_tax))}]}
          [:scan {:table #xt/table lineitem, :columns [l_returnflag l_linestatus
            {l_shipdate (<= l_shipdate ?ship_date)}
            l_quantity l_extendedprice l_discount l_tax]}]]]]
      (with-args {:ship-date (LocalDate/parse "1998-09-02")})))

(def q2-minimum-cost-supplier
  (-> '[:let [PartSupp [:join {:conditions [{s_suppkey ps_suppkey}]}
                        [:join {:conditions [{n_nationkey s_nationkey}]}
                         [:join {:conditions [{n_regionkey r_regionkey}]}
                          [:rename {_id n_nationkey}
                           [:scan {:table #xt/table nation, :columns [_id n_name n_regionkey]}]]
                          [:rename {_id r_regionkey}
                           [:scan {:table #xt/table region, :columns [_id {r_name (== r_name ?region)}]}]]]
                         [:rename {_id s_suppkey}
                          [:scan {:table #xt/table supplier, :columns [_id s_nationkey s_acctbal s_name s_address s_phone s_comment]}]]]
                        [:scan {:table #xt/table partsupp, :columns [ps_suppkey ps_partkey ps_supplycost]}]]]
        [:top {:limit 100}
         [:order-by {:order-specs [[s_acctbal {:direction :desc}] [n_name] [s_name] [p_partkey]]}
          [:project {:projections [s_acctbal s_name n_name p_partkey p_mfgr s_address s_phone s_comment]}
           [:join {:conditions [{ps_partkey ps_partkey} {ps_supplycost min_ps_supplycost}]}
            [:join {:conditions [{ps_partkey p_partkey}]}
             [:relation PartSupp {:col-names [ps_partkey ps_supplycost]}]
             [:rename {_id p_partkey}
              [:scan {:table #xt/table part, :columns [_id p_mfgr {p_size (== p_size ?size)} {p_type (like p_type "%BRASS")}]}]]]
            [:group-by [ps_partkey {min_ps_supplycost (min ps_supplycost)}]
             [:relation PartSupp {:col-names [ps_partkey ps_supplycost]}]]]]]]]
      (with-args {:region "EUROPE"
                  ;; :type "BRASS"
                  :size 15})))

(def q3-shipping-priority
  (-> '[:top {:limit 10}
        [:order-by {:order-specs [[revenue {:direction :desc}] [o_orderdate {:direction :desc}]]}
         [:group-by [l_orderkey
                     {revenue (sum disc_price)}
                     o_orderdate
                     o_shippriority]
          [:project {:projections [l_orderkey o_orderdate o_shippriority
                                    {disc_price (* l_extendedprice (- 1 l_discount))}]}
           [:join {:conditions [{o_orderkey l_orderkey}]}
            [:join {:conditions [{c_custkey o_custkey}]}
             [:rename {_id c_custkey}
              [:scan {:table #xt/table customer, :columns [_id {c_mktsegment (== c_mktsegment ?segment)}]}]]
             [:rename {_id o_orderkey}
              [:scan {:table #xt/table orders, :columns [_id o_custkey o_shippriority
                {o_orderdate (< o_orderdate ?date)}]}]]]
            [:scan {:table #xt/table lineitem, :columns [l_orderkey l_extendedprice l_discount
              {l_shipdate (> l_shipdate ?date)}]}]]]]]]
      (with-args {:segment "BUILDING"
                  :date (LocalDate/parse "1995-03-15")})))

(def q4-order-priority-checking
  (-> '[:order-by {:order-specs [[o_orderpriority]]}
        [:group-by [o_orderpriority {order_count (row-count)}]
         [:semi-join {:conditions [{o_orderkey l_orderkey}]}
          [:rename {_id o_orderkey}
           [:scan {:table #xt/table orders, :columns [o_orderpriority _id
             {o_orderdate (and (>= o_orderdate ?start_date)
                               (< o_orderdate ?end_date))}]}]]
          [:select {:predicate (< l_commitdate l_receiptdate)}
           [:scan {:table #xt/table lineitem, :columns [l_orderkey l_commitdate l_receiptdate]}]]]]]
      (with-args {:start-date (LocalDate/parse "1993-07-01")
                  ;; in the spec this is one date with `+ INTERVAL 3 MONTHS`
                  :end-date (LocalDate/parse "1993-10-01")})))

(def q5-local-supplier-volume
  (-> '[:order-by {:order-specs [[revenue {:direction :desc}]]}
        [:group-by [n_name {revenue (sum disc_price)}]
         [:project {:projections [n_name {disc_price (* l_extendedprice (- 1 l_discount))}]}
          [:join {:conditions [{o_orderkey l_orderkey} {s_suppkey l_suppkey}]}
           [:join {:conditions [{s_nationkey c_nationkey}]}
            [:join {:conditions [{n_nationkey s_nationkey}]}
             [:join {:conditions [{r_regionkey n_regionkey}]}
              [:rename {_id r_regionkey}
               [:scan {:table #xt/table region, :columns [_id {r_name (== r_name "ASIA")}]}]]
              [:rename {_id n_nationkey}
               [:scan {:table #xt/table nation, :columns [_id n_name n_regionkey]}]]]
             [:rename {_id s_suppkey}
              [:scan {:table #xt/table supplier, :columns [_id s_nationkey]}]]]
            [:join {:conditions [{o_custkey c_custkey}]}
             [:rename {_id o_orderkey}
              [:scan {:table #xt/table orders, :columns [_id o_custkey
                {o_orderdate (and (>= o_orderdate ?start_date)
                                  (< o_orderdate ?end_date))}]}]]
             [:rename {_id c_custkey}
              [:scan {:table #xt/table customer, :columns [_id c_nationkey]}]]]]
           [:scan {:table #xt/table lineitem, :columns [l_orderkey l_extendedprice l_discount l_suppkey]}]]]]]
      (with-args {:start-date (LocalDate/parse "1994-01-01")
                  :end-date (LocalDate/parse "1995-01-01")})))

(def q6-forecasting-revenue-change
  (-> '[:group-by [{revenue (sum disc_price)}]
        [:project {:projections [{disc_price (* l_extendedprice l_discount)}]}
         [:scan {:table #xt/table lineitem, :columns [{l_shipdate (and (>= l_shipdate ?start_date)
                            (< l_shipdate ?end_date))}
           l_extendedprice
           {l_discount (and (>= l_discount ?min_discount)
                            (<= l_discount ?max_discount))}
           {l_quantity (< l_quantity 24.0)}]}]]]
      (with-args {:start-date (LocalDate/parse "1994-01-01")
                  :end-date (LocalDate/parse "1995-01-01")
                  :min-discount 0.05
                  :max-discount 0.07})))

(def q7-volume-shipping
  (-> '[:order-by {:order-specs [[supp_nation] [cust_nation] [l_year]]}
        [:group-by [supp_nation cust_nation l_year {revenue (sum volume)}]
         [:project {:projections [supp_nation cust_nation
                                   {l_year (extract "YEAR" l_shipdate)}
                                   {volume (* l_extendedprice (- 1 l_discount))}]}
          [:rename {n1/n_name supp_nation, n2/n_name cust_nation}
           [:join {:conditions [{c_nationkey n2/n_nationkey}
                   (or (and (== n1/n_name ?nation1)
                            (== n2/n_name ?nation2))
                       (and (== n1/n_name ?nation2)
                            (== n2/n_name ?nation1)))]}
            [:join {:conditions [{o_custkey c_custkey}]}
             [:join {:conditions [{n1/n_nationkey s_nationkey}]}
              [:rename n1
               [:rename {_id n_nationkey}
                [:scan {:table #xt/table nation, :columns [_id {n_name (or (== n_name ?nation1) (== n_name ?nation2))}]}]]]
              [:join {:conditions [{o_orderkey l_orderkey}]}
               [:rename {_id o_orderkey}
                [:scan {:table #xt/table orders, :columns [_id o_custkey]}]]
               [:join {:conditions [{s_suppkey l_suppkey}]}
                [:rename {_id s_suppkey}
                 [:scan {:table #xt/table supplier, :columns [_id s_nationkey]}]]
                [:scan {:table #xt/table lineitem, :columns [l_orderkey l_extendedprice l_discount l_suppkey
                  {l_shipdate (and (>= l_shipdate ?start_date)
                                   (<= l_shipdate ?end_date))}]}]]]]
             [:rename {_id c_custkey}
              [:scan {:table #xt/table customer, :columns [_id c_nationkey]}]]]
            [:rename n2
             [:rename {_id n_nationkey}
              [:scan {:table #xt/table nation, :columns [_id {n_name (or (== n_name ?nation1) (== n_name ?nation2))}]}]]]]]]]]
      (with-args {:nation1 "FRANCE"
                  :nation2 "GERMANY"
                  :start-date (LocalDate/parse "1995-01-01")
                  :end-date (LocalDate/parse "1996-12-31")})))

(def q8-national-market-share
  (-> '[:order-by {:order-specs [[o_year]]}
        [:project {:projections [o_year {mkt_share (/ brazil_revenue revenue)}]}
         [:group-by [o_year {brazil_revenue (sum brazil_volume)} {revenue (sum volume)}]
          [:project {:projections [{o_year (extract "YEAR" o_orderdate)}
                                   {brazil_volume (if (== nation ?nation)
                                                    (* l_extendedprice (- 1 l_discount))
                                                    0.0)}
                                   {volume (* l_extendedprice (- 1 l_discount))}
                                   nation]}
           [:rename {n2/n_name nation}
            [:join {:conditions [{s_nationkey n2/n_nationkey}]}
             [:join {:conditions [{c_nationkey n1/n_nationkey}]}
              [:join {:conditions [{o_custkey c_custkey}]}
               [:join {:conditions [{l_orderkey o_orderkey}]}
                [:join {:conditions [{l_suppkey s_suppkey}]}
                 [:join {:conditions [{p_partkey l_partkey}]}
                  [:rename {_id p_partkey}
                   [:scan {:table #xt/table part, :columns [_id {p_type (== p_type ?type)}]}]]
                  [:scan {:table #xt/table lineitem, :columns [l_orderkey l_extendedprice l_discount l_suppkey l_partkey]}]]
                 [:rename {_id s_suppkey}
                  [:scan {:table #xt/table supplier, :columns [_id s_nationkey]}]]]
                [:rename {_id o_orderkey}
                 [:scan {:table #xt/table orders, :columns [_id o_custkey
                   {o_orderdate (and (>= o_orderdate ?start_date)
                                     (<= o_orderdate ?end_date))}]}]]]
               [:rename {_id c_custkey}
                [:scan {:table #xt/table customer, :columns [_id c_nationkey]}]]]
              [:join {:conditions [{r_regionkey n1/n_regionkey}]}
               [:rename {_id r_regionkey}
                [:scan {:table #xt/table region, :columns [_id {r_name (== r_name ?region)}]}]]
               [:rename n1
                [:rename {_id n_nationkey}
                 [:scan {:table #xt/table nation, :columns [_id n_name n_regionkey]}]]]]]
             [:rename n2
              [:rename {_id n_nationkey}
               [:scan {:table #xt/table nation, :columns [_id n_name]}]]]]]]]]]
      (with-args {:nation "BRAZIL"
                  :region "AMERICA"
                  :type "ECONOMY ANODIZED STEEL"
                  :start-date (LocalDate/parse "1995-01-01")
                  :end-date (LocalDate/parse "1996-12-31")})))

(def q9-product-type-profit-measure
  (-> '[:order-by {:order-specs [[nation] [o_year {:direction :desc}]]}
        [:group-by [nation o_year {sum_profit (sum amount)}]
         [:rename {n_name nation}
          [:project {:projections [n_name
                                    {o_year (extract "YEAR" o_orderdate)}
                                    {amount (- (* l_extendedprice (- 1 l_discount))
                                               (* ps_supplycost l_quantity))}]}
           [:join {:conditions [{s_nationkey n_nationkey}]}
            [:join {:conditions [{l_orderkey o_orderkey}]}
             [:join {:conditions [{l_suppkey s_suppkey}]}
              [:join {:conditions [{l_partkey ps_partkey} {l_suppkey ps_suppkey}]}
               [:join {:conditions [{p_partkey l_partkey}]}
                [:rename {_id p_partkey}
                 [:scan {:table #xt/table part, :columns [_id {p_name (like p_name "%green%")}]}]]
                [:scan {:table #xt/table lineitem, :columns [l_orderkey l_extendedprice l_discount l_suppkey l_partkey l_quantity]}]]
               [:scan {:table #xt/table partsupp, :columns [ps_partkey ps_suppkey ps_supplycost]}]]
              [:rename {_id s_suppkey}
               [:scan {:table #xt/table supplier, :columns [_id s_nationkey]}]]]
             [:rename {_id o_orderkey}
              [:scan {:table #xt/table orders, :columns [_id o_orderdate]}]]]
            [:rename {_id n_nationkey}
             [:scan {:table #xt/table nation, :columns [_id n_name]}]]]]]]]
      #_(with-params {:color "green"})))

(def q10-returned-item-reporting
  (-> '[:top {:limit 20}
        [:order-by {:order-specs [[revenue {:direction :desc}]]}
         [:group-by [c_custkey c_name c_acctbal c_phone n_name c_address c_comment
                     {revenue (sum disc_price)}]
          [:project {:projections [c_custkey c_name c_acctbal c_phone n_name c_address c_comment
                                    {disc_price (* l_extendedprice (- 1 l_discount))}]}
           [:join {:conditions [{c_nationkey n_nationkey}]}
            [:join {:conditions [{o_orderkey l_orderkey}]}
             [:join {:conditions [{c_custkey o_custkey}]}
              [:rename {_id c_custkey}
               [:scan {:table #xt/table customer, :columns [_id c_name c_acctbal c_address c_phone c_comment c_nationkey]}]]
              [:rename {_id o_orderkey}
               [:scan {:table #xt/table orders, :columns [_id o_custkey
                 {o_orderdate (and (>= o_orderdate ?start_date)
                                   (< o_orderdate ?end_date))}]}]]]
             [:scan {:table #xt/table lineitem, :columns [l_orderkey {l_returnflag (== l_returnflag "R")} l_extendedprice l_discount]}]]
            [:rename {_id n_nationkey}
             [:scan {:table #xt/table nation, :columns [_id n_name]}]]]]]]]
      (with-args {:start-date (LocalDate/parse "1993-10-01")
                  :end-date (LocalDate/parse "1994-01-01")})))

(def q11-important-stock-identification
  (-> '[:let [PartSupp [:project {:projections [ps_partkey {value (* ps_supplycost ps_availqty)}]}
                        [:join {:conditions [{s_suppkey ps_suppkey}]}
                         [:join {:conditions [{n_nationkey s_nationkey}]}
                          [:rename {_id n_nationkey}
                           [:scan {:table #xt/table nation, :columns [_id {n_name (== n_name ?nation)}]}]]
                          [:rename {_id s_suppkey}
                           [:scan {:table #xt/table supplier, :columns [_id s_nationkey]}]]]
                         [:scan {:table #xt/table partsupp, :columns [ps_partkey ps_suppkey ps_supplycost ps_availqty]}]]]]
        [:order-by {:order-specs [[value {:direction :desc}]]}
         [:project {:projections [ps_partkey value]}
          [:join {:conditions [(> value total)]}
           [:group-by [ps_partkey {value (sum value)}]
            [:relation PartSupp {:col-names [ps_partkey value]}]]
           [:project {:projections [{total (* total ?fraction)}]}
            [:group-by [{total (sum value)}]
             [:relation PartSupp {:col-names [ps_partkey value]}]]]]]]]
      (with-args {:nation "GERMANY"
                  :fraction 0.0001})))

(def q12-shipping-modes-and-order-priority
  (-> '[:order-by {:order-specs [[l_shipmode]]}
        [:group-by [l_shipmode
                    {high_line_count (sum high_line)}
                    {low_line_count (sum low_line)}]
         [:project {:projections [l_shipmode
                                   {high_line (if (or (== o_orderpriority "1-URGENT")
                                                      (== o_orderpriority "2-HIGH"))
                                                1
                                                0)}
                                   {low_line (if (and (<> o_orderpriority "1-URGENT")
                                                      (<> o_orderpriority "2-HIGH"))
                                               1
                                               0)}]}
          [:join {:conditions [{o_orderkey l_orderkey}]}
           [:rename {_id o_orderkey}
            [:scan {:table #xt/table orders, :columns [_id o_orderpriority]}]]
           [:select {:predicate (and (< l_commitdate l_receiptdate)
                                     (< l_shipdate l_commitdate))}
            [:scan {:table #xt/table lineitem, :columns [l_orderkey l_commitdate l_shipdate
              {l_shipmode (or (== l_shipmode ?ship_mode1)
                              (== l_shipmode ?ship_mode2))}
              {l_receiptdate (and (>= l_receiptdate ?start_date)
                                  (< l_receiptdate ?end_date))}]}]]]]]]
      (with-args {:ship-mode1 "MAIL"
                  :ship-mode2 "SHIP"
                  :start-date (LocalDate/parse "1994-01-01")
                  :end-date (LocalDate/parse "1995-01-01")})))

(def q13-customer-distribution
  (-> '[:order-by {:order-specs [[custdist {:direction :desc}] [c_count {:direction :desc}]]}
        [:group-by [c_count {custdist (row-count)}]
         [:group-by [c_custkey {c_count (count o_comment)}]
          [:left-outer-join {:conditions [{c_custkey o_custkey}]}
           [:rename {_id c_custkey}
            [:scan {:table #xt/table customer, :columns [_id]}]]
           [:rename {_id o_orderkey}
            [:scan {:table #xt/table orders, :columns [_id {o_comment (not (like o_comment "%special%requests%"))} o_custkey]}]]]]]]
      #_(with-params {:word1 "special"
                      :word2 "requests"})))

(def q14-promotion-effect
  (-> '[:project {:projections [{promo_revenue (* 100 (/ promo_revenue revenue))}]}
        [:group-by [{promo_revenue (sum promo_disc_price)}
                    {revenue (sum disc_price)}]
         [:project {:projections [{promo_disc_price (if (like p_type "PROMO%")
                                                      (* l_extendedprice (- 1 l_discount))
                                                      0.0)}
                                  {disc_price (* l_extendedprice (- 1 l_discount))}]}
          [:join {:conditions [{p_partkey l_partkey}]}
           [:rename {_id p_partkey}
            [:scan {:table #xt/table part, :columns [_id p_type]}]]
           [:scan {:table #xt/table lineitem, :columns [l_partkey l_extendedprice l_discount
             {l_shipdate (and (>= l_shipdate ?start_date)
                              (< l_shipdate ?end_date))}]}]]]]]
      (with-args {:start-date (LocalDate/parse "1995-09-01")
                  :end-date (LocalDate/parse "1995-10-01")})))

(def q15-top-supplier
  (-> '[:let [Revenue [:group-by [supplier_no {total_revenue (sum disc_price)}]
                       [:rename {l_suppkey supplier_no}
                        [:project {:projections [l_suppkey {disc_price (* l_extendedprice (- 1 l_discount))}]}
                         [:scan {:table #xt/table lineitem, :columns [l_suppkey l_extendedprice l_discount
                           {l_shipdate (and (>= l_shipdate ?start_date)
                                            (< l_shipdate ?end_date))}]}]]]]]
        [:project {:projections [s_suppkey s_name s_address s_phone total_revenue]}
         [:join {:conditions [{total_revenue max_total_revenue}]}
          [:join {:conditions [{supplier_no s_suppkey}]}
           [:relation Revenue {:col-names [supplier_no total_revenue]}]
           [:rename {_id s_suppkey}
            [:scan {:table #xt/table supplier, :columns [_id s_name s_address s_phone]}]]]
          [:group-by [{max_total_revenue (max total_revenue)}]
           [:relation Revenue {:col-names [supplier_no total_revenue]}]]]]]
      (with-args {:start-date (LocalDate/parse "1996-01-01")
                  :end-date (LocalDate/parse "1996-04-01")})))

(def q16-part-supplier-relationship
  (-> '[:order-by {:order-specs [[supplier_cnt {:direction :desc}] [p_brand] [p_type] [p_size]]}
        [:group-by [p_brand p_type p_size {supplier_cnt (count ps_suppkey)}]
         [:distinct {}
          [:project {:projections [p_brand p_type p_size ps_suppkey]}
           [:join {:conditions [{p_partkey ps_partkey}]}
            [:semi-join {:conditions [{p_size p_size}]}
             [:rename {_id p_partkey}
              [:scan {:table #xt/table part, :columns [_id {p_brand (<> p_brand ?brand)} {p_type (not (like p_type "MEDIUM POLISHED%"))} p_size]}]]
             [:table ?sizes]]
            [:anti-join {:conditions [{ps_suppkey s_suppkey}]}
             [:scan {:table #xt/table partsupp, :columns [ps_partkey ps_suppkey]}]
             [:rename {_id s_suppkey}
              [:scan {:table #xt/table supplier, :columns [_id {s_comment (like s_comment "%Customer%Complaints%")}]}]]]]]]]]
      (with-args {:brand "Brand#45"
                  ;; :type "MEDIUM POLISHED%"

                  :sizes [{:p_size 49}
                          {:p_size 14}
                          {:p_size 23}
                          {:p_size 45}
                          {:p_size 19}
                          {:p_size 3}
                          {:p_size 36}
                          {:p_size 9}]})))

(def q17-small-quantity-order-revenue
  (-> '[:project {:projections [{avg_yearly (/ sum_extendedprice 7)}]}
        [:group-by [{sum_extendedprice (sum l_extendedprice)}]
         [:join {:conditions [{p_partkey l_partkey} (< l_quantity small_avg_qty)]}
          [:rename {_id p_partkey}
           [:scan {:table #xt/table part, :columns [_id {p_brand (== p_brand ?brand)} {p_container (== p_container ?container)}]}]]
          [:join {:conditions [{l_partkey l_partkey}]}
           [:project {:projections [l_partkey {small_avg_qty (* 0.2 avg_qty)}]}
            [:group-by [l_partkey {avg_qty (avg l_quantity)}]
             [:scan {:table #xt/table lineitem, :columns [l_partkey l_quantity]}]]]
           [:scan {:table #xt/table lineitem, :columns [l_partkey l_quantity]}]]]]]
      (with-args {:brand "Brand#23"
                  :container "MED_BOX"})))

(def q18-large-volume-customer
  (-> '[:top {:limit 100}
        [:order-by {:order-specs [[o_totalprice {:direction :desc}] [o_orderdate {:direction :desc}]]}
         [:group-by [c_name c_custkey o_orderkey o_orderdate o_totalprice {sum_qty (sum l_quantity)}]
          [:join {:conditions [{o_orderkey l_orderkey}]}
           [:join {:conditions [{o_custkey c_custkey}]}
            [:semi-join {:conditions [{o_orderkey l_orderkey}]}
             [:rename {_id o_orderkey}
              [:scan {:table #xt/table orders, :columns [_id o_custkey o_orderdate o_totalprice]}]]
             [:select {:predicate (> sum_qty ?qty)}
              [:group-by [l_orderkey {sum_qty (sum l_quantity)}]
               [:scan {:table #xt/table lineitem, :columns [l_orderkey l_quantity]}]]]]
            [:rename {_id c_custkey}
             [:scan {:table #xt/table customer, :columns [_id c_name]}]]]
           [:scan {:table #xt/table lineitem, :columns [l_orderkey l_quantity]}]]]]]
      (with-args {:qty 300})))

(def q19-discounted-revenue
  (-> '[:group-by [{revenue (sum disc_price)}]
        [:project {:projections [{disc_price (* l_extendedprice (- 1 l_discount))}]}
         [:select {:predicate (or (and (== p_brand ?brand1)
                                       (or (== p_container "SM CASE")
                                           (== p_container "SM BOX")
                                           (== p_container "SM PACK")
                                           (== p_container "SM PKG"))
                                       (>= l_quantity ?qty1)
                                       (<= l_quantity (+ ?qty1 10))
                                       (>= p_size 1)
                                       (<= p_size 5))
                                  (and (== p_brand ?brand2)
                                       (or (== p_container "MED CASE")
                                           (== p_container "MED BOX")
                                           (== p_container "MED PACK")
                                           (== p_container "MED PKG"))
                                       (>= l_quantity ?qty2)
                                       (<= l_quantity (+ ?qty2 10))
                                       (>= p_size 1)
                                       (<= p_size 10))
                                  (and (== p_brand ?brand3)
                                       (or (== p_container "LG CASE")
                                           (== p_container "LG BOX")
                                           (== p_container "LG PACK")
                                           (== p_container "LG PKG"))
                                       (>= l_quantity ?qty3)
                                       (<= l_quantity (+ ?qty3 10))
                                       (>= p_size 1)
                                       (<= p_size 15)))}
          [:join {:conditions [{p_partkey l_partkey}]}
           [:rename {_id p_partkey}
            [:scan {:table #xt/table part, :columns [_id p_brand p_container p_size]}]]
           [:scan {:table #xt/table lineitem, :columns [l_partkey l_extendedprice l_discount l_quantity
             {l_shipmode (or (== l_shipmode "AIR") (== l_shipmode "AIR REG"))}
             {l_shipinstruct (== l_shipinstruct "DELIVER IN PERSON")}]}]]]]]
      (with-args {:qty1 1, :qty2 10, :qty3 20
                  :brand1 "Brand#12", :brand2 "Brand23", :brand3 "Brand#34"})))

(def q20-potential-part-promotion
  (-> '[:order-by {:order-specs [[s_name]]}
        [:project {:projections [s_name s_address]}
         [:semi-join {:conditions [{s_suppkey ps_suppkey}]}
          [:join {:conditions [{n_nationkey s_nationkey}]}
           [:rename {_id n_nationkey}
            [:scan {:table #xt/table nation, :columns [_id {n_name (== n_name ?nation)}]}]]
           [:rename {_id s_suppkey}
            [:scan {:table #xt/table supplier, :columns [_id s_name s_address s_nationkey]}]]]
          [:join {:conditions [{ps_partkey l_partkey} {ps_suppkey l_suppkey} (> ps_availqty sum_qty)]}
           [:semi-join {:conditions [{ps_partkey p_partkey}]}
            [:scan {:table #xt/table partsupp, :columns [ps_suppkey ps_partkey ps_availqty]}]
            [:rename {_id p_partkey}
             [:scan {:table #xt/table part, :columns [_id {p_name (like p_name "forest%")}]}]]]
           [:project {:projections [l_partkey l_suppkey {sum_qty (* 0.5 sum_qty)}]}
            [:group-by [l_partkey l_suppkey {sum_qty (sum l_quantity)}]
             [:scan {:table #xt/table lineitem, :columns [l_partkey l_suppkey l_quantity
               {l_shipdate (and (>= l_shipdate ?start_date)
                                (< l_shipdate ?end_date))}]}]]]]]]]
      (with-args {;:color "forest"
                  :start-date (LocalDate/parse "1994-01-01")
                  :end-date (LocalDate/parse "1995-01-01")
                  :nation "CANADA"})))

(def q21-suppliers-who-kept-orders-waiting
  (-> '[:let [L1 [:join {:conditions [{l1/l_orderkey l2/l_orderkey} (<> l1/l_suppkey l2/l_suppkey)]}
                  [:join {:conditions [{l1/l_suppkey s_suppkey}]}
                   [:join {:conditions [{l1/l_orderkey o_orderkey}]}
                    [:rename l1
                     [:select {:predicate (> l_receiptdate l_commitdate)}
                      [:scan {:for-valid-time [:at :now], :table #xt/table lineitem, :columns [l_orderkey l_suppkey l_receiptdate l_commitdate]}]]]
                    [:rename {_id o_orderkey}
                     [:scan {:for-valid-time [:at :now], :table #xt/table orders, :columns [_id {o_orderstatus (== o_orderstatus "F")}]}]]]
                   [:semi-join {:conditions [{s_nationkey n_nationkey}]}
                    [:rename {_id s_suppkey}
                     [:scan {:for-valid-time [:at :now], :table #xt/table supplier, :columns [_id s_nationkey s_name]}]]
                    [:rename {_id n_nationkey}
                     [:scan {:for-valid-time [:at :now], :table #xt/table nation, :columns [_id {n_name (== n_name ?nation)}]}]]]]
                  [:rename l2
                   [:scan {:for-valid-time [:at :now], :table #xt/table lineitem, :columns [l_orderkey l_suppkey]}]]]]
        [:top {:limit 100}
         [:order-by {:order-specs [[numwait {:direction :desc}] [s_name]]}
          [:group-by [s_name {numwait (row-count)}]
           [:distinct {}
            [:project {:projections [s_name l1/l_orderkey]}
             [:anti-join {:conditions [{l1/l_orderkey l3/l_orderkey}]}
              [:relation L1 {:col-names [l_orderkey l_suppkey l_receiptdate l_commitdate]}]
              [:join {:conditions [{l1/l_orderkey l3/l_orderkey} (<> l3/l_suppkey l1/l_suppkey)]}
               [:relation L1 {:col-names [l_orderkey l_suppkey l_receiptdate l_commitdate]}]
               [:select {:predicate (> l3/l_receiptdate l3/l_commitdate)}
                [:rename l3
                 [:scan {:table #xt/table lineitem, :columns [l_orderkey l_suppkey l_receiptdate l_commitdate]}]]]]]]]]]]]
      (with-args {:nation "SAUDI ARABIA"})))

(def q22-global-sales-opportunity
  (-> '[:let [Customer [:semi-join {:conditions [{cntrycode cntrycode}]}
                        [:project {:projections [c_custkey {cntrycode (substring c_phone 1 2)} c_acctbal]}
                         [:rename {_id c_custkey}
                          [:scan {:for-valid-time [:at :now], :table #xt/table customer, :columns [_id c_phone c_acctbal]}]]]
                        [:table ?cntrycodes]]]
        [:order-by {:order-specs [[cntrycode]]}
         [:group-by [cntrycode {numcust (row-count)} {totacctbal (sum c_acctbal)}]
          [:anti-join {:conditions [{c_custkey o_custkey}]}
           [:join {:conditions [(> c_acctbal avg_acctbal)]}
            [:relation Customer {:col-names [c_custkey cntrycode c_acctbal]}]
            [:group-by [{avg_acctbal (avg c_acctbal)}]
             [:select {:predicate (> c_acctbal 0.0)}
              [:relation Customer {:col-names [c_custkey cntrycode c_acctbal]}]]]]
           [:scan {:for-valid-time [:at :now], :table #xt/table orders, :columns [o_custkey]}]]]]]
      (with-args {:cntrycodes [{:cntrycode "13"}
                               {:cntrycode "31"}
                               {:cntrycode "23"}
                               {:cntrycode "29"}
                               {:cntrycode "30"}
                               {:cntrycode "18"}
                               {:cntrycode "17"}]})))

(def queries
  [#'q1-pricing-summary-report
   #'q2-minimum-cost-supplier
   #'q3-shipping-priority
   #'q4-order-priority-checking
   #'q5-local-supplier-volume
   #'q6-forecasting-revenue-change
   #'q7-volume-shipping
   #'q8-national-market-share
   #'q9-product-type-profit-measure
   #'q10-returned-item-reporting
   #'q11-important-stock-identification
   #'q12-shipping-modes-and-order-priority
   #'q13-customer-distribution
   #'q14-promotion-effect
   #'q15-top-supplier
   #'q16-part-supplier-relationship
   #'q17-small-quantity-order-revenue
   #'q18-large-volume-customer
   #'q19-discounted-revenue
   #'q20-potential-part-promotion
   #'q21-suppliers-who-kept-orders-waiting
   #'q22-global-sales-opportunity])
