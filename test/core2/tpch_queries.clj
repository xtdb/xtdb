(ns core2.tpch-queries
  (:require [clojure.string :as str]
            [core2.core :as c2]
            [core2.expression :as expr]
            [core2.test-util :as tu]
            [core2.tpch :as tpch]
            [core2.util :as util])
  (:import [java.time Duration Instant ZoneOffset]
           java.time.temporal.ChronoField
           java.util.Date))

(def ^:dynamic ^:private *node*)
(def ^:dynamic ^:private *watermark*)

;; (slurp (io/resource (format "io/airlift/tpch/queries/q%d.sql" 1)))

(defn with-tpch-data [scale-factor test-name]
  (fn [f]
    (try
      (let [node-dir (util/->path (str "target/" test-name))]
        (util/delete-dir node-dir)

        (with-open [node (tu/->local-node {:node-dir node-dir})
                    tx-producer (tu/->local-tx-producer {:node-dir node-dir})]
          (let [last-tx (tpch/submit-docs! tx-producer scale-factor)]
            (c2/await-tx node last-tx (Duration/ofMinutes 2))

            (tu/finish-chunk node))

          (with-open [watermark (c2/open-watermark node)]
            (binding [*node* node
                      *watermark* watermark]
              (f)))))
      (catch Throwable e
        (.printStackTrace e)))))

(defmethod expr/codegen-call [:like Comparable String] [{[{x :code} {y :code}] :args}]
  {:code `(boolean (re-find ~(re-pattern (str/replace y #"%" ".*")) ~x))
   :return-type Boolean})

(defmethod expr/codegen-call [:extract String Date] [{[{x :code} {y :code}] :args}]
  {:code `(.get (.atOffset (Instant/ofEpochMilli ~y) ZoneOffset/UTC)
                ~(case x
                   "YEAR" `ChronoField/YEAR
                   "MONTH" `ChronoField/MONTH_OF_YEAR
                   "DAY" `ChronoField/DAY_OF_MONTH
                   "HOUR" `ChronoField/HOUR_OF_DAY
                   "MINUTE" `ChronoField/MINUTE_OF_HOUR))
   :return-type Long})

(defn tpch-q1-pricing-summary-report []
  (with-open [res (c2/open-q *node* *watermark*
                             '[:order-by [{l_returnflag :asc} {l_linestatus :asc}]
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
                                         {l_shipdate (<= l_shipdate #inst "1998-09-02")}
                                         l_quantity l_extendedprice l_discount l_tax]]]]])]
    (->> (tu/<-cursor res)
         (into [] (mapcat seq)))))

(defn tpch-q3-shipping-priority []
  (with-open [res (c2/open-q *node* *watermark*
                             '[:slice {:limit 10}
                               [:order-by [{revenue :desc}, {o_orderdate :asc}]
                                [:group-by [l_orderkey
                                            {revenue (sum disc_price)}
                                            o_orderdate
                                            o_shippriority]
                                 [:project [l_orderkey o_orderdate o_shippriority
                                            {disc_price (* l_extendedprice (- 1 l_discount))}]
                                  [:join {o_orderkey l_orderkey}
                                   [:join {c_custkey o_custkey}
                                    [:scan [c_custkey {c_mktsegment (= c_mktsegment "BUILDING")}]]
                                    [:scan [o_orderkey o_custkey o_shippriority
                                            {o_orderdate (< o_orderdate #inst "1995-03-15")}]]]
                                   [:scan [l_orderkey l_extendedprice l_discount
                                           {l_shipdate (> l_shipdate #inst "1995-03-15")}]]]]]]])]
    (->> (tu/<-cursor res)
         (into [] (mapcat seq)))))

(defn tpch-q5-local-supplier-volume []
  (with-open [res (c2/open-q *node* *watermark*
                             '[:order-by [{revenue :desc}]
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
                                   [:scan [l_orderkey l_extendedprice l_discount l_suppkey]]]]]]])]
    (->> (tu/<-cursor res)
         (into [] (mapcat seq)))))

(defn tpch-q6-forecasting-revenue-change []
  (with-open [res (c2/open-q *node* *watermark*
                             '[:group-by [{revenue (sum disc_price)}]
                               [:project [{disc_price (* l_extendedprice l_discount)}]
                                [:scan [{l_shipdate (and (>= l_shipdate #inst "1994-01-01")
                                                         (< l_shipdate #inst "1995-01-01"))}
                                        l_extendedprice
                                        {l_discount (and (>= l_discount 0.05)
                                                         (<= l_discount 0.07))}
                                        {l_quantity (< l_quantity 24.0)}]]]])]
    (->> (tu/<-cursor res)
         (into [] (mapcat seq)))))

(defn tpch-q7-volume-shipping []
  (with-open [res (c2/open-q *node* *watermark*
                             '[:order-by [{supp_nation :asc} {cust_nation :asc} {l_year :asc}]
                               [:group-by [supp_nation cust_nation l_year {revenue (sum volume)}]
                                [:project [supp_nation cust_nation
                                           {l_year (extract "YEAR" l_shipdate)}
                                           {volume (* l_extendedprice (- 1 l_discount))}]
                                 [:rename {n1_n_name supp_nation, n2_n_name cust_nation}
                                  [:select (or (and (= n1_n_name "FRANCE")
                                                    (= n2_n_name "GERMANY"))
                                               (and (= n1_n_name "GERMANY")
                                                    (= n2_n_name "FRANCE")))
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
                                       [:scan [{n_name (or (= n_name "GERMANY") (= n_name "FRANCE"))} n_nationkey]]]]
                                     [:scan [c_custkey c_nationkey]]]
                                    [:rename n2
                                     [:scan [{n_name (or (= n_name "GERMANY") (= n_name "FRANCE"))} n_nationkey]]]]]]]]])]
    (->> (tu/<-cursor res)
         (into [] (mapcat seq)))))

(defn tpch-q8-national-market-share []
  (with-open [res (c2/open-q *node* *watermark*
                             '[:order-by [{o_year :asc}]
                               [:project [o_year {mkt_share (/ brazil_revenue revenue)}]
                                [:group-by [o_year {brazil_revenue (sum brazil_volume)} {revenue (sum volume)}]
                                 [:project [{o_year (extract "YEAR" o_orderdate)}
                                            {brazil_volume (if (= nation "BRAZIL")
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
                                         [:scan [p_partkey {p_type (= p_type "ECONOMY ANODIZED STEEL")}]]
                                         [:scan [l_orderkey l_extendedprice l_discount l_suppkey l_partkey]]]
                                        [:scan [s_suppkey s_nationkey]]]
                                       [:scan [o_orderkey o_custkey
                                               {o_orderdate (and (>= o_orderdate #inst "1995-01-01")
                                                                 (<= o_orderdate #inst "1996-12-31"))}]]]
                                      [:scan [c_custkey c_nationkey]]]
                                     [:join {r_regionkey n1_n_regionkey}
                                      [:scan [r_regionkey {r_name (= r_name "AMERICA")}]]
                                      [:rename n1
                                       [:scan [n_name n_nationkey n_regionkey]]]]]
                                    [:rename n2
                                     [:scan [n_name n_nationkey]]]]]]]]])]
    (->> (tu/<-cursor res)
         (into [] (mapcat seq)))))

(defn tpch-q9-product-type-profit-measure []
  (with-open [res (c2/open-q *node* *watermark*
                             '[:order-by [{nation :asc}, {o_year :desc}]
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
                                   [:scan [n_name n_nationkey]]]]]]])]
    (->> (tu/<-cursor res)
         (into [] (mapcat seq)))))

(defn tpch-q10-returned-item-reporting []
  (with-open [res (c2/open-q *node* *watermark*
                             '[:slice {:limit 20}
                               [:order-by [{revenue :desc}]
                                [:group-by [c_custkey c_name c_acctbal c_phone n_name c_address c_comment
                                            {revenue (sum disc_price)}]
                                 [:project [c_custkey c_name c_acctbal c_phone n_name c_address c_comment
                                            {disc_price (* l_extendedprice (- 1 l_discount))}]
                                  [:join {c_nationkey n_nationkey}
                                   [:join {o_orderkey l_orderkey}
                                    [:join {c_custkey o_custkey}
                                     [:scan [c_custkey c_name c_acctbal c_address c_phone c_comment c_nationkey]]
                                     [:scan [o_orderkey o_custkey
                                             {o_orderdate (and (>= o_orderdate #inst "1993-10-01")
                                                               (< o_orderdate #inst "1994-01-01"))}]]]
                                    [:scan [l_orderkey {l_returnflag (= l_returnflag "R")} l_extendedprice l_discount]]]
                                   [:scan [n_nationkey n_name]]]]]]])]
    (->> (tu/<-cursor res)
         (into [] (mapcat seq)))))

(defn tpch-q12-shipping-modes-and-order-priority []
  (with-open [res (c2/open-q *node* *watermark*
                             '[:order-by [{l_shipmode :asc}]
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
                                           {l_shipmode (or (= l_shipmode "MAIL")
                                                           (= l_shipmode "SHIP"))}
                                           {l_receiptdate (and (>= l_receiptdate #inst "1994-01-01")
                                                               (< l_receiptdate #inst "1995-01-01"))}]]]]]]])]
    (->> (tu/<-cursor res)
         (into [] (mapcat seq)))))

(defn tpch-q13-customer-distribution []
  (with-open [res (c2/open-q *node* *watermark*
                             '[:order-by [{custdist :desc}, {c_count :desc}]
                               [:group-by [c_count {custdist (count c_custkey)}]
                                [:group-by [c_custkey {c_count (count-not-null o_comment)}]
                                 [:union
                                  [:project [c_custkey o_comment]
                                   [:join {c_custkey o_custkey}
                                    [:scan [c_custkey]]
                                    [:scan [{o_comment (not (like o_comment "%special%requests%"))} o_custkey]]]]
                                  [:cross-join
                                   [:anti-join {c_custkey o_custkey}
                                    [:scan [c_custkey]]
                                    [:scan [{o_comment (not (like o_comment "%special%requests%"))} o_custkey]]]
                                   [:table [{:o_comment nil}]]]]]]])]
    (->> (tu/<-cursor res)
         (into [] (mapcat seq)))))

(defn tpch-q14-promotion-effect []
  (with-open [res (c2/open-q *node* *watermark*
                             '[:project [{promo_revenue (* 100 (/ promo_revenue revenue))}]
                               [:group-by [{promo_revenue (sum promo_disc_price)}
                                           {revenue (sum disc_price)}]
                                [:project [{promo_disc_price (if (like p_type "PROMO%")
                                                               (* l_extendedprice (- 1 l_discount))
                                                               0.0)}
                                           {disc_price (* l_extendedprice (- 1 l_discount))}]
                                 [:join {p_partkey l_partkey}
                                  [:scan [p_partkey p_type]]
                                  [:scan [l_partkey l_extendedprice l_discount
                                          {l_shipdate (and (>= l_shipdate #inst "1995-09-01")
                                                           (< l_shipdate #inst "1995-10-01"))}]]]]]])]
    (->> (tu/<-cursor res)
         (into [] (mapcat seq)))))

(defn tpch-q19-discounted-revenue []
  (with-open [res (c2/open-q *node* *watermark*
                             '[:group-by [{revenue (sum disc_price)}]
                               [:project [{disc_price (* l_extendedprice (- 1 l_discount))}]
                                [:select (or (and (= p_brand "Brand#12")
                                                  (or (= p_container "SM CASE")
                                                      (= p_container "SM BOX")
                                                      (= p_container "SM PACK")
                                                      (= p_container "SM PKG"))
                                                  (>= l_quantity 1)
                                                  (<= l_quantity (+ 1 10))
                                                  (>= p_size 1)
                                                  (<= p_size 5))
                                             (and (= p_brand "Brand#23")
                                                  (or (= p_container "MED CASE")
                                                      (= p_container "MED BOX")
                                                      (= p_container "MED PACK")
                                                      (= p_container "MED PKG"))
                                                  (>= l_quantity 10)
                                                  (<= l_quantity (+ 10 10))
                                                  (>= p_size 1)
                                                  (<= p_size 10))
                                             (and (= p_brand "Brand#34")
                                                  (or (= p_container "LG CASE")
                                                      (= p_container "LG BOX")
                                                      (= p_container "LG PACK")
                                                      (= p_container "LG PKG"))
                                                  (>= l_quantity 20)
                                                  (<= l_quantity (+ 20 10))
                                                  (>= p_size 1)
                                                  (<= p_size 15)))
                                 [:join {p_partkey l_partkey}
                                  [:scan [p_partkey p_brand p_container p_size]]
                                  [:scan [l_partkey l_extendedprice l_discount l_quantity
                                          {l_shipmode (or (= l_shipmode "AIR") (= l_shipmode "AIR REG"))}
                                          {l_shipinstruct (= l_shipinstruct "DELIVER IN PERSON")}]]]]]])]
    (->> (tu/<-cursor res)
         (into [] (mapcat seq)))))
