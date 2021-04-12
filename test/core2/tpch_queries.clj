(ns core2.tpch-queries
  (:require [clojure.test :as t]
            [clojure.string :as str]
            [core2.core :as c2]
            [core2.expression :as expr]
            [core2.operator :as op]
            [core2.operator.group-by :as group-by]
            [core2.operator.order-by :as order-by]
            [core2.operator.project :as project]
            [core2.test-util :as tu]
            [core2.tpch :as tpch]
            [core2.util :as util])
  (:import [java.time Duration Instant ZoneOffset]
           java.time.temporal.ChronoField
           java.util.Date
           org.apache.arrow.vector.holders.NullableTimeStampMilliHolder
           org.apache.arrow.vector.util.Text))

(def ^:dynamic ^:private *node*)
(def ^:dynamic ^:private ^core2.operator.IOperatorFactory *op-factory*)
(def ^:dynamic ^:private *watermark*)

;; (slurp (io/resource (format "io/airlift/tpch/queries/q%d.sql" 1)))

(defn with-tpch-data [scale-factor test-name]
  (fn [f]
    (try
      (let [node-dir (util/->path (str "target/" test-name))]
        (util/delete-dir node-dir)

        (with-open [node (c2/->local-node node-dir)
                    tx-producer (c2/->local-tx-producer node-dir)]
          (let [last-tx (tpch/submit-docs! tx-producer scale-factor)]
            (c2/await-tx node last-tx (Duration/ofMinutes 2))

            (tu/finish-chunk node))

          (with-open [watermark (c2/open-watermark node)]
            (binding [*node* node
                      *watermark* watermark
                      *op-factory* (op/->operator-factory (.allocator node)
                                                          (.metadata-manager node)
                                                          (.temporal-manager node)
                                                          (.buffer-pool node))]
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
  (with-open [scan-cursor (.scan *op-factory* *watermark*
                                 ["l_returnflag" "l_linestatus" "l_shipdate"
                                  "l_quantity" "l_extendedprice" "l_discount" "l_tax"]
                                 (expr/->metadata-selector '(<= l_shipdate #inst "1998-09-02"))
                                 {"l_shipdate"
                                  (expr/->expression-vector-selector '(<= l_shipdate #inst "1998-09-02"))}
                                 nil nil)

              project-cursor (.project *op-factory* scan-cursor
                                       [(project/->identity-projection-spec "l_returnflag")
                                        (project/->identity-projection-spec "l_linestatus")
                                        (project/->identity-projection-spec "l_quantity")
                                        (project/->identity-projection-spec "l_extendedprice")
                                        (project/->identity-projection-spec "l_discount")
                                        (expr/->expression-projection-spec "disc_price"
                                                                           '(* l_extendedprice (- 1 l_discount)))
                                        (expr/->expression-projection-spec "charge"
                                                                           '(* (* l_extendedprice (- 1 l_discount))
                                                                               (+ 1 l_tax)))])

              group-by-cursor (.groupBy *op-factory*
                                        project-cursor
                                        [(group-by/->group-spec "l_returnflag")
                                         (group-by/->group-spec "l_linestatus")
                                         (group-by/->sum-long-spec "l_quantity" "sum_qty")
                                         (group-by/->sum-double-spec "l_extendedprice" "sum_base_price")
                                         (group-by/->sum-double-spec "disc_price" "sum_disc_price")
                                         (group-by/->sum-double-spec "charge" "sum_charge")
                                         (group-by/->avg-long-spec "l_quantity" "avg_qty")
                                         (group-by/->avg-double-spec "l_extendedprice" "avg_price")
                                         (group-by/->avg-double-spec "l_discount" "avg_disc")
                                         (group-by/->count-spec "l_returnflag" "count_order")])
              order-by-cursor (.orderBy *op-factory*
                                        group-by-cursor
                                        [(order-by/->order-spec "l_returnflag" :asc)
                                         (order-by/->order-spec "l_linestatus" :asc)])]
    (->> (tu/<-cursor order-by-cursor)
         (into [] (mapcat seq)))))

(defn tpch-q3-shipping-priority []
  (with-open [customer (.scan *op-factory* *watermark*
                              ["c_custkey" "c_mktsegment"]
                              (expr/->metadata-selector '(= c_mktsegment "BUILDING"))
                              {"c_mktsegment"
                               (expr/->expression-vector-selector '(= c_mktsegment "BUILDING"))}
                              nil nil)
              orders (.scan *op-factory* *watermark*
                            ["o_orderkey" "o_orderdate" "o_custkey" "o_shippriority"]
                            (expr/->metadata-selector '(< o_orderdate #inst "1995-03-15"))
                            {"o_orderdate"
                             (expr/->expression-vector-selector '(< o_orderdate #inst "1995-03-15"))}
                            nil nil)
              lineitem (.scan *op-factory* *watermark*
                              ["l_orderkey" "l_shipdate" "l_extendedprice" "l_discount"]
                              (expr/->metadata-selector '(> l_shipdate #inst "1995-03-15"))
                              {"l_shipdate"
                               (expr/->expression-vector-selector '(> l_shipdate #inst "1995-03-15"))}
                              nil nil)

              orders+customers (.equiJoin *op-factory* customer "c_custkey" orders "o_custkey")
              lineitem+customers+orders (.equiJoin *op-factory* orders+customers "o_orderkey" lineitem "l_orderkey")

              project-cursor (.project *op-factory* lineitem+customers+orders
                                       [(project/->identity-projection-spec "l_orderkey")
                                        (project/->identity-projection-spec "o_orderdate")
                                        (project/->identity-projection-spec "o_shippriority")
                                        (expr/->expression-projection-spec "disc_price"
                                                                           '(* l_extendedprice (- 1 l_discount)))])

              group-by-cursor (.groupBy *op-factory*
                                        project-cursor
                                        [(group-by/->group-spec "l_orderkey")
                                         (group-by/->sum-double-spec "disc_price" "revenue")
                                         (group-by/->group-spec "o_orderdate")
                                         (group-by/->group-spec "o_shippriority")])
              order-by-cursor (.orderBy *op-factory*
                                        group-by-cursor
                                        [(order-by/->order-spec "revenue" :desc)
                                         (order-by/->order-spec "o_orderdate" :asc)])
              limit-cursor (.slice *op-factory* order-by-cursor nil 10)]
    (->> (tu/<-cursor limit-cursor)
         (into [] (mapcat seq)))))

(defn tpch-q5-local-supplier-volume []
  (with-open [customer (.scan *op-factory* *watermark*
                              ["c_custkey" "c_nationkey"]
                              (constantly true) {}
                              nil nil)
              orders (.scan *op-factory* *watermark*
                            ["o_orderkey" "o_custkey" "o_orderdate"]
                            (expr/->metadata-selector '(and (>= o_orderdate #inst "1994-01-01")
                                                            (< o_orderdate #inst "1995-01-01")))
                            {"o_orderdate"
                             (expr/->expression-vector-selector '(and (>= o_orderdate #inst "1994-01-01")
                                                                      (< o_orderdate #inst "1995-01-01")))}
                            nil nil)
              lineitem (.scan *op-factory* *watermark*
                              ["l_orderkey" "l_extendedprice" "l_discount" "l_suppkey"]
                              (constantly true) {}
                              nil nil)

              supplier (.scan *op-factory* *watermark*
                              ["s_suppkey" "s_nationkey"]
                              (constantly true) {}
                              nil nil)
              nation (.scan *op-factory* *watermark*
                            ["n_name" "n_nationkey" "n_regionkey"]
                            (constantly true) {}
                            nil nil)
              region (.scan *op-factory* *watermark*
                            ["r_name" "r_regionkey"]
                            (expr/->metadata-selector '(= r_name "ASIA"))
                            {"r_name" (expr/->expression-vector-selector '(= r_name "ASIA"))}
                            nil nil)

              orders+customer (.equiJoin *op-factory* orders "o_custkey" customer "c_custkey")

              nation+region (.equiJoin *op-factory* region "r_regionkey" nation "n_regionkey")
              supplier+region+nation (.equiJoin *op-factory* nation+region "n_nationkey" supplier "s_nationkey")

              orders+customers+supplier+region+nation (.equiJoin *op-factory* supplier+region+nation "s_nationkey" orders+customer "c_nationkey")
              lineitem+orders+customers+supplier+region+nation (.equiJoin *op-factory* orders+customers+supplier+region+nation "o_orderkey" lineitem "l_orderkey")

              select-cursor (.select *op-factory* lineitem+orders+customers+supplier+region+nation
                                     (expr/->expression-root-selector '(= l_suppkey s_suppkey)))

              project-cursor (.project *op-factory* select-cursor
                                       [(project/->identity-projection-spec "n_name")
                                        (expr/->expression-projection-spec "disc_price"
                                                                           '(* l_extendedprice (- 1 l_discount)))])

              group-by-cursor (.groupBy *op-factory*
                                        project-cursor
                                        [(group-by/->group-spec "n_name")
                                         (group-by/->sum-double-spec "disc_price" "revenue")])
              order-by-cursor (.orderBy *op-factory*
                                        group-by-cursor
                                        [(order-by/->order-spec "revenue" :desc)])]
    (->> (tu/<-cursor order-by-cursor)
         (into [] (mapcat seq)))))

(defn tpch-q6-forecasting-revenue-change []
  (with-open [lineitem (.scan *op-factory* *watermark*
                              ["l_shipdate" "l_extendedprice" "l_discount" "l_quantity"]
                              (expr/->metadata-selector '(and (>= l_shipdate #inst "1994-01-01")
                                                              (< l_shipdate #inst "1995-01-01")
                                                              (>= l_discount 0.05)
                                                              (<= l_discount 0.07)
                                                              (< l_quantity 24.0)))
                              {"l_shipdate" (expr/->expression-vector-selector '(and (>= l_shipdate #inst "1994-01-01")
                                                                                     (< l_shipdate #inst "1995-01-01")))
                               "l_discount" (expr/->expression-vector-selector '(and (>= l_discount 0.05)
                                                                                     (<= l_discount 0.07)))
                               "l_quantity" (expr/->expression-vector-selector '(< l_quantity 24.0))}
                              nil nil)

              project-cursor (.project *op-factory* lineitem
                                       [(expr/->expression-projection-spec "disc_price"
                                                                           '(* l_extendedprice l_discount))])

              group-by-cursor (.groupBy *op-factory*
                                        project-cursor
                                        [(group-by/->sum-double-spec "disc_price" "revenue")])]
    (->> (tu/<-cursor group-by-cursor)
         (into [] (mapcat seq)))))

(defn tpch-q7-volume-shipping []
  (with-open [supplier (.scan *op-factory* *watermark*
                              ["s_suppkey" "s_nationkey"]
                              (constantly true) {}
                              nil nil)
              lineitem (.scan *op-factory* *watermark*
                              ["l_orderkey" "l_extendedprice" "l_discount" "l_suppkey" "l_shipdate"]
                              (expr/->metadata-selector '(and (>= l_shipdate #inst "1995-01-01")
                                                              (<= l_shipdate #inst "1996-12-31")))
                              {"l_shipdate" (expr/->expression-vector-selector '(and (>= l_shipdate #inst "1995-01-01")
                                                                                     (<= l_shipdate #inst "1996-12-31")))}
                              nil nil)

              orders (.scan *op-factory* *watermark*
                            ["o_orderkey" "o_custkey"]
                            (constantly true) {}
                            nil nil)

              customer (.scan *op-factory* *watermark*
                              ["c_custkey" "c_nationkey"]
                              (constantly true) {}
                              nil nil)

              n1 (.scan *op-factory* *watermark*
                        ["n_name" "n_nationkey"]
                        (constantly true)
                        {"n_name" (expr/->expression-vector-selector '(or (= n_name "GERMANY")
                                                                          (= n_name "FRANCE")))}
                        nil nil)
              n1 (.rename *op-factory* n1 {"n_name" "n1_n_name"
                                           "n_nationkey" "n1_n_nationkey"})
              n2 (.scan *op-factory* *watermark*
                        ["n_name" "n_nationkey"]
                        (constantly true)
                        {"n_name" (expr/->expression-vector-selector '(or (= n_name "GERMANY")
                                                                          (= n_name "FRANCE")))}
                        nil nil)
              n2 (.rename *op-factory* n2 {"n_name" "n2_n_name"
                                           "n_nationkey" "n2_n_nationkey"})

              supplier+lineitem (.equiJoin *op-factory* supplier "s_suppkey" lineitem "l_suppkey")
              orders+supplier+lineitem (.equiJoin *op-factory* supplier+lineitem "l_orderkey" orders "o_orderkey")
              n1+orders+supplier+lineitem (.equiJoin *op-factory* orders+supplier+lineitem "s_nationkey" n1 "n1_n_nationkey")
              customer+n1+orders+supplier+lineitem (.equiJoin *op-factory* n1+orders+supplier+lineitem "o_custkey" customer "c_custkey")
              n2+customer+n1+orders+supplier+lineitem (.equiJoin *op-factory* customer+n1+orders+supplier+lineitem "c_nationkey" n2 "n2_n_nationkey")

              n2+customer+n1+orders+supplier+lineitem (.select *op-factory* n2+customer+n1+orders+supplier+lineitem
                                                               (expr/->expression-root-selector '(or (and (= n1_n_name "FRANCE")
                                                                                                          (= n2_n_name "GERMANY"))
                                                                                                     (and (= n1_n_name "GERMANY")
                                                                                                          (= n2_n_name "FRANCE")))))

              rename-cursor (.rename *op-factory* n2+customer+n1+orders+supplier+lineitem
                                     {"n1_n_name" "supp_nation"
                                      "n2_n_name" "cust_nation"})
              project-cursor (.project *op-factory* rename-cursor
                                       [(project/->identity-projection-spec "supp_nation")
                                        (project/->identity-projection-spec "cust_nation")
                                        (expr/->expression-projection-spec "l_year"
                                                                           '(extract "YEAR" l_shipdate))
                                        (expr/->expression-projection-spec "volume"
                                                                           '(* l_extendedprice (- 1 l_discount)))])

              group-by-cursor (.groupBy *op-factory*
                                        project-cursor
                                        [(group-by/->group-spec "supp_nation")
                                         (group-by/->group-spec "cust_nation")
                                         (group-by/->group-spec "l_year")
                                         (group-by/->sum-double-spec "volume" "revenue")])
              order-by-cursor (.orderBy *op-factory*
                                        group-by-cursor
                                        [(order-by/->order-spec "supp_nation" :asc)
                                         (order-by/->order-spec "cust_nation" :asc)
                                         (order-by/->order-spec "l_year" :asc)])]
    (->> (tu/<-cursor order-by-cursor)
         (into [] (mapcat seq)))))

(defn tpch-q8-national-market-share []
  (with-open [part (.scan *op-factory* *watermark*
                          ["p_partkey" "p_type"]
                          (expr/->metadata-selector '(= p_type "ECONOMY ANODIZED STEEL"))
                          {"p_type" (expr/->expression-vector-selector '(= p_type "ECONOMY ANODIZED STEEL"))}
                          nil nil)
              supplier (.scan *op-factory* *watermark*
                              ["s_suppkey" "s_nationkey"]
                              (constantly true) {}
                              nil nil)
              lineitem (.scan *op-factory* *watermark*
                              ["l_orderkey" "l_extendedprice" "l_discount" "l_suppkey" "l_partkey"]
                              (constantly true) {}
                              nil nil)

              orders (.scan *op-factory* *watermark*
                            ["o_orderkey" "o_custkey" "o_orderdate"]
                            (expr/->metadata-selector '(and (>= o_orderdate #inst "1995-01-01")
                                                            (<= o_orderdate #inst "1996-12-31")))
                            {"o_orderdate" (expr/->expression-vector-selector '(and (>= o_orderdate #inst "1995-01-01")
                                                                                    (<= o_orderdate #inst "1996-12-31")))}
                            nil nil)

              customer (.scan *op-factory* *watermark*
                              ["c_custkey" "c_nationkey"]
                              (constantly true) {}
                              nil nil)

              n1 (.scan *op-factory* *watermark*
                        ["n_name" "n_nationkey" "n_regionkey"]
                        (constantly true) {}
                        nil nil)
              n1 (.rename *op-factory* n1 {"n_name" "n1_n_name"
                                           "n_nationkey" "n1_n_nationkey"
                                           "n_regionkey" "n1_n_regionkey"})
              n2 (.scan *op-factory* *watermark*
                        ["n_name" "n_nationkey"]
                        (constantly true) {}
                        nil nil)
              n2 (.rename *op-factory* n2 {"n_name" "n2_n_name"
                                           "n_nationkey" "n2_n_nationkey"})

              region (.scan *op-factory* *watermark*
                            ["r_regionkey" "r_name"]
                            (expr/->metadata-selector '(= r_name "AMERICA"))
                            {"r_name" (expr/->expression-vector-selector '(= r_name "AMERICA"))}
                            nil nil)

              part+lineitem (.equiJoin *op-factory* part "p_partkey" lineitem "l_partkey")
              supplier+part+lineitem (.equiJoin *op-factory* part+lineitem "l_suppkey" supplier "s_suppkey")
              orders+supplier+part+lineitem (.equiJoin *op-factory* supplier+part+lineitem "l_orderkey" orders "o_orderkey")
              customer+orders+supplier+part+lineitem (.equiJoin *op-factory* orders+supplier+part+lineitem "o_custkey" customer "c_custkey")
              region+n1 (.equiJoin *op-factory* region "r_regionkey" n1 "n1_n_regionkey")
              region+n1+customer+orders+supplier+part+lineitem (.equiJoin *op-factory* customer+orders+supplier+part+lineitem "c_nationkey" region+n1 "n1_n_nationkey")
              n2+region+n1+customer+orders+supplier+part+lineitem (.equiJoin *op-factory* region+n1+customer+orders+supplier+part+lineitem "s_nationkey" n2 "n2_n_nationkey")

              rename-cursor (.rename *op-factory* n2+region+n1+customer+orders+supplier+part+lineitem
                                     {"n2_n_name" "nation"})
              project-cursor (.project *op-factory* rename-cursor
                                       [(expr/->expression-projection-spec "o_year"
                                                                           '(extract "YEAR" o_orderdate))
                                        (expr/->expression-projection-spec "brazil_volume"
                                                                           '(if (= nation "BRAZIL")
                                                                              (* l_extendedprice (- 1 l_discount))
                                                                              0.0))
                                        (expr/->expression-projection-spec "volume"
                                                                           '(* l_extendedprice (- 1 l_discount)))
                                        (project/->identity-projection-spec "nation")])

              group-by-cursor (.groupBy *op-factory*
                                        project-cursor
                                        [(group-by/->group-spec "o_year")
                                         (group-by/->sum-double-spec "brazil_volume" "brazil_revenue")
                                         (group-by/->sum-double-spec "volume" "revenue")])
              project-cursor (.project *op-factory* group-by-cursor
                                       [(project/->identity-projection-spec "o_year")
                                        (expr/->expression-projection-spec "mkt_share"
                                                                           '(/ brazil_revenue revenue))])
              order-by-cursor (.orderBy *op-factory*
                                        project-cursor
                                        [(order-by/->order-spec "o_year" :asc)])]
    (->> (tu/<-cursor order-by-cursor)
         (into [] (mapcat seq)))))

(defn tpch-q9-product-type-profit-measure []
  (with-open [part (.scan *op-factory* *watermark*
                          ["p_partkey" "p_name"]
                          (constantly true) {"p_name" (expr/->expression-vector-selector '(like p_name "%green%"))}
                          nil nil)

              supplier (.scan *op-factory* *watermark*
                              ["s_suppkey" "s_nationkey"]
                              (constantly true) {}
                              nil nil)
              lineitem (.scan *op-factory* *watermark*
                              ["l_orderkey" "l_extendedprice" "l_discount" "l_suppkey" "l_partkey" "l_quantity"]
                              (constantly true) {}
                              nil nil)

              partsupp (.scan *op-factory* *watermark*
                          ["ps_partkey" "ps_suppkey" "ps_supplycost"]
                          (constantly true) {}
                          nil nil)

              orders (.scan *op-factory* *watermark*
                            ["o_orderkey" "o_orderdate"]
                            (constantly true) {}
                            nil nil)

              nation (.scan *op-factory* *watermark*
                            ["n_name" "n_nationkey"]
                            (constantly true) {}
                            nil nil)

              lineitem+part (.equiJoin *op-factory* part "p_partkey" lineitem "l_partkey")
              partsupp+lineitem+part (.equiJoin *op-factory* lineitem+part "l_partkey" partsupp "ps_partkey")
              partsupp+lineitem+part (.select *op-factory* partsupp+lineitem+part (expr/->expression-root-selector '(= ps_suppkey l_suppkey)))
              supplier+partsupp+lineitem+part (.equiJoin *op-factory* partsupp+lineitem+part "l_suppkey" supplier "s_suppkey")
              orders+supplier+partsupp+lineitem+part (.equiJoin *op-factory* supplier+partsupp+lineitem+part "l_orderkey" orders "o_orderkey")
              nation+orders+supplier+partsupp+lineitem+part (.equiJoin *op-factory* orders+supplier+partsupp+lineitem+part "s_nationkey" nation "n_nationkey")

              project-cursor (.project *op-factory* nation+orders+supplier+partsupp+lineitem+part
                                       [(project/->identity-projection-spec "n_name")
                                        (expr/->expression-projection-spec "o_year"
                                                                           '(extract "YEAR" o_orderdate))
                                        (expr/->expression-projection-spec "amount"
                                                                           '(- (* l_extendedprice (- 1 l_discount))
                                                                               (* ps_supplycost l_quantity)))])
              rename-cursor (.rename *op-factory* project-cursor {"n_name" "nation"})

              group-by-cursor (.groupBy *op-factory*
                                        rename-cursor
                                        [(group-by/->group-spec "nation")
                                         (group-by/->group-spec "o_year")
                                         (group-by/->sum-double-spec "amount" "sum_profit")])
              order-by-cursor (.orderBy *op-factory*
                                        group-by-cursor
                                        [(order-by/->order-spec "nation" :asc)
                                         (order-by/->order-spec "o_year" :desc)])]
    (->> (tu/<-cursor order-by-cursor)
         (into [] (mapcat seq)))))

(defn tpch-q10-returned-item-reporting []
  (with-open [customer (.scan *op-factory* *watermark*
                              ["c_custkey" "c_name" "c_acctbal" "c_address" "c_phone" "c_comment" "c_nationkey"]
                              (constantly true) {}
                              nil nil)
              orders (.scan *op-factory* *watermark*
                            ["o_orderkey" "o_orderdate" "o_custkey"]
                            (expr/->metadata-selector '(and (>= o_orderdate #inst "1993-10-01")
                                                            (< o_orderdate #inst "1994-01-01")))
                            {"o_orderdate"
                             (expr/->expression-vector-selector '(and (>= o_orderdate #inst "1993-10-01")
                                                                      (< o_orderdate #inst "1994-01-01")))}
                            nil nil)
              lineitem (.scan *op-factory* *watermark*
                              ["l_orderkey" "l_returnflag" "l_extendedprice" "l_discount"]
                              (expr/->metadata-selector '(= l_returnflag "R"))
                              {"l_returnflag"
                               (expr/->expression-vector-selector '(= l_returnflag "R"))}
                              nil nil)
              nation (.scan *op-factory* *watermark*
                            ["n_nationkey" "n_name"]
                            (constantly true) {}
                            nil nil)

              orders+customers (.equiJoin *op-factory* customer "c_custkey" orders "o_custkey")
              lineitem+customers+orders (.equiJoin *op-factory* orders+customers "o_orderkey" lineitem "l_orderkey")
              nation+lineitem+customers+orders (.equiJoin *op-factory* lineitem+customers+orders "c_nationkey" nation "n_nationkey")

              project-cursor (.project *op-factory* nation+lineitem+customers+orders
                                       [(project/->identity-projection-spec "c_custkey")
                                        (project/->identity-projection-spec "c_name")
                                        (expr/->expression-projection-spec "disc_price"
                                                                           '(* l_extendedprice (- 1 l_discount)))
                                        (project/->identity-projection-spec "c_acctbal")
                                        (project/->identity-projection-spec "c_phone")
                                        (project/->identity-projection-spec "n_name")
                                        (project/->identity-projection-spec "c_address")
                                        (project/->identity-projection-spec "c_comment")])

              group-by-cursor (.groupBy *op-factory*
                                        project-cursor
                                        [(group-by/->group-spec "c_custkey")
                                         (group-by/->group-spec "c_name")
                                         (group-by/->sum-double-spec "disc_price" "revenue")
                                         (group-by/->group-spec "c_acctbal")
                                         (group-by/->group-spec "c_phone")
                                         (group-by/->group-spec "n_name")
                                         (group-by/->group-spec "c_address")
                                         (group-by/->group-spec "c_comment")])
              order-by-cursor (.orderBy *op-factory*
                                        group-by-cursor
                                        [(order-by/->order-spec "revenue" :desc)])
              limit-cursor (.slice *op-factory* order-by-cursor nil 20)]
    (->> (tu/<-cursor limit-cursor)
         (into [] (mapcat seq)))))

(defn tpch-q12-shipping-modes-and-order-priority []
  (with-open [orders (.scan *op-factory* *watermark*
                            ["o_orderkey" "o_orderpriority"]
                            (constantly true) {}
                            nil nil)
              lineitem (.scan *op-factory* *watermark*
                              ["l_orderkey" "l_shipmode" "l_commitdate" "l_shipdate" "l_receiptdate"]
                              (expr/->metadata-selector '(and (>= l_receiptdate #inst "1994-01-01")
                                                              (< l_receiptdate #inst "1995-01-01")
                                                              (or (= l_shipmode "MAIL")
                                                                  (= l_shipmode "SHIP"))))
                              {"l_receiptdate" (expr/->expression-vector-selector '(and (>= l_receiptdate #inst "1994-01-01")
                                                                                        (< l_receiptdate #inst "1995-01-01")))
                               "l_shipmode" (expr/->expression-vector-selector '(or (= l_shipmode "MAIL")
                                                                                    (= l_shipmode "SHIP")))}
                              nil nil)
              lineitem (.select *op-factory* lineitem (expr/->expression-root-selector '(and (< l_commitdate l_receiptdate)
                                                                                             (< l_shipdate l_commitdate))))
              lineitem+orders (.equiJoin *op-factory* orders "o_orderkey" lineitem "l_orderkey")

              project-cursor (.project *op-factory* lineitem+orders
                                       [(project/->identity-projection-spec "l_shipmode")
                                        (expr/->expression-projection-spec "high_line"
                                                                           '(if (or (= o_orderpriority "1-URGENT")
                                                                                    (= o_orderpriority "2-HIGH"))
                                                                              1
                                                                              0))
                                        (expr/->expression-projection-spec "low_line"
                                                                           '(if (and (!= o_orderpriority "1-URGENT")
                                                                                     (!= o_orderpriority "2-HIGH"))
                                                                              1
                                                                              0))])

              group-by-cursor (.groupBy *op-factory*
                                        project-cursor
                                        [(group-by/->group-spec "l_shipmode")
                                         (group-by/->sum-long-spec "high_line" "high_line_count")
                                         (group-by/->sum-long-spec "low_line" "low_line_count")])
              order-by-cursor (.orderBy *op-factory*
                                        group-by-cursor
                                        [(order-by/->order-spec "l_shipmode" :asc)])]
    (->> (tu/<-cursor order-by-cursor)
         (into [] (mapcat seq)))))

;; TODO: should behave as a left outer join and return customers
;; without orders as well.
(defn tpch-q13-customer-distribution []
  (with-open [customer (.scan *op-factory* *watermark*
                              ["c_custkey"]
                              (constantly true) {}
                              nil nil)
              orders (.scan *op-factory* *watermark*
                            ["o_orderkey" "o_comment" "o_custkey"]
                            (constantly true)
                            {"o_comment"
                             (expr/->expression-vector-selector '(not (like c_comment "%special%requests%")))}
                            nil nil)
              orders+customer (.equiJoin *op-factory* customer "c_custkey" orders "o_custkey")

              group-by-cursor (.groupBy *op-factory*
                                        orders+customer
                                        [(group-by/->group-spec "c_custkey")
                                         (group-by/->count-spec "o_comment" "c_count")])
              group-by-cursor (.groupBy *op-factory*
                                        group-by-cursor
                                        [(group-by/->group-spec "c_count")
                                         (group-by/->count-spec "c_custkey" "custdist")])
              order-by-cursor (.orderBy *op-factory*
                                        group-by-cursor
                                        [(order-by/->order-spec "custdist" :desc)
                                         (order-by/->order-spec "c_count" :desc)])]
    (->> (tu/<-cursor order-by-cursor)
         (into [] (mapcat seq)))))

(defn tpch-q14-promotion-effect []
  (with-open [part (.scan *op-factory* *watermark*
                          ["p_partkey" "p_type"]
                          (constantly true) {}
                          nil nil)
              lineitem (.scan *op-factory* *watermark*
                              ["l_partkey" "l_extendedprice" "l_discount" "l_shipdate"]
                              (expr/->metadata-selector '(and (>= l_shipdate #inst "1995-09-01")
                                                              (< l_shipdate #inst "1995-10-01")))
                              {"l_shipdate" (expr/->expression-vector-selector '(and (>= l_shipdate #inst "1995-09-01")
                                                                                     (< l_shipdate #inst "1995-10-01")))}
                              nil nil)
              lineitem+parts (.equiJoin *op-factory* part "p_partkey" lineitem "l_partkey")

              project-cursor (.project *op-factory* lineitem+parts
                                       [(expr/->expression-projection-spec "promo_disc_price"
                                                                           '(if (like p_type "PROMO%")
                                                                              (* l_extendedprice (- 1 l_discount))
                                                                              0.0))
                                        (expr/->expression-projection-spec "disc_price"
                                                                           '(* l_extendedprice (- 1 l_discount)))])

              group-by-cursor (.groupBy *op-factory*
                                        project-cursor
                                        [(group-by/->sum-double-spec "promo_disc_price" "promo_revenue")
                                         (group-by/->sum-double-spec "disc_price" "revenue")])
              project-cursor (.project *op-factory* group-by-cursor [(expr/->expression-projection-spec "promo_revenue"
                                                                                                        '(* 100 (/ promo_revenue revenue)))])]
    (->> (tu/<-cursor project-cursor)
         (into [] (mapcat seq)))))

(defn tpch-q19-discounted-revenue []
  (with-open [part (.scan *op-factory* *watermark*
                          ["p_partkey" "p_brand" "p_container" "p_size"]
                          (constantly true) {}
                          nil nil)
              lineitem (.scan *op-factory* *watermark*
                              ["l_partkey" "l_extendedprice" "l_discount" "l_quantity" "l_shipmode" "l_shipinstruct"]
                              (expr/->metadata-selector '(and (or (= l_shipmode "AIR")
                                                                  (= l_shipmode "AIR REG"))
                                                              (= l_shipinstruct "DELIVER IN PERSON")))
                              {"l_shipmode" (expr/->expression-vector-selector '(or (= l_shipmode "AIR")
                                                                                    (= l_shipmode "AIR REG")))
                               "l_shipinstruct" (expr/->expression-vector-selector '(= l_shipinstruct "DELIVER IN PERSON"))}
                              nil nil)

              lineitem+parts (.equiJoin *op-factory* part "p_partkey" lineitem "l_partkey")
              select-cursor (.select *op-factory* lineitem+parts (expr/->expression-root-selector '(or (and (= p_brand "Brand#12")
                                                                                                            (and (or (= p_container "SM CASE")
                                                                                                                     (or (= p_container "SM BOX")
                                                                                                                         (or (= p_container "SM PACK")
                                                                                                                             (= p_container "SM PKG"))))
                                                                                                                 (and (>= l_quantity 1)
                                                                                                                      (and (<= l_quantity (+ 1 10))
                                                                                                                           (and (>= p_size 1)
                                                                                                                                (<= p_size 5))))))
                                                                                                       (or (and (= p_brand "Brand#23")
                                                                                                                (and (or (= p_container "MED CASE")
                                                                                                                         (or (= p_container "MED BOX")
                                                                                                                             (or (= p_container "MED PACK")
                                                                                                                                 (= p_container "MED PKG"))))
                                                                                                                     (and (>= l_quantity 10)
                                                                                                                          (and (<= l_quantity (+ 10 10))
                                                                                                                               (and (>= p_size 1)
                                                                                                                                    (<= p_size 10))))))
                                                                                                           (and (= p_brand "Brand#34")
                                                                                                                (and (or (= p_container "LG CASE")
                                                                                                                         (or (= p_container "LG BOX")
                                                                                                                             (or (= p_container "LG PACK")
                                                                                                                                 (= p_container "LG PKG"))))
                                                                                                                     (and (>= l_quantity 20)
                                                                                                                          (and (<= l_quantity (+ 20 10))
                                                                                                                               (and (>= p_size 1)
                                                                                                                                    (<= p_size 15))))))))))

              project-cursor (.project *op-factory* select-cursor
                                       [(expr/->expression-projection-spec "disc_price"
                                                                           '(* l_extendedprice (- 1 l_discount)))])

              group-by-cursor (.groupBy *op-factory*
                                        project-cursor
                                        [(group-by/->sum-double-spec "disc_price" "revenue")])]
    (->> (tu/<-cursor group-by-cursor)
         (into [] (mapcat seq)))))
