(ns core2.tpch-queries
  (:require [clojure.test :as t]
            [core2.core :as c2]
            [core2.expression :as expr]
            [core2.metadata :as meta]
            [core2.operator :as op]
            [core2.operator.group-by :as group-by]
            [core2.operator.order-by :as order-by]
            [core2.operator.project :as project]
            [core2.select :as sel]
            [core2.tpch :as tpch]
            [core2.test-util :as tu]
            [core2.types :as types]
            [core2.util :as util])
  (:import java.time.Duration
           org.apache.arrow.vector.holders.NullableTimeStampMilliHolder
           org.apache.arrow.vector.util.Text))

(def ^:dynamic ^:private *node*)
(def ^:dynamic ^:private ^core2.operator.IOperatorFactory *op-factory*)
(def ^:dynamic ^:private *watermark*)

;; TPC-H queries without sub-queries: 12, 14
;; (slurp (io/resource (format "io/airlift/tpch/queries/q%d.sql" 12)))

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

(defn tpch-q1-pricing-summary-report []
  (let [shipdate-pred (sel/->vec-pred sel/pred<= (doto (NullableTimeStampMilliHolder.)
                                                   (-> .isSet (set! 1))
                                                   (-> .value (set! (.getTime #inst "1998-09-02")))))
        metadata-pred (meta/matching-chunk-pred "l_shipdate" shipdate-pred
                                                (types/->minor-type :timestampmilli))]
    (with-open [scan-cursor (.scan *op-factory* *watermark*
                                   ["l_returnflag" "l_linestatus" "l_shipdate"
                                    "l_quantity" "l_extendedprice" "l_discount" "l_tax"]
                                   metadata-pred
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
           (into [] (mapcat seq))))))

(defn tpch-q3-shipping-priority []
  (let [shipdate-pred (sel/->vec-pred sel/pred> (doto (NullableTimeStampMilliHolder.)
                                                  (-> .isSet (set! 1))
                                                  (-> .value (set! (.getTime #inst "1995-03-15")))))
        orderdate-pred (sel/->vec-pred sel/pred< (doto (NullableTimeStampMilliHolder.)
                                                   (-> .isSet (set! 1))
                                                   (-> .value (set! (.getTime #inst "1995-03-15")))))
        mktsegment-pred (sel/->str-pred sel/pred= "BUILDING")]
    (with-open [customer (.scan *op-factory* *watermark*
                                ["c_custkey" "c_mktsegment"]
                                (meta/matching-chunk-pred "c_mktsegment" mktsegment-pred
                                                          (types/->minor-type :varchar))
                                {"c_mktsegment"
                                 (expr/->expression-vector-selector '(= c_mktsegment "BUILDING"))}
                                nil nil)
                orders (.scan *op-factory* *watermark*
                              ["o_orderkey" "o_orderdate" "o_custkey" "o_shippriority"]
                              (meta/matching-chunk-pred "o_orderdate" orderdate-pred
                                                        (types/->minor-type :timestampmilli))
                              {"o_orderdate"
                               (expr/->expression-vector-selector '(< o_orderdate #inst "1995-03-15"))}
                              nil nil)
                lineitem (.scan *op-factory* *watermark*
                                ["l_orderkey" "l_shipdate" "l_extendedprice" "l_discount"]
                                (meta/matching-chunk-pred "l_shipdate" shipdate-pred
                                                          (types/->minor-type :timestampmilli))
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
           (into [] (mapcat seq))))))

(defn tpch-q5-local-supplier-volume []
  (let [orderdate-pred (sel/->vec-pred sel/pred< (doto (NullableTimeStampMilliHolder.)
                                                   (-> .isSet (set! 1))
                                                   (-> .value (set! (.getTime #inst "1995-01-01")))))
        region-name-pred (sel/->str-pred sel/pred= "ASIA")]
    (with-open [customer (.scan *op-factory* *watermark*
                                ["c_custkey" "c_nationkey"]
                                (constantly true) {}
                                nil nil)
                orders (.scan *op-factory* *watermark*
                              ["o_orderkey" "o_custkey" "o_orderdate"]
                              (meta/matching-chunk-pred "o_orderdate" orderdate-pred
                                                        (types/->minor-type :timestampmilli))
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
                              (meta/matching-chunk-pred "r_name" region-name-pred
                                                        (types/->minor-type :varchar))
                              {"r_name" (expr/->expression-vector-selector '(= r_name "ASIA"))}
                              nil nil)

                nation+region (.equiJoin *op-factory* region "r_regionkey" nation "n_regionkey")
                supplier+region+nation (.equiJoin *op-factory* nation+region "n_nationkey" supplier "s_nationkey")

                customers+supplier+region+nation (.equiJoin *op-factory* supplier+region+nation "n_nationkey" customer "c_nationkey")
                lineitem+customers+supplier+region+nation (.equiJoin *op-factory* customers+supplier+region+nation "s_suppkey" lineitem "l_suppkey")
                orders+lineitem+customers+supplier+region+nation (.equiJoin *op-factory*
                                                                            lineitem+customers+supplier+region+nation "l_orderkey"
                                                                            orders "o_orderkey")
                select-cursor (.select *op-factory* orders+lineitem+customers+supplier+region+nation
                                       (expr/->expression-root-selector '(= o_custkey c_custkey)))

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
           (into [] (mapcat seq))))))

(defn tpch-q6-forecasting-revenue-change []
  (let [shipdate-pred (sel/->vec-pred sel/pred< (doto (NullableTimeStampMilliHolder.)
                                                  (-> .isSet (set! 1))
                                                  (-> .value (set! (.getTime #inst "1995-01-01")))))]
    (with-open [lineitem (.scan *op-factory* *watermark*
                                ["l_shipdate" "l_extendedprice" "l_discount" "l_quantity"]
                                (meta/matching-chunk-pred "l_shipdate" shipdate-pred
                                                          (types/->minor-type :timestampmilli))
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
           (into [] (mapcat seq))))))

(defn tpch-q10-returned-item-reporting []
  (let [orderdate-pred (sel/->vec-pred sel/pred< (doto (NullableTimeStampMilliHolder.)
                                                   (-> .isSet (set! 1))
                                                   (-> .value (set! (.getTime #inst "1994-01-01")))))
        returnflag-pred (sel/->str-pred sel/pred= "R")]
    (with-open [customer (.scan *op-factory* *watermark*
                                ["c_custkey" "c_name" "c_acctbal" "c_address" "c_phone" "c_comment" "c_nationkey"]
                                (constantly true) {}
                                nil nil)
                orders (.scan *op-factory* *watermark*
                              ["o_orderkey" "o_orderdate" "o_custkey"]
                              (meta/matching-chunk-pred "o_orderdate" orderdate-pred
                                                        (types/->minor-type :timestampmilli))
                              {"o_orderdate"
                               (expr/->expression-vector-selector '(and (>= o_orderdate #inst "1993-10-01")
                                                                        (< o_orderdate #inst "1994-01-01")))}
                              nil nil)
                lineitem (.scan *op-factory* *watermark*
                                ["l_orderkey" "l_returnflag" "l_extendedprice" "l_discount"]
                                (meta/matching-chunk-pred "l_returnflag" returnflag-pred
                                                          (types/->minor-type :varchar))
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
           (into [] (mapcat seq))))))
