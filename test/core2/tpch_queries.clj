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
