(ns core2.tpch-queries
  (:require [clojure.test :as t]
            [core2.core :as c2]
            [core2.metadata :as meta]
            [core2.operator :as op]
            [core2.operator.group-by :as group-by]
            [core2.operator.order-by :as order-by]
            [core2.operator.project :as project]
            [core2.select :as sel]
            [core2.tpch-queries :as tpch-queries]
            [core2.test-util :as tu]
            [core2.types :as types]
            [core2.util :as util])
  (:import core2.operator.project.ProjectionSpec
           java.time.Duration
           [java.util.stream Collectors]
           [java.util DoubleSummaryStatistics LongSummaryStatistics]
           [java.util.function Function Supplier ObjDoubleConsumer ObjLongConsumer]
           org.apache.arrow.vector.Float8Vector
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
                                   {"l_shipdate" (sel/->dense-union-pred
                                                  shipdate-pred
                                                  (-> (types/primitive-type->arrow-type :timestampmilli)
                                                      types/arrow-type->type-id))}
                                   nil nil)

                project-cursor (.project *op-factory* scan-cursor
                                         [(project/->identity-projection-spec "l_returnflag")
                                          (project/->identity-projection-spec "l_linestatus")
                                          (project/->identity-projection-spec "l_quantity")
                                          (project/->identity-projection-spec "l_extendedprice")
                                          (project/->identity-projection-spec "l_discount")

                                          (reify ProjectionSpec
                                            ;; extendedprice * (1-l_discount) as disc_price
                                            (project [_ in-root allocator]
                                              (let [row-count (.getRowCount in-root)
                                                    disc-price-vec (doto (Float8Vector. "disc_price" allocator)
                                                                     (.allocateNew row-count))
                                                    extended-price-vec (.getVector in-root "l_extendedprice")
                                                    discount-vec (.getVector in-root "l_discount")]
                                                (dotimes [idx row-count]
                                                  (.set disc-price-vec idx
                                                        ^double (* (.getObject extended-price-vec idx)
                                                                   (- 1 (.getObject discount-vec idx)))))
                                                disc-price-vec)))

                                          (reify ProjectionSpec
                                            ;; extendedprice * (1-l_discount) * (1+l_tax) as charge
                                            (project [_ in-root allocator]
                                              (let [row-count (.getRowCount in-root)
                                                    charge-vec (doto (Float8Vector. "charge" allocator)
                                                                 (.allocateNew row-count))
                                                    extended-price-vec (.getVector in-root "l_extendedprice")
                                                    discount-vec (.getVector in-root "l_discount")
                                                    tax-vec (.getVector in-root "l_tax")]
                                                (dotimes [idx row-count]
                                                  (.set charge-vec idx
                                                        ^double (* (.getObject extended-price-vec idx)
                                                                   (- 1 (.getObject discount-vec idx))
                                                                   (+ 1 (.getObject tax-vec idx)))))
                                                charge-vec)))])

                group-by-cursor (.groupBy *op-factory*
                                          project-cursor
                                          (let [long-summary-supplier (reify Supplier
                                                                        (get [_]
                                                                          (LongSummaryStatistics.)))
                                                long-summary-accumulator (reify ObjLongConsumer
                                                                           (accept [_ acc x]
                                                                             (.accept ^LongSummaryStatistics acc x)))
                                                long-sum-finisher (reify Function
                                                                    (apply [_ acc]
                                                                      (.getSum ^LongSummaryStatistics acc)))
                                                long-avg-finisher (reify Function
                                                                    (apply [_ acc]
                                                                      (.getAverage ^LongSummaryStatistics acc)))
                                                double-summary-supplier (reify Supplier
                                                                          (get [_]
                                                                            (DoubleSummaryStatistics.)))
                                                double-summary-accumulator (reify ObjDoubleConsumer
                                                                             (accept [_ acc x]
                                                                               (.accept ^DoubleSummaryStatistics acc x)))
                                                double-sum-finisher (reify Function
                                                                      (apply [_ acc]
                                                                        (.getSum ^DoubleSummaryStatistics acc)))
                                                double-avg-finisher (reify Function
                                                                      (apply [_ acc]
                                                                        (.getAverage ^DoubleSummaryStatistics acc)))]
                                            [(group-by/->group-spec "l_returnflag")
                                             (group-by/->group-spec "l_linestatus")
                                             (group-by/->long-function-spec "l_quantity" "sum_qty"
                                                                            long-summary-supplier
                                                                            long-summary-accumulator
                                                                            long-sum-finisher)
                                             (group-by/->long-function-spec "l_extendedprice" "sum_base_price"
                                                                            long-summary-supplier
                                                                            long-summary-accumulator
                                                                            long-sum-finisher)
                                             (group-by/->double-function-spec "disc_price" "sum_disc_price"
                                                                              double-summary-supplier
                                                                              double-summary-accumulator
                                                                              double-sum-finisher)
                                             (group-by/->double-function-spec "charge" "sum_charge"
                                                                              double-summary-supplier
                                                                              double-summary-accumulator
                                                                              double-sum-finisher)
                                             (group-by/->long-function-spec "l_quantity" "avg_qty"
                                                                            long-summary-supplier
                                                                            long-summary-accumulator
                                                                            long-avg-finisher)
                                             (group-by/->long-function-spec "l_extendedprice" "avg_price"
                                                                            long-summary-supplier
                                                                            long-summary-accumulator
                                                                            long-avg-finisher)
                                             (group-by/->double-function-spec "l_discount" "avg_disc"
                                                                              double-summary-supplier
                                                                              double-summary-accumulator
                                                                              double-avg-finisher)
                                             (group-by/->function-spec "l_returnflag" "count_order"
                                                                       (Collectors/counting))]))
                order-by-cursor (.orderBy *op-factory*
                                          group-by-cursor
                                          [(order-by/->order-spec "l_returnflag" :asc)
                                           (order-by/->order-spec "l_linestatus" :asc)])]
      (->> (tu/<-cursor order-by-cursor)
           (into [] (mapcat seq))))))
