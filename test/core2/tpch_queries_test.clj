(ns core2.tpch-queries-test
  (:require [clojure.test :as t]
            [core2.core :as c2]
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
  (:import core2.operator.project.ProjectionSpec
           java.time.Duration
           java.util.stream.Collectors
           [java.util.function ToDoubleFunction ToLongFunction]
           org.apache.arrow.vector.Float8Vector
           org.apache.arrow.vector.holders.NullableTimeStampMilliHolder
           org.apache.arrow.vector.util.Text))

(def ^:dynamic ^:private *node*)
(def ^:dynamic ^:private ^core2.operator.IOperatorFactory *op-factory*)
(def ^:dynamic ^:private *watermark*)

(t/use-fixtures :once
  (fn [f]
    (try
      (let [node-dir (util/->path "target/tpch-queries")]
        (util/delete-dir node-dir)


        (with-open [node (c2/->local-node node-dir)
                    tx-producer (c2/->local-tx-producer node-dir)]
          (let [last-tx (tpch/submit-docs! tx-producer 0.001)]
            (c2/await-tx node last-tx (Duration/ofSeconds 10))

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

(t/deftest test-q1-pricing-summary-report
  (let [shipdate-pred (sel/->vec-pred sel/pred<= (doto (NullableTimeStampMilliHolder.)
                                                   (-> .isSet (set! 1))
                                                   (-> .value (set! (.getTime #inst "1998-09-01")))))
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
                                          (let [sum-long-collector (Collectors/summingLong
                                                                    (reify ToLongFunction
                                                                      (applyAsLong [_ x] x)))
                                                sum-double-collector (Collectors/summingDouble
                                                                      (reify ToDoubleFunction
                                                                        (applyAsDouble [_ x] x)))
                                                avg-long-collector (Collectors/averagingLong
                                                                    (reify ToLongFunction
                                                                      (applyAsLong [_ x] x)))
                                                avg-double-collector (Collectors/averagingDouble
                                                                      (reify ToDoubleFunction
                                                                        (applyAsDouble [_ x] x)))]
                                            [(group-by/->group-spec "l_returnflag")
                                             (group-by/->group-spec "l_linestatus")
                                             (group-by/->function-spec "l_quantity" "sum_qty"
                                                                       sum-long-collector)
                                             (group-by/->function-spec "l_extendedprice" "sum_base_price"
                                                                       sum-long-collector)
                                             (group-by/->function-spec "disc_price" "sum_disc_price"
                                                                       sum-double-collector)
                                             (group-by/->function-spec "charge" "sum_charge"
                                                                       sum-double-collector)
                                             (group-by/->function-spec "l_quantity" "avg_qty"
                                                                       avg-long-collector)
                                             (group-by/->function-spec "l_extendedprice" "avg_price"
                                                                       avg-long-collector)
                                             (group-by/->function-spec "l_discount" "avg_disc"
                                                                       avg-double-collector)
                                             (group-by/->function-spec "l_returnflag" "count_order"
                                                                       (Collectors/counting))]))
                order-by-cursor (.orderBy *op-factory*
                                          group-by-cursor
                                          [(order-by/->order-spec "l_returnflag" :asc)
                                           (order-by/->order-spec "l_linestatus" :asc)])]
      (t/is (= [{:l_returnflag (Text. "A")
                 :l_linestatus (Text. "F")
                 :sum_qty 37474
                 :sum_base_price 37568959
                 :sum_disc_price 3.5676192097E7
                 :sum_charge 3.7101416222424E7
                 :avg_qty 25.354533152909337
                 :avg_price 25418.78146143437
                 :avg_disc 0.0508660351826793
                 :count_order 1478}
                {:l_returnflag (Text. "N")
                 :l_linestatus (Text. "F")
                 :sum_qty 1041
                 :sum_base_price 1041285
                 :sum_disc_price 999060.898
                 :sum_charge 1036450.8022800001
                 :avg_qty 27.394736842105264,
                 :avg_price 27402.236842105263,
                 :avg_disc 0.04289473684210526,
                 :count_order 38}
                {:l_returnflag (Text. "N")
                 :l_linestatus (Text. "O")
                 :sum_qty 75163
                 :sum_base_price 75378782
                 :sum_disc_price 7.16486117214E7
                 :sum_charge 7.4494106913613E7
                 :avg_qty 25.565646258503403,
                 :avg_price 25639.04149659864,
                 :avg_disc 0.04969387755102041,
                 :count_order 2940}
                {:l_returnflag (Text. "R")
                 :l_linestatus (Text. "F")
                 :sum_qty 36511
                 :sum_base_price 36570197
                 :sum_disc_price 3.47384728758E7
                 :sum_charge 3.6169060112193E7
                 :avg_qty 25.059025394646532,
                 :avg_price 25099.654770075496,
                 :avg_disc 0.05002745367192862,
                 :count_order 1457}]
               (->> (tu/<-cursor order-by-cursor)
                    (into [] (mapcat seq))))))))
