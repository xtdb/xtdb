(ns core2.operator.order-by-test
  (:require [clojure.test :as t]
            [core2.operator.order-by :as order-by]
            [core2.test-util :as tu]
            [core2.types :as ty])
  (:import org.apache.arrow.vector.types.pojo.Schema
           org.apache.arrow.vector.types.Types$MinorType))

(t/use-fixtures :each tu/with-allocator)

(t/deftest test-order-by
  (let [a-field (ty/->field "a" (.getType Types$MinorType/BIGINT) false)
        b-field (ty/->field "b" (.getType Types$MinorType/BIGINT) false)]
    (with-open [cursor (tu/->cursor (Schema. [a-field b-field])
                                    [[{:a 12, :b 10}
                                      {:a 0, :b 15}]
                                     [{:a 100, :b 83}]
                                     [{:a 83, :b 100}]])
                order-by-cursor (order-by/->order-by-cursor tu/*allocator*
                                                            cursor
                                                            [(order-by/->order-spec "a" :asc)]
                                                            {:out-block-size 3})]
      (t/is (= [[{:a 12, :b 10}
                 {:a 0, :b 15}
                 {:a 100, :b 83}]
                [{:a 83, :b 100}]]
               (tu/<-cursor order-by-cursor))))))
