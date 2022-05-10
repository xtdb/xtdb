(ns core2.operator.order-by-test
  (:require [clojure.test :as t]
            [core2.operator :as op]
            [core2.operator.order-by :as order-by]
            [core2.test-util :as tu]
            [core2.types :as ty])
  (:import org.apache.arrow.vector.types.pojo.Schema))

(t/use-fixtures :each tu/with-allocator)

(t/deftest test-order-by
  (let [a-field (ty/->field "a" ty/bigint-type false)
        b-field (ty/->field "b" ty/bigint-type false)]
    (with-open [cursor (tu/->cursor (Schema. [a-field b-field])
                                    [[{:a 12, :b 10}
                                      {:a 0, :b 15}]
                                     [{:a 100, :b 83}]
                                     [{:a 83, :b 100}]])
                order-by-cursor (order-by/->order-by-cursor tu/*allocator*
                                                            cursor
                                                            [{:col-name "a", :direction :asc, :null-ordering :nulls-last}])]
      (t/is (= [[{:a 0, :b 15}
                 {:a 12, :b 10}
                 {:a 83, :b 100}
                 {:a 100, :b 83}]]
               (tu/<-cursor order-by-cursor)))))

  (t/is (= [{:a 0, :b 15}
            {:a 12.4, :b 10}
            {:a 83.0, :b 100}
            {:a 100, :b 83}]
           (op/query-ra '[:order-by [[a :asc]]
                          [:table [{:a 12.4, :b 10}
                                   {:a 0, :b 15}
                                   {:a 100, :b 83}
                                   {:a 83.0, :b 100}]]]
                        {}))
        "mixed numeric types")

  (let [table-with-nil [{:a 12.4, :b 10}, {:a nil, :b 15}, {:a 100, :b 83}, {:a 83.0, :b 100}]]
    (t/is (= [{:a nil, :b 15}, {:a 12.4, :b 10}, {:a 83.0, :b 100}, {:a 100, :b 83}]
             (op/query-ra '[:order-by [[a :nulls-first]]
                            [:table $table]]
                          {'$table table-with-nil}))
          "nulls first")

    (t/is (= [{:a 12.4, :b 10}, {:a 83.0, :b 100}, {:a 100, :b 83}, {:a nil, :b 15}]
             (op/query-ra '[:order-by [[a :nulls-last]]
                            [:table $table]]
                          {'$table table-with-nil}))
          "nulls last")

    (t/is (= [{:a 12.4, :b 10}, {:a 83.0, :b 100}, {:a 100, :b 83}, {:a nil, :b 15}]
             (op/query-ra '[:order-by [[a]]
                            [:table $table]]
                          {'$table table-with-nil}))
          "default nulls last")))
