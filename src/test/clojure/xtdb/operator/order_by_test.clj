(ns xtdb.operator.order-by-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.node :as xtn]
            [xtdb.operator.order-by :as order-by]
            [xtdb.test-util :as tu])
  (:import (java.time Instant Duration)))

(t/use-fixtures :each tu/with-allocator)

(t/deftest test-order-by
  (t/is (= [[{:a 0, :b 15}
             {:a 12, :b 10}
             {:a 83, :b 100}
             {:a 100, :b 83}]]
           (tu/query-ra [:order-by {:order-specs '[[a]]}
                         [::tu/pages
                          [[{:a 12, :b 10}
                            {:a 0, :b 15}]
                           [{:a 100, :b 83}]
                           [{:a 83, :b 100}]]]]
                        {:preserve-pages? true})))

  (t/is (= [{:a 0, :b 15}
            {:a 12.4, :b 10}
            {:a 83.0, :b 100}
            {:a 100, :b 83}]
           (tu/query-ra '[:order-by {:order-specs [[a]]}
                          [:table {:rows [{:a 12.4, :b 10}
                                          {:a 0, :b 15}
                                          {:a 100, :b 83}
                                          {:a 83.0, :b 100}]}]]
                        {}))
        "mixed numeric types")

  (t/is (= []
           (tu/query-ra '[:order-by {:order-specs [[a]]}
                          [::tu/pages
                           [[] []]]]
                        {}))
        "empty batches"))

(t/deftest test-order-by-with-nulls
  (let [table-with-nil [{:a 12.4, :b 10}, {:a nil, :b 15}, {:a 100, :b 83}, {:a 83.0, :b 100}]]
    (t/is (= [{:b 15}, {:a 12.4, :b 10}, {:a 83.0, :b 100}, {:a 100, :b 83}]
             (tu/query-ra '[:order-by {:order-specs [[a {:null-ordering :nulls-first}]]}
                          [:table {:param ?table}]]
                          {:args {:table table-with-nil}}))
          "nulls first")

    (t/is (= [{:a 12.4, :b 10}, {:a 83.0, :b 100}, {:a 100, :b 83}, {:b 15}]
             (tu/query-ra '[:order-by {:order-specs [[a {:null-ordering :nulls-last}]]}
                          [:table {:param ?table}]]
                          {:args {:table table-with-nil}}))
          "nulls last")

    (t/is (= [{:a 12.4, :b 10}, {:a 83.0, :b 100}, {:a 100, :b 83}, {:b 15}]
             (tu/query-ra '[:order-by {:order-specs [[a]]}
                          [:table {:param ?table}]]
                          {:args {:table table-with-nil}}))
          "default nulls last")))

(t/deftest ^:integration test-order-by-temporal-range
  ;; End-to-end: ORDER BY _valid_from DESC with a temporal range query
  ;; that returns enough rows to trigger the external sort / k-way merge
  ;; path in the order-by operator (default *block-size* is 102400).
  (with-open [node (xtn/start-node (merge tu/*node-opts*
                                          {:compactor {:threads 0}}))]
    (let [base-vf (Instant/parse "2020-01-01T00:00:00Z")]
      ;; 330k rows, each with a distinct valid-from 1 minute apart
      (doseq [batch (partition-all 1000 (range 330000))]
        (xt/execute-tx node (mapv (fn [i]
                                    [:put-docs {:into :docs
                                                :valid-from (.plus base-vf (Duration/ofMinutes i))}
                                     {:xt/id "doc1" :val i}])
                                  batch))))

    (t/testing "ORDER BY valid from ascending"
      (let [results-asc (xt/q node
                              (str "FROM docs"
                                   "  FOR VALID_TIME"
                                   "    FROM TIMESTAMP '2020-01-01T00:00:00Z'"
                                   "    TO TIMESTAMP '2021-01-01T00:00:00Z'"
                                   " SELECT _id, _valid_from"
                                   " ORDER BY _valid_from"))]
        (t/is (= 330000 (count results-asc))) 
        (t/is (= #xt/zdt "2020-01-01T00:00Z[UTC]"
                 (:xt/valid-from (first results-asc))))
        (t/is (= #xt/zdt "2020-08-17T03:59Z[UTC]"
                 (:xt/valid-from (last results-asc))))))
    
    (t/testing "ORDER BY valid from descending"
      (let [results-desc (xt/q node
                               (str "FROM docs"
                                    "  FOR VALID_TIME"
                                    "    FROM TIMESTAMP '2020-01-01T00:00:00Z'"
                                    "    TO TIMESTAMP '2021-01-01T00:00:00Z'"
                                    " SELECT _id, _valid_from"
                                    " ORDER BY _valid_from DESC"))]
        (t/is (= 330000 (count results-desc)))
        (t/is (= #xt/zdt "2020-08-17T03:59Z[UTC]"
                 (:xt/valid-from (first results-desc))))
        (t/is (= #xt/zdt "2020-01-01T00:00Z[UTC]"
                 (:xt/valid-from (last results-desc))))))))
