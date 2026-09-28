(ns xtdb.operator.order-by-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.node :as xtn]
            [xtdb.operator.order-by :as order-by]
            [xtdb.test-util :as tu]
            [xtdb.util :as util])
  (:import (java.nio.file Files)
           (java.nio.file.attribute FileAttribute)
           (java.time Instant Duration ZoneId)))

(t/use-fixtures :each tu/with-allocator)

(t/deftest test-order-by
  (t/is (= [[{:a 0, :b 15}
             {:a 12, :b 10}
             {:a 83, :b 100}
             {:a 100, :b 83}]]
           (tu/query-ra [:sort {:order-specs '[[a]]}
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
           (tu/query-ra '[:sort {:order-specs [[a]]}
                          [:table {:rows [{:a 12.4, :b 10}
                                          {:a 0, :b 15}
                                          {:a 100, :b 83}
                                          {:a 83.0, :b 100}]}]]
                        {}))
        "mixed numeric types")

  (t/is (= []
           (tu/query-ra '[:sort {:order-specs [[a]]}
                          [::tu/pages
                           [[] []]]]
                        {}))
        "empty batches"))

(t/deftest test-order-by-with-nulls
  (let [table-with-nil [{:a 12.4, :b 10}, {:a nil, :b 15}, {:a 100, :b 83}, {:a 83.0, :b 100}]]
    (t/is (= [{:b 15}, {:a 12.4, :b 10}, {:a 83.0, :b 100}, {:a 100, :b 83}]
             (tu/query-ra '[:sort {:order-specs [[a {:null-ordering :nulls-first}]]}
                            [:table {:param ?table}]]
                          {:args {:table table-with-nil}}))
          "nulls first")

    (t/is (= [{:a 12.4, :b 10}, {:a 83.0, :b 100}, {:a 100, :b 83}, {:b 15}]
             (tu/query-ra '[:sort {:order-specs [[a {:null-ordering :nulls-last}]]}
                            [:table {:param ?table}]]
                          {:args {:table table-with-nil}}))
          "nulls last")

    (t/is (= [{:a 12.4, :b 10}, {:a 83.0, :b 100}, {:a 100, :b 83}, {:b 15}]
             (tu/query-ra '[:sort {:order-specs [[a]]}
                            [:table {:param ?table}]]
                          {:args {:table table-with-nil}}))
          "default nulls last")))

(t/deftest test-order-by-spill
  (binding [order-by/*block-size* 10]
    (let [data (map-indexed (fn [i d] {:a d :b i}) (repeatedly 1000 #(rand-int 1000000)))
          batches (mapv vec (partition-all 13 data))
          sorted (sort-by (juxt :a :b) data)]
      (t/is (= sorted
               (tu/query-ra [:sort {:order-specs '[[a] [b]]}
                             [::tu/pages batches]]
                            {}))
            "spilling to disk")

      (t/is (= (->> sorted (drop 25) (take 30))
               (tu/query-ra [:sort {:order-specs '[[a] [b]], :skip 25, :limit 30}
                             [::tu/pages batches]]
                            {}))
            "spilling to disk, with skip and limit"))))

(t/deftest test-order-by-spill-cleans-up
  (let [parent (Files/createTempDirectory "test-spill-" (make-array FileAttribute 0))]
    (try
      (with-redefs [util/tmp-dir (fn
                                   ([] (Files/createTempDirectory parent "" (make-array FileAttribute 0)))
                                   ([prefix] (Files/createTempDirectory parent prefix (make-array FileAttribute 0))))]
        (binding [order-by/*block-size* 10]
          (let [data (map-indexed (fn [i d] {:a d :b i})
                                  (repeatedly 1000 #(rand-int 1000000)))
                batches (mapv vec (partition-all 13 data))]

            (t/testing "happy path — loader fully drained"
              (tu/query-ra [:sort {:order-specs '[[a] [b]]}
                            [::tu/pages batches]]
                           {})
              (t/is (empty? (.list (.toFile parent)))
                    "spill dir cleaned up after full drain"))

            (t/testing "early close — LIMIT 1 closes cursor before loader exhausts"
              (tu/query-ra [:sort {:order-specs '[[a] [b]], :limit 1}
                            [::tu/pages batches]]
                           {})
              (t/is (empty? (.list (.toFile parent)))
                    "spill dir cleaned up after early cursor close")))))
      (finally
        (util/delete-dir parent)))))

(t/deftest test-order-by-temporal-range-5269
  ;; #5269 — IOOBE in ORDER BY DESC with external sort.
  ;; .reversed() on the k-way merge comparator swapped index arguments between
  ;; relations with different row counts, reading past the smaller one's buffer.
  (with-redefs [order-by/*block-size* (int 10)]
    (with-open [node (xtn/start-node (merge tu/*node-opts*
                                            {:compactor {:threads 0}
                                             :indexer {:rows-per-block 10}}))]
      (let [base-vf (Instant/parse "2020-01-01T00:00:00Z")]
        (doseq [batch (partition-all 10 (range 24))]
          (xt/execute-tx node (mapv (fn [i]
                                      [:put-docs {:into :docs
                                                  :valid-from (.plus base-vf (Duration/ofMinutes i))}
                                       {:xt/id "doc1" :val i}])
                                    batch)))

        (let [expected-vfs (mapv #(.atZone (.plus base-vf (Duration/ofMinutes %)) (ZoneId/of "UTC")) (range 24))]
          (t/testing "ORDER BY valid from ascending"
            (let [results (xt/q node "FROM docs FOR VALID_TIME ALL SELECT _id, _valid_from ORDER BY _valid_from")]
              (t/is (= expected-vfs
                       (mapv :xt/valid-from results)))))

          (t/testing "ORDER BY valid from descending"
            (let [results (xt/q node "FROM docs FOR VALID_TIME ALL SELECT _id, _valid_from ORDER BY _valid_from DESC")]
              (t/is (= (rseq expected-vfs)
                       (mapv :xt/valid-from results))))))))))
