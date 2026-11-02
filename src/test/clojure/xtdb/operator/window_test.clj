(ns xtdb.operator.window-test
  (:require [clojure.test :as t :refer [deftest]]
            [xtdb.test-util :as tu]))

(t/use-fixtures :each tu/with-allocator)

(deftest test-window-operator
  (letfn [(run-test
            [window-spec projection-specs batches]
            (let [window-name (gensym "window")]
              (-> (tu/query-ra [:window {:windows {window-name window-spec}
                                         :projections (mapv (fn [[col-name projection]]
                                                              {col-name {:window-name window-name
                                                                         :window-agg projection}}) projection-specs) }
                                [::tu/pages '{a #xt/type :i64, b #xt/type :i64} batches]])
                  set)))]

    (t/is (= #{} (run-test '{:partition-cols [a]
                             :order-specs [[b]]}
                           '{rn (row-number)}
                           [[] []])))

    (t/is (= #{{:a 1, :b 20, :rn 1}
               {:a 1, :b 10, :rn 2}
               {:a 1, :b 50, :rn 3}
               {:a 1, :b 60, :rn 4}
               {:a 2, :b 30, :rn 1}
               {:a 2, :b 40, :rn 2}
               {:a 2, :b 70, :rn 3}
               {:a 3, :b 80, :rn 1}
               {:a 3, :b 90, :rn 2}}
             (run-test '{:partition-cols [a]}
                       '{rn (row-number)}
                       [[{:a 1 :b 20}
                         {:a 1 :b 10}
                         {:a 2 :b 30}
                         {:a 2 :b 40}]
                        [{:a 1 :b 50}
                         {:a 1 :b 60}
                         {:a 2 :b 70}
                         {:a 3 :b 80}
                         {:a 3 :b 90}]]))
          "only partition by")
    (t/is (= #{{:a 1, :b 10, :rn 1}
               {:a 1, :b 20, :rn 2}
               {:a 2, :b 30, :rn 3}
               {:a 2, :b 40, :rn 4}
               {:a 1, :b 50, :rn 5}
               {:a 1, :b 60, :rn 6}
               {:a 2, :b 70, :rn 7}
               {:a 3, :b 80, :rn 8}
               {:a 3, :b 90, :rn 9}}
             (run-test '{:order-specs [[b]]}
                       '{rn (row-number)}
                       [[{:a 1 :b 20}
                         {:a 1 :b 10}
                         {:a 2 :b 30}
                         {:a 2 :b 40}]
                        [{:a 1 :b 50}
                         {:a 1 :b 60}
                         {:a 2 :b 70}
                         {:a 3 :b 80}
                         {:a 3 :b 90}]]))
          "only order-by")

    (t/is (= #{{:a 1, :b 10, :rn 1}
               {:a 1, :b 20, :rn 2}
               {:a 1, :b 50, :rn 3}
               {:a 1, :b 60, :rn 4}
               {:a 2, :b 30, :rn 1}
               {:a 2, :b 40, :rn 2}
               {:a 2, :b 70, :rn 3}
               {:a 3, :b 80, :rn 1}
               {:a 3, :b 90, :rn 2}}
             (run-test '{:partition-cols [a]
                         :order-specs [[b]]}
                       '{rn (row-number)}
                       [[{:a 1 :b 20}
                         {:a 1 :b 10}
                         {:a 2 :b 30}
                         {:a 2 :b 40}]
                        [{:a 1 :b 50}
                         {:a 1 :b 60}
                         {:a 2 :b 70}
                         {:a 3 :b 80}
                         {:a 3 :b 90}]]))
          "partition by + order-by")))

(deftest test-lead-lag-window-functions
  (letfn [(run-test
            [window-spec projection-specs batches]
            (let [window-name (gensym "window")]
              (-> (tu/query-ra [:window {:windows {window-name window-spec}
                                         :projections (mapv (fn [[col-name projection]]
                                                              {col-name {:window-name window-name
                                                                         :window-agg projection}}) projection-specs)}
                                [::tu/pages '{a #xt/type :i64, b #xt/type :i64} batches]])
                  set)))]

    (t/testing "LAG function"
      (t/is (= #{{:a 1, :b 10}
                 {:a 1, :b 20, :prevb 10}
                 {:a 1, :b 30, :prevb 20}
                 {:a 2, :b 40}
                 {:a 2, :b 50, :prevb 40}}
               (run-test '{:partition-cols [a]
                           :order-specs [[b]]}
                         '{prevb (lag b 1)}
                         [[{:a 1 :b 10}
                           {:a 1 :b 20}
                           {:a 1 :b 30}
                           {:a 2 :b 40}
                           {:a 2 :b 50}]]))
            "lag returns previous value within partition, null when no previous row")

      (t/is (= #{{:a 1, :b 10}
                 {:a 1, :b 20}
                 {:a 1, :b 30, :prevb 10}}
               (run-test '{:partition-cols [a]
                           :order-specs [[b]]}
                         '{prevb (lag b 2)}
                         [[{:a 1 :b 10}
                           {:a 1 :b 20}
                           {:a 1 :b 30}]]))
            "lag with offset 2"))

    (t/testing "LEAD function"
      (t/is (= #{{:a 1, :b 10, :nextb 20}
                 {:a 1, :b 20, :nextb 30}
                 {:a 1, :b 30}
                 {:a 2, :b 40, :nextb 50}
                 {:a 2, :b 50}}
               (run-test '{:partition-cols [a]
                           :order-specs [[b]]}
                         '{nextb (lead b 1)}
                         [[{:a 1 :b 10}
                           {:a 1 :b 20}
                           {:a 1 :b 30}
                           {:a 2 :b 40}
                           {:a 2 :b 50}]]))
            "lead returns next value within partition, null when no next row")

      (t/is (= #{{:a 1, :b 10, :nextb 30}
                 {:a 1, :b 20}
                 {:a 1, :b 30}}
               (run-test '{:partition-cols [a]
                           :order-specs [[b]]}
                         '{nextb (lead b 2)}
                         [[{:a 1 :b 10}
                           {:a 1 :b 20}
                           {:a 1 :b 30}]]))
            "lead with offset 2"))

    (t/testing "across multiple batches"
      (t/is (= #{{:a 1, :b 10}
                 {:a 1, :b 20, :prevb 10}
                 {:a 1, :b 30, :prevb 20}
                 {:a 1, :b 40, :prevb 30}}
               (run-test '{:partition-cols [a]
                           :order-specs [[b]]}
                         '{prevb (lag b 1)}
                         [[{:a 1 :b 10}
                           {:a 1 :b 20}]
                          [{:a 1 :b 30}
                           {:a 1 :b 40}]]))
            "lag works across batches"))))

(deftest test-row-number-ignores-peers
  (let [window-name 'w]
    (t/is (= [[1 10 1] [1 20 2] [1 20 3] [1 nil 4] [1 nil 5] [2 5 1] [2 5 2]]
             (->> (tu/query-ra [:window {:windows {window-name '{:partition-cols [a], :order-specs [[b]]}}
                                         :projections [{'rn {:window-name window-name, :window-agg '(row-number)}}]}
                                [::tu/pages '{a #xt/type :i64, b #xt/type [:? :i64]}
                                 [[{:a 1 :b 20} {:a 1 :b 10} {:a 1 :b nil}]
                                  [{:a 1 :b 20} {:a 1 :b nil} {:a 2 :b 5} {:a 2 :b 5}]]]])
                  (map (juxt :a :b :rn))
                  (sort-by (fn [[a b rn]] [a rn (or b Long/MAX_VALUE)]))))
          "row_number numbers tied and NULL order keys distinctly")))

(deftest test-ranking-window-functions
  (letfn [(run-test [window-spec agg batches]
            (let [window-name (gensym "window")]
              (->> (tu/query-ra [:window {:windows {window-name window-spec}
                                          :projections [{'x {:window-name window-name, :window-agg agg}}]}
                                 [::tu/pages '{a #xt/type :i64, b #xt/type [:? :i64]} batches]])
                   (map (juxt :a :b :x))
                   (sort-by (fn [[a b x]] [a x (or b Long/MAX_VALUE)])))))]
    (let [batches [[{:a 1 :b 10} {:a 1 :b 20} {:a 1 :b 20}]
                   [{:a 1 :b 30} {:a 2 :b 5} {:a 2 :b 5}]]
          spec '{:partition-cols [a], :order-specs [[b]]}]
      (t/is (= [[1 10 1] [1 20 2] [1 20 2] [1 30 4] [2 5 1] [2 5 1]]
               (run-test spec '(rank) batches))
            "rank skips after ties, per partition, across batches")

      (t/is (= [[1 10 1] [1 20 2] [1 20 2] [1 30 3] [2 5 1] [2 5 1]]
               (run-test spec '(dense_rank) batches))
            "dense_rank doesn't skip")

      (t/is (= [[1 10 0.0] [1 20 (/ 1.0 3)] [1 20 (/ 1.0 3)] [1 30 1.0] [2 5 0.0] [2 5 0.0]]
               (run-test spec '(percent_rank) batches))
            "percent_rank of a single-peer-group partition is 0")

      (t/is (= [[1 10 0.25] [1 20 0.75] [1 20 0.75] [1 30 1.0] [2 5 1.0] [2 5 1.0]]
               (run-test spec '(cume_dist) batches))))

    (let [batches [[{:a 1} {:a 1} {:a 2}]]]
      (doseq [[agg expected] {'(rank) 1, '(dense_rank) 1, '(percent_rank) 0.0, '(cume_dist) 1.0}]
        (t/is (= [[1 nil expected] [1 nil expected] [2 nil expected]]
                 (run-test '{:partition-cols [a]} agg batches))
              (str "no ORDER BY: every row is a peer - " agg))))

    (t/is (= [[1 10 1] [1 20 2] [1 nil 3] [1 nil 3]]
             (run-test '{:partition-cols [a], :order-specs [[b]]}
                       '(rank)
                       [[{:a 1 :b 10} {:a 1 :b nil} {:a 1 :b 20} {:a 1 :b nil}]]))
          "nulls are peers of each other")

    (t/is (= [[1 20 1] [1 10 2] [1 10 2]]
             (run-test '{:partition-cols [a], :order-specs [[b {:direction :desc}]]}
                       '(rank)
                       [[{:a 1 :b 10} {:a 1 :b 20} {:a 1 :b 10}]]))
          "descending order")

    (t/is (= [] (run-test '{:partition-cols [a], :order-specs [[b]]} '(rank) [[] []]))
          "empty input")))
