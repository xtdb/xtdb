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
