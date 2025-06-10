(ns xtdb.operator.list-test
  (:require [clojure.test :as t]
            [xtdb.operator.list :as ol]
            [xtdb.test-util :as tu]))

(t/use-fixtures :each tu/with-allocator tu/with-node)

(t/deftest test-generate-series
  (t/is (= [{:a 1} {:a 2} {:a 3} {:a 4} {:a 5}]
           (tu/query-ra [:list '{a (generate_series 1 6 1)}])))

  (t/is (= [{:a 1} {:a 2} {:a 3} {:a 4} {:a 5}]
           (tu/query-ra [:top {:limit 5}
                         [:list '{a (generate_series 1 2000000000 1)}]]))
        "large generate_series with limit")

  (t/is (= [{:a 1000000001}
            {:a 1000000002}
            {:a 1000000003}
            {:a 1000000004}
            {:a 1000000005}]
           (tu/query-ra [:top {:skip 1000000000
                               :limit 5}
                         [:list '{a (generate_series 1 2000000000 1)}]]))
        "large generate_series with skip + limit")

  (t/is (= [{:a 2} {:a 3} {:a 4}]
           (tu/query-ra [:list '{a (generate_series ?start ?stop ?step)}]
                        {:args {:start 2 :stop 5 :step 1}}))
        "generate_series with parameterised start, stop, step"))

(t/deftest test-batch-boundaries
  (binding [ol/*batch-size* 3]
    (t/is (= [[{:a 1} {:a 2}]]
             (tu/query-ra [:list '{a (generate_series 1 3 1)}]
                          {:preserve-pages? true}))
          "Should handle size smaller than a single batch")

    (t/is (= [[{:a 1} {:a 2} {:a 3}]]
             (tu/query-ra [:list '{a (generate_series 1 4 1)}]
                          {:preserve-pages? true}))
          "Should handle size of a single full batch")

    (t/is (= [[{:a 1} {:a 2} {:a 3}]
              [{:a 4} {:a 5} {:a 6}]]
             (tu/query-ra [:list '{a (generate_series 1 7 1)}]
                          {:preserve-pages? true}))
          "Should yield two full batches")

    (t/is (= [[{:a 1} {:a 2} {:a 3}]
              [{:a 4} {:a 5}]]
             (tu/query-ra [:list '{a (generate_series 1 6 1)}]
                          {:preserve-pages? true}))
          "Should yield one full batch and one partial batch")

    (t/is (= []
             (tu/query-ra [:list '{a (generate_series 1 1 1)}]
                          {:preserve-pages? true}))
          "Should yield no pages with empty list")))
