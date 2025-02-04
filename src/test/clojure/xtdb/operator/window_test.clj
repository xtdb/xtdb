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
                                [::tu/pages '{a :i64, b :i64} batches]])
                  set)))]

    (t/is (= #{} (run-test '{:partition-cols [a]
                             :order-specs [[b]]}
                           '{rn (row-number)}
                           [[] []])))

    (t/is (= #{{:a 1, :b 20, :rn 0}
               {:a 1, :b 10, :rn 1}
               {:a 1, :b 50, :rn 2}
               {:a 1, :b 60, :rn 3}
               {:a 2, :b 30, :rn 0}
               {:a 2, :b 40, :rn 1}
               {:a 2, :b 70, :rn 2}
               {:a 3, :b 80, :rn 0}
               {:a 3, :b 90, :rn 1}}
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
    (t/is (= #{{:a 1, :b 10, :rn 0}
               {:a 1, :b 20, :rn 1}
               {:a 2, :b 30, :rn 2}
               {:a 2, :b 40, :rn 3}
               {:a 1, :b 50, :rn 4}
               {:a 1, :b 60, :rn 5}
               {:a 2, :b 70, :rn 6}
               {:a 3, :b 80, :rn 7}
               {:a 3, :b 90, :rn 8}}
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

    (t/is (= #{{:a 1, :b 10, :rn 0}
               {:a 1, :b 20, :rn 1}
               {:a 1, :b 50, :rn 2}
               {:a 1, :b 60, :rn 3}
               {:a 2, :b 30, :rn 0}
               {:a 2, :b 40, :rn 1}
               {:a 2, :b 70, :rn 2}
               {:a 3, :b 80, :rn 0}
               {:a 3, :b 90, :rn 1}}
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
