(ns crux.decorators.aggregation-test
  (:require [crux.decorators.aggregation :as aggr]
            [crux.fixtures :as f :refer [*kv*]]
            [crux.query :as q]
            [clojure.test :as t]))

(t/use-fixtures :each f/with-kv-store)

(t/deftest test-count-aggrigation
  (f/transact-entity-maps!
    *kv*
    [{:crux.db/id :a1 :user/name "patrik" :user/post 1 :post/cost 30}
     {:crux.db/id :a2 :user/name "patrik" :user/post 2 :post/cost 35}
     {:crux.db/id :a3 :user/name "patrik" :user/post 3 :post/cost 5}
     {:crux.db/id :a4 :user/name "niclas" :user/post 1 :post/cost 8}])

  (t/testing "with vector syntax"
    (t/is (= [{:user-name "niclas" :post-count 1 :cost-sum 8}
              {:user-name "patrik" :post-count 3 :cost-sum 70}]
             (aggr/q
               (q/db *kv*)
               '{:aggr {:partition-by [?user-name]
                        :select
                        {?cost-sum [0 (+ acc ?post-cost)]
                         ?post-count [0 (inc acc) ?e]}}
                 :where [[?e :user/name ?user-name]
                         [?e :post/cost ?post-cost]]}))))

  (t/testing "with reducer function syntax"
    (t/is (= [{:user-name "niclas" :post-count 1 :cost-sum 8}
              {:user-name "patrik" :post-count 3 :cost-sum 70}]
             (aggr/q
               (q/db *kv*)
               '{:aggr {:partition-by [?user-name]
                        :select
                        {?cost-sum (+ ?post-cost)
                         ?post-count [0 (inc acc) ?e]}}
                 :where [[?e :user/name ?user-name]
                         [?e :post/cost ?post-cost]]})))))
