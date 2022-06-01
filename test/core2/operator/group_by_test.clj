(ns core2.operator.group-by-test
  (:require [clojure.test :as t]
            [core2.operator :as op]
            [core2.operator.group-by :as group-by]
            [core2.test-util :as tu]
            [core2.types :as types]
            [core2.util :as util]
            [core2.vector.indirect :as iv])
  (:import org.apache.arrow.vector.types.pojo.Schema))

(t/use-fixtures :each tu/with-allocator)

(t/deftest test-group-by
  (let [a-field (types/->field "a" types/bigint-type false)
        b-field (types/->field "b" types/bigint-type false)]
    (letfn [(run-test [group-by-spec blocks]
              (with-open [res (op/open-ra [:group-by group-by-spec
                                           [::tu/blocks (Schema. [a-field b-field])
                                            blocks]])]
                (into #{} cat (tu/<-cursor res))))]

      (let [agg-specs '[{sum (sum b)}
                        {avg (avg b)}
                        {cnt (count b)}
                        {min (min b)}
                        {max (max b)}
                        {variance (variance b)}
                        {std-dev (std-dev b)}]]

        (t/is (= #{{:a 1, :sum 140, :avg 35.0, :cnt 4 :min 10 :max 60,
                    :variance 425.0, :std-dev 20.615528128088304}
                   {:a 2, :sum 140, :avg 46.666666666666664, :cnt 3 :min 30 :max 70
                    :variance 288.88888888888914, :std-dev 16.996731711975958}
                   {:a 3, :sum 170, :avg 85.0, :cnt 2 :min 80 :max 90,
                    :variance 25.0, :std-dev 5.0}}
                 (run-test (cons 'a agg-specs)
                           [[{:a 1 :b 20}
                             {:a 1 :b 10}
                             {:a 2 :b 30}
                             {:a 2 :b 40}]
                            [{:a 1 :b 50}
                             {:a 1 :b 60}
                             {:a 2 :b 70}
                             {:a 3 :b 80}
                             {:a 3 :b 90}]])))

        (t/is (empty? (run-test (cons 'a agg-specs) []))
              "empty input"))

      (t/is (= #{{:a 1} {:a 2} {:a 3}}
               (run-test '[a]
                         [[{:a 1 :b 10}
                           {:a 1 :b 20}
                           {:a 2 :b 10}
                           {:a 2 :b 20}]
                          [{:a 1 :b 10}
                           {:a 1 :b 20}
                           {:a 2 :b 10}
                           {:a 3 :b 20}
                           {:a 3 :b 10}]]))
            "group without aggregate")

      (t/is (= #{{:a 1, :b 10, :cnt 2}
                 {:a 1, :b 20, :cnt 2}
                 {:a 2, :b 10, :cnt 2}
                 {:a 2, :b 20, :cnt 1}
                 {:a 3, :b 10, :cnt 1}
                 {:a 3, :b 20, :cnt 1}}
               (run-test '[a b {cnt (count b)}]
                         [[{:a 1 :b 10}
                           {:a 1 :b 20}
                           {:a 2 :b 10}
                           {:a 2 :b 20}]
                          [{:a 1 :b 10}
                           {:a 1 :b 20}
                           {:a 2 :b 10}
                           {:a 3 :b 20}
                           {:a 3 :b 10}]]))
            "multiple group columns (distinct)")

      (t/is (= #{{:cnt 9}}
               (run-test '[{cnt (count b)}]
                         [[{:a 1 :b 10}
                           {:a 1 :b 20}
                           {:a 2 :b 10}
                           {:a 2 :b 20}]
                          [{:a 1 :b 10}
                           {:a 1 :b 20}
                           {:a 2 :b 10}
                           {:a 3 :b 20}
                           {:a 3 :b 10}]]))
            "aggregate without group"))))

(t/deftest test-promoting-sum
  (with-open [group-mapping (tu/->mono-vec "gm" types/int-type (map int [0 0 0]))
              v0 (tu/->mono-vec "v" types/bigint-type [1 2 3])
              v1 (tu/->duv "v" [1 2.0 3])]
    (let [sum-spec (-> (group-by/->aggregate-factory :sum "v" [:union #{:i64 :f64}] "vsum")
                       (.build tu/*allocator*))]
      (try
        (.aggregate sum-spec (iv/->indirect-rel [(iv/->direct-vec v0)]) group-mapping)
        (.aggregate sum-spec (iv/->indirect-rel [(iv/->direct-vec v1)]) group-mapping)
        (t/is (= [12.0] (tu/<-column (.finish sum-spec))))
        (finally
          (util/try-close sum-spec))))))

(t/deftest test-array-agg
  (with-open [gm0 (tu/->mono-vec "gm0" types/int-type (map int [0 1 0]))
              k0 (tu/->mono-vec "k" types/bigint-type [1 2 3])

              gm1 (tu/->mono-vec "gm1" types/int-type (map int [1 2 0]))
              k1 (tu/->mono-vec "k" types/bigint-type [4 5 6])]
    (let [agg-spec (-> (group-by/->aggregate-factory :array-agg "k" :i64 "vs")
                       (.build tu/*allocator*))]
      (try
        (.aggregate agg-spec (iv/->indirect-rel [(iv/->direct-vec k0)]) gm0)
        (.aggregate agg-spec (iv/->indirect-rel [(iv/->direct-vec k1)]) gm1)
        (t/is (= [[1 3 6] [2 4] [5]] (tu/<-column (.finish agg-spec))))
        (finally
          (util/try-close agg-spec))))))

(t/deftest test-bool-aggs
  (with-open [res (op/open-ra [:group-by '[k {all-vs (all v)} {any-vs (any v)}]
                               [::tu/blocks (Schema. [(types/->field "k" types/varchar-type false)
                                                      (types/->field "v" types/bool-type true)])
                                [[{:k "t", :v true} {:k "f", :v false} {:k "n", :v nil}
                                  {:k "t", :v true} {:k "f", :v false} {:k "n", :v nil}
                                  {:k "tn", :v true} {:k "tn", :v nil} {:k "tn", :v true}
                                  {:k "fn", :v false} {:k "fn", :v nil} {:k "fn", :v false}
                                  {:k "tf", :v true} {:k "tf", :v false} {:k "tf", :v true}
                                  {:k "tfn", :v true} {:k "tfn", :v false} {:k "tfn", :v nil}]]]])]
    (t/is (= #{{:k "t", :all-vs true, :any-vs true}
               {:k "f", :all-vs false, :any-vs false}
               {:k "n", :all-vs nil, :any-vs nil}
               {:k "fn", :all-vs false, :any-vs nil}
               {:k "tn", :all-vs nil, :any-vs true}
               {:k "tf", :all-vs false, :any-vs true}
               {:k "tfn", :all-vs false, :any-vs true}}
             (set (first (tu/<-cursor res)))))))

(t/deftest test-distinct
  (with-open [res (op/open-ra [:group-by '[k
                                           {cnt (count v)}
                                           {cnt-distinct (count-distinct v)}
                                           {sum (sum v)}
                                           {sum-distinct (sum-distinct v)}
                                           {avg (avg v)}
                                           {avg-distinct (avg-distinct v)}
                                           {array-agg (array-agg v)}
                                           {array-agg-distinct (array-agg-distinct v)}]
                               [::tu/blocks (Schema. [(types/->field "k" types/keyword-type false)
                                                      (types/->field "v" types/bigint-type true)])
                                [[{:k :a, :v 10}
                                  {:k :b, :v 12}
                                  {:k :b, :v 15}
                                  {:k :b, :v 15}
                                  {:k :b, :v 10}]
                                 [{:k :a, :v 12}
                                  {:k :a, :v 10}]]]])]
    (t/is (= #{{:k :a,
                :cnt 3, :cnt-distinct 2,
                :sum 32, :sum-distinct 22,
                :avg 10.666666666666666, :avg-distinct 11.0
                :array-agg [10 12 10], :array-agg-distinct [10 12]}
               {:k :b,
                :cnt 4, :cnt-distinct 3,
                :sum 52, :sum-distinct 37,
                :avg 13.0, :avg-distinct 12.333333333333334
                :array-agg [12 15 15 10], :array-agg-distinct [12 15 10]}}
             (set (first (tu/<-cursor res)))))))

(t/deftest test-group-by-with-nils-coerce-to-boolean-npe-regress
  (t/is
    (-> '[:group-by [a]
          [:table [{:a 42, :b 42} {:a nil, :b 42} {:a nil, :b 42}]]]
        (op/query-ra {})
        any?)))

(t/deftest test-group-by-groups-nils
  (t/is
    (=
      [{:a nil, :b 1, :n 85}]
      (-> '[:group-by [a b {n (sum c)}]
            [:table [{:a nil, :b 1, :c 42}
                     {:a nil, :b 1, :c 43}]]]
          (op/query-ra {})))))

(t/deftest test-min-of-empty-rel-returns-nil
  (t/is (= [{:a nil}] (op/query-ra '[:group-by [{a (min b)}] [:select false [:table [{:b 0}]]]] {}))))

(t/deftest test-count-of-empty-rel-returns-zero
  (t/is (= [{:a 0}] (op/query-ra '[:group-by [{a (count b)}] [:select false [:table [{:b 0}]]]] {}))))

(t/deftest test-sum-empty-returns-null
  (t/is (= [{:n nil}]  (op/query-ra '[:group-by [{n (sum a)}] [:table []]] {}))))

(t/deftest test-no-groups-sum-empty-returns-empty
  (t/is (= [] (op/query-ra '[:group-by [b {n (sum a)}] [:table []]] {}))))

;; FIXME least-upper-bound https://github.com/xtdb/core2/issues/192
#_#_
(t/deftest test-sum-all-nulls-returns-null
  (t/is (= [{:n nil}]  (op/query-ra '[:group-by [{n (sum a)}] [:table [{:a nil}]]] {}))))

(t/deftest test-summed-group-all-null
  (t/is (= [{:a 42, :n nil}] (op/query-ra '[:group-by [a {n (sum b)}] [:table [{:a 42, :b nil}]]] {})))
  (t/is (= [{:a 42, :n nil} {:a 45, :b 1}] (op/query-ra '[:group-by [a {n (sum b)}] [:table [{:a 42, :b nil} {:a 45, :b 1}]]] {}))))
