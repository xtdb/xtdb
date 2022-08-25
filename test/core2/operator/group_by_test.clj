(ns core2.operator.group-by-test
  (:require [clojure.test :as t]
            [core2.operator.group-by :as group-by]
            [core2.test-util :as tu]
            [core2.util :as util]
            [core2.vector.indirect :as iv]))

(t/use-fixtures :each tu/with-allocator)

(t/deftest test-group-by
  (letfn [(run-test [group-by-spec blocks]
            (-> (tu/query-ra [:group-by group-by-spec
                              [::tu/blocks '{a :i64, b :i64} blocks]]
                             {:with-col-types? true})
                (update :res set)))]

    (let [agg-specs '[{sum (sum b)}
                      {avg (avg b)}
                      {cnt (count b)}
                      {min (min b)}
                      {max (max b)}
                      {var-pop (var_pop b)}
                      {var-samp (var_samp b)}
                      {stddev-pop (stddev_pop b)}
                      {stddev-samp (stddev_samp b)}]
          expected-col-types '{a :i64, cnt :i64
                               sum [:union #{:null :i64}], avg [:union #{:null :f64}]
                               min [:union #{:null :i64}], max [:union #{:null :i64}]
                               var-pop [:union #{:null :f64}], stddev-pop [:union #{:null :f64}]
                               var-samp [:union #{:null :f64}], stddev-samp [:union #{:null :f64}]}]

      (t/is (= {:res #{{:a 1, :sum 140, :avg 35.0, :cnt 4 :min 10 :max 60,
                        :var-pop 425.0, :stddev-pop 20.615528128088304
                        :var-samp 566.6666666666666, :stddev-samp 23.804761428476166}
                       {:a 2, :sum 140, :avg 46.666666666666664, :cnt 3 :min 30 :max 70
                        :var-pop 288.88888888888897, :stddev-pop 16.99673171197595
                        :var-samp 433.3333333333335, :stddev-samp 20.816659994661332}
                       {:a 3, :sum 170, :avg 85.0, :cnt 2 :min 80 :max 90,
                        :var-pop 25.0, :stddev-pop 5.0
                        :var-samp 50.0, :stddev-samp 7.0710678118654755}}
                :col-types expected-col-types}

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

      (t/is {:res (run-test (cons 'a agg-specs) [])
             :col-types expected-col-types}
            "empty input"))

    (t/is (= #{{:a 1, :var-pop 25.0, :var-samp 50.0, :stddev-pop 5.0, :stddev-samp 7.0710678118654755}
               {:a 2, :var-pop 0.0, :var-samp nil, :stddev-pop 0.0, :stddev-samp nil}
               {:a 3, :var-pop nil, :var-samp nil, :stddev-pop nil, :stddev-samp nil}}
             (-> (run-test '[a
                             {var-pop (var_pop b)}
                             {var-samp (var_samp b)}
                             {stddev-pop (stddev_pop b)}
                             {stddev-samp (stddev_samp b)}]
                           [[{:a 1 :b 20}
                             {:a 1 :b 10}
                             {:a 2 :b 30}
                             {:a 3 :b nil}]])
                 (:res))))

    (t/is (= {:res #{{:a 1} {:a 2} {:a 3}}
              :col-types '{a :i64}}
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

    (t/is (= {:res #{{:a 1, :b 10, :cnt 2}
                     {:a 1, :b 20, :cnt 2}
                     {:a 2, :b 10, :cnt 2}
                     {:a 2, :b 20, :cnt 1}
                     {:a 3, :b 10, :cnt 1}
                     {:a 3, :b 20, :cnt 1}}
              :col-types '{a :i64, b :i64, cnt :i64}}
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

    (t/is (= {:res #{{:cnt 9}}, :col-types '{cnt :i64}}
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
          "aggregate without group")))

(t/deftest test-promoting-sum
  (with-open [group-mapping (tu/open-vec "gm" (map int [0 0 0]))
              v0 (tu/open-vec "v" [1 2 3])
              v1 (tu/open-vec "v" [1 2.0 3])]
    (let [sum-factory (group-by/->aggregate-factory {:f :sum, :from-name 'v, :from-type [:union #{:i64 :f64}]
                                                     :to-name 'vsum, :zero-row? true})
          sum-spec (.build sum-factory tu/*allocator*)]
      (try
        (t/is (= [:union #{:null :f64}] (.getToColumnType sum-factory)))

        (.aggregate sum-spec (iv/->indirect-rel [(iv/->direct-vec v0)]) group-mapping)
        (.aggregate sum-spec (iv/->indirect-rel [(iv/->direct-vec v1)]) group-mapping)
        (t/is (= [12.0] (tu/<-column (.finish sum-spec))))
        (finally
          (util/try-close sum-spec))))))

(t/deftest test-count-star
  (t/is (= [{:n 0}]
           (tu/query-ra '[:group-by [{n (count-star)}]
                          [::tu/blocks {a [:union #{:null :i64}]}
                           []]])))

  (t/is (= [{:n 1}]
           (tu/query-ra '[:group-by [{n (count-star)}]
                          [::tu/blocks {a [:union #{:null :i64}]}
                           [[{:a nil}]]]])))

  (t/is (= [{:n 2}]
           (tu/query-ra '[:group-by [{n (count-star)}]
                          [::tu/blocks {a [:union #{:null :i64}]}
                           [[{:a nil}
                             {:a 1}]]]])))

  (t/is (= [{:a 1, :n 1}, {:a 2, :n 2}]
           (tu/query-ra '[:group-by [a {n (count-star)}]
                          [::tu/blocks {a :i64, b [:union #{:null :i64}]}
                           [[{:a 1, :b nil}
                             {:a 2, :b 1}
                             {:a 2, :b nil}]]]])))

  (t/is (= []
           (tu/query-ra '[:group-by [a {bs (count-star)}]
                          [::tu/blocks {a :i64, b [:union #{:null :i64}]}
                           []]]))
        "empty if there's a grouping key"))

(t/deftest test-count-empty-null-behaviour
  (t/is (= [{:n 0}]
           (tu/query-ra '[:group-by [{n (count a)}]
                          [::tu/blocks {a [:union #{:null :i64}]}
                           []]])))

  (t/is (= [{:n 0}]
           (tu/query-ra '[:group-by [{n (count a)}]
                          [::tu/blocks {a [:union #{:null :i64}]}
                           [[{:a nil}]]]])))

  (t/is (= [{:n 1}]
           (tu/query-ra '[:group-by [{n (count a)}]
                          [::tu/blocks {a [:union #{:null :i64}]}
                           [[{:a nil}
                             {:a 1}]]]])))

  (t/is (= [{:a 1, :n 0}, {:a 2, :n 1}]
           (tu/query-ra '[:group-by [a {n (count b)}]
                          [::tu/blocks
                           [[{:a 1, :b nil}
                             {:a 2, :b 1}
                             {:a 2, :b nil}]]]])))

  (t/is (= []
           (tu/query-ra '[:group-by [a {bs (count b)}]
                          [::tu/blocks {a :i64, b [:union #{:null :i64}]}
                           []]]))
        "empty if there's a grouping key"))

(t/deftest test-sum-empty-null-behaviour
  (t/is (= [{:n nil}]
           (tu/query-ra '[:group-by [{n (sum a)}]
                          [:table []]]))
        "sum empty returns null")

  (t/is (= []
           (tu/query-ra '[:group-by [b {n (sum a)}]
                          [:table []]]))
        "sum empty returns empty when there are groups")

  (t/is (= [{:n nil}]
           (tu/query-ra '[:group-by [{n (sum a)}]
                          [:table [{:a nil}]]]))
        "sum all nulls returns null")

  (t/testing "summed group all null"
    (t/is (= [{:a 42, :n nil}]
             (tu/query-ra '[:group-by [a {n (sum b)}]
                            [:table [{:a 42, :b nil}]]])))

    (t/is (= [{:a 42, :n nil} {:a 45, :n 1}]
             (tu/query-ra '[:group-by [a {n (sum b)}]
                            [:table [{:a 42, :b nil} {:a 45, :b 1}]]])))))

(t/deftest test-min-of-empty-rel-returns-nil
  (t/is (= [{:n nil}]
           (tu/query-ra '[:group-by [{n (min a)}]
                          [:table []]]))
        "min empty returns null")

  (t/is (= []
           (tu/query-ra '[:group-by [b {n (min a)}]
                          [:table []]]))
        "min empty returns empty when there are groups")

  (t/is (= [{:n nil}]
           (tu/query-ra '[:group-by [{n (min a)}]
                          [:table [{:a nil}]]]))
        "min all nulls returns null")

  (t/testing "min'd group all null"
    (t/is (= [{:a 42, :n nil}]
             (tu/query-ra '[:group-by [a {n (min b)}]
                            [:table [{:a 42, :b nil}]]])))

    (t/is (= [{:a 42, :n nil} {:a 45, :n 1}]
             (tu/query-ra '[:group-by [a {n (min b)}]
                            [:table [{:a 42, :b nil} {:a 45, :b 1}]]])))))

(t/deftest test-array-agg
  (with-open [gm0 (tu/open-vec "gm0" (map int [0 1 0]))
              k0 (tu/open-vec "k" [1 2 3])

              gm1 (tu/open-vec "gm1" (map int [1 2 0]))
              k1 (tu/open-vec "k" [4 5 6])]
    (let [agg-factory (group-by/->aggregate-factory {:f :array-agg, :from-name 'k, :from-type :i64
                                                     :to-name 'vs, :zero-row? true})
          agg-spec (.build agg-factory tu/*allocator*)]
      (try
        (t/is (= [:list :i64] (.getToColumnType agg-factory)))

        (.aggregate agg-spec (iv/->indirect-rel [(iv/->direct-vec k0)]) gm0)
        (.aggregate agg-spec (iv/->indirect-rel [(iv/->direct-vec k1)]) gm1)
        (t/is (= [[1 3 6] [2 4] [5]] (tu/<-column (.finish agg-spec))))
        (finally
          (util/try-close agg-spec))))))

(t/deftest test-bool-aggs
  (t/is (= {:res #{{:k "t", :all-vs true, :any-vs true}
                   {:k "f", :all-vs false, :any-vs false}
                   {:k "n", :all-vs nil, :any-vs nil}
                   {:k "fn", :all-vs false, :any-vs false}
                   {:k "tn", :all-vs true, :any-vs true}
                   {:k "tf", :all-vs false, :any-vs true}
                   {:k "tfn", :all-vs false, :any-vs true}}
            :col-types '{k :utf8, all-vs [:union #{:null :bool}], any-vs [:union #{:null :bool}]}}

           (-> (tu/query-ra [:group-by '[k {all-vs (all v)} {any-vs (any v)}]
                             [::tu/blocks
                              [[{:k "t", :v true} {:k "f", :v false} {:k "n", :v nil}
                                {:k "t", :v true} {:k "f", :v false} {:k "n", :v nil}
                                {:k "tn", :v true} {:k "tn", :v nil} {:k "tn", :v true}
                                {:k "fn", :v false} {:k "fn", :v nil} {:k "fn", :v false}
                                {:k "tf", :v true} {:k "tf", :v false} {:k "tf", :v true}
                                {:k "tfn", :v true} {:k "tfn", :v false} {:k "tfn", :v nil}]]]]
                            {:with-col-types? true})
               (update :res set))))

  (t/is (= []
           (tu/query-ra [:group-by '[k {all-vs (all v)} {any-vs (any v)}]
                         [::tu/blocks '{k :utf8, v [:union #{:bool :null}]}
                          []]])))

  (t/is (= [{:all-vs nil, :any-vs nil}]
           (tu/query-ra [:group-by '[{all-vs (all v)} {any-vs (any v)}]
                         [::tu/blocks '{v [:union #{:bool :null}]}
                          []]])))

  (t/is (= [{:n nil}]
           (tu/query-ra '[:group-by [{n (all b)}]
                          [::tu/blocks
                           [[{:b nil}]]]]))))

(t/deftest test-distinct
  (t/is (= {:res #{{:k :a,
                    :cnt 3, :cnt-distinct 2,
                    :sum 32, :sum-distinct 22,
                    :avg 10.666666666666666, :avg-distinct 11.0
                    :array-agg [10 12 10], :array-agg-distinct [10 12]}
                   {:k :b,
                    :cnt 4, :cnt-distinct 3,
                    :sum 52, :sum-distinct 37,
                    :avg 13.0, :avg-distinct 12.333333333333334
                    :array-agg [12 15 15 10], :array-agg-distinct [12 15 10]}}

            :col-types '{k [:extension-type :keyword :utf8 ""],
                         cnt :i64, cnt-distinct :i64,
                         sum [:union #{:null :i64}], sum-distinct [:union #{:null :i64}],
                         avg [:union #{:null :f64}], avg-distinct [:union #{:null :f64}],
                         array-agg [:list :i64],
                         array-agg-distinct [:list :i64]}}

           (-> (tu/query-ra [:group-by '[k
                                         {cnt (count v)}
                                         {cnt-distinct (count-distinct v)}
                                         {sum (sum v)}
                                         {sum-distinct (sum-distinct v)}
                                         {avg (avg v)}
                                         {avg-distinct (avg-distinct v)}
                                         {array-agg (array-agg v)}
                                         {array-agg-distinct (array-agg-distinct v)}]
                             [::tu/blocks
                              [[{:k :a, :v 10}
                                {:k :b, :v 12}
                                {:k :b, :v 15}
                                {:k :b, :v 15}
                                {:k :b, :v 10}]
                               [{:k :a, :v 12}
                                {:k :a, :v 10}]]]]
                            {:with-col-types? true})
               (update :res set)))))

(t/deftest test-group-by-with-nils-coerce-to-boolean-npe-regress
  (t/is (= {:res #{{:a 42} {:a nil}}
            :col-types '{a [:union #{:i64 :null}]}}
           (-> (tu/query-ra '[:group-by [a]
                              [:table [{:a 42, :b 42}, {:a nil, :b 42}, {:a nil, :b 42}]]]
                            {:with-col-types? true})
               (update :res set)))))

(t/deftest test-group-by-groups-nils
  (t/is (= {:res #{{:a nil, :b 1, :n 85}}
            :col-types '{a :null, b :i64, n [:union #{:null :i64}]}}
           (-> (tu/query-ra '[:group-by [a b {n (sum c)}]
                              [:table [{:a nil, :b 1, :c 42}
                                       {:a nil, :b 1, :c 43}]]]
                            {:with-col-types? true})
               (update :res set)))))
