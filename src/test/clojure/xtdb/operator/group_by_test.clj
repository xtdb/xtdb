(ns xtdb.operator.group-by-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.operator.group-by :as group-by]
            [xtdb.test-util :as tu]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.reader :as vr]))

(t/use-fixtures :each tu/with-allocator tu/with-node)

(t/deftest test-group-by
  (letfn [(run-test [group-by-spec batches]
            (-> (tu/query-ra [:group-by group-by-spec
                              [::tu/pages '{a :i64, b :i64} batches]]
                             {:with-col-types? true})
                (update :res set)))]

    (let [agg-specs '[{sum (sum b)}
                      {avg (avg b)}
                      {cnt (count b)}
                      {min (min b)}
                      {max (max b)}
                      {var-pop (var-pop b)}
                      {var-samp (var-samp b)}
                      {stddev-pop (stddev-pop b)}
                      {stddev-samp (stddev-samp b)}]
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
               {:a 2, :var-pop 0.0, :stddev-pop 0.0}
               {:a 3}}
             (set (tu/query-ra [:group-by '[a
                                            {var-pop (var-pop b)}
                                            {var-samp (var-samp b)}
                                            {stddev-pop (stddev-pop b)}
                                            {stddev-samp (stddev-samp b)}]
                                [::tu/pages '{a :i64, b [:union #{:null :i64}]}
                                 [[{:a 1 :b 20}
                                   {:a 1 :b 10}
                                   {:a 2 :b 30}
                                   {:a 3 :b nil}]]]]))))

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
  (with-open [group-mapping (tu/open-vec (types/->field "gm" #xt.arrow/type :i32 false) (map int [0 0 0]))
              v0 (tu/open-vec "v" [1 2 3])
              v1 (tu/open-vec "v" [1 2.0 3])]
    (let [sum-factory (group-by/->aggregate-factory {:f :sum, :from-name 'v, :from-type [:union #{:i64 :f64}]
                                                     :to-name 'vsum, :zero-row? true})
          sum-spec (.build sum-factory tu/*allocator*)]
      (t/is (= [:union #{:null :f64}] (types/field->col-type (.getToColumnField sum-factory))))
      (try

        (.aggregate sum-spec (vr/rel-reader [(vr/vec->reader v0)]) group-mapping)
        (.aggregate sum-spec (vr/rel-reader [(vr/vec->reader v1)]) group-mapping)
        (t/is (= [12.0] (tu/<-reader (.finish sum-spec))))
        (finally
          (util/try-close sum-spec))))))

(t/deftest test-row-count
  (t/is (= [{:n 0}]
           (tu/query-ra '[:group-by [{n (row-count)}]
                          [::tu/pages {a [:union #{:null :i64}]}
                           []]])))

  (t/is (= [{:n 2}]
           (tu/query-ra '[:group-by [{n (row_count)}]
                          [::tu/pages {a [:union #{:null :i64}]}
                           [[{:a nil}
                             {:a 1}]]]])))

  (t/is (= [{:a 1, :n 1}, {:a 2, :n 2}]
           (tu/query-ra '[:group-by [a {n (row-count)}]
                          [::tu/pages {a :i64, b [:union #{:null :i64}]}
                           [[{:a 1, :b nil}
                             {:a 2, :b 1}
                             {:a 2, :b nil}]]]])))

  (t/is (= []
           (tu/query-ra '[:group-by [a {bs (row-count)}]
                          [::tu/pages {a :i64, b [:union #{:null :i64}]}
                           []]]))
        "empty if there's a grouping key"))

(t/deftest test-count-empty-null-behaviour
  (t/is (= [{:n 0}]
           (tu/query-ra '[:group-by [{n (count a)}]
                          [::tu/pages {a [:union #{:null :i64}]}
                           []]])))

  (t/is (= [{:n 0}]
           (tu/query-ra '[:group-by [{n (count a)}]
                          [::tu/pages {a [:union #{:null :i64}]}
                           [[{:a nil}]]]])))

  (t/is (= [{:n 1}]
           (tu/query-ra '[:group-by [{n (count a)}]
                          [::tu/pages {a [:union #{:null :i64}]}
                           [[{:a nil}
                             {:a 1}]]]])))

  (t/is (= [{:a 1, :n 0}, {:a 2, :n 1}]
           (tu/query-ra '[:group-by [a {n (count b)}]
                          [::tu/pages
                           [[{:a 1, :b nil}
                             {:a 2, :b 1}
                             {:a 2, :b nil}]]]])))

  (t/is (= []
           (tu/query-ra '[:group-by [a {bs (count b)}]
                          [::tu/pages {a :i64, b [:union #{:null :i64}]}
                           []]]))
        "empty if there's a grouping key"))

(t/deftest test-sum-empty-null-behaviour
  (t/is (= [{}]
           (tu/query-ra '[:group-by [{n (sum a)}]
                          [:table []]]))
        "sum empty returns null")

  (t/is (= []
           (tu/query-ra '[:group-by [b {n (sum a)}]
                          [:table []]]))
        "sum empty returns empty when there are groups")

  (t/is (= [{}]
           (tu/query-ra '[:group-by [{n (sum a)}]
                          [:table [{:a nil}]]]))
        "sum all nulls returns null")

  (t/testing "summed group all null"
    (t/is (= [{:a 42}]
             (tu/query-ra '[:group-by [a {n (sum b)}]
                            [:table [{:a 42, :b nil}]]])))

    (t/is (= [{:a 42} {:a 45, :n 1}]
             (tu/query-ra '[:group-by [a {n (sum b)}]
                            [:table [{:a 42, :b nil} {:a 45, :b 1}]]])))))

(t/deftest test-avg-empty-null-behaviour
  (t/is (= [{}]
           (tu/query-ra '[:group-by [{n (avg a)}]
                          [:table []]]))
        "avg empty returns null")

  (t/is (= []
           (tu/query-ra '[:group-by [b {n (avg a)}]
                          [:table []]]))
        "avg empty returns empty when there are groups")

  (t/is (= [{}]
           (tu/query-ra '[:group-by [{n (avg a)}]
                          [:table [{:a nil}]]]))
        "avg all nulls returns null")

  (t/testing "averaged group all null"
    (t/is (= [{:a 42}]
             (tu/query-ra '[:group-by [a {n (avg b)}]
                            [:table [{:a 42, :b nil}]]])))

    (t/is (= [{:a 42} {:a 45, :n 1.0}]
             (tu/query-ra '[:group-by [a {n (avg b)}]
                            [:table [{:a 42, :b nil} {:a 45, :b 1}]]])))))

(t/deftest test-min-of-empty-rel-returns-nil
  (t/is (= [{}]
           (tu/query-ra '[:group-by [{n (min a)}]
                          [:table []]]))
        "min empty returns null")

  (t/is (= []
           (tu/query-ra '[:group-by [b {n (min a)}]
                          [:table []]]))
        "min empty returns empty when there are groups")

  (t/is (= [{}]
           (tu/query-ra '[:group-by [{n (min a)}]
                          [:table [{:a nil}]]]))
        "min all nulls returns null")

  (t/testing "min'd group all null"
    (t/is (= [{:a 42}]
             (tu/query-ra '[:group-by [a {n (min b)}]
                            [:table [{:a 42, :b nil}]]])))

    (t/is (= [{:a 42} {:a 45, :n 1}]
             (tu/query-ra '[:group-by [a {n (min b)}]
                            [:table [{:a 42, :b nil} {:a 45, :b 1}]]])))))

(t/deftest test-min-max-temporal-399
  (t/is (= [{:min #xt/duration "PT0.0001S"
             :max #xt/duration "PT15M"}]
           (tu/query-ra '[:group-by [{min (min a)}
                                     {max (max a)}]
                          [:table [{:a #xt/duration "PT10S"}
                                   {:a #xt/duration "PT0.0001S"}
                                   {:a #xt/duration "PT15M"}]]]))
        "durations")

  (t/is (= [{:min #xt/date "2018-05-01",
             :max #xt/date "2022-01-01"}]
           (tu/query-ra '[:group-by [{min (min a)}
                                     {max (max a)}]
                          [:table [{:a #xt/date "2022-01-01"}
                                   {:a #xt/date "2022-01-01"}
                                   {:a #xt/date "2018-05-01"}]]]))
        "dates")

  (t/is (= [{:min #xt/date-time "2018-05-01T12:00",
             :max #xt/date-time "2022-01-01T15:34:24.523286"}]
           (tu/query-ra '[:group-by [{min (min a)}
                                     {max (max a)}]
                          [:table [{:a #xt/date-time "2022-01-01T15:34:24.523284"}
                                   {:a #xt/date-time "2022-01-01T15:34:24.523286"}
                                   {:a #xt/date-time "2018-05-01T12:00"}]]]))
        "timestamps")

  (t/testing "tstzs"
    (t/is (= [{:min #xt/zoned-date-time "2018-05-01T12:00+01:00[Europe/London]",
               :max #xt/zoned-date-time "2022-08-01T13:34+01:00[Europe/London]"}]
             (tu/query-ra '[:group-by [{min (min a)}
                                       {max (max a)}]
                            [:table [{:a #xt/zoned-date-time "2022-08-01T13:34+01:00[Europe/London]"}
                                     {:a #xt/zoned-date-time "2022-08-01T12:58+01:00[Europe/London]"}
                                     {:a #xt/zoned-date-time "2018-05-01T12:00+01:00[Europe/London]"}]]]))
          "preserves TZ if all the same")

    (t/is (= [{:min #xt/zoned-date-time "2018-05-01T19:00Z",
               :max #xt/zoned-date-time "2022-08-01T11:58Z"}]
             (tu/query-ra '[:group-by [{min (min a)}
                                       {max (max a)}]
                            [:table [{:a #xt/zoned-date-time "2022-08-01T13:34+02:00[Europe/Stockholm]"}
                                     {:a #xt/zoned-date-time "2022-08-01T12:58+01:00[Europe/London]"}
                                     {:a #xt/zoned-date-time "2018-05-01T12:00-07:00[America/Los_Angeles]"}]]]))
          "chooses Z if there are different TZs"))

  (t/is (thrown-with-msg? xtdb.RuntimeException
                          #"Incomparable types in min/max aggregate"
                          (tu/query-ra '[:group-by [{min (min a)}
                                                    {max (max a)}]
                                         [:table [{:a #xt/date-time "2022-08-01T13:34"}
                                                  {:a #xt/zoned-date-time "2022-08-01T12:58+01:00[Europe/London]"}
                                                  {:a 12}]]]))))

(t/deftest test-array-agg
  (with-open [gm0 (tu/open-vec (types/->field "gm0" #xt.arrow/type :i32 false) (map int [0 1 0]))
              k0 (tu/open-vec "k" [1 2 3])

              gm1 (tu/open-vec (types/->field "gm1" #xt.arrow/type :i32 false) (map int [1 2 0]))
              k1 (tu/open-vec "k" [4 5 6])]
    (let [agg-factory (group-by/->aggregate-factory {:f :array-agg, :from-name 'k, :from-type :i64
                                                     :to-name 'vs, :zero-row? true})
          agg-spec (.build agg-factory tu/*allocator*)]
      (try
        (t/is (= [:list :i64] (types/field->col-type (.getToColumnField agg-factory))))

        (.aggregate agg-spec (vr/rel-reader [(vr/vec->reader k0)]) gm0)
        (.aggregate agg-spec (vr/rel-reader [(vr/vec->reader k1)]) gm1)
        (t/is (= [[1 3 6] [2 4] [5]] (tu/<-reader (.finish agg-spec))))
        (finally
          (util/try-close agg-spec))))))

(t/deftest test-array-agg-of-empty-rel-returns-empty-array-3819
  (t/is (= [{}]
           (tu/query-ra '[:group-by [{arr-out (array-agg a)}]
                          [:table []]]))
        "array agg empty returns null")

  (t/is (= []
           (tu/query-ra '[:group-by [b {arr-out (array-agg a)}]
                          [:table []]]))
        "array-agg empty returns empty when there are groups")

  (t/is (= [{:arr-out [nil]}]
           (tu/query-ra '[:group-by [{arr-out (array-agg a)}]
                          [:table [{:a nil}]]]))
        "array-agg preserves nulls")

  (t/testing "array-agg group all null"
    (t/is (= [{:a 42, :arr-out [nil]}]
             (tu/query-ra '[:group-by [a {arr-out (array-agg b)}]
                            [:table [{:a 42, :b nil}]]]))))

  (t/is (= [{:a 42, :arr-out [nil]} {:a 45, :arr-out [1 nil]}]
           (tu/query-ra '[:group-by [a {arr-out (array-agg b)}]
                          [:table [{:a 42, :b nil} {:a 45, :b 1} {:a 45, :b nil}]]]))))

(t/deftest test-vec-agg
  (t/is (= [{:vec-out []}]
           (tu/query-ra '[:group-by [{vec-out (vec-agg a)}]
                          [:table []]]))
        "array agg empty returns null")

  (t/is (= []
           (tu/query-ra '[:group-by [b {vec-out (vec-agg a)}]
                          [:table []]]))
        "vec-agg empty returns empty when there are groups")

  (t/is (= [{:vec-out [nil]}]
           (tu/query-ra '[:group-by [{vec-out (vec-agg a)}]
                          [:table [{:a nil}]]]))
        "vec-agg preserves nulls")

  (t/testing "vec-agg group all null"
    (t/is (= [{:a 42, :vec-out [nil]}]
             (tu/query-ra '[:group-by [a {vec-out (vec-agg b)}]
                            [:table [{:a 42, :b nil}]]]))))

  (t/is (= [{:a 42, :vec-out [nil]} {:a 45, :vec-out [1 nil]}]
           (tu/query-ra '[:group-by [a {vec-out (vec-agg b)}]
                          [:table [{:a 42, :b nil} {:a 45, :b 1} {:a 45, :b nil}]]]))))

(t/deftest test-bool-aggs
  (t/is (= {:res #{{:k "t", :all-vs true, :any-vs true}
                   {:k "f", :all-vs false, :any-vs false}
                   {:k "n"}
                   {:k "fn", :all-vs false, :any-vs false}
                   {:k "tn", :all-vs true, :any-vs true}
                   {:k "tf", :all-vs false, :any-vs true}
                   {:k "tfn", :all-vs false, :any-vs true}}
            :col-types '{k :utf8, all-vs [:union #{:null :bool}], any-vs [:union #{:null :bool}]}}

           (-> (tu/query-ra [:group-by '[k {all-vs (bool-and v)} {any-vs (bool-or v)}]
                             [::tu/pages
                              [[{:k "t", :v true} {:k "f", :v false} {:k "n", :v nil}
                                {:k "t", :v true} {:k "f", :v false} {:k "n", :v nil}
                                {:k "tn", :v true} {:k "tn", :v nil} {:k "tn", :v true}
                                {:k "fn", :v false} {:k "fn", :v nil} {:k "fn", :v false}
                                {:k "tf", :v true} {:k "tf", :v false} {:k "tf", :v true}
                                {:k "tfn", :v true} {:k "tfn", :v false} {:k "tfn", :v nil}]]]]
                            {:with-col-types? true})
               (update :res set))))

  (t/is (= []
           (tu/query-ra [:group-by '[k {all-vs (bool-and v)} {any-vs (bool-or v)}]
                         [::tu/pages '{k :utf8, v [:union #{:bool :null}]}
                          []]])))

  (t/is (= [{}]
           (tu/query-ra [:group-by '[{all-vs (bool-and v)} {any-vs (bool-or v)}]
                         [::tu/pages '{v [:union #{:bool :null}]}
                          []]])))

  (t/is (= [{}]
           (tu/query-ra '[:group-by [{n (bool-and b)}]
                          [::tu/pages
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

            :col-types '{k :keyword,
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
                             [::tu/pages
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
  (t/is (= {:res #{{:a 42} {}}
            :col-types '{a [:union #{:i64 :null}]}}
           (-> (tu/query-ra '[:group-by [a]
                              [:table [{:a 42, :b 42}, {:a nil, :b 42}, {:a nil, :b 42}]]]
                            {:with-col-types? true})
               (update :res set)))))

(t/deftest test-group-by-groups-nils
  (t/is (= {:res #{{:b 1, :n 85}}
            :col-types '{a :null, b :i64, n [:union #{:null :i64}]}}
           (-> (tu/query-ra '[:group-by [a b {n (sum c)}]
                              [:table [{:a nil, :b 1, :c 42}
                                       {:a nil, :b 1, :c 43}]]]
                            {:with-col-types? true})
               (update :res set)))))

(t/deftest test-throws-for-variable-width-minmax-340
  ;; HACK only for now, until we support it properly
  (t/is (thrown-with-msg? RuntimeException #"Unsupported types in min/max aggregate"
                          (tu/query-ra '[:group-by [{min (min a)}]
                                         [:table [{:a 32} {:a "foo"}]]]))))

(t/deftest test-handles-absent-3057
  (let [data [{:id 1 :a 1}
              {:id 1} ; NOTE: absent :a
              {:id 2 :a 2}
              {:id 2 :a 3}]]

    (t/is (= [{:id 1, :a 1} {:id 2, :a 5}]
             (xt/q tu/*node* '(-> (rel $data [id a])
                                  (aggregate id {:a (sum a)}))
                   {:args {:data data}}))
          "no default provided")

    (t/is (= [{:id 1, :a 3} {:id 2, :a 5}]
             (xt/q tu/*node* '(-> (rel $data [id a])
                                  (with {:a (coalesce a 2)})
                                  (aggregate id {:a (sum a)}))
                   {:args {:data data}}))
          "default provided with `coalesce`")

    (t/is (= [{:id 1, :a 6} {:id 2, :a 5}]
             (xt/q tu/*node* '(-> (rel $data [id a])
                                  (with {:a (if a a 5)})
                                  (aggregate id {:a (sum a)}))
                   {:args {:data data}}))
          "default provided with `if` (which mentions `absent` values)")))
