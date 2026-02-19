(ns xtdb.operator.group-by.percentile-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.test-util :as tu]))

(t/use-fixtures :each tu/with-allocator tu/with-node)

(t/deftest test-percentile-cont-basic
  (t/is (= [{:median 20.0}]
           (tu/query-ra '[:group-by {:columns [{median (percentile_cont 0.5 [v {:direction :asc}])}]}
                          [:table {:rows [{:v 10} {:v 20} {:v 30}]}]]))
        "median of 3 values"))

(t/deftest test-percentile-cont-interpolation
  (t/is (= [{:p 2.5}]
           (tu/query-ra '[:group-by {:columns [{p (percentile_cont 0.5 [v {:direction :asc}])}]}
                          [:table {:rows [{:v 1} {:v 2} {:v 3} {:v 4}]}]]))
        "interpolates between 2 and 3")

  (t/is (= [{:p 1.0}]
           (tu/query-ra '[:group-by {:columns [{p (percentile_cont 0.0 [v {:direction :asc}])}]}
                          [:table {:rows [{:v 1} {:v 2} {:v 3}]}]]))
        "fraction=0 returns first")

  (t/is (= [{:p 3.0}]
           (tu/query-ra '[:group-by {:columns [{p (percentile_cont 1.0 [v {:direction :asc}])}]}
                          [:table {:rows [{:v 1} {:v 2} {:v 3}]}]]))
        "fraction=1 returns last"))

(t/deftest test-percentile-cont-single-value
  (t/is (= [{:p 42.0}]
           (tu/query-ra '[:group-by {:columns [{p (percentile_cont 0.5 [v {:direction :asc}])}]}
                          [:table {:rows [{:v 42}]}]]))
        "single value returns that value"))

(t/deftest test-percentile-cont-with-nulls
  (t/is (= [{:p 20.0}]
           (tu/query-ra '[:group-by {:columns [{p (percentile_cont 0.5 [v {:direction :asc}])}]}
                          [:table {:rows [{:v 10} {:v nil} {:v 30}]}]]))
        "nulls are excluded from percentile computation"))

(t/deftest test-percentile-cont-empty
  (t/is (= [{}]
           (tu/query-ra '[:group-by {:columns [{p (percentile_cont 0.5 [v {:direction :asc}])}]}
                          [::tu/pages {v #xt/type [:? :i64]} []]]))
        "empty input returns null (zero-row)"))

(t/deftest test-percentile-cont-desc
  (t/is (= [{:p 30.0}]
           (tu/query-ra '[:group-by {:columns [{p (percentile_cont 0.0 [v {:direction :desc}])}]}
                          [:table {:rows [{:v 10} {:v 20} {:v 30}]}]]))
        "desc ordering: fraction=0 returns the largest (first in desc order)"))

(t/deftest test-percentile-cont-grouped
  (t/is (= #{{:g 1, :p 15.0}
             {:g 2, :p 35.0}}
           (set (tu/query-ra '[:group-by {:columns [g {p (percentile_cont 0.5 [v {:direction :asc}])}]}
                               [:table {:rows [{:g 1, :v 10}
                                               {:g 1, :v 20}
                                               {:g 2, :v 30}
                                               {:g 2, :v 40}]}]])))
        "different medians per group"))

(t/deftest test-percentile-disc-basic
  (t/is (= [{:p 20}]
           (tu/query-ra '[:group-by {:columns [{p (percentile_disc 0.5 [v {:direction :asc}])}]}
                          [:table {:rows [{:v 10} {:v 20} {:v 30}]}]]))
        "disc picks exact value, no interpolation")

  (t/is (= [{:p 10}]
           (tu/query-ra '[:group-by {:columns [{p (percentile_disc 0.0 [v {:direction :asc}])}]}
                          [:table {:rows [{:v 10} {:v 20} {:v 30}]}]]))
        "fraction=0 returns first")

  (t/is (= [{:p 30}]
           (tu/query-ra '[:group-by {:columns [{p (percentile_disc 1.0 [v {:direction :asc}])}]}
                          [:table {:rows [{:v 10} {:v 20} {:v 30}]}]]))
        "fraction=1 returns last"))

(t/deftest test-percentile-disc-desc
  (t/is (= [{:p 30}]
           (tu/query-ra '[:group-by {:columns [{p (percentile_disc 0.0 [v {:direction :desc}])}]}
                          [:table {:rows [{:v 10} {:v 20} {:v 30}]}]]))
        "desc: fraction=0 returns largest"))

(t/deftest test-percentile-disc-grouped
  (t/is (= #{{:g 1, :p 10}
             {:g 2, :p 30}}
           (set (tu/query-ra '[:group-by {:columns [g {p (percentile_disc 0.5 [v {:direction :asc}])}]}
                               [:table {:rows [{:g 1, :v 10}
                                               {:g 1, :v 20}
                                               {:g 2, :v 30}
                                               {:g 2, :v 40}]}]])))
        "disc with groups"))

(t/deftest test-percentile-disc-strings
  (t/is (= [{:p "banana"}]
           (tu/query-ra '[:group-by {:columns [{p (percentile_disc 0.5 [v {:direction :asc}])}]}
                          [:table {:rows [{:v "apple"} {:v "banana"} {:v "cherry"}]}]]))
        "disc works with non-numeric types"))

(t/deftest test-percentile-cont-float-input
  (t/is (= [{:p 2.0}]
           (tu/query-ra '[:group-by {:columns [{p (percentile_cont 0.5 [v {:direction :asc}])}]}
                          [:table {:rows [{:v 1.0} {:v 3.0}]}]]))
        "float inputs work without promotion"))

(t/deftest test-percentile-via-sql
  (xt/submit-tx tu/*node* [[:sql "INSERT INTO scores RECORDS {_id: 1, team: 'a', score: 10},
                                                              {_id: 2, team: 'a', score: 20},
                                                              {_id: 3, team: 'a', score: 30},
                                                              {_id: 4, team: 'b', score: 100},
                                                              {_id: 5, team: 'b', score: 200}"]])

  (t/is (= [{:median 20.0}]
           (xt/q tu/*node* "SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY score) AS median FROM scores WHERE team = 'a'"))
        "SQL PERCENTILE_CONT")

  (t/is (= [{:median 20}]
           (xt/q tu/*node* "SELECT PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY score) AS median FROM scores WHERE team = 'a'"))
        "SQL PERCENTILE_DISC")

  (t/is (= #{{:team "a", :median 20.0}
             {:team "b", :median 150.0}}
           (set (xt/q tu/*node* "SELECT team, PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY score) AS median FROM scores GROUP BY team")))
        "SQL PERCENTILE_CONT with GROUP BY"))

(t/deftest test-percentile-cont-all-nulls
  (t/is (= [{}]
           (tu/query-ra '[:group-by {:columns [{p (percentile_cont 0.5 [v {:direction :asc}])}]}
                          [:table {:rows [{:v nil} {:v nil}]}]]))
        "all null values returns null"))

(t/deftest test-percentile-across-batches
  (t/is (= [{:p 25.0}]
           (tu/query-ra '[:group-by {:columns [{p (percentile_cont 0.5 [v {:direction :asc}])}]}
                          [::tu/pages {v #xt/type :i64}
                           [[{:v 10} {:v 20}]
                            [{:v 30} {:v 40}]]]]))
        "works across multiple batches"))
