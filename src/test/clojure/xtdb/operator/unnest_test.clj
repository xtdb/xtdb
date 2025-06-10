(ns xtdb.operator.unnest-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.test-util :as tu]))

(t/use-fixtures :each tu/with-allocator tu/with-node)

(t/deftest test-simple-unnest
  (let [in-vals [[{:a 1 :b [1 2]} {:a 2 :b [3 4 5]}]
                 [{:a 3 :b []}]
                 [{:a 4 :b [6 7 8]} {:a 5 :b []}]]]

    (t/is (= {:col-types '{a :i64, b [:list :i64], b* :i64}
              :res [[{:a 1, :b [1 2], :b* 1}
                     {:a 1, :b [1 2], :b* 2}
                     {:a 2, :b [3 4 5], :b* 3}
                     {:a 2, :b [3 4 5], :b* 4}
                     {:a 2, :b [3 4 5], :b* 5}]
                    [{:a 4, :b [6 7 8], :b* 6}
                     {:a 4, :b [6 7 8], :b* 7}
                     {:a 4, :b [6 7 8], :b* 8}]]}
             (tu/query-ra [:unnest '{b* b}
                           [::tu/pages '{a :i64, b [:list :i64]} in-vals]]
                          {:preserve-pages? true
                           :with-col-types? true})))

    (t/is (= {:col-types '{a :i64, b [:list :i64], b* :i64, ordinal :i32}
              :res [[{:a 1, :b [1 2], :b* 1, :ordinal 1}
                     {:a 1, :b [1 2], :b* 2, :ordinal 2}
                     {:a 2, :b [3 4 5], :b* 3, :ordinal 1}
                     {:a 2, :b [3 4 5], :b* 4, :ordinal 2}
                     {:a 2, :b [3 4 5], :b* 5, :ordinal 3}]
                    [{:a 4, :b [6 7 8], :b* 6, :ordinal 1}
                     {:a 4, :b [6 7 8], :b* 7, :ordinal 2}
                     {:a 4, :b [6 7 8], :b* 8, :ordinal 3}]]}
             (tu/query-ra [:unnest '{b* b} '{:ordinality-column ordinal}
                           [::tu/pages '{a :i64, b [:list :i64]} in-vals]]
                          {:preserve-pages? true
                           :with-col-types? true
                           :key-fn :snake-case-keyword})))))

(t/deftest test-unnest-operator
  (t/is (= [{:a 1, :b [1 2], :b* 1}
            {:a 1, :b [1 2], :b* 2}
            {:a 2, :b [3 4 5], :b* 3}
            {:a 2, :b [3 4 5], :b* 4}
            {:a 2, :b [3 4 5], :b* 5}]
           (tu/query-ra '[:unnest {b* b}
                          [:table ?x]]
                        {:args {:x [{:a 1, :b [1 2]} {:a 2, :b [3 4 5]}]}})))

  (t/is (= [{:a 1, :b* 1} {:a 1, :b* 2}]
           (tu/query-ra '[:project [a b*]
                          [:unnest {b* b}
                           [:table ?x]]]
                        {:args {:x [{:a 1, :b [1 2]} {:a 2, :b []}]}}))
        "skips rows with empty lists")

  (t/is (= [{:a 1, :b* 1} {:a 1, :b* 2}]
           (tu/query-ra '[:project [a b*]
                          [:unnest {b* b}
                           [:table ?x]]]
                        {:args {:x [{:a 2, :b 1} {:a 1, :b [1 2]}]}}))
        "skips rows with non-list unnest column")

  (t/is (= [{:a 1, :b* 1} {:a 1, :b* "foo"}]
           (tu/query-ra '[:project [a b*]
                          [:unnest {b* b}
                           [:table ?x]]]
                        {:args {:x [{:a 1, :b [1 "foo"]}]}}))
        "handles multiple types")

  (t/is (= {{:a 1, :b* 1} 1, {:a 1, :b* "foo"} 1}
           (frequencies
            (tu/query-ra '[:project [a b*]
                           [:unnest {b* b}
                            [:table ?x]]]
                         {:args {:x [{:a 1, :b #{1 "foo"}}]}})))
        "handles sets")

  (t/is (= {{:a 1, :b* 1} 1, {:a 1, :b* "foo"} 1, {:a 3, :b* 2} 1, {:a 3, :b* "bar"} 1}
           (frequencies
            (tu/query-ra '[:project [a b*]
                           [:unnest {b* b}
                            [:table ?x]]]
                         {:args {:x [{:a 1, :b #{1 "foo"}}
                                     {:a 2, :b "not-a-set"}
                                     {:a 3, :b #{2 "bar"}}]}})))
        "handles mixed lists + sets + other things")

  (t/is (= [{:a 1, :b* 1, :ordinal 1}
            {:a 1, :b* 2, :ordinal 2}
            {:a 2, :b* 3, :ordinal 1}
            {:a 2, :b* 4, :ordinal 2}
            {:a 2, :b* 5, :ordinal 3}]
           (tu/query-ra '[:project [a b* ordinal]
                          [:unnest {b* b} {:ordinality-column ordinal}
                           [:table ?x]]]
                        {:args {:x [{:a 1 :b [1 2]} {:a 2 :b [3 4 5]}]}
                         :key-fn :snake-case-keyword}))
        "with ordinality")

  (t/is (= [{:ordinal 1, :s 4}
            {:ordinal 2, :s 6}
            {:ordinal 3, :s 5}]
           (tu/query-ra '[:project [ordinal s]
                          [:group-by [ordinal {s (sum b*)}]
                           [:unnest {b* b} {:ordinality-column ordinal}
                            [:table ?x]]]]
             {:args {:x [{:a 1 :b [1 2]} {:a 2 :b [3 4 5]}]}}))
        "group by ordinality"))

(t/deftest test-vector-not-found-4343
  (xt/execute-tx tu/*node* [[:put-docs :f
                             {:xt/id 1, :type "foo", :pid 1, :fields [{:value "v1"} {:value "v2"}]}
                             {:xt/id 2, :type "foo", :pid 2, :fields []}]])

  (t/is (= [{:xt/id 1, :field {:value "v1"}} {:xt/id 1, :field {:value "v2"}}]
           (xt/q tu/*node* ["SELECT f._id, field FROM f, UNNEST(f.fields) fields_table (field) WHERE f.type IN (?)" "foo"]))))


(t/deftest fix-ordinality-field-naming
  (xt/execute-tx tu/*node* [[:put-docs :r1 {:xt/id 0, :xs [10 20 30]} ]])

  (t/is (= [{:o1 1, :o2 1} {:o1 2, :o2 2} {:o1 3, :o2 3}]
           (tu/query-ra
            '[:project [o1 o2]
              [:map
               [{o2 o1}]
               [:unnest
                {x1 unnest}
                {:ordinality-column o1}
                [:map
                 [{unnest xs}]
                 [:scan {:table public/r1} [xs]]]]]]
            {:node tu/*node*}))))

(t/deftest unnest-issue-4441
  (xt/execute-tx tu/*node* [[:put-docs :r1 {:xt/id 0, :xs [10 20 30]} {:xt/id 1, :xs [100 200 300]}]])

  (t/is (= [{:x1 100, :xs [100 200 300]}
            {:x1 200, :xs [100 200 300]}
            {:x1 300, :xs [100 200 300]}
            {:x1 10, :xs [10 20 30]}
            {:x1 20, :xs [10 20 30]}
            {:x1 30, :xs [10 20 30]}]
           (tu/query-ra
            '[:unnest
              {x1 xs}
              [:scan {:table public/r1} [xs]]]
            {:node tu/*node*}))))
