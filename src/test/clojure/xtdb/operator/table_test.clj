(ns xtdb.operator.table-test
  (:require [clojure.spec.alpha :as s]
            [clojure.test :as t]
            [xtdb.logical-plan :as lp]
            [xtdb.test-util :as tu]
            [xtdb.time :as time])
  (:import java.time.Duration))

(t/use-fixtures :each tu/with-allocator tu/with-node)

(t/deftest test-table
  (t/is (= {:res [{:a 12, :b "foo" :c 1.2, :e true, :f (Duration/ofHours 1)}
                  {:a 100, :b "bar", :c 3.14, :d (time/->zdt #inst "2020"), :e 10, :f (Duration/ofMinutes 1)}]
            :types {'a #xt/type :i64, 'b #xt/type :utf8, 'c #xt/type :f64,
                    'd #xt/type [:? :instant]
                    'e #xt/type [:union :bool :i64]
                    'f #xt/type [:duration :micro]}}
           (-> (tu/query-ra '[:table [a b c d e f] ?table]
                            {:args {:table [{:a 12, :b "foo" :c 1.2 :d nil :e true :f (Duration/ofHours 1)}
                                            {:a 100, :b "bar", :c 3.14, :d #inst "2020", :e 10, :f (Duration/ofMinutes 1)}]}
                             :with-types? true}))))

  (t/is (= {:res [{:a 12, :b "foo", :c 1.2, :e true}
                  {:a 100, :b "bar", :c 3.14, :d (time/->zdt #inst "2020"), :e 10}]
            :types '{a #xt/type :i64, b #xt/type :utf8, c #xt/type :f64,
                     d #xt/type [:? :instant]
                     e #xt/type [:union :i64 :bool]}}
           (-> (tu/query-ra '[:table [{:a 12, :b "foo", :c 1.2, :d nil, :e true}
                                      {:a 100, :b "bar", :c 3.14, :d #inst "2020", :e 10}]]
                            {:with-types? true})))
        "inline table")

  (t/is (= {:res [], :types {}}
           (-> (tu/query-ra '[:table ?table]
                            {:args {:table []}
                             :with-types? true})))
        "empty")

  (t/is (= {:res [{:a 12, :b "foo"}, {:a 100}]
            :types '{a #xt/type :i64, b #xt/type [:? :utf8]}}
           (-> (tu/query-ra '[:table ?table]
                            {:args {:table [{:a 12, :b "foo"}
                                            {:a 100}]}
                             :with-types? true})))
        "differing columns")

  (t/is (= {:res [{:a 12}]
            :types '{a #xt/type :i64}}
           (-> (tu/query-ra '[:table [a] ?table]
                            {:args {:table [{:a 12, :b "foo"}]}
                             :with-types? true})))
        "restricts to provided col-names")

  (t/is (= [{} {} {}]
           (tu/query-ra [:table [{} {} {}]]
                        {}))
        "table with no cols")

  (t/is (= [{:a #{1 3 2}}]
           (tu/query-ra [:table [{:a #{1 2 3}}]]
                        {}))
        "table with sets"))

(t/deftest test-table-handles-exprs
  (t/is (= [{:a 3, :b false}
            {:b [24 24]}
            {:a 3, :b 4}]
           (tu/query-ra '[:table [{:a (+ 1 2), :b (> 3 4)}
                                  {:a nil, :b [24 (* 3 8)]}
                                  {:a 3, :b 4}]]
                        {})))

  (t/is (= [{:a 1, :b 2}, {:a 3, :b 4}]
           (tu/query-ra '[:table [{:a ?p1, :b ?p2}
                                  {:a ?p3, :b ?p4}]]
                        {:args {:p1 1, :p2 2, :p3 3, :p4 4}})))

  (t/is (= [{:a {:baz 1}, :b 2}]
           (tu/query-ra '[:table [{:a {:baz ?p1} :b ?p2}]]
                        {:args {:p1 1, :p2 2, :p3 3, :p4 4}}))
        "nested param")

  (t/is (= [{:foo :bar, :baz {:nested-foo :bar}}]
           (tu/query-ra ' [:table [{:foo :bar, :baz {:nested_foo ?nested_param}}]]
                          {:args {:nested_param :bar}}))
        "nested param with need for normalisation"))

(t/deftest test-table-handles-symbols
  (t/is (= '[{:x50 true}]
           (tu/query-ra '[:top {:limit 1}
                          [:union-all
                           [:project
                            [{x50 true}]
                            [:select (== ?x53 x48) [:table [{x48 "AIR"} {x48 "AIR REG"}]]]]
                           [:table [{x50 false}]]]]
                        {:args {:x53 "AIR"}})))

  (t/is (= '[{:x50 true}]
           (tu/query-ra '[:top {:limit 1}
                          [:union-all
                           [:project
                            [{x50 true}]
                            [:select (== ?x53 x48) [:table [{x48 "AIR"} {x48 "AIR REG"}]]]]
                           [:table [{x50 false}]]]]
                        {:args {:x53 "AIR REG"}})))

  (t/is (= '[{:x50 false}]
           (tu/query-ra '[:top {:limit 1}
                          [:union-all
                           [:project
                            [{x50 true}]
                            [:select (== ?x53 x48) [:table [{x48 "AIR"} {x48 "AIR REG"}]]]]
                           [:table [{x50 false}]]]]
                        {:args {:x53 "RAIL"}}))))

(t/deftest test-incorrect-relation-params
  (t/is
   (thrown-with-msg? RuntimeException #"Table param must be of type struct list"
                     (tu/query-ra '[:table ?a]
                                  {:args {:a 1}}))
   "param not list type")
  (t/is
   (thrown-with-msg? RuntimeException #"Table param must be of type struct list"
                     (tu/query-ra '[:table ?a]
                                  {:args {:a [1 "foo" :foo]}}))
   "param not struct list type"))

(t/deftest do-not-leak-memory-on-expression-errors
  (t/is
   (thrown-with-msg? RuntimeException #"division by zero"
                     (tu/query-ra '[:table [x9] [{x9 -54} {x9 (/ 35 0)}]]
                                  {:node tu/*node*}))))

(t/deftest test-absent-columns
  (t/is (= '{:res [{:x5 1} {:x6 -77}],
             :types {x5 #xt/type [:? :i64], x6 #xt/type [:? :i64]}}
           (tu/query-ra
            '[:table [{:x5 1} {:x6 -77}]]
            {:with-types? true})))

  (t/is (= '{:res [{:x5 1} {:x6 -77}],
             :types {x5 #xt/type [:? :i64], x6 #xt/type [:? :i64]}}
           (tu/query-ra '[:table ?table]
                        {:args {:table [{:x5 1} {:x6 -77}]}
                         :with-types? true}))
        "differing columns")

  (t/is (= {:res [{:a 12.4, :b 10} {:b 15} {:a 100, :b 83} {:a 83.0, :b 100}],
            :types '{a #xt/type [:union [:? :f64] :i64], b #xt/type :i64}}

           (tu/query-ra '[:table ?table]
                        {:args {:table [{:a 12.4, :b 10}, {:a nil, :b 15}, {:a 100, :b 83}, {:a 83.0, :b 100}]}
                         :with-types? true}))
        "actual nils"))

(t/deftest test-single-col
  (t/is (= '{:res [{:x5 1} {:x5 2} {:x5 3}],
             :types {x5 #xt/type :i64}}
           (tu/query-ra '[:table {x5 [1 2 3]}]
                        {:with-types? true})))

  (t/is (= '{:res [{:x5 1} {:x5 2.0} {:x5 3}],
             :types {x5 #xt/type [:union :f64 :i64]}}
           (tu/query-ra '[:table {x5 [1 2.0 3]}]
                        {:with-types? true})))

  (t/is (= {:res [{:unnest-param 12.4} {} {:unnest-param 100} {:unnest-param 83.0}],
            :types '{unnest-param #xt/type [:union :f64 [:? :null] :i64]}}

           (tu/query-ra '[:table {unnest-param ?coll}]
                        {:args {:coll [12.4, nil, 100, 83.0]}
                         :with-types? true})))

  (t/is (= {:res [], :types '{b #xt/type [:? :null]}}
           (tu/query-ra '[:table {b nil}]
                        {:with-types? true}))
        "nil value - #4075"))

(t/deftest test-table-with-map-params
  (t/is (= [{:a 4.2} {:b "1", :c 2, :a 0} {:b 2, :a 1}]
           (tu/query-ra '[:table [{:a 4.2} ?record {:a 1, :b 2}]]
                        {:args {:record {:a 0, :b "1", :c 2}}}))))

(t/deftest test-table-stats
  (t/testing "row table stats - should return stats/row-count"
    (t/is (= {:row-count 4}
             (:stats
              (lp/emit-expr (s/conform ::lp/logical-plan
                                       '[:table [{:foo 1 :bar "woo"}
                                                 {:foo 2 :bar "yay"}
                                                 {:foo 3 :bar "yipee"}
                                                 {:foo 4 :bar "huzzah"}]])
                            {})))))

  ;; Other types of table do NOT currently return stats
  (t/testing "col table stats"
    (t/is (nil? (:stats (lp/emit-expr (s/conform ::lp/logical-plan
                                                 '[:table {foo [1 2 3 4]}])
                                      {}))))))

