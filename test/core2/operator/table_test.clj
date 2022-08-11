(ns core2.operator.table-test
  (:require [clojure.test :as t]
            [core2.test-util :as tu]
            [core2.util :as util])
  (:import java.time.Duration))

(t/use-fixtures :each tu/with-allocator)

(t/deftest test-table
  (t/is (= {:res [{:a 12, :b "foo" :c 1.2, :d nil, :e true, :f (Duration/ofHours 1)}
                  {:a 100, :b "bar", :c 3.14, :d (util/->zdt #inst "2020"), :e 10, :f (Duration/ofMinutes 1)}]
            :col-types '{a :i64, b :utf8, c :f64,
                         d [:union #{:null [:timestamp-tz :micro "UTC"]}]
                         e [:union #{:bool :i64}]
                         f [:duration :micro]}}
           (-> (tu/query-ra '[:table [a b c d e f] ?table]
                            {:params {'?table [{:a 12, :b "foo" :c 1.2 :d nil :e true :f (Duration/ofHours 1)}
                                               {:a 100, :b "bar", :c 3.14, :d #inst "2020", :e 10, :f (Duration/ofMinutes 1)}]}
                             :with-col-types? true}))))

  (t/is (= {:res [{:a 12, :b "foo" :c 1.2 :d nil :e true}
                  {:a 100, :b "bar" :c 3.14 :d (util/->zdt #inst "2020") :e 10}]
            :col-types '{a :i64, b :utf8, c :f64,
                         d [:union #{:null [:timestamp-tz :micro "UTC"]}]
                         e [:union #{:bool :i64}]}}
           (-> (tu/query-ra '[:table [{:a 12, :b "foo", :c 1.2, :d nil, :e true}
                                      {:a 100, :b "bar", :c 3.14, :d #inst "2020", :e 10}]]
                             {:with-col-types? true})))
        "inline table")

  (t/is (= {:res [], :col-types {}}
           (-> (tu/query-ra '[:table ?table]
                            {:params {'?table []}
                             :with-col-types? true})))
        "empty")

  (t/is (= {:res [{:a 12, :b "foo"}, {:a 100, :b nil}]
            :col-types '{a :i64, b [:union #{:utf8 :null}]}}
           (-> (tu/query-ra '[:table ?table]
                            {:params {'?table [{:a 12, :b "foo"}
                                               {:a 100}]}
                             :with-col-types? true})))
        "differing columns")

  (t/is (= {:res [{:a 12}]
            :col-types '{a :i64}}
           (-> (tu/query-ra '[:table [a] ?table]
                            {:params {'?table [{:a 12, :b "foo"}]}
                             :with-col-types? true})))
        "restricts to provided col-names")

  (t/is (= [{} {} {}]
           (tu/query-ra [:table [{} {} {}]]
                        {}))
        "table with no cols"))

(t/deftest test-table-handles-exprs
  (t/is (= [{:a 3, :b false}
            {:a nil, :b [24 24]}
            {:a 3, :b 4}]
           (tu/query-ra '[:table [{:a (+ 1 2), :b (> 3 4)}
                                  {:a nil, :b [24 (* 3 8)]}
                                  {:a 3, :b 4}]]
                        {})))

  (t/is (= [{:a 1, :b 2}, {:a 3, :b 4}]
           (tu/query-ra '[:table [{:a ?p1, :b ?p2}
                                  {:a ?p3, :b ?p4}]]
                        {:params {'?p1 1, '?p2 2, '?p3 3, '?p4 4}}))))

(t/deftest test-table-handles-symbols
  (t/is (= '[{:x50 true}]
           (tu/query-ra '[:top {:limit 1}
                          [:union-all
                           [:project
                            [{x50 true}]
                            [:select (= ?x53 x48) [:table [{x48 "AIR"} {x48 "AIR REG"}]]]]
                           [:table [{x50 false}]]]]
                        {:params {'?x53 "AIR"}})))

  (t/is (= '[{:x50 true}]
           (tu/query-ra '[:top {:limit 1}
                          [:union-all
                           [:project
                            [{x50 true}]
                            [:select (= ?x53 x48) [:table [{x48 "AIR"} {x48 "AIR REG"}]]]]
                           [:table [{x50 false}]]]]
                        {:params {'?x53 "AIR REG"}})))

  (t/is (= '[{:x50 false}]
           (tu/query-ra '[:top {:limit 1}
                          [:union-all
                           [:project
                            [{x50 true}]
                            [:select (= ?x53 x48) [:table [{x48 "AIR"} {x48 "AIR REG"}]]]]
                           [:table [{x50 false}]]]]
                        {:params {'?x53 "RAIL"}}))))
