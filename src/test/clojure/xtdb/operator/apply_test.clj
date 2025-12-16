(ns xtdb.operator.apply-test
  (:require [clojure.test :as t :refer [deftest]]
            [xtdb.test-util :as tu]))

(t/use-fixtures :each tu/with-allocator)

(t/deftest test-apply-operator
  (letfn [(q [mode]
            (tu/query-ra [:apply mode '{c_id ?c_id}
                          [::tu/pages
                           [[{:c_id "c1", :c_name "Alan"}
                             {:c_id "c2", :c_name "Bob"}
                             {:c_id "c3", :c_name "Charlie"}]]]
                          [:select {:predicate '(== o_customer_id ?c_id)}
                           [::tu/pages
                            [[{:o_customer_id "c1", :o_value 12.34}
                              {:o_customer_id "c1", :o_value 14.80}]
                             [{:o_customer_id "c2", :o_value 91.46}
                              {:o_customer_id "c4", :o_value 55.32}]]]]]
                         {}))]

    (t/is (= [{:c-id "c1", :c-name "Alan", :o-customer-id "c1", :o-value 12.34}
              {:c-id "c1", :c-name "Alan", :o-customer-id "c1", :o-value 14.80}
              {:c-id "c2", :c-name "Bob", :o-customer-id "c2", :o-value 91.46}]

             (q :cross-join)))

    (t/is (= [{:c-id "c1", :c-name "Alan", :o-customer-id "c1", :o-value 12.34}
              {:c-id "c1", :c-name "Alan", :o-customer-id "c1", :o-value 14.80}
              {:c-id "c2", :c-name "Bob", :o-customer-id "c2", :o-value 91.46}
              {:c-id "c3", :c-name "Charlie"}]

             (q :left-outer-join)))

    (t/is (= [{:c-id "c1", :c-name "Alan"}, {:c-id "c2", :c-name "Bob"}]
             (q :semi-join)))

    (t/is (= [{:c-id "c3", :c-name "Charlie"}]
             (q :anti-join)))))

(t/deftest test-apply-mark
  (t/is (= {:res [{:c-id "c1", :c-name "Alan", :match true}
                  {:c-id "c2", :c-name "Bob", :match true}
                  {:c-id "c3", :c-name "Charlie", :match false}]
            :types '{c_id #xt/type :utf8, c_name #xt/type :utf8, match #xt/type [:? :bool]}}
           (-> (tu/query-ra [:apply '{:mark-join {match (== ?c_id o_customer_id)}} '{c_id ?c_id}
                             [::tu/pages
                              [[{:c_id "c1", :c_name "Alan"}
                                {:c_id "c2", :c_name "Bob"}
                                {:c_id "c3", :c_name "Charlie"}]]]
                             [::tu/pages
                              [[{:o_customer_id "c1"}
                                {:o_customer_id "c1"}
                                {:o_customer_id "c2"}
                                {:o_customer_id "c4"}]]]]
                            {:with-types? true}))))

  (t/is (= {:res [{:x 0}]
            :types '{x #xt/type :i64, match #xt/type [:? :bool]}}
           (-> (tu/query-ra '[:apply {:mark-join {match (== 4 y)}} {}
                              [:table [{x 0}]]
                              [:table [{y nil}]]]
                            {:with-types? true})))
        "nil in RHS")

  (t/is (= [{:x 0, :match false} {:x 1, :match false}]
           (tu/query-ra '[:apply {:mark-join {match (== nil z)}} {}
                          [:table [{:x 0}, {:x 1}]]
                          [:project [{z 1}]
                           [:select {:predicate false}
                            [:table [{:y 0}, {:y 1}]]]]]))
        "NULL IN {}"))

(t/deftest test-apply-single
  (t/is (= [{:y 0}
            {:y 1, :a 1, :b 2}]
           (tu/query-ra '[:apply :single-join {y ?y}
                          [:table [{:y 0} {:y 1}]]
                          [:select {:predicate (== ?y a)}
                           [:table ?x]]]
                        {:args {:x [{:a 1, :b 2}]}})))

  (t/is (thrown-with-msg? RuntimeException
                          #"cardinality violation"
                          (tu/query-ra '[:apply :single-join {}
                                         [:table [{:y 0}]]
                                         [:table ?x]]
                                       {:args {:x [{:a 1, :b 2} {:a 3, :b 4}]}}))
        "throws on cardinality > 1")

  (t/testing "returns null on empty"
    (t/is (= [{:y 0}]
             (tu/query-ra '[:apply :single-join {}
                            [:table [{:y 0}]]
                            [:table ?x]]
                          {:args {:x []}})))

    (t/is (= [{:y 0}]
             (tu/query-ra '[:apply :single-join {}
                            [:table [{:y 0}]]
                            [:table [a b] ?x]]
                          {:args {:x []}})))))

(t/deftest test-apply-empty-rel-bug-237
  (t/is (= {:res [{}], :types '{x3 #xt/type [:? :i64]}}
           (-> (tu/query-ra
                '[:group-by [{x3 (sum x2)}]
                  [:apply :cross-join {}
                   [:table [{x1 15}]]
                   [:select {:predicate false}
                    [:table [{x2 20}]]]]]
                {:with-types? true}))))

  (t/is (= {:res [], :types '{x1 #xt/type :i64, x2 #xt/type :i64}}
           (-> (tu/query-ra '[:project [x1 x2]
                              [:apply :cross-join {}
                               [:table [{x1 15}]]
                               [:select {:predicate false}
                                [:table [{x2 20}]]]]]
                            {:with-types? true})))))

(t/deftest test-nested-apply
  (t/is (= [{:z 0, :x 0, :y 0}
            {:z 0, :x 1, :y 0}
            {:z 1, :x 0, :y 1}
            {:z 1, :x 1, :y 1}]
           (tu/query-ra
             '[:apply :cross-join {z ?x2}
               [:table [{:z 0}, {:z 1}]]
               [:apply :single-join {}
                [:table [{:x 0}, {:x 1}]]
                [:select {:predicate (== ?x2 y)}
                 [:table [{:y 0}, {:y 1}]]]]] {}))))

(t/deftest test-shadowed-param
  (t/is (= [{:z 0, :x 1}
            {:z 1, :x 1}]
           (tu/query-ra
             '[:apply :cross-join {z ?foo}
               [:table [{:z 0}, {:z 1}]]
               [:apply :single-join {x ?foo}
                [:table [{:x 1}]]
                [:select {:predicate (== ?foo y)}
                 [:table [{:y 0}]]]]] {}))))

(t/deftest test-forwarding-nullable-type-information-494
  (t/is (= [{:x 0, :z 0, :y 0}]
           (tu/query-ra
             '[:select {:predicate (== z y)}
               [:apply :single-join {x ?x1}
                [:apply :single-join {x ?x2}
                 [:table [{:x 0}, {:x 1}]]
                 [:select {:predicate (== ?x2 z)}
                  [:table [{:z 0}, {:z 2}]]]]
                [:select {:predicate (== ?x1 y)}
                 [:table [{:y 0}, {:y 2}]]]]] {}))))

(deftest test-missing-column-in-independent-rel
  (t/is
    (= [{:foo 1 :baz 1}]
       (tu/query-ra
         '[:apply :cross-join
           {foo ?bar}
           [:table [{:foo 1}]]
           [:select {:predicate (== baz ?bar)}
            [:table [{:baz 1}]]]] {}))
    "col with non null type")

  (t/is
    (= [{:baz 1}]
       (tu/query-ra
         '[:apply :cross-join
           {foo ?bar}
           [:table [{:foo nil}]]
           [:select {:predicate (nil? ?bar)}
            [:table [{:baz 1}]]]] {}))
    "col with null type")

  (t/is
    (thrown-with-msg?
      Exception
      #"Column missing from independent relation: not_foo"
      (tu/query-ra
        '[:apply :cross-join
          {not_foo ?bar}
          [:table [{:foo 1}]]
          [:select {:predicate (== baz ?bar)}
           [:table [{:baz 1}]]]] {})
      "not_foo missing")))
