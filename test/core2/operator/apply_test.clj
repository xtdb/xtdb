(ns core2.operator.apply-test
  (:require [clojure.test :as t]
            [core2.test-util :as tu]))

(t/deftest test-apply-operator
  (letfn [(q [mode]
            (tu/query-ra [:apply mode '{c-id ?c-id}
                          [::tu/blocks
                           [[{:c-id "c1", :c-name "Alan"}
                             {:c-id "c2", :c-name "Bob"}
                             {:c-id "c3", :c-name "Charlie"}]]]
                          [:select '(= o-customer-id ?c-id)
                           [::tu/blocks
                            [[{:o-customer-id "c1", :o-value 12.34}
                              {:o-customer-id "c1", :o-value 14.80}]
                             [{:o-customer-id "c2", :o-value 91.46}
                              {:o-customer-id "c4", :o-value 55.32}]]]]]
                         {}))]

    (t/is (= [{:c-id "c1", :c-name "Alan", :o-customer-id "c1", :o-value 12.34}
              {:c-id "c1", :c-name "Alan", :o-customer-id "c1", :o-value 14.80}
              {:c-id "c2", :c-name "Bob", :o-customer-id "c2", :o-value 91.46}]

             (q :cross-join)))

    (t/is (= [{:c-id "c1", :c-name "Alan", :o-customer-id "c1", :o-value 12.34}
              {:c-id "c1", :c-name "Alan", :o-customer-id "c1", :o-value 14.80}
              {:c-id "c2", :c-name "Bob", :o-customer-id "c2", :o-value 91.46}
              {:c-id "c3", :c-name "Charlie", :o-customer-id nil, :o-value nil}]

             (q :left-outer-join)))

    (t/is (= [{:c-id "c1", :c-name "Alan"}, {:c-id "c2", :c-name "Bob"}]
             (q :semi-join)))

    (t/is (= [{:c-id "c3", :c-name "Charlie"}]
             (q :anti-join)))))

(t/deftest test-apply-mark
  (t/is (= {:res [{:c-id "c1", :c-name "Alan", :match true}
                  {:c-id "c2", :c-name "Bob", :match true}
                  {:c-id "c3", :c-name "Charlie", :match false}]
            :col-types '{c-id :utf8, c-name :utf8, match [:union #{:null :bool}]}}
           (-> (tu/query-ra [:apply '{:mark-join {match (= ?c-id o-customer-id)}} '{c-id ?c-id}
                             [::tu/blocks
                              [[{:c-id "c1", :c-name "Alan"}
                                {:c-id "c2", :c-name "Bob"}
                                {:c-id "c3", :c-name "Charlie"}]]]
                             [::tu/blocks
                              [[{:o-customer-id "c1"}
                                {:o-customer-id "c1"}
                                {:o-customer-id "c2"}
                                {:o-customer-id "c4"}]]]]
                            {:with-col-types? true}))))

  (t/is (= {:res [{:x 0, :match nil}]
            :col-types '{x :i64, match [:union #{:null :bool}]}}
           (-> (tu/query-ra '[:apply {:mark-join {match (= 4 y)}} {}
                              [:table [{x 0}]]
                              [:table [{y nil}]]]
                            {:with-col-types? true})))
        "nil in RHS")

  (t/is (= [{:x 0, :match false} {:x 1, :match false}]
           (tu/query-ra '[:apply {:mark-join {match (= nil z)}} {}
                          [:table [{:x 0}, {:x 1}]]
                          [:project [{z 1}]
                           [:select false
                            [:table [{:y 0}, {:y 1}]]]]]))
        "NULL IN {}"))

(t/deftest test-apply-single
  (t/is (= [{:y 0, :a 1, :b 2}]
           (tu/query-ra '[:apply :single-join {}
                          [:table [{:y 0}]]
                          [:table ?x]]
                        {:params '{?x [{:a 1, :b 2}]}})))

  (t/is (thrown-with-msg? RuntimeException
                          #"cardinality violation"
                          (tu/query-ra '[:apply :single-join {}
                                         [:table [{:y 0}]]
                                         [:table ?x]]
                                       {:params '{?x [{:a 1, :b 2} {:a 3, :b 4}]}}))
        "throws on cardinality > 1")

  (t/testing "returns null on empty"
    (t/is (= [{:y 0}]
             (tu/query-ra '[:apply :single-join {}
                            [:table [{:y 0}]]
                            [:table ?x]]
                          {:params '{?x []}})))

    (t/is (= [{:y 0, :a nil, :b nil}]
             (tu/query-ra '[:apply :single-join {}
                            [:table [{:y 0}]]
                            [:table [a b] ?x]]
                          {:params '{?x []}})))))

(t/deftest test-apply-empty-rel-bug-237
  (t/is (= {:res [{:x3 nil}], :col-types '{x3 [:union #{:null :i64}]}}
           (-> (tu/query-ra
                '[:group-by [{x3 (sum x2)}]
                  [:apply :cross-join {}
                   [:table [{x1 15}]]
                   [:select false
                    [:table [{x2 20}]]]]]
                {:with-col-types? true}))))

  (t/is (= {:res [], :col-types '{x1 :i64, x2 :i64}}
           (-> (tu/query-ra '[:project [x1 x2]
                              [:apply :cross-join {}
                               [:table [{x1 15}]]
                               [:select false
                                [:table [{x2 20}]]]]]
                            {:with-col-types? true})))))
