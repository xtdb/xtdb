(ns xtdb.logical-plan-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.logical-plan :as lp]
            [xtdb.sql :as sql]
            xtdb.sql-test
            [xtdb.test-util :as tu]))

(t/use-fixtures :each tu/with-mock-clock tu/with-node)

(t/deftest test-count-star-rule-9
  (t/testing "count-star should be rewritten to count(dep-col) where dep col is a projected inner col of value 1")
  (xt/execute-tx tu/*node* [[:put-docs :t1 {:xt/id 1 :x 1}]
                            [:put-docs :t2 {:xt/id 2 :y 2}]])

  (t/is (= [{:t1-count 10, :y 2}]
           (xt/q tu/*node* "SELECT (SELECT (10 + count(*) + count(*)) FROM t1 WHERE t1.x = t2.y ) AS t1_count, t2.y FROM t2"))))

(t/deftest test-not-equal
  (xt/execute-tx tu/*node* [[:put-docs :t {:xt/id 1 :x 1}]
                            [:put-docs :t {:xt/id 2 :x 2}]])
  (t/is (= [{:cnt 1}]
           (xt/q tu/*node* "SELECT count(*) as cnt FROM t WHERE x != 1"))))

(t/deftest test-push-predicate-down-past-period-constructor
  (t/is
   (=plan-file
    "test-push-predicate-down-past-period-constructor-valid"
    (lp/push-predicate-down-past-period-constructor
     true
     (xt/template
      [:select
       {:predicate (and
                    (<=
                     (lower ~(sql/->col-sym '_valid_time))
                     (lower
                      (period
                       #xt/zoned-date-time "2000-01-01T00:00Z"
                       #xt/zoned-date-time "2001-01-01T00:00Z")))
                    (>=
                     (coalesce (upper ~(sql/->col-sym '_valid_time)) xtdb/end-of-time)
                     (coalesce
                      (upper
                       (period
                        #xt/zoned-date-time "2000-01-01T00:00Z"
                        #xt/zoned-date-time "2001-01-01T00:00Z"))
                      xtdb/end-of-time)))}
       [:project
        {:projections [{~(sql/->col-sym '_valid_time)
                        (period ~(sql/->col-sym '_valid_from)
                                ~(sql/->col-sym '_valid_to))}]}
        [:scan {:db-name "xtdb", :table public/docs
                :columns [~(sql/->col-sym '_valid_from) ~(sql/->col-sym '_valid_to)]}]]]))))

  (t/testing "only pushes past period constructors"
    (t/is
     (= nil
        (lp/push-predicate-down-past-period-constructor
         true
         (xt/template
          [:select
           {:predicate (and
                        (<=
                         (lower ~(sql/->col-sym '_valid_time))
                         (lower
                          (period
                           #xt/zoned-date-time "2000-01-01T00:00Z"
                           #xt/zoned-date-time "2001-01-01T00:00Z")))
                        (>=
                         (coalesce (upper ~(sql/->col-sym '_valid_time)) xtdb/end-of-time)
                         (coalesce
                          (upper
                           (period
                            #xt/zoned-date-time "2000-01-01T00:00Z"
                            #xt/zoned-date-time "2001-01-01T00:00Z"))
                          xtdb/end-of-time)))}
           [:project
            {:projections [{~(sql/->col-sym '_valid_time) (+ 1 ~(sql/->col-sym '_valid_from))}]}
            [:scan {:db-name "xtdb", :table public/docs
                    :columns [~(sql/->col-sym '_valid_from) ~(sql/->col-sym '_valid_to)]}]]])))))

  (t/testing "scalar extends expressions are handled"
    (t/is
     (=plan-file
      "test-push-predicate-down-past-period-constructor-scalar-extends"
      (lp/push-predicate-down-past-period-constructor
       true
       (xt/template
        [:select
         {:predicate '(== ~(sql/->col-sym '_valid_time) 1)}
         [:project
          {:projections [{~(sql/->col-sym '_foo) 4}
                         {~(sql/->col-sym '_valid_time)
                          (period ~(sql/->col-sym '_valid_from)
                                  ~(sql/->col-sym '_valid_to))}]}
          [:scan {:db-name "xtdb", :table public/docs, :columns [~(sql/->col-sym '_bar)]}]]])))))

  (t/testing "only push predicate if all columns referenced (aside from the new period) present in inner rel"
    (t/is
     (nil?
      (lp/push-predicate-down-past-period-constructor
       true
       (xt/template
        [:select
         {:predicate '(== ~(sql/->col-sym '_valid_time) ~(sql/->col-sym '_foo))}
         [:project
          {:projections [{~(sql/->col-sym '_foo) 4}
                         {~(sql/->col-sym '_valid_time)
                          (period ~(sql/->col-sym '_valid_from)
                                  ~(sql/->col-sym '_valid_to))}]}
          [:scan {:db-name "xtdb", :table public/docs, :columns [~(sql/->col-sym '_bar)]}]]]))))))

(t/deftest test-remove-redundant-period-constructors
  (t/is
   (= #xt/zoned-date-time "2001-01-01T00:00Z"
      (lp/remove-redudant-period-constructors
       (xt/template
        (upper
         (period
          #xt/zoned-date-time "2000-01-01T00:00Z"
          #xt/zoned-date-time "2001-01-01T00:00Z"))))))

  (t/is
   (= #xt/zoned-date-time "2000-01-01T00:00Z"
      (lp/remove-redudant-period-constructors
       (xt/template
        (lower
         (period
          #xt/zoned-date-time "2000-01-01T00:00Z"
          #xt/zoned-date-time "2001-01-01T00:00Z"))))))

  (t/is
   (= nil
      (lp/remove-redudant-period-constructors
       (xt/template
        (lower
         (+ 1 1))))))

  (t/is
   (= nil
      (lp/remove-redudant-period-constructors
       (xt/template
        (period
         #xt/zoned-date-time "2000-01-01T00:00Z"
         #xt/zoned-date-time "2001-01-01T00:00Z"))))))

(t/deftest test-optimise-contains-period-predicate
  (t/testing "only able to optimise the case where both arguments are period constructors"
    (let [f1 #xt/zoned-date-time "2000-01-01T00:00Z"
          t1 #xt/zoned-date-time "2001-01-01T00:00Z"
          f2 #xt/zoned-date-time "2002-01-01T00:00Z"
          t2 #xt/zoned-date-time "2003-01-01T00:00Z"]
      (t/is
       (=
        (xt/template
         (and (<= ~f1 ~f2)
              (>=
               (coalesce ~t1 xtdb/end-of-time)
               (coalesce ~t2 xtdb/end-of-time))))
        (lp/optimise-contains-period-predicate
         (xt/template
          (contains?
           (period ~f1 ~t1)
           (period ~f2 ~t2))))))

      (t/is
       (nil? (lp/optimise-contains-period-predicate
              (xt/template
               (contains?
                (period ~f1 ~t1)
                ~(sql/->col-sym 'col1)))))
       "not possible to tell if col1 is a period or a scalar temporal value (timestamp etc.)"))))

(t/deftest test-push-selection-down-past-apply
  (t/testing "pushes selection down to independent relation"
    (t/is
     (= (xt/template
         [:apply
          {:mode :cross-join, :columns {}}
          [:select
           {:predicate (== ~(sql/->col-sym 'x) 10)}
           [:scan {:db-name "xtdb", :table #xt/table foo, :columns [~(sql/->col-sym 'x)]}]]
          [:scan {:db-name "xtdb", :table #xt/table bar, :columns [~(sql/->col-sym 'y)]}]])
        (lp/push-selection-down-past-apply
         (xt/template
          [:select
           {:predicate (== ~(sql/->col-sym 'x) 10)}
           [:apply
            {:mode :cross-join, :columns {}}
            [:scan {:db-name "xtdb", :table #xt/table foo, :columns [~(sql/->col-sym 'x)]}]
            [:scan {:db-name "xtdb", :table #xt/table bar, :columns [~(sql/->col-sym 'y)]}]]])))))

  (t/testing "pushes selection down to dependent relation for cross-join"
    (t/is
     (= (xt/template
         [:apply
          {:mode :cross-join, :columns {}}
          [:scan {:db-name "xtdb", :table #xt/table foo, :columns [~(sql/->col-sym 'x)]}]
          [:select
           {:predicate (== ~(sql/->col-sym 'y) 20)}
           [:scan {:db-name "xtdb", :table #xt/table bar, :columns [~(sql/->col-sym 'y)]}]]])
        (lp/push-selection-down-past-apply
         (xt/template
          [:select
           {:predicate (== ~(sql/->col-sym 'y) 20)}
           [:apply
            {:mode :cross-join, :columns {}}
            [:scan {:db-name "xtdb", :table #xt/table foo, :columns [~(sql/->col-sym 'x)]}]
            [:scan {:db-name "xtdb", :table #xt/table bar, :columns [~(sql/->col-sym 'y)]}]]])))))

  (t/testing "does not push down to dependent relation for non-cross-join modes"
    (t/is
     (nil?
      (lp/push-selection-down-past-apply
       (xt/template
        [:select
         {:predicate (== ~(sql/->col-sym 'bar/y) 20)}
         [:apply
          {:mode :semi-join, :columns {}}
          [:scan {:db-name "xtdb", :table #xt/table foo, :columns [~(sql/->col-sym 'x)]}]
          [:scan {:db-name "xtdb", :table #xt/table bar, :columns [~(sql/->col-sym 'y)]}]]]))))))

(t/deftest test-fuse-sorts
  (t/is (= '[:project {:projections [a]}
             [:sort {:order-specs [[b]], :skip 2, :limit 3}
              [:table {:rows [{:a 1, :b 2}]}]]]
           (lp/rewrite-plan '[:sort {:limit 3}
                              [:sort {:skip 2}
                               [:project {:projections [a]}
                                [:sort {:order-specs [[b]]}
                                 [:table {:rows [{:a 1, :b 2}]}]]]]]))
        "XTQL-shaped order-by, offset, limit fuse through the project")

  (t/is (= '[:sort {:limit 3}
             [:sort {:limit 5}
              [:table {:rows [{:a 1}]}]]]
           (lp/rewrite-plan '[:sort {:limit 3}
                              [:sort {:limit 5}
                               [:table {:rows [{:a 1}]}]]]))
        "an inner limit is not overridden by an outer one")

  (t/is (= '[:sort {:skip 1}
             [:sort {:order-specs [[a]], :skip 2}
              [:table {:rows [{:a 1}]}]]]
           (lp/rewrite-plan '[:sort {:skip 1}
                              [:sort {:order-specs [[a]], :skip 2}
                               [:table {:rows [{:a 1}]}]]]))
        "two skips stay separate")

  (t/is (= '[:sort {:limit 3}
             [:map {:projections [{rn (row-number)}]}
              [:sort {:order-specs [[a]]}
               [:table {:rows [{:a 1}]}]]]]
           (lp/rewrite-plan '[:sort {:limit 3}
                              [:map {:projections [{rn (row-number)}]}
                               [:sort {:order-specs [[a]]}
                                [:table {:rows [{:a 1}]}]]]]))
        "bounds aren't pushed past a row-numbering map"))

(t/deftest test-order-by-offset-limit-results
  (xt/execute-tx tu/*node* [[:put-docs :docs {:xt/id 1, :a 5} {:xt/id 2, :a 3} {:xt/id 3, :a 4}
                             {:xt/id 4, :a 1} {:xt/id 5, :a 2}]])

  (t/is (= [{:a 2} {:a 3}]
           (xt/q tu/*node* "SELECT a FROM docs ORDER BY a OFFSET 1 LIMIT 2")))

  (t/is (= [{:a 2} {:a 3}]
           (xt/q tu/*node* '(-> (from :docs [a]) (order-by a) (offset 1) (limit 2))))
        "XTQL")

  (t/is (= [{:a 4} {:a 3}]
           (xt/q tu/*node* ["SELECT a FROM docs ORDER BY a DESC OFFSET ? LIMIT ?" 1 2]))
        "params"))
