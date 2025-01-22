(ns xtdb.logical-plan-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.logical-plan :as lp]
            [xtdb.sql.plan :as plan]
            [xtdb.sql-test]
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
       (and
        (<=
         (lower ~(plan/->col-sym '_valid_time))
         (lower
          (period
           #xt/zoned-date-time "2000-01-01T00:00Z"
           #xt/zoned-date-time "2001-01-01T00:00Z")))
        (>=
         (coalesce (upper ~(plan/->col-sym '_valid_time)) xtdb/end-of-time)
         (coalesce
          (upper
           (period
            #xt/zoned-date-time "2000-01-01T00:00Z"
            #xt/zoned-date-time "2001-01-01T00:00Z"))
          xtdb/end-of-time)))
       [:project
        [{~(plan/->col-sym '_valid_time)
          (period ~(plan/->col-sym '_valid_from)
                  ~(plan/->col-sym '_valid_to))}]
        [:scan {:table public/docs}
         [~(plan/->col-sym '_valid_from) ~(plan/->col-sym '_valid_to)]]]]))))

  (t/testing "only pushes past period constructors"
    (t/is
     (= nil
        (lp/push-predicate-down-past-period-constructor
         true
         (xt/template
          [:select
           (and
            (<=
             (lower ~(plan/->col-sym '_valid_time))
             (lower
              (period
               #xt/zoned-date-time "2000-01-01T00:00Z"
               #xt/zoned-date-time "2001-01-01T00:00Z")))
            (>=
             (coalesce (upper ~(plan/->col-sym '_valid_time)) xtdb/end-of-time)
             (coalesce
              (upper
               (period
                #xt/zoned-date-time "2000-01-01T00:00Z"
                #xt/zoned-date-time "2001-01-01T00:00Z"))
              xtdb/end-of-time)))
           [:project
            [{~(plan/->col-sym '_valid_time) (+ 1 ~(plan/->col-sym '_valid_from))}]
            [:scan {:table public/docs}
             [~(plan/->col-sym '_valid_from) ~(plan/->col-sym '_valid_to)]]]]))))))

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
                ~(plan/->col-sym 'col1)))))
       "not possible to tell if col1 is a period or a scalar temporal value (timestamp etc.)"))))
