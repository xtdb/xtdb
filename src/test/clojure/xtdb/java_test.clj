(ns xtdb.java-test
  (:require [clojure.test :as t :refer [deftest]]
            [xtdb.api :as xt]
            [xtdb.protocols :as xtp]
            [xtdb.test-util :as tu])
  (:import (xtdb IResultSet)
           (xtdb.query Query OutSpec Expr QueryOpts Basis)
           (xtdb.tx TxOptions)))

(t/use-fixtures :each tu/with-mock-clock tu/with-node)

(defn- result-set->vec [res]
  (with-open [^IResultSet res res]
    (vec (iterator-seq res))))

(deftest java-api-test
  (t/testing "transactions"
    (let [tx (xt/submit-tx tu/*node* [(xt/put :docs {"xt/id" 1 "foo" "bar"})]
                           (TxOptions. #time/instant "2020-01-01T12:34:56.000Z" nil))]
      (t/is (= [{:xt/id 1 :xt/system-from #time/zoned-date-time "2020-01-01T12:34:56Z[UTC]"}]
               (xt/q tu/*node*
                     '(from :docs [xt/id xt/system-from])
                     {:basis {:at-tx tx}}))))


    (let [sql-op "INSERT INTO docs (xt$id, tstz) VALUES (2, CAST(DATE '2020-08-01' AS TIMESTAMP WITH TIME ZONE))"
          tx (xt/submit-tx tu/*node* [(xt/sql-op sql-op) ]
                           (TxOptions. nil #time/zone "America/Los_Angeles"))]
      (t/is (= [{:tstz #time/zoned-date-time "2020-08-01T00:00-07:00[America/Los_Angeles]"}]
               (xt/q tu/*node*
                     '(from :docs [{:xt/id 2} tstz])
                     {:basis {:at-tx tx}}))
            "default-tz")))

  (t/testing "queries"
    (let [tx (xt/submit-tx tu/*node* [(xt/put :docs2 {:xt/id 1 :foo "bar"})])]

      (t/is (= [{:my-foo "bar"}]
               (-> @(xtp/open-query& tu/*node* (-> (Query/from "docs2")
                                                   (.binding [(OutSpec/of "foo" (Expr/lVar "my-foo"))]))
                                     (QueryOpts. nil (Basis. tx nil) nil nil nil false nil))
                   result-set->vec))
            "java ast queries")

      (t/is (= [{:my_foo "bar"}]
               (-> @(xtp/open-query& tu/*node* (-> (Query/from "docs2")
                                                   (.binding [(OutSpec/of "foo" (Expr/lVar "my-foo"))]))
                                     (QueryOpts. nil (Basis. tx nil) nil nil nil false "snake_case"))
                   result-set->vec))
            "key-fn")

      (t/is (= [{:foo "bar"}]
               (-> @(xtp/open-query& tu/*node* '(from :docs [{:xt/id $id} foo])
                                     (QueryOpts. {"id" 1} (Basis. tx nil) nil nil nil false nil))
                   result-set->vec))
            "params")

      (t/is (= [{:current-time #time/date "2020-01-01"}]
               (-> @(xtp/open-query& tu/*node* '(-> (rel [{}] [])
                                                    (with {:current-time (current-date)}))
                                     (QueryOpts. nil (Basis. tx #time/instant "2020-01-01T12:34:56.000Z") nil nil nil false nil))
                   result-set->vec))
            "current-time")

      (t/is (= [{:timestamp #time/zoned-date-time "2020-01-01T04:34:56-08:00[America/Los_Angeles]"}]
               (-> @(xtp/open-query& tu/*node* '(-> (rel [{}] [])
                                                    (with {:timestamp (current-timestamp 10)}))
                                     (QueryOpts. nil (Basis. tx #time/instant "2020-01-01T12:34:56.000Z") nil nil
                                                 #time/zone "America/Los_Angeles" false nil))
                   result-set->vec))
            "default tz")

      (t/is (= '[{:plan [:scan
                         {:table docs, :for-valid-time nil, :for-system-time nil}
                         [foo]]}]
               (-> @(xtp/open-query& tu/*node* '(from :docs [foo])
                                     (QueryOpts. nil nil nil nil nil true nil))
                   result-set->vec))
            "default tz"))))
