(ns xtdb.java-test
  (:require [clojure.test :as t :refer [deftest]]
            [xtdb.api :as xt]
            [xtdb.test-util :as tu])
  (:import (xtdb.tx TxOptions)))

(t/use-fixtures :each tu/with-mock-clock tu/with-node)

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
            "default-tz"))))
