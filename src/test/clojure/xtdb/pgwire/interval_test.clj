(ns xtdb.pgwire.interval-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.test-util :as tu]))

(t/use-fixtures :each tu/with-node)

(t/deftest pg-style-interval-test
  (t/testing "PostgreSQL-style interval literals (Metabase date filter pattern)"
    (xt/execute-tx tu/*node* [[:put-docs :events {:xt/id 1 :name "recent"}]])

    (t/testing "negative interval with unit in string"
      (let [result (xt/q tu/*node*
                         "SELECT NOW() + INTERVAL '-30 day' AS past_date")]
        (t/is (seq result))
        (t/is (some? (:past-date (first result))))))

    (t/testing "positive interval"
      (let [result (xt/q tu/*node*
                         "SELECT NOW() + INTERVAL '7 days' AS future_date")]
        (t/is (seq result))
        (t/is (some? (:future-date (first result))))))

    (t/testing "compound interval with multiple units"
      (let [result (xt/q tu/*node*
                         "SELECT NOW() + INTERVAL '1 year 2 months' AS future_date")]
        (t/is (seq result))
        (t/is (some? (:future-date (first result))))))

    (t/testing "time-based interval"
      (let [result (xt/q tu/*node*
                         "SELECT NOW() + INTERVAL '3 hours 30 minutes' AS future_time")]
        (t/is (seq result))
        (t/is (some? (:future-time (first result))))))

    (t/testing "Metabase date filter pattern - records from last 30 days"
      (let [result (xt/q tu/*node*
                         "SELECT COUNT(*) AS count
                          FROM events
                          WHERE _system_from >= CAST(CAST((NOW() + INTERVAL '-30 day') AS date) AS timestamptz)")]
        (t/is (= [{:count 1}] result))))))
