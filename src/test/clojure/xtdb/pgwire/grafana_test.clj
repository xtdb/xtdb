(ns xtdb.pgwire.grafana-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.test-util :as tu]))

(t/use-fixtures :each tu/with-node)

(t/deftest comment-only-query-test
  (t/testing "SQL comment-only queries don't throw (pgx driver sends '-- ping')"
    (t/is (some? (xt/q tu/*node* "-- ping"))))

  (t/testing "double-dash inside string literals is not treated as a comment"
    (t/is (= [{:bar "--foo"}]
             (xt/q tu/*node* "SELECT '--foo' AS bar")))))
