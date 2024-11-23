(ns xtdb.client-test
  (:require [clojure.test :as t :refer [deftest]]
            [xtdb.api :as xt]
            [xtdb.test-util :as tu])
  (:import [clojure.lang ExceptionInfo]))

(t/use-fixtures :each (tu/with-opts {:authn [:user-table {:rules [{:user "xtdb" :method :password :address "127.0.0.1"}
                                                                  {:user "ada" :method :trust :address "127.0.0.1"}]}]})
  tu/with-mock-clock tu/with-http-client-node)

(deftest authentication
  (t/is (thrown-with-msg? ExceptionInfo #"authn failed"
                          (xt/status tu/*node*)))

  (t/is (= {:latest-completed-tx nil, :latest-submitted-tx-id -1}
           (xt/status tu/*node* {:authn ["xtdb" "xtdb"]})))

  (t/is (= {:latest-completed-tx nil, :latest-submitted-tx-id -1}
           (xt/status tu/*node* {:authn ["ada"]}))
        "password is optional")

  (t/is (thrown-with-msg? ExceptionInfo #"authn failed"
                          (xt/q tu/*node* "SELECT 1 AS x")))

  (t/is (= [{:x 1}]
           (xt/q tu/*node* "SELECT 1 AS x" {:authn ["xtdb" "xtdb"]})))

  (t/is (thrown-with-msg? ExceptionInfo #"authn failed"
                          (xt/submit-tx tu/*node* [[:put-docs :docs {:xt/id 1}]])))

  (t/is (= 0 (xt/submit-tx tu/*node* [[:put-docs :docs {:xt/id 1}]] {:authn ["xtdb" "xtdb"]}))))
