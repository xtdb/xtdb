(ns xtdb.vector.arrow-test
  "Mainly for tests of previously existing bugs in arrow."
  (:require [clojure.test :as t :refer [deftest]]
            [xtdb.api :as xt]
            [xtdb.test-util :as tu]))

(t/use-fixtures :each tu/with-node)

(deftest test-extensiontype-in-struct-transfer-pairs-3305
  (xt/submit-tx tu/*node* [[:put-docs :table
                            {:data {:type :foo}
                             :xt/id "doc-1"}
                            {:type :bar,
                             :xt/id "doc-2"}]])
  (t/is (= [{:data {:type :foo} :xt/id "doc-1"}
            {:type :bar, :xt/id "doc-2"}]
           (xt/q tu/*node* '(from :table [*])))))

(deftest test-promotion-of-null-to-list-3376
  (t/testing "lists"
    (xt/submit-tx tu/*node* [[:put-docs :table {:xt/id "doc-1" :data nil}]])
    (xt/submit-tx tu/*node* [[:put-docs :table {:xt/id "doc-1" :data [1]}]])
    (t/is (= [{:xt/id "doc-1" :data [1]}]
             (xt/q tu/*node* '(from :table [*])))))

  (t/testing "sets"
    (xt/submit-tx tu/*node* [[:put-docs :table1 {:xt/id "doc-1" :data nil}]])
    (xt/submit-tx tu/*node* [[:put-docs :table1 {:xt/id "doc-1" :data #{1}}]])
    (t/is (= [{:xt/id "doc-1" :data #{1}}]
             (xt/q tu/*node* '(from :table1 [*]))))))
