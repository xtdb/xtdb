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
