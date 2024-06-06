(ns xtdb.issue-test
  "A test namespace for explicitly raised and now fixed bugs"
  (:require [clojure.test :as t :refer [deftest]]
            [xtdb.api :as xt]
            [xtdb.test-util :as tu]))

(t/use-fixtures :each tu/with-node)

(deftest test-nested-lists-3341
  (xt/execute-tx tu/*node* [[:put-docs :docs {:xt/id 1 :data '(())}]
                            [:put-docs :docs {:xt/id 2 :data '((1))}]
                            [:put-docs :docs {:xt/id 3 :data [[2] [1]]}]])

  (t/is (= [{:data [[1]]} {:data [[]]} {:data [[2] [1]]}]
           (xt/q tu/*node* '(from :docs [data])))))
