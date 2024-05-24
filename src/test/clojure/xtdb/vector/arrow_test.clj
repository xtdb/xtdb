(ns xtdb.vector.arrow-test
  "Mainly for tests of previously existing bugs in arrow."
  (:require [clojure.test :as t :refer [deftest]]
            [xtdb.api :as xt]
            [xtdb.test-util :as tu]
            [xtdb.util :as util]
            [xtdb.vector.reader :as vr]
            [xtdb.vector.writer :as vw]))

(t/use-fixtures :each tu/with-node tu/with-allocator)

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

(deftest test-extension-vector-slicing
  (with-open [vec (vw/open-vec tu/*allocator* #xt.arrow/field ["0" #xt.arrow/field-type [#xt.arrow/type :keyword false]] [:A])
              copied-vec (util/slice-vec vec)]
    (t/is (= #xt.arrow/field ["0" #xt.arrow/field-type [#xt.arrow/type :keyword false]]
             (.getField copied-vec)))
    (t/is (= :A (.getObject (vr/vec->reader copied-vec) 0)))))
