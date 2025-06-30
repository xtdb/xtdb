(ns xtdb.arrow-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.test-util :as tu]))

(t/use-fixtures :each tu/with-node tu/with-allocator)

(t/deftest test-extensiontype-in-struct-transfer-pairs-3305
  (xt/submit-tx tu/*node* [[:put-docs :table
                            {:data {:type :foo}
                             :xt/id "doc-1"}
                            {:type :bar,
                             :xt/id "doc-2"}]])
  (t/is (= [{:data {:type :foo} :xt/id "doc-1"}
            {:type :bar, :xt/id "doc-2"}]
           (xt/q tu/*node* '(from :table [*])))))

(t/deftest test-promotion-of-null-to-list-3376
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

(t/deftest test-extension-vector-slicing
  (with-open [vec (tu/open-vec "0" [:A])
              copied-vec (.openSlice vec tu/*allocator*)]
    (t/is (= #xt.arrow/field ["0" #xt.arrow/field-type [#xt.arrow/type :keyword false]]
             (.getField copied-vec)))
    (t/is (= :A (.getObject copied-vec 0)))))

(t/deftest empty-list-with-nested-lists-slicing-3377
  (t/testing "empty list of lists"
    (with-open [vec (tu/open-vec #xt.arrow/field ["0" #xt.arrow/field-type [#xt.arrow/type :list false]
                                                  #xt.arrow/field ["1" #xt.arrow/field-type [#xt.arrow/type :list false]
                                                                   #xt.arrow/field ["2" #xt.arrow/field-type [#xt.arrow/type :i64 false]]]]
                                 [[]])
                copied-vec (.openSlice vec tu/*allocator*)]
      (t/is (= (.toList vec) (.toList copied-vec)))))

  (t/testing "empty set of lists"
    (with-open [vec (tu/open-vec #xt.arrow/field ["0" #xt.arrow/field-type [#xt.arrow/type :set false]
                                                  #xt.arrow/field ["1" #xt.arrow/field-type [#xt.arrow/type :list false]
                                                                   #xt.arrow/field ["2" #xt.arrow/field-type [#xt.arrow/type :i64 false]]]]
                                 [#{}])
                copied-vec (.openSlice vec tu/*allocator*)]
      (= (.toList vec) (.toList copied-vec))))

  (t/testing "empty list of sets"
    (with-open [vec (tu/open-vec #xt.arrow/field ["0" #xt.arrow/field-type [#xt.arrow/type :list false]
                                                  #xt.arrow/field ["1" #xt.arrow/field-type [#xt.arrow/type :set false]
                                                                   #xt.arrow/field ["2" #xt.arrow/field-type [#xt.arrow/type :i64 false]]]]
                                 [[] #{}])
                copied-vec (.openSlice vec tu/*allocator*)]
      (= (.toList vec) (.toList copied-vec)))))
