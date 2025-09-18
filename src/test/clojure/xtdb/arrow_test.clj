(ns xtdb.arrow-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.test-util :as tu]
            [xtdb.util :as util])
  (:import org.apache.arrow.vector.types.pojo.Schema
           [xtdb.arrow Vector Relation]))

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
    (t/is (= #xt/field ["0" :keyword]
             (.getField copied-vec)))
    (t/is (= :A (.getObject copied-vec 0)))))

(t/deftest empty-list-with-nested-lists-slicing-3377
  (t/testing "empty list of lists"
    (with-open [vec (tu/open-vec #xt/field ["0" :list ["1" :list ["2" :i64]]]
                                 [[]])
                copied-vec (.openSlice vec tu/*allocator*)]
      (t/is (= (.toList vec) (.toList copied-vec)))))

  (t/testing "empty set of lists"
    (with-open [vec (tu/open-vec #xt/field ["0" :set ["1" :list ["2" :i64]]]
                                 [#{}])
                copied-vec (.openSlice vec tu/*allocator*)]
      (= (.toList vec) (.toList copied-vec))))

  (t/testing "empty list of sets"
    (with-open [vec (tu/open-vec #xt/field ["0" :list ["1" :set ["2" :i64]]]
                                 [[] #{}])
                copied-vec (.openSlice vec tu/*allocator*)]
      (= (.toList vec) (.toList copied-vec)))))

(t/deftest copy-list-into-empty-rel-4748
  (with-open [src-vec1 (Vector/fromList tu/*allocator* "0" [[1]])
              dest-vec (Vector/open tu/*allocator* #xt/field ["list" :list])]
    (let [copier1 (.rowCopier src-vec1 dest-vec)]
      (.copyRow copier1 0)
      (t/is (= [[1]] (.toList dest-vec)))))

  (with-open [src-rel (Relation/openFromRows tu/*allocator* [{"list" [1]}])
              dest-rel (Relation. tu/*allocator* (Schema. []))]
    (let [copier (.rowCopier src-rel dest-rel)]
      (.copyRow copier 0)
      (t/is (= [{:list [1]}] (.toMaps dest-rel))))))

(t/deftest cant-set-duv-nullable-4787
  (xt/execute-tx tu/*node* [[:put-docs :docs {:a false, :xt/id 1}]])
  (xt/execute-tx tu/*node* [[:put-docs :docs {:a false, :xt/id 1} {:a 0, :xt/id 1}]])
  (t/is (= [{:a 0 :xt/id 1}]
           (xt/q tu/*node* "SELECT * FROM docs ORDER BY _id"))))
