(ns xtdb.vector-test
  (:require [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.compactor :as c]
            [xtdb.test-util :as tu]))

(t/use-fixtures :each tu/with-allocator tu/with-mock-clock tu/with-node)

(t/deftest iae-on-null-subcol-4692
  (binding [c/*ignore-signal-block?* true]
    (xt/execute-tx tu/*node* [[:put-docs :docs {:b {:d nil}, :xt/id 1}]])

    (tu/flush-block! tu/*node*)

    (xt/execute-tx tu/*node* [[:put-docs :docs {:b {:d 456789}, :xt/id 1}]])
    (tu/flush-block! tu/*node*)

    (t/is (= [{:xt/id 1, :b {:d 456789}}]
             (xt/q tu/*node* "SELECT * FROM docs ORDER BY _id")))))

(t/deftest nested-null-in-live-index-4693
  ;; Upstream issue: https://github.com/apache/arrow-java/issues/830
  (binding [c/*ignore-signal-block?* true]
    (xt/execute-tx tu/*node* [[:put-docs :docs {:a {:d nil}, :xt/id 0}]])

    (xt/execute-tx tu/*node* [[:put-docs :docs {:xt/id 1}]])

    (t/is (= [{:xt/id 0, :a {}} {:xt/id 1}] (xt/q tu/*node* "SELECT * FROM docs ORDER BY _id")))))

(t/deftest set-field-mismatch-4690
  (binding [c/*ignore-signal-block?* true]
    (xt/execute-tx tu/*node* [[:put-docs :docs {:b #{1}, :xt/id 0}]])
    (tu/flush-block! tu/*node*)

    (xt/execute-tx tu/*node* [[:put-docs :docs {:xt/id 1}]])

    (t/is (= [{:xt/id 0, :b #{1}} {:xt/id 1}] (xt/q tu/*node* "SELECT * FROM docs ORDER BY _id")))))
