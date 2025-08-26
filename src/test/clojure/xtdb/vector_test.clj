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
