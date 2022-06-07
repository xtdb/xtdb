(ns core2.operator.select-test
  (:require [clojure.test :as t]
            [core2.test-util :as tu]
            [core2.types :as ty]
            [core2.operator :as op])
  (:import org.apache.arrow.vector.types.pojo.Schema))

(t/use-fixtures :each tu/with-allocator)

(t/deftest test-select
  (let [a-field (ty/->field "a" ty/bigint-type false)
        b-field (ty/->field "b" ty/bigint-type false)]
    (with-open [res (op/open-ra [:select '(> a b)
                                 [::tu/blocks (Schema. [a-field b-field])
                                  [[{:a 12, :b 10}
                                    {:a 0, :b 15}]
                                   [{:a 100, :b 83}]
                                   [{:a 83, :b 100}]]]])]
      (t/is (= [[{:a 12, :b 10}]
                [{:a 100, :b 83}]]
               (tu/<-cursor res))))

    (t/testing "param"
      (with-open [res (op/open-ra [:select '(> a ?b)
                                   [::tu/blocks (Schema. [a-field b-field])
                                    [[{:a 12, :b 10}
                                      {:a 0, :b 15}]
                                     [{:a 100, :b 83}]
                                     [{:a 83, :b 100}]]]]
                                  {'?b 50})]
        (t/is (= [[{:a 100, :b 83}]
                  [{:a 83, :b 100}]]
                 (tu/<-cursor res)))))))
