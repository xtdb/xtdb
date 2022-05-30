(ns core2.operator.rename-test
  (:require [clojure.test :as t]
            [core2.operator.rename :as rename]
            [core2.test-util :as tu]
            [core2.types :as ty]
            [core2.operator :as op])
  (:import org.apache.arrow.vector.types.pojo.Schema))

(t/use-fixtures :each tu/with-allocator)

(t/deftest test-rename
  (let [blocks-expr [::tu/blocks (Schema. [(ty/->field "a" ty/bigint-type false)
                                           (ty/->field "b" ty/bigint-type false)])
                     [[{:a 12, :b 10}, {:a 0, :b 15}]
                      [{:a 100, :b 83}]]]]
    (with-open [res (op/open-ra [:rename '{a c}
                                 blocks-expr])]
      (t/is (= [[{:c 12, :b 10}, {:c 0, :b 15}]
                [{:c 100, :b 83}]]
               (tu/<-cursor res))))

    (t/testing "prefix"
      (with-open [res (op/open-ra [:rename 'R '{a c}
                                   blocks-expr])]
        (t/is (= [[{:R_c 12, :R_b 10}, {:R_c 0, :R_b 15}]
                  [{:R_c 100, :R_b 83}]]
                 (tu/<-cursor res)))))

    (t/testing "prefix only"
      (with-open [res (op/open-ra [:rename 'R
                                   blocks-expr])]
        (t/is (= [[{:R_a 12, :R_b 10}, {:R_a 0, :R_b 15}]
                  [{:R_a 100, :R_b 83}]]
                 (tu/<-cursor res)))))))
