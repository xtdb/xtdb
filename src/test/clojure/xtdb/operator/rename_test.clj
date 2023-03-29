(ns xtdb.operator.rename-test
  (:require [clojure.test :as t]
            [xtdb.test-util :as tu]))

(t/use-fixtures :each tu/with-allocator)

(t/deftest test-rename
  (let [blocks-expr [::tu/blocks
                     [[{:a 12, :b 10}, {:a 0, :b 15}]
                      [{:a 100, :b 83}]]]]
    (t/is (= {:col-types '{b :i64, c :i64}
              :res [[{:c 12, :b 10}, {:c 0, :b 15}]
                    [{:c 100, :b 83}]]}
             (tu/query-ra [:rename '{a c}
                           blocks-expr]
                          {:preserve-blocks? true
                           :with-col-types? true})))

    (t/testing "prefix"
      (t/is (= {:col-types '{R_b :i64, R_c :i64}
                :res [[{:R_c 12, :R_b 10}, {:R_c 0, :R_b 15}]
                      [{:R_c 100, :R_b 83}]]}
               (tu/query-ra [:rename 'R '{a c}
                             blocks-expr]
                            {:preserve-blocks? true
                             :with-col-types? true}))))

    (t/testing "prefix only"
      (t/is (= {:col-types '{R_a :i64, R_b :i64}
                :res [[{:R_a 12, :R_b 10}, {:R_a 0, :R_b 15}]
                      [{:R_a 100, :R_b 83}]]}
               (tu/query-ra [:rename 'R
                             blocks-expr]
                            {:preserve-blocks? true
                             :with-col-types? true}))))))
