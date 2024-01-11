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
      (t/is (= {:col-types '{r_b :i64, r_c :i64}
                :res [[{:r_c 12, :r_b 10}, {:r_c 0, :r_b 15}]
                      [{:r_c 100, :r_b 83}]]}
               (tu/query-ra [:rename 'r '{a c}
                             blocks-expr]
                            {:preserve-blocks? true
                             :with-col-types? true
                             :key-fn :snake-case-kw}))))

    (t/testing "prefix only"
      (t/is (= {:col-types '{r_a :i64, r_b :i64}
                :res [[{:r_a 12, :r_b 10}, {:r_a 0, :r_b 15}]
                      [{:r_a 100, :r_b 83}]]}
               (tu/query-ra [:rename 'r
                             blocks-expr]
                            {:preserve-blocks? true
                             :with-col-types? true
                             :key-fn :snake-case-kw}))))))
