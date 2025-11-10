(ns xtdb.operator.rename-test
  (:require [clojure.test :as t]
            [xtdb.test-util :as tu]))

(t/use-fixtures :each tu/with-allocator)

(t/deftest test-rename
  (let [batches-expr [::tu/pages
                      [[{:a 12, :b 10}, {:a 0, :b 15}]
                       [{:a 100, :b 83}]]]]
    (t/is (= {:types '{b #xt/type :i64, c #xt/type :i64}
              :res [[{:c 12, :b 10}, {:c 0, :b 15}]
                    [{:c 100, :b 83}]]}
             (tu/query-ra [:rename '{a c}
                           batches-expr]
                          {:preserve-pages? true
                           :with-types? true})))

    (t/testing "prefix"
      (t/is (= {:types '{r/b #xt/type :i64, r/c #xt/type :i64}
                :res [[{:r/c 12, :r/b 10}, {:r/c 0, :r/b 15}]
                      [{:r/c 100, :r/b 83}]]}
               (tu/query-ra [:rename 'r '{a c}
                             batches-expr]
                            {:preserve-pages? true
                             :with-types? true
                             :key-fn :snake-case-keyword}))))

    (t/testing "prefix only"
      (t/is (= {:types '{r/a #xt/type :i64, r/b #xt/type :i64}
                :res [[{:r/a 12, :r/b 10}, {:r/a 0, :r/b 15}]
                      [{:r/a 100, :r/b 83}]]}
               (tu/query-ra [:rename 'r
                             batches-expr]
                            {:preserve-pages? true
                             :with-types? true
                             :key-fn :snake-case-keyword}))))))
