(ns core2.operator.select-test
  (:require [clojure.test :as t]
            [core2.expression :as expr]
            [core2.operator.select :as select]
            [core2.test-util :as tu]
            [core2.types :as ty])
  (:import org.apache.arrow.vector.types.pojo.Schema))

(t/use-fixtures :each tu/with-allocator)

(t/deftest test-select
  (let [a-field (ty/->field "a" ty/bigint-type false)
        b-field (ty/->field "b" ty/bigint-type false)]
    (with-open [cursor (tu/->cursor (Schema. [a-field b-field])
                                    [[{:a 12, :b 10}
                                      {:a 0, :b 15}]
                                     [{:a 100, :b 83}]
                                     [{:a 83, :b 100}]])
                select-cursor (select/->select-cursor tu/*allocator* cursor
                                                      (expr/->expression-relation-selector '(> a b) '#{a b} {}))]
      (t/is (= [[{:a 12, :b 10}]
                [{:a 100, :b 83}]]
               (tu/<-cursor select-cursor))))))
