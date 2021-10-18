(ns core2.operator.unwind-test
  (:require [clojure.test :as t]
            [core2.operator.unwind :as unwind]
            [core2.test-util :as tu]
            [core2.types :as ty])
  (:import org.apache.arrow.vector.types.pojo.Schema))

(t/use-fixtures :each tu/with-allocator)

(t/deftest test-unwind
  (let [a-field (ty/->field "a" ty/bigint-type false)
        ;; TODO: ArrowWritable assumes the child to be a DenseUnion.
        b-field (ty/->field "b" ty/list-type false (ty/->field "$data$" ty/dense-union-type false))]
    (with-open [cursor (tu/->cursor (Schema. [a-field b-field])
                                    [[{:a 1 :b [1 2]} {:a 2 :b [3 4 5]}]
                                     [{:a 3 :b []}]
                                     [{:a 4 :b [6 7 8]} {:a 5 :b []}]])
                unwind-cursor (unwind/->unwind-cursor tu/*allocator* cursor "b")]
      (t/is (= [[{:a 1 :b 1} {:a 1 :b 2} {:a 2 :b 3} {:a 2 :b 4} {:a 2 :b 5}]
                [{:a 4 :b 6} {:a 4 :b 7} {:a 4 :b 8}]]
               (tu/<-cursor unwind-cursor))))))
