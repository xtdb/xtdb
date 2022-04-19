(ns core2.operator.project-test
  (:require [clojure.test :as t]
            [core2.expression :as expr]
            [core2.operator.project :as project]
            [core2.test-util :as tu]
            [core2.types :as ty])
  (:import org.apache.arrow.vector.types.pojo.Schema))

(t/use-fixtures :each tu/with-allocator)

(t/deftest test-project
  (with-open [cursor (tu/->cursor (Schema. [(ty/->field "a" ty/bigint-type false)
                                            (ty/->field "b" ty/bigint-type false)])
                                  [[{:a 12, :b 10}
                                    {:a 0, :b 15}]
                                   [{:a 100, :b 83}]])
              project-cursor (project/->project-cursor tu/*allocator* cursor
                                                       [(project/->identity-projection-spec "a")
                                                        (expr/->expression-projection-spec "c" '(+ a b) '#{a b} {})])]
    (t/is (= [[{:a 12, :c 22}, {:a 0, :c 15}]
              [{:a 100, :c 183}]]
             (tu/<-cursor project-cursor)))))

(t/deftest test-project-row-number
  (with-open [cursor (tu/->cursor (Schema. [(ty/->field "a" ty/bigint-type false)
                                            (ty/->field "b" ty/bigint-type false)])
                                  [[{:a 12, :b 10} {:a 0, :b 15}]
                                   [{:a 100, :b 83}]])
              project-cursor (project/->project-cursor tu/*allocator* cursor
                                                       [(project/->identity-projection-spec "a")
                                                        (project/->row-number-projection-spec "$row-num")])]
    (t/is (= [[{:a 12, :$row-num 1}, {:a 0, :$row-num 2}]
              [{:a 100, :$row-num 3}]]
             (tu/<-cursor project-cursor)))))
