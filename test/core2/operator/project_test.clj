(ns core2.operator.project-test
  (:require [clojure.test :as t]
            [core2.operator :as op]
            [core2.test-util :as tu]
            [core2.types :as ty])
  (:import org.apache.arrow.vector.types.pojo.Schema))

(t/use-fixtures :each tu/with-allocator)

(t/deftest test-project
  (with-open [project-cursor (op/open-ra [:project '[a {c (+ a b)}]
                                          [::tu/blocks (Schema. [(ty/->field "a" ty/bigint-type false)
                                                                 (ty/->field "b" ty/bigint-type false)])
                                           [[{:a 12, :b 10}
                                             {:a 0, :b 15}]
                                            [{:a 100, :b 83}]]]])]
    (t/is (= [[{:a 12, :c 22}, {:a 0, :c 15}]
              [{:a 100, :c 183}]]
             (tu/<-cursor project-cursor))))

  (t/testing "param"
    (with-open [project-cursor (op/open-ra [:project '[a {b (+ b ?p)}]
                                            [::tu/blocks (Schema. [(ty/->field "a" ty/bigint-type false)
                                                                   (ty/->field "b" ty/bigint-type false)])
                                             [[{:a 12, :b 10}
                                               {:a 0, :b 15}]
                                              [{:a 100, :b 83}]]]]
                                           '{?p 42})]
      (t/is (= [[{:a 12, :b 52}, {:a 0, :b 57}]
                [{:a 100, :b 125}]]
               (tu/<-cursor project-cursor))))))

(t/deftest test-project-row-number
  (with-open [project-cursor (op/open-ra [:project '[a {$row-num (row-number)}]
                                          [::tu/blocks (Schema. [(ty/->field "a" ty/bigint-type false)
                                                                 (ty/->field "b" ty/bigint-type false)])
                                           [[{:a 12, :b 10} {:a 0, :b 15}]
                                            [{:a 100, :b 83}]]]])]
    (t/is (= [[{:a 12, :$row-num 1}, {:a 0, :$row-num 2}]
              [{:a 100, :$row-num 3}]]
             (tu/<-cursor project-cursor)))))
