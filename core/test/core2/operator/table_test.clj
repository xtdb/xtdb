(ns core2.operator.table-test
  (:require [clojure.test :as t]
            [core2.test-util :as tu]
            [core2.operator :as op])
  (:import java.time.Duration))

(t/use-fixtures :each tu/with-allocator)

(t/deftest test-table
  (t/is (= [{:a 12, :b "foo" :c 1.2 :d nil :e true :f (Duration/ofHours 1)}
            {:a 100, :b "bar" :c 3.14 :d #inst "2020" :e 10 :f (Duration/ofMinutes 1)}]
           (into [] (op/plan-ra '[:table $table]
                                {'$table [{:a 12, :b "foo" :c 1.2 :d nil :e true :f (Duration/ofHours 1)}
                                          {:a 100, :b "bar" :c 3.14 :d #inst "2020" :e 10 :f (Duration/ofMinutes 1)}]}))))

  (t/testing "inline table"
    (t/is (= [{:a 12, :b "foo" :c 1.2 :d nil :e true}
              {:a 100, :b "bar" :c 3.14 :d #inst "2020" :e 10}]
             (into [] (op/plan-ra '[:table [{:a 12, :b "foo" :c 1.2 :d nil :e true}
                                            {:a 100, :b "bar" :c 3.14 :d #inst "2020" :e 10}]]
                                  {})))))

  (t/testing "empty"
    (t/is (empty? (into [] (op/plan-ra '[:table $table]
                                       {'$table []})))))

  (t/testing "requires same columns"
    (t/is (thrown? IllegalArgumentException
                   (into [] (op/plan-ra '[:table $table]
                                        {'$table [{:a 12, :b "foo"}
                                                  {:a 100}]}))))))
