(ns core2.operator.group-by-test
  (:require [clojure.test :as t]
            [core2.operator.group-by :as group-by]
            [core2.test-util :as tu]
            [core2.types :as ty])
  (:import org.apache.arrow.vector.types.pojo.Schema))

(t/use-fixtures :each tu/with-allocator)

(t/deftest test-group-by
  (let [a-field (ty/->field "a" ty/bigint-type false)
        b-field (ty/->field "b" ty/bigint-type false)
        aggregate-spec [(group-by/->group-spec "a")
                        (group-by/->sum-number-spec "b" "sum")
                        (group-by/->avg-number-spec "b" "avg")
                        (group-by/->count-spec "b" "cnt")
                        (group-by/->min-number-spec "b" "min")
                        (group-by/->max-number-spec "b" "max")]]
    (with-open [in-cursor (tu/->cursor (Schema. [a-field b-field])
                                       [[{:a 1 :b 10}
                                         {:a 1 :b 20}
                                         {:a 2 :b 30}
                                         {:a 2 :b 40}]
                                        [{:a 1 :b 50}
                                         {:a 1 :b 60}
                                         {:a 2 :b 70}
                                         {:a 3 :b 80}
                                         {:a 3 :b 90}]])
                group-by-cursor (group-by/->group-by-cursor tu/*allocator* in-cursor aggregate-spec)]

      (t/is (= [#{{:a 1, :sum 140, :avg 35.0, :cnt 4 :min 10 :max 60}
                  {:a 2, :sum 140, :avg 46.666666666666664, :cnt 3 :min 30 :max 70}
                  {:a 3, :sum 170, :avg 85.0, :cnt 2 :min 80 :max 90}}]
               (mapv set (tu/<-cursor group-by-cursor)))))

    (t/testing "empty input"
      (with-open [in-cursor (tu/->cursor (Schema. [a-field b-field])
                                         [])
                  group-by-cursor (group-by/->group-by-cursor tu/*allocator* in-cursor aggregate-spec)]
        (t/is (empty? (tu/<-cursor group-by-cursor))))

      (with-open [in-cursor (tu/->cursor (Schema. [a-field b-field])
                                         [[]])
                  group-by-cursor (group-by/->group-by-cursor tu/*allocator* in-cursor aggregate-spec)]
        (t/is (empty? (tu/<-cursor group-by-cursor)))))

    (t/testing "multiple group columns (distinct)"
      (with-open [in-cursor (tu/->cursor (Schema. [a-field b-field])
                                         [[{:a 1 :b 10}
                                           {:a 1 :b 20}
                                           {:a 2 :b 10}
                                           {:a 2 :b 20}]
                                          [{:a 1 :b 10}
                                           {:a 1 :b 20}
                                           {:a 2 :b 10}
                                           {:a 3 :b 20}
                                           {:a 3 :b 10}]])
                  group-by-cursor (group-by/->group-by-cursor tu/*allocator* in-cursor [(group-by/->group-spec "a")
                                                                                        (group-by/->group-spec "b")
                                                                                        (group-by/->count-spec "b" "cnt")])]

        (t/is (= [#{{:a 1, :b 10, :cnt 2}
                    {:a 1, :b 20, :cnt 2}
                    {:a 2, :b 10, :cnt 2}
                    {:a 2, :b 20, :cnt 1}
                    {:a 3, :b 10, :cnt 1}
                    {:a 3, :b 20, :cnt 1}}]
                 (mapv set (tu/<-cursor group-by-cursor))))))

    (t/testing "group without aggregate"
      (with-open [in-cursor (tu/->cursor (Schema. [a-field b-field])
                                         [[{:a 1 :b 10}
                                           {:a 1 :b 20}
                                           {:a 2 :b 10}
                                           {:a 2 :b 20}]
                                          [{:a 1 :b 10}
                                           {:a 1 :b 20}
                                           {:a 2 :b 10}
                                           {:a 3 :b 20}
                                           {:a 3 :b 10}]])
                  group-by-cursor (group-by/->group-by-cursor tu/*allocator* in-cursor [(group-by/->group-spec "a")])]

        (t/is (= [#{{:a 1}
                    {:a 2}
                    {:a 3}}]
                 (mapv set (tu/<-cursor group-by-cursor))))))

    (t/testing "aggregate without group"
      (with-open [in-cursor (tu/->cursor (Schema. [a-field b-field])
                                         [[{:a 1 :b 10}
                                           {:a 1 :b 20}
                                           {:a 2 :b 10}
                                           {:a 2 :b 20}]
                                          [{:a 1 :b 10}
                                           {:a 1 :b 20}
                                           {:a 2 :b 10}
                                           {:a 3 :b 20}
                                           {:a 3 :b 10}]])
                  group-by-cursor (group-by/->group-by-cursor tu/*allocator* in-cursor [(group-by/->count-spec "b" "cnt")])]

        (t/is (= [#{{:cnt 9}}]
                 (mapv set (tu/<-cursor group-by-cursor))))))))
