(ns core2.operator.group-by-test
  (:require [clojure.test :as t]
            [core2.operator.group-by :as group-by]
            [core2.test-util :as tu]
            [core2.types :as types]
            [core2.util :as util]
            [core2.vector.indirect :as iv])
  (:import org.apache.arrow.vector.types.pojo.Schema))

(t/use-fixtures :each tu/with-allocator)

(t/deftest test-group-by
  (let [a-field (types/->field "a" types/bigint-type false)
        b-field (types/->field "b" types/bigint-type false)]
    (letfn [(run-test [group-cols agg-specs blocks]
              (with-open [in-cursor (tu/->cursor (Schema. [a-field b-field]) blocks)
                          group-by-cursor (group-by/->group-by-cursor tu/*allocator* in-cursor group-cols agg-specs)]
                (set (first (tu/<-cursor group-by-cursor)))))]

      (let [agg-specs [(group-by/->aggregate-factory :sum "b" "sum")
                       (group-by/->aggregate-factory :avg "b" "avg")
                       (group-by/->aggregate-factory :count "b" "cnt")
                       (group-by/->aggregate-factory :min "b" "min")
                       (group-by/->aggregate-factory :max "b" "max")
                       (group-by/->aggregate-factory :variance "b" "variance")
                       (group-by/->aggregate-factory :std-dev "b" "std-dev")]]

        (t/is (= #{{:a 1, :sum 140, :avg 35.0, :cnt 4 :min 10 :max 60,
                    :variance 425.0, :std-dev 20.615528128088304}
                   {:a 2, :sum 140, :avg 46.666666666666664, :cnt 3 :min 30 :max 70
                    :variance 288.88888888888914, :std-dev 16.996731711975958}
                   {:a 3, :sum 170, :avg 85.0, :cnt 2 :min 80 :max 90,
                    :variance 25.0, :std-dev 5.0}}
                 (run-test ["a"] agg-specs
                           [[{:a 1 :b 20}
                             {:a 1 :b 10}
                             {:a 2 :b 30}
                             {:a 2 :b 40}]
                            [{:a 1 :b 50}
                             {:a 1 :b 60}
                             {:a 2 :b 70}
                             {:a 3 :b 80}
                             {:a 3 :b 90}]])))

        (t/is (empty? (run-test ["a"] agg-specs []))
              "empty input"))

      (t/is (= #{{:a 1} {:a 2} {:a 3}}
               (run-test ["a"] []
                         [[{:a 1 :b 10}
                           {:a 1 :b 20}
                           {:a 2 :b 10}
                           {:a 2 :b 20}]
                          [{:a 1 :b 10}
                           {:a 1 :b 20}
                           {:a 2 :b 10}
                           {:a 3 :b 20}
                           {:a 3 :b 10}]]))
            "group without aggregate")

      (t/is (= #{{:a 1, :b 10, :cnt 2}
                 {:a 1, :b 20, :cnt 2}
                 {:a 2, :b 10, :cnt 2}
                 {:a 2, :b 20, :cnt 1}
                 {:a 3, :b 10, :cnt 1}
                 {:a 3, :b 20, :cnt 1}}
               (run-test ["a" "b"] [(group-by/->aggregate-factory :count "b" "cnt")]
                         [[{:a 1 :b 10}
                           {:a 1 :b 20}
                           {:a 2 :b 10}
                           {:a 2 :b 20}]
                          [{:a 1 :b 10}
                           {:a 1 :b 20}
                           {:a 2 :b 10}
                           {:a 3 :b 20}
                           {:a 3 :b 10}]]))
            "multiple group columns (distinct)")

      (t/is (= #{{:cnt 9}}
               (run-test [] [(group-by/->aggregate-factory :count "b" "cnt")]
                         [[{:a 1 :b 10}
                           {:a 1 :b 20}
                           {:a 2 :b 10}
                           {:a 2 :b 20}]
                          [{:a 1 :b 10}
                           {:a 1 :b 20}
                           {:a 2 :b 10}
                           {:a 3 :b 20}
                           {:a 3 :b 10}]]))
            "aggregate without group"))))

(t/deftest test-promoting-sum
  (with-open [group-mapping (tu/->mono-vec "gm" types/int-type (map int [0 0 0]))
              v0 (tu/->mono-vec "v" types/bigint-type [1 2 3])
              v1 (tu/->duv "v" [1 2.0 3])]
    (let [sum-spec (-> (group-by/->aggregate-factory :sum "v" "vsum")
                       (.build tu/*allocator*))]
      (try
        (.aggregate sum-spec (iv/->indirect-rel [(iv/->direct-vec v0)]) group-mapping)
        (.aggregate sum-spec (iv/->indirect-rel [(iv/->direct-vec v1)]) group-mapping)
        (t/is (= [12.0] (tu/<-column (.finish sum-spec))))
        (finally
          (util/try-close sum-spec))))))
