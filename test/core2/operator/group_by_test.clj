(ns core2.operator.group-by-test
  (:require [clojure.test :as t]
            [core2.operator.group-by :as group-by]
            [core2.test-util :as tu]
            [core2.types :as ty])
  (:import org.apache.arrow.vector.types.pojo.Schema
           org.apache.arrow.vector.types.Types$MinorType
           java.util.function.ToLongFunction
           java.util.stream.Collectors))

(t/use-fixtures :each tu/with-allocator)

(t/deftest test-group-by
  (let [a-field (ty/->field "a" (.getType Types$MinorType/BIGINT) false)
        b-field (ty/->field "b" (.getType Types$MinorType/BIGINT) false)
        aggregate-spec [(group-by/->group-spec "a")
                        (group-by/->function-spec "b" "sum" (Collectors/summingLong
                                                             (reify ToLongFunction
                                                               (applyAsLong [_ x] x))))]]
    (with-open [in-cursor (tu/->cursor (Schema. [a-field b-field])
                                       [[{:a 1 :b 10},
                                         {:a 1 :b 20}
                                         {:a 2 :b 30}
                                         {:a 2 :b 40}]
                                        [{:a 1 :b 50}
                                         {:a 1 :b 60}
                                         {:a 2 :b 70}
                                         {:a 3 :b 80}
                                         {:a 3 :b 90}]])
                group-by-cursor (group-by/->group-by-cursor tu/*allocator* in-cursor aggregate-spec)]

      (t/is (= [#{{:a 1 :sum 140}
                  {:a 2 :sum 140}
                  {:a 3 :sum 170}}]
               (mapv set (tu/<-cursor group-by-cursor)))))

    (t/testing "empty input"
      (with-open [in-cursor (tu/->cursor (Schema. [a-field b-field])
                                         [])
                  group-by-cursor (group-by/->group-by-cursor tu/*allocator* in-cursor aggregate-spec)]
        (t/is (empty? (tu/<-cursor group-by-cursor)))))))
