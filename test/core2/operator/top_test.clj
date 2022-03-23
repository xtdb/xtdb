(ns core2.operator.top-test
  (:require [clojure.test :as t]
            [core2.operator.top :as top]
            [core2.test-util :as tu]
            [core2.types :as ty])
  (:import org.apache.arrow.vector.types.pojo.Schema))

(t/use-fixtures :each tu/with-allocator)

(t/deftest test-offset+length
  (t/testing "no limit"
    (t/is (nil? (top/offset+length 23 Long/MAX_VALUE 10 10)))
    (t/is (= [2 8] (top/offset+length 12 Long/MAX_VALUE 10 10)))
    (t/is (= [0 10] (top/offset+length 10 Long/MAX_VALUE 12 10)))
    (t/is (= [0 10] (top/offset+length 0 Long/MAX_VALUE 12 10))))

  (t/testing "zero limit"
    (t/is (nil? (top/offset+length 10 0 12 10)))
    (t/is (nil? (top/offset+length 10 0 7 10)))
    (t/is (nil? (top/offset+length 0 0 12 10))))

  (t/testing "not yet reached the offset"
    (t/is (nil? (top/offset+length 12 5 0 10)))
    (t/is (nil? (top/offset+length 10 5 0 10))))

  (t/testing "partially offset"
    (t/is (= [3 7] (top/offset+length 3 15 0 10)))
    (t/is (= [2 8] (top/offset+length 12 15 10 10))))

  (t/testing "used up all the offset"
    (t/is (= [0 10] (top/offset+length 18 30 20 10)))
    (t/is (= [0 10] (top/offset+length 20 30 20 10))))

  (t/testing "not using all of rel"
    (t/is (= [0 3] (top/offset+length 0 3 0 10)))
    (t/is (= [0 3] (top/offset+length 0 15 12 10)))
    (t/is (= [0 3] (top/offset+length 10 5 12 10)))
    (t/is (= [0 5] (top/offset+length 18 7 20 10)))

    (t/is (= [2 5] (top/offset+length 12 5 10 10))
          "combining limit and offset"))

  (t/testing "past offset + limit, we're done"
    (t/is (nil? (top/offset+length 0 5 5 10)))
    (t/is (nil? (top/offset+length 10 5 15 10)))
    (t/is (nil? (top/offset+length 20 15 35 10)))))

(t/deftest test-top
  (let [blocks [[{:idx 0}, {:idx 1}]
                [{:idx 2}, {:idx 3}]]]
    (letfn [(top [offset length]
              (with-open [cursor (tu/->cursor (Schema. [(ty/->field "idx" ty/bigint-type false)])
                                              blocks)
                          top-cursor (top/->top-cursor cursor offset length)]
                (tu/<-cursor top-cursor)))]
      (t/is (= blocks (top nil nil)))

      (t/is (= [[{:idx 0}, {:idx 1}]
                [{:idx 2}]]
               (top nil 3)))

      (t/is (= [[{:idx 1}]
                [{:idx 2}]]
               (top 1 2)))

      (t/testing "doesn't yield empty rels"
        (t/is (= [[{:idx 0}, {:idx 1}]]
                 (top nil 2)))

        (t/is (= [[{:idx 2}, {:idx 3}]]
                 (top 2 nil)))

        (t/is (= [[{:idx 3}]]
                 (top 3 nil)))))))
