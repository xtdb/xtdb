(ns core2.operator.slice-test
  (:require [core2.operator.slice :as slice]
            [clojure.test :as t]))

(t/deftest test-offset+length
  (t/testing "no limit"
    (t/is (nil? (slice/offset+length 23 Long/MAX_VALUE 10 10)))
    (t/is (= [2 8] (slice/offset+length 12 Long/MAX_VALUE 10 10)))
    (t/is (= [0 10] (slice/offset+length 10 Long/MAX_VALUE 12 10)))
    (t/is (= [0 10] (slice/offset+length 0 Long/MAX_VALUE 12 10))))

  (t/testing "zero limit"
    (t/is (nil? (slice/offset+length 10 0 12 10)))
    (t/is (nil? (slice/offset+length 10 0 7 10)))
    (t/is (nil? (slice/offset+length 0 0 12 10))))

  (t/testing "not yet reached the offset"
    (t/is (nil? (slice/offset+length 12 5 0 10)))
    (t/is (nil? (slice/offset+length 10 5 0 10))))

  (t/testing "partially offset"
    (t/is (= [3 7] (slice/offset+length 3 15 0 10)))
    (t/is (= [2 8] (slice/offset+length 12 15 10 10))))

  (t/testing "used up all the offset"
    (t/is (= [0 10] (slice/offset+length 18 30 20 10)))
    (t/is (= [0 10] (slice/offset+length 20 30 20 10))))

  (t/testing "not using all of root"
    (t/is (= [0 3] (slice/offset+length 0 3 0 10)))
    (t/is (= [0 3] (slice/offset+length 0 15 12 10)))
    (t/is (= [0 3] (slice/offset+length 10 5 12 10)))
    (t/is (= [0 5] (slice/offset+length 18 7 20 10)))

    (t/is (= [2 5] (slice/offset+length 12 5 10 10))
          "combining limit and offset"))

  (t/testing "past offset + limit, we're done"
    (t/is (nil? (slice/offset+length 0 5 5 10)))
    (t/is (nil? (slice/offset+length 10 5 15 10)))
    (t/is (nil? (slice/offset+length 20 15 35 10)))))
