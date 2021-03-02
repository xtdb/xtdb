(ns core2.operator.slice-test
  (:require [core2.operator.slice :as slice]
            [clojure.test :as t]))

(t/deftest test-offset+length
  (t/is (= [2 8] (slice/offset+length 12 Long/MAX_VALUE 10 10)))
  (t/is (= [0 10] (slice/offset+length 10 Long/MAX_VALUE 12 10)))

  (t/is (= [0 10] (slice/offset+length 0 Long/MAX_VALUE 12 10)))

  (t/is (= [2 5] (slice/offset+length 12 5 10 10)))

  (t/is (= [2 8] (slice/offset+length 12 15 10 10))
        "root doesn't have enough to satisfy limit")

  (t/is (= [0 5] (slice/offset+length 10 5 12 10))
        "this root is too big, needs clamping")

  (t/is (nil? (slice/offset+length 10 5 17 10))
        "we're past offset + limit, we're done")

  (t/is (nil? (slice/offset+length 10 0 12 10))
        "zero limit")
  (t/is (nil? (slice/offset+length 10 0 7 10))
        "zero limit")

  (t/is (nil? (slice/offset+length 2 2 4 10))
        "past (just)")

  (t/is (nil? (slice/offset+length 12 5 0 10))
        "not yet reached the offset")

  (t/is (nil? (slice/offset+length 10 5 0 10))
        "not yet reached the offset (quite)")

  (t/is (nil? (slice/offset+length 0 0 12 10))))
