(ns crux.morton-test
  (:require [clojure.test :as t]
            [crux.codec :as c]
            [crux.morton :as morton]))

(t/deftest test-can-encode-and-decode-morton-numbers
  (t/is (= 27 (morton/longs->morton-number 3 5)))
  (t/is (= [3 5] (morton/morton-number->longs 27)))

  (t/testing "handles unsigned longs"
    (t/is (= morton/z-max-mask (morton/longs->morton-number -1 -1)))
    (t/is (= [-1 -1] (morton/morton-number->longs morton/z-max-mask))))

  (t/testing "can take out upper and lower part"
    (let [[upper lower] (morton/longs->morton-number-parts -1 -1)]
      (t/is (= morton/z-max-mask (morton/interleaved-longs->morton-number upper lower))))))

(t/deftest test-can-check-range-without-decoding-morton-number
  (t/is (morton/morton-number-within-range?
         (morton/longs->morton-number 1 1)
         (morton/longs->morton-number 1 1)
         (morton/longs->morton-number 1 1)))

  (t/is (morton/morton-number-within-range?
         (morton/longs->morton-number 2 2)
         (morton/longs->morton-number 3 6)
         (morton/longs->morton-number 3 4)))

  (t/is (not (morton/morton-number-within-range?
              (morton/longs->morton-number 2 2)
              (morton/longs->morton-number 3 6)
              (morton/longs->morton-number 0 0))))

  (t/is (not (morton/morton-number-within-range?
              (morton/longs->morton-number 2 2)
              (morton/longs->morton-number 3 6)
              (morton/longs->morton-number 5 4)))))

(t/deftest test-can-calculate-litmax-and-bigmin
  (t/is (= [55 74] (morton/zdiv 27 102 58)))
  (t/is (= [55 74] (morton/zdiv (morton/longs->morton-number 3 5)
                                (morton/longs->morton-number 5 10)
                                (morton/longs->morton-number 7 4))))

  ;; Example from https://en.wikipedia.org/wiki/Z-order_curve#Use_with_one-dimensional_data_structures_for_range_searching
  (t/is (= [15 36] (morton/zdiv 12 45 19)))
  (t/is (= [15 36] (morton/zdiv (morton/longs->morton-number 2 2)
                                (morton/longs->morton-number 6 3)
                                (morton/longs->morton-number 1 5)))))
