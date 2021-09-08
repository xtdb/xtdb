(ns xtdb.morton-test
  (:require [clojure.test :as t]
            [xtdb.morton :as morton]))

;; TODO: the x and y axis encoding has gone confused again, this is
;; not necessarily a big issue, but it will confuse when comparing
;; with various other resources. Most examples assume y dimension is
;; before x, but our code has been aiming to not do this. But this is
;; a bit misguided, as one can separate encoding from which value one
;; puts on which dimension, so plan to fix this.

(t/deftest test-can-encode-and-decode-morton-numbers
  (t/is (= 27 (morton/longs->morton-number 3 5)))
  (t/is (= [3 5] (morton/morton-number->longs 27)))

  (t/testing "handles unsigned longs"
    (t/is (= morton/z-max-mask (morton/longs->morton-number -1 -1)))
    (t/is (= [-1 -1] (morton/morton-number->longs morton/z-max-mask))))

  (t/testing "can take out upper and lower part"
    (let [[upper lower] (morton/morton-number->interleaved-longs (morton/longs->morton-number -1 -1))]
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
  ;; https://www.vision-tools.com/h-tropf/multidimensionalrangequery.pdf
  ;; page 74.
  (t/is (= [55 74] (morton/morton-range-search 27 102 58)))
  (t/is (= [55 74] (morton/morton-range-search (morton/longs->morton-number 3 5)
                                               (morton/longs->morton-number 5 10)
                                               (morton/longs->morton-number 7 4))))

  ;; Example from
  ;; https://en.wikipedia.org/wiki/Z-order_curve#Use_with_one-dimensional_data_structures_for_range_searching
  ;; NOTE: The example above have y/x while we use x/y. But the
  ;; important thing is which number comes first, it will generate the
  ;; same codes. So when y=6 and x=3 here, it just means that the
  ;; first dimension is y according to the examples encoding.
  (t/is (= [15 36] (morton/morton-range-search 12 45 19)))
  (t/is (= [15 36] (morton/morton-range-search (morton/longs->morton-number 2 2)
                                               (morton/longs->morton-number 6 3)
                                               (morton/longs->morton-number 1 5))))

  (t/testing "can search below"
    (t/is (= [0 12] (morton/morton-range-search 12 45 11)))
    (t/is (= [0 12] (morton/morton-range-search 12 45 0)))
    (t/is (= [0 51] (morton/morton-range-search 51 193 50))))

  (t/testing "can search above"
    (t/is (= [45 0] (morton/morton-range-search 12 45 46)))
    (t/is (= [193 0] (morton/morton-range-search 51 193 196))))

  (t/is (= [107 145] (morton/morton-get-next-address 51 193)))
  (t/is (= [63 98] (morton/morton-get-next-address 51 107)))
  (t/is (= [99 104] (morton/morton-get-next-address 98 107)))
  (t/is (= [149 192] (morton/morton-get-next-address 145 193))))
