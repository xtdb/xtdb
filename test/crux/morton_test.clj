(ns crux.morton-test
  (:require [clojure.test :as t]
            [crux.codec :as c]
            [crux.db :as db]
            [crux.index :as idx]
            [crux.kv :as kv]
            [crux.morton :as morton]
            [crux.fixtures :as f])
  (:import org.agrona.ExpandableDirectByteBuffer))

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

;; NOTE: this test is a bit unusual in that it creates the index
;; without transacting, as we need to test certain relationships
;; between vt and tt in Z order space. The times here are the raw
;; reversed times.
(t/deftest test-can-find-latest-value-on-x-axis
  (f/with-kv-store
    (fn []
      (let [content-hash (c/new-id "0a4d55a8d778e5022fab701977c5d840bbc486d0")
            eid (c/->id-buffer :foo)
            valid-time 3
            tx-time 2
            z (morton/longs->morton-number valid-time tx-time)
            tx-id 0
            eb (ExpandableDirectByteBuffer.)
            seek-at (fn [i vt tt]
                      (db/seek-values (idx/->EntityMortonAsOfIndex i
                                                                   (morton/longs->morton-number vt tt)
                                                                   eb)
                                      eid))]
        (kv/store f/*kv* [[(c/encode-entity+z+tx-id-key-to
                            nil
                            eid
                            z
                            tx-id)
                           (c/->id-buffer content-hash)]])

        (with-open [snapshot (kv/new-snapshot f/*kv*)
                    i (kv/new-iterator snapshot)]

          (t/testing "visible after valid time at transaction time"
            (let [[_ entity-tx] (seek-at i 2 2)]
              (t/is (= valid-time (c/date->reverse-time-ms (:vt entity-tx))))
              (t/is (= tx-time (c/date->reverse-time-ms (:tt entity-tx))))
              (t/is (= tx-id (:tx-id entity-tx)))

              (t/testing "stays visible"
                (dotimes [vt 3]
                  (dotimes [tt 2]
                    (t/is (seek-at i vt tt)))))))

          (t/testing "not visible before valid time"
            (t/is (nil? (seek-at i 4 2))))

          (t/testing "not visible before transaction time"
            (t/is (nil? (seek-at i 2 3))))

          (t/testing "not visible before both times"
            (t/is (nil? (seek-at i 4 3)))))

        (t/testing "with a value later in valid time from earlier transaction"
          (let [valid-time 2
                tx-time 4
                z (morton/longs->morton-number valid-time tx-time)
                tx-id 1]

            (kv/store f/*kv* [[(c/encode-entity+z+tx-id-key-to
                                nil
                                eid
                                z
                                tx-id)
                               (c/->id-buffer content-hash)]])

            (t/testing "visible after valid time at transaction time"
              (with-open [snapshot (kv/new-snapshot f/*kv*)
                          i (kv/new-iterator snapshot)]
                (let [[_ entity-tx] (seek-at i 2 2)]
                  (t/is (= valid-time (c/date->reverse-time-ms (:vt entity-tx))))
                  (t/is (= tx-time (c/date->reverse-time-ms (:tt entity-tx))))
                  (t/is (= tx-id (:tx-id entity-tx))))))))))))
