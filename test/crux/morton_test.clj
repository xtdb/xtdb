(ns crux.morton-test
  (:require [clojure.test :as t]
            [crux.codec :as c]
            [crux.db :as db]
            [crux.index :as idx]
            [crux.kv :as kv]
            [crux.morton :as morton]
            [crux.fixtures :as f])
  (:import java.util.Date
           org.agrona.ExpandableDirectByteBuffer))

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

  (t/is (= [107 145] (morton/morton-get-next-address 51 193)))
  (t/is (= [63 98] (morton/morton-get-next-address 51 107)))
  (t/is (= [99 104] (morton/morton-get-next-address 98 107)))
  (t/is (= [149 192] (morton/morton-get-next-address 145 193))))

;; NOTE: this test is a bit unusual in that it creates the index
;; without transacting, as we need to test certain relationships
;; between vt and tt in Z order space. The times here are the raw
;; reversed times. That is, they both shrink towards 0 as time passes.

;; TODO: redo this test in a saner way.
;;
(t/deftest test-can-find-latest-value-on-x-axis
  (binding [morton/*use-space-filling-curve-index?* true]
    (f/with-kv-store
      (fn []
        (let [content-hash (c/new-id "0a4d55a8d778e5022fab701977c5d840bbc486d0")
              eid (c/->id-buffer :foo)
              valid-time 3
              tx-time 2
              z (morton/longs->morton-number valid-time tx-time)
              tx-id 2
              eb (ExpandableDirectByteBuffer.)
              seek-at (fn [i vt tt]
                        (db/seek-values (idx/->EntityAsOfIndex i
                                                               (c/reverse-time-ms->date vt)
                                                               (c/reverse-time-ms->date tt)
                                                               eb)
                                        eid))]
          (kv/store f/*kv* [[(c/encode-entity+vt+tt+tx-id-key-to
                              nil
                              eid
                              (c/reverse-time-ms->date valid-time)
                              (c/reverse-time-ms->date tx-time)
                              tx-id)
                             (c/->id-buffer content-hash)]
                            [(c/encode-entity+z+tx-id-key-to
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

              (kv/store f/*kv* [[(c/encode-entity+vt+tt+tx-id-key-to
                                  nil
                                  eid
                                  (c/reverse-time-ms->date valid-time)
                                  (c/reverse-time-ms->date tx-time)
                                  tx-id)
                                 (c/->id-buffer content-hash)]
                                [(c/encode-entity+z+tx-id-key-to
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
                    (t/is (= tx-id (:tx-id entity-tx)))))))))))))

(def stress-entities nil)
(def stress-queries nil)

(t/deftest test-stress-bitemporal-lookup
  (f/with-kv-store
    (fn []
      (let [eid (c/->id-buffer :foo)
            eb (ExpandableDirectByteBuffer.)
            seek-at (fn [i vt tt]
                      (db/seek-values (idx/->EntityAsOfIndex i
                                                             vt
                                                             tt
                                                             eb)
                                      eid))
            start-date #inst "2019"
            end-date #inst "2020"
            diff (- (inst-ms end-date) (inst-ms start-date))
            n ( * 2 5)
            bitemp-pairs (->> (repeatedly n #(Date. (long (+ (inst-ms start-date)
                                                             (long (* (rand) diff))))))
                              (partition-all 2))
            entities (or stress-entities
                         (vec (for [[tx-id [vt tt]] (map-indexed vector bitemp-pairs)
                                    :let [content-hash (c/new-id (keyword (str tx-id)))]]
                                {:crux.db/id (str (c/new-id eid))
                                 :crux.db/content-hash (str content-hash)
                                 :crux.db/valid-time vt
                                 :crux.tx/tx-time tt
                                 :crux.tx/tx-id tx-id})))
            queries (or stress-queries
                        (partition-all 2 (repeatedly n #(Date. (long (+ (inst-ms start-date)
                                                                        (long (* (rand) diff))))))))
            vt+tt->entity (into (sorted-map)
                                (zipmap
                                 (for [entity-tx entities]
                                   [(c/date->reverse-time-ms (:crux.db/valid-time entity-tx))
                                    (c/date->reverse-time-ms (:crux.tx/tx-time entity-tx))])
                                 entities))]

        (doseq [entity-tx entities
                :let [eid (c/->id-buffer (:crux.db/id entity-tx))
                      vt (:crux.db/valid-time entity-tx)
                      tt (:crux.tx/tx-time entity-tx)
                      tx-id (:crux.tx/tx-id entity-tx)
                      z (c/encode-entity-tx-z-number vt tt)
                      content-hash (c/new-id (:crux.db/content-hash entity-tx))]]
          (kv/store f/*kv* [[(c/encode-entity+vt+tt+tx-id-key-to
                              nil
                              eid
                              vt
                              tt
                              tx-id)
                             (c/->id-buffer content-hash)]
                            [(c/encode-entity+z+tx-id-key-to
                              nil
                              eid
                              z
                              tx-id)
                             (c/->id-buffer content-hash)]]))

        (with-open [snapshot (kv/new-snapshot f/*kv*)
                    i (kv/new-iterator snapshot)]
          (doseq [entity-tx entities
                  :let [eid (c/->id-buffer (:crux.db/id entity-tx))
                        vt (:crux.db/valid-time entity-tx)
                        tt (:crux.tx/tx-time entity-tx)
                        tx-id (:crux.tx/tx-id entity-tx)
                        z (c/encode-entity-tx-z-number vt tt)
                        content-hash (c/new-id (:crux.db/content-hash entity-tx))]]
            (t/is (= {:crux.db/id (str (c/new-id eid))
                      :crux.db/content-hash (str content-hash)
                      :crux.db/valid-time vt
                      :crux.tx/tx-time tt
                      :crux.tx/tx-id tx-id}
                     (c/entity-tx->edn (second (seek-at i vt tt))))))

          (doseq [[vt tt] queries
                  :let [expected (for [[_ entity-tx] (subseq vt+tt->entity >= [(c/date->reverse-time-ms vt)
                                                                               (c/date->reverse-time-ms tt)])
                                       :when (<= (compare (:crux.tx/tx-time entity-tx) tt) 0)]
                                   entity-tx)]]
            (t/is (= (first expected)
                     (binding [morton/*use-space-filling-curve-index?* false]
                       (c/entity-tx->edn (second (seek-at i vt tt))))))
            ;; TODO: There are failures here, needs investigating where
            ;; the underlying cause is.
            (when-not (= (first expected)
                         (binding [morton/*use-space-filling-curve-index?* true]
                           (c/entity-tx->edn (second (seek-at i vt tt)))))
              (def stress-queries queries)
              (def stress-entities entities))
            #_(t/is (= (first expected)
                       (binding [morton/*use-space-filling-curve-index?* true]
                         (c/entity-tx->edn (second (seek-at i vt tt)))))
                    (pr-str [vt tt]))))))))
